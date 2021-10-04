package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
)

var (
	brokers   = flag.String("brokers", "localhost:9092", "comma delimited list of brokers")
	topic     = flag.String("topic", "", "topic to produce to or consume from")
	linger    = flag.Duration("linger", 0, "if non-zero, linger to use when producing")
	group     = flag.String("group", "", "consumer group")
	producers = flag.Int("producers", 1, "number of producers")
	consumers = flag.Int("consumers", 1, "number of consumers")
	messages  = flag.Int64("messages", 200000, "number of messages to produce")
	logLevel  = flag.String("log-level", "error", "franz-go log level")
)

func die(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func chk(err error, msg string, args ...interface{}) {
	if err != nil {
		die(msg, args...)
	}
}

// TODO: generate random data for the value. the value should be verifiable in
// terms of both data integrity and that the value is the correct value for the
// associated key.
func newRecord(producerId int, sequence int64) *kgo.Record {
	var key bytes.Buffer
	fmt.Fprintf(&key, "%06d.%018d", producerId, sequence)

	var r *kgo.Record
	r = kgo.KeySliceRecord(key.Bytes(), key.Bytes())
	return r
}

func appendLogLevel(opts []kgo.Opt) []kgo.Opt {
	switch strings.ToLower(*logLevel) {
	case "":
	case "debug":
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	case "info":
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)))
	case "warn":
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelWarn, nil)))
	case "error":
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelError, nil)))
	default:
		die("unrecognized log level %s", *logLevel)
	}
	return opts
}

type Verifier struct {
	lock sync.Mutex

	// map[partition][offset] -> key
	producedRecords map[int32]map[int64][]byte
	consumedRecords map[int32]map[int64][]byte

	totalProduced int64
	totalConsumed int64
	lastProduced  int64
	lastConsumed  int64

	produceCtx    context.Context
	cancelProduce func()

	wg sync.WaitGroup
}

func NewVerifier() Verifier {
	produceCtx, cancelProduce := context.WithCancel(context.Background())

	return Verifier{
		producedRecords: make(map[int32]map[int64][]byte),
		consumedRecords: make(map[int32]map[int64][]byte),
		produceCtx:      produceCtx,
		cancelProduce:   cancelProduce,
	}
}

func (v *Verifier) ProduceRecord(r *kgo.Record) {
	v.lock.Lock()
	defer v.lock.Unlock()

	if partition, ok := v.producedRecords[r.Partition]; ok {
		if _, ok := partition[r.Offset]; ok {
			die("produced duplicate offset")
		}
		partition[r.Offset] = r.Key
	} else {
		v.producedRecords[r.Partition] = make(map[int64][]byte)
		v.producedRecords[r.Partition][r.Offset] = r.Key
	}
}

func (v *Verifier) ConsumeRecord(r *kgo.Record) {
	v.lock.Lock()
	defer v.lock.Unlock()

	if partition, ok := v.producedRecords[r.Partition]; ok {
		if key, ok := partition[r.Offset]; ok {
			if !bytes.Equal(key, r.Key) {
				die("mismatched keys")
			}
			delete(partition, r.Offset)
			return
		}
	}

	// fetched a record that wasn't produced. this could happen if (1)
	// the fetch received the data before the producer had a chance to
	// register the record in the tracking data structure, or (2) the
	// topic contained extra records, or (3) bugs!

	if partition, ok := v.consumedRecords[r.Partition]; ok {
		if key, ok := partition[r.Offset]; ok {
			if !bytes.Equal(key, r.Key) {
				die("mismatched keys")
			}
			// at least once delivery...
		}
		partition[r.Offset] = r.Key
	} else {
		v.consumedRecords[r.Partition] = make(map[int64][]byte)
		v.consumedRecords[r.Partition][r.Offset] = r.Key
	}
}

// A record may be consumed before it is registered in the index by the
// producer. If this happens then the consumer registers the record, and this
// reconcillation method replays the consumed records against the latest
// producer state. This should happen rarely, so it's efficient to call whenever
// convenient (e.g. before printing periodic stats, reports).
func (v *Verifier) reconcile() {
	for partition, offsets := range v.consumedRecords {
		for offset, consumed_key := range offsets {
			if partition, ok := v.producedRecords[partition]; ok {
				if produced_key, ok := partition[offset]; ok {
					if !bytes.Equal(consumed_key, produced_key) {
						die("key mismatch")
					}
					delete(partition, offset)
				}
			}
		}
	}
}

func (v *Verifier) printSummary() {
	v.lock.Lock()
	defer v.lock.Unlock()
	v.reconcile()
	for partition, offsets := range v.producedRecords {
		fmt.Println("Partition:", partition, "Unconsumed offsets:", len(offsets))
	}
}

func (v *Verifier) PrintSummary() {
	const interval_seconds = 3
	for range time.Tick(time.Second * interval_seconds) {
		produced := atomic.LoadInt64(&v.totalProduced)
		consumed := atomic.LoadInt64(&v.totalConsumed)
		if produced > *messages {
			v.Stop()
		}
		v.printSummary()
		fmt.Printf("Total produced %d (%.2f msg/sec), total consumed: %d (%.2f msg/sec)\n",
			produced, (float64)(produced-v.lastProduced)/3.0, consumed,
			(float64)(consumed-v.lastConsumed)/3.0)
		v.lastConsumed = consumed
		v.lastProduced = produced
	}
}

func (v *Verifier) Consume() {
	defer v.wg.Done()

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(*topic),
	}

	if *group != "" {
		opts = append(opts, kgo.ConsumerGroup(*group))
	}
	opts = appendLogLevel(opts)
	client, err := kgo.NewClient(opts...)
	chk(err, "unable to initialize client: %v", err)

	for {
		fetches := client.PollFetches(context.Background())

		fetches.EachError(func(t string, p int32, err error) {
			chk(err, "topic %s partition %d had error: %v", t, p, err)
		})

		fetches.EachRecord(func(r *kgo.Record) {
			atomic.AddInt64(&v.totalConsumed, 1)
			v.ConsumeRecord(r)
		})
	}
}

func (v *Verifier) Produce(producerId int) {
	defer v.wg.Done()

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.MaxBufferedRecords(4096),
		kgo.ProducerBatchMaxBytes(1024 * 1024),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	}

	metrics := kprom.NewMetrics("kgo")
	http.Handle("/metrics", metrics.Handler())
	opts = append(opts, kgo.WithHooks(metrics))

	if *linger != 0 {
		opts = append(opts, kgo.ProducerLinger(*linger))
	}
	opts = appendLogLevel(opts)

	client, err := kgo.NewClient(opts...)
	chk(err, "unable to initialize client: %v", err)

	handler := func(r *kgo.Record, err error) {
		chk(err, "produce error: %v", err)
		atomic.AddInt64(&v.totalProduced, 1)
		v.ProduceRecord(r)
	}

	var sequence int64
	for {
		select {
		case <-v.produceCtx.Done():
			break
		default:
			r := newRecord(producerId, sequence)
			// TODO we should probably be passing in a cancellable context but
			// when we do that we also need to deal with the handler receiving
			// the context cancelled error.
			client.Produce(context.Background(), r, handler)
			sequence++
		}
	}
}

func (v *Verifier) Start() {
	for i := 0; i < *consumers; i++ {
		v.wg.Add(1)
		go v.Consume()
	}

	for i := 0; i < *producers; i++ {
		v.wg.Add(1)
		go v.Produce(i)
	}

	go v.PrintSummary()
}

func (v *Verifier) Stop() {
	v.cancelProduce()
}

func (v *Verifier) Done() <-chan struct{} {
	return v.produceCtx.Done()
}

func (v *Verifier) Wait() {
	v.wg.Wait()
}

func main() {
	flag.Parse()

	verifier := NewVerifier()
	verifier.Start()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	select {
	case <-c:
		verifier.Stop()
	case <-verifier.Done():
	}

	verifier.Wait()
}
