package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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

// If we re-run the program on an existing topic, we skip everything from
// previous runs.
var runHeader = strconv.Itoa(int(time.Now().UnixNano()))

// TODO: generate random data for the value. the value should be verifiable in
// terms of both data integrity and that the value is the correct value for the
// associated key.
func newRecord(producerId int, sequence uint64) *kgo.Record {
	buf := make([]byte, 0, 6+1+18)
	buf = strconv.AppendInt(buf, int64(producerId), 10)
	buf = append(buf, 0)
	buf = strconv.AppendUint(buf, sequence, 10)

	i, j := len(buf)-1, cap(buf)-1
	buf = buf[:cap(buf)]
	for buf[i] != 0 {
		buf[j] = buf[i]
		j--
		i--
	}
	for j != 6 {
		buf[j] = '0'
		j--
	}
	buf[j] = '.'
	i--
	j--
	for i >= 0 {
		buf[j] = buf[i]
		j--
		i--
	}
	for j >= 0 {
		buf[j] = '0'
		j--
	}

	r := kgo.KeySliceRecord(buf, buf)
	r.Headers = append(r.Headers, kgo.RecordHeader{runHeader, nil})
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

type d struct {
	data []byte
	rem  int32
}

type verifier struct {
	outstanding sync.Map // map[uint64]produced, key: uint64(partition)<<48 | uint64(offset)

	totalProduced int64
	totalConsumed int64

	ctx       context.Context
	cancelCtx func()
}

func newVerifier() *verifier {
	ctx, cancelCtx := context.WithCancel(context.Background())
	return &verifier{
		ctx:       ctx,
		cancelCtx: cancelCtx,
	}
}

func (v *verifier) producedRecord(r *kgo.Record) {
	atomic.AddInt64(&v.totalProduced, 1) // after tracking in produced map
	mkey := uint64(r.Partition)<<48 | uint64(r.Offset)

	rem := 1
	if *group == "" {
		rem = *consumers
	}

	actual, loaded := v.outstanding.LoadOrStore(mkey, &d{
		data: r.Key,
		rem:  int32(rem),
	})
	if !loaded {
		return
	}

	// Record existed: it was consumed before we could track it was
	// produced in our produced callback.
	//
	// If it was consumed as many times as we expect (and so rem hits 0),
	// we delete it from our outstanding map.
	d := actual.(*d)
	if !bytes.Equal(d.data, r.Key) {
		die("mismatched keys")
	}
	if drem := atomic.AddInt32(&d.rem, int32(rem)); drem == 0 {
		v.outstanding.Delete(mkey)
	}
}

func (v *verifier) consumeRecord(r *kgo.Record) {
	if len(r.Headers) == 0 || r.Headers[0].Key != runHeader {
		return
	}

	atomic.AddInt64(&v.totalConsumed, 1)
	mkey := uint64(r.Partition)<<48 | uint64(r.Offset)

	actual, loaded := v.outstanding.LoadOrStore(mkey, &d{
		data: r.Key,
		rem:  -1,
	})
	if !loaded {
		// We consumed before we produced. We have added our consume
		// with a negative rem; now we return.
		return
	}

	// Record existed: it was either produced, or it was consumed
	// previously.  We need our data to match and we add our rem.
	d := actual.(*d)
	if !bytes.Equal(d.data, r.Key) {
		die("mismatched keys")
	}
	if drem := atomic.AddInt32(&d.rem, -1); drem == 0 {
		v.outstanding.Delete(mkey)
	}
}

func (v *verifier) printSummary() {
	unconsumed := make(map[int32]int)
	overconsumed := make(map[int32]int)
	var maxPartition int32
	v.outstanding.Range(func(k, v interface{}) bool {
		partition := int32((k.(uint64)) >> 48)
		d := v.(*d)
		if partition > maxPartition {
			maxPartition = partition
		}
		switch {
		case d.rem < 0:
			overconsumed[partition]++
		case d.rem == 0:
			// about to be deleted
		case d.rem > 0:
			unconsumed[partition]++
		}
		return true
	})

	type p struct {
		u int
		o int
	}
	ps := make([]p, maxPartition+1)
	for p, i := range unconsumed {
		ps[p].u = i
	}
	for p, i := range overconsumed {
		ps[p].o = i
	}

	for i, p := range ps {
		if p.u == 0 && p.o == 0 {
			continue
		}
		fmt.Println("Partition:", i, "Unconsumed offsets:", p.u, "Overconsumed offsets:", p.o)
	}
}

func (v *verifier) doneOutstanding() bool {
	done := true
	v.outstanding.Range(func(_, _ interface{}) bool {
		done = false
		return false
	})
	return done
}

func (v *verifier) loopSummary() {
	var lastConsumed, lastProduced int64

	for range time.Tick(3 * time.Second) {
		produced := atomic.LoadInt64(&v.totalProduced)
		consumed := atomic.LoadInt64(&v.totalConsumed)
		v.printSummary()

		if produced > *messages && v.doneOutstanding() {
			fmt.Println("Quitting from finished produce/consume verification.")
			v.cancelCtx()
		}

		fmt.Printf("Total produced %d (%.2f msg/sec), total consumed: %d (%.2f msg/sec)\n",
			produced, float64(produced-lastProduced)/3.0,
			consumed, float64(consumed-lastConsumed)/3.0,
		)

		lastProduced = produced
		lastConsumed = consumed
	}
}

func ctxDone(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (v *verifier) loopConsume() {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(*topic),
	}

	if *group != "" {
		opts = append(opts, kgo.ConsumerGroup(*group))
	}
	opts = appendLogLevel(opts)
	cl, err := kgo.NewClient(opts...)
	chk(err, "unable to initialize client: %v", err)

	for !ctxDone(v.ctx.Done()) {
		fetches := cl.PollFetches(v.ctx)

		fetches.EachError(func(t string, p int32, err error) {
			chk(err, "topic %s partition %d had error: %v", t, p, err)
		})

		fetches.EachRecord(func(r *kgo.Record) {
			v.consumeRecord(r)
		})
	}
}

func (v *verifier) loopProduce(id int) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.MaxBufferedRecords(4096),
		kgo.ProducerBatchMaxBytes(1024 * 1024),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	}
	if *linger != 0 {
		opts = append(opts, kgo.ProducerLinger(*linger))
	}
	opts = appendLogLevel(opts)

	cl, err := kgo.NewClient(opts...)
	chk(err, "unable to initialize client: %v", err)

	for sequence := uint64(0); atomic.LoadInt64(&v.totalProduced) < int64(*messages); sequence++ {
		cl.Produce(v.ctx, newRecord(id, sequence), func(r *kgo.Record, err error) {
			if err == context.Canceled {
				return
			}
			chk(err, "produce error: %v", err)
			v.producedRecord(r)
		})
	}
}

func (v *verifier) start() func() {
	var wg sync.WaitGroup
	for i := 0; i < *producers; i++ {
		wg.Add(1)
		id := i
		go func() {
			defer wg.Done()
			v.loopProduce(id)
		}()
	}

	for i := 0; i < *consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v.loopConsume()
		}()
	}

	go v.loopSummary()

	return func() { wg.Wait() }
}

func main() {
	flag.Parse()

	v := newVerifier()
	wait := v.start()
	defer wait()

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt)

	select {
	case <-sigs:
		fmt.Println("Quitting from signal.")
		os.Exit(1)
	case <-v.ctx.Done():
	}
}
