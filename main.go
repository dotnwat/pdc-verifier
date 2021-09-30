package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	brokers = flag.String("brokers", "localhost:9092", "comma delimited list of brokers")
	topic   = flag.String("topic", "", "topic to produce to or consume from")
	linger  = flag.Duration("linger", 0, "if non-zero, linger to use when producing")

	group = flag.String("group", "", "consumer group")
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

// TODO: write a variable length random value that can later be verified by
// doing something like using the key as a seed to into a random bytes
// generator. maybe even include the producer id and crc.
func newRecord(id int, num int64) *kgo.Record {
	var key bytes.Buffer
	fmt.Fprintf(&key, "%06d.%018d", id, num)

	var r *kgo.Record
	r = kgo.KeySliceRecord(key.Bytes(), key.Bytes())
	return r
}

type StreamJoiner struct {
	lock     sync.Mutex
	produced map[int32]map[int64][]byte
	consumed map[int32]map[int64][]byte
}

func NewStreamJoiner() StreamJoiner {
	return StreamJoiner{
		produced: make(map[int32]map[int64][]byte),
		consumed: make(map[int32]map[int64][]byte),
	}
}

func (s *StreamJoiner) ProduceRecord(r *kgo.Record) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if partition, ok := s.produced[r.Partition]; ok {
		if _, ok := partition[r.Offset]; ok {
			die("produced duplicate offset")
		}
		partition[r.Offset] = r.Key
	} else {
		s.produced[r.Partition] = make(map[int64][]byte)
		s.produced[r.Partition][r.Offset] = r.Key
	}
}

func (s *StreamJoiner) ConsumeRecord(r *kgo.Record) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if partition, ok := s.produced[r.Partition]; ok {
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

	if partition, ok := s.consumed[r.Partition]; ok {
		if key, ok := partition[r.Offset]; ok {
			if !bytes.Equal(key, r.Key) {
				die("mismatched keys")
			}
			// at least once delivery...
		}
		partition[r.Offset] = r.Key
	} else {
		s.consumed[r.Partition] = make(map[int64][]byte)
		s.consumed[r.Partition][r.Offset] = r.Key
	}
}

func (s *StreamJoiner) PrintSummary() {
	s.lock.Lock()
	defer s.lock.Unlock()

	for partition, offsets := range s.consumed {
		for offset, consumed_key := range offsets {
			if partition, ok := s.produced[partition]; ok {
				if produced_key, ok := partition[offset]; ok {
					if !bytes.Equal(consumed_key, produced_key) {
						die("key mismatch")
					}
					delete(partition, offset)
				}
			}
		}
	}

	for partition, offsets := range s.produced {
		fmt.Println("Partition:", partition, "Unconsumed offsets:", len(offsets))
	}
}

type Verifier struct {
	joiner   StreamJoiner
	produced int64
	consumed int64
}

func NewVerifier() Verifier {
	return Verifier{
		joiner: NewStreamJoiner(),
	}
}

func (v *Verifier) Consume(wg *sync.WaitGroup) {
	defer wg.Done()

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(*topic),
	}

	if *group != "" {
		opts = append(opts, kgo.ConsumerGroup(*group))
	}

	client, err := kgo.NewClient(opts...)
	chk(err, "unable to initialize client: %v", err)

	for {
		fetches := client.PollFetches(context.Background())

		fetches.EachError(func(t string, p int32, err error) {
			chk(err, "topic %s partition %d had error: %v", t, p, err)
		})

		fetches.EachRecord(func(r *kgo.Record) {
			atomic.AddInt64(&v.consumed, 1)
			v.joiner.ConsumeRecord(r)
		})
	}
}

func (v *Verifier) Produce(wg *sync.WaitGroup, id int) {
	defer wg.Done()

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

	client, err := kgo.NewClient(opts...)
	chk(err, "unable to initialize client: %v", err)

	var num int64
	for {
		client.Produce(context.Background(), newRecord(id, num), func(r *kgo.Record, err error) {
			chk(err, "produce error: %v", err)
			atomic.AddInt64(&v.produced, 1)
			v.joiner.ProduceRecord(r)
		})
		num++
	}
}

func (v *Verifier) PrintSummary() {
	for range time.Tick(time.Second * 3) {
		v.joiner.PrintSummary()
		fmt.Println("Total produced", atomic.LoadInt64(&v.produced),
			"consumed", atomic.LoadInt64(&v.consumed))
	}
}

func main() {
	flag.Parse()

	verifier := NewVerifier()

	go verifier.PrintSummary()

	var wg sync.WaitGroup

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go verifier.Consume(&wg)
	}

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go verifier.Produce(&wg, i)
	}

	wg.Wait()
}
