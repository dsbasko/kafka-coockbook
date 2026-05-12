package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic       = "lecture-08-04-fixed"
	defaultPartitions  = 4
	defaultReplication = 3
	defaultDuration    = 10 * time.Second
	defaultHotRate     = 1000
	defaultNormalRate  = 10
	defaultBuckets     = 4
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик для balanced-сценария")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций")
	rf := flag.Int("rf", defaultReplication, "replication factor")
	duration := flag.Duration("duration", defaultDuration, "сколько генерировать")
	hotRate := flag.Int("hot-rate", defaultHotRate, "msgs/sec на 'hot' (раскидано по бакетам)")
	normalRate := flag.Int("normal-rate", defaultNormalRate, "msgs/sec на каждого user")
	buckets := flag.Int("buckets", defaultBuckets, "сколько бакетов делает composite key (hot:bucket-N)")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	if err := run(ctx, runOpts{
		topic:      *topic,
		partitions: int32(*partitions),
		rf:         int16(*rf),
		duration:   *duration,
		hotRate:    *hotRate,
		normalRate: *normalRate,
		buckets:    *buckets,
	}); err != nil {
		logger.Error("composite-key-fix failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic      string
	partitions int32
	rf         int16
	duration   time.Duration
	hotRate    int
	normalRate int
	buckets    int
}

func run(ctx context.Context, o runOpts) error {
	cl, err := kafka.NewClient()
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)

	if err := ensureTopic(ctx, admin, o); err != nil {
		return fmt.Errorf("ensure topic: %w", err)
	}

	startCounts, err := readEndOffsets(ctx, admin, o.topic)
	if err != nil {
		return fmt.Errorf("ListEndOffsets (start): %w", err)
	}

	hotKeys := make([]string, o.buckets)
	for i := 0; i < o.buckets; i++ {
		hotKeys[i] = fmt.Sprintf("hot:bucket-%d", i)
	}

	fmt.Printf("\nстартуем: hot raked across %d buckets, total rate=%d/sec, duration=%s\n\n",
		o.buckets, o.hotRate, o.duration)

	loadCtx, loadCancel := context.WithTimeout(ctx, o.duration)
	defer loadCancel()

	hotPerKey := o.hotRate / o.buckets
	if hotPerKey < 1 {
		hotPerKey = 1
	}

	var wg sync.WaitGroup
	var hotSent, normalSent int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		hotSent = produceLoop(loadCtx, cl, o.topic, hotKeys, hotPerKey)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		users := make([]string, 10)
		for i := range users {
			users[i] = fmt.Sprintf("user-%d", i)
		}
		normalSent = produceLoop(loadCtx, cl, o.topic, users, o.normalRate)
	}()
	wg.Wait()

	if err := cl.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	endCounts, err := readEndOffsets(ctx, admin, o.topic)
	if err != nil {
		return fmt.Errorf("ListEndOffsets (end): %w", err)
	}

	fmt.Printf("отправлено: hot=%d (по %d бакетам), normal=%d, всего=%d\n\n",
		hotSent, o.buckets, normalSent, hotSent+normalSent)
	fmt.Println("распределение по партициям (delta = end - start):")
	printPerPartitionDelta(startCounts, endCounts, o.partitions)
	return nil
}

func produceLoop(ctx context.Context, cl *kgo.Client, topic string, keys []string, ratePerKey int) int64 {
	if ratePerKey <= 0 || len(keys) == 0 {
		return 0
	}
	tick := time.Second / time.Duration(ratePerKey)
	if tick <= 0 {
		tick = time.Microsecond
	}
	t := time.NewTicker(tick)
	defer t.Stop()

	var sent int64
	for {
		select {
		case <-ctx.Done():
			return sent
		case <-t.C:
			for _, k := range keys {
				rec := &kgo.Record{
					Topic: topic,
					Key:   []byte(k),
					Value: []byte("event"),
				}
				cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
					if err == nil {
						return
					}
				})
				sent++
			}
		}
	}
}

func ensureTopic(ctx context.Context, admin *kadm.Client, o runOpts) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	resp, err := admin.CreateTopic(rpcCtx, o.partitions, o.rf, nil, o.topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("topic %q создан: partitions=%d rf=%d\n", o.topic, o.partitions, o.rf)
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if errors.Is(cause, kerr.TopicAlreadyExists) {
		fmt.Printf("topic %q уже существует — пишем поверх\n", o.topic)
		return nil
	}
	return cause
}

func readEndOffsets(ctx context.Context, admin *kadm.Client, topic string) (map[int32]int64, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ends, err := admin.ListEndOffsets(rpcCtx, topic)
	if err != nil {
		return nil, err
	}
	out := make(map[int32]int64)
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			return
		}
		out[o.Partition] = o.Offset
	})
	return out, nil
}

func printPerPartitionDelta(start, end map[int32]int64, partitions int32) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PARTITION\tDELTA\tBAR")

	deltas := make([]int64, partitions)
	var maxDelta, totalDelta int64
	for p := int32(0); p < partitions; p++ {
		d := end[p] - start[p]
		if d < 0 {
			d = 0
		}
		deltas[p] = d
		totalDelta += d
		if d > maxDelta {
			maxDelta = d
		}
	}

	for p := int32(0); p < partitions; p++ {
		share := 0.0
		if totalDelta > 0 {
			share = float64(deltas[p]) / float64(totalDelta) * 100
		}
		bar := ""
		if maxDelta > 0 {
			barLen := int(float64(deltas[p]) / float64(maxDelta) * 40)
			for i := 0; i < barLen; i++ {
				bar += "█"
			}
		}
		fmt.Fprintf(tw, "%d\t%d (%.1f%%)\t%s\n", p, deltas[p], share, bar)
	}
	_ = tw.Flush()
}
