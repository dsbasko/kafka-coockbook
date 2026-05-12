package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic     = "lecture-03-05-events"
	defaultGroup     = "lecture-03-05-sequential"
	defaultWorkDelay = 10 * time.Millisecond
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "входной топик")
	group := flag.String("group", defaultGroup, "group.id")
	workDelay := flag.Duration("work-delay", defaultWorkDelay, "имитация работы — sleep на каждое сообщение")
	fromStart := flag.Bool("from-start", true, "при первом старте группы читать с earliest")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, *topic, *group, *workDelay, *fromStart); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("sequential failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, topic, group string, workDelay time.Duration, fromStart bool) error {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-03-05-sequential"),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			fmt.Fprintf(os.Stderr, "assigned: %v\n", m)
		}),
	}
	if fromStart {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	cl, err := kafka.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("sequential запущен: topic=%q group=%q work-delay=%s\n", topic, group, workDelay)
	fmt.Println("обрабатываем по одной записи в один поток. Ctrl+C — выход.")
	fmt.Println()

	var processed atomic.Int64
	start := time.Now()

	tickerCtx, tickerCancel := context.WithCancel(ctx)
	defer tickerCancel()
	go reportThroughput(tickerCtx, &processed)

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					printSummary(processed.Load(), time.Since(start))
					return nil
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}

		batch := make([]*kgo.Record, 0)
		fetches.EachRecord(func(r *kgo.Record) { batch = append(batch, r) })
		if len(batch) == 0 {
			continue
		}

		for _, r := range batch {
			select {
			case <-ctx.Done():
				printSummary(processed.Load(), time.Since(start))
				return nil
			case <-time.After(workDelay):
			}
			processed.Add(1)
			_ = r
		}

		commitCtx, commitCancel := context.WithTimeout(ctx, 10*time.Second)
		err := cl.CommitRecords(commitCtx, batch...)
		commitCancel()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				printSummary(processed.Load(), time.Since(start))
				return nil
			}
			return fmt.Errorf("CommitRecords: %w", err)
		}
	}
}

func reportThroughput(ctx context.Context, processed *atomic.Int64) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var prev int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cur := processed.Load()
			delta := cur - prev
			prev = cur
			fmt.Printf("[seq] processed=%d  rate=%d msg/sec\n", cur, delta)
		}
	}
}

func printSummary(processed int64, elapsed time.Duration) {
	rate := float64(0)
	if elapsed.Seconds() > 0 {
		rate = float64(processed) / elapsed.Seconds()
	}
	fmt.Printf("\nостановлен по сигналу. processed=%d elapsed=%s avg=%.0f msg/sec\n",
		processed, elapsed.Truncate(time.Millisecond), rate)
}
