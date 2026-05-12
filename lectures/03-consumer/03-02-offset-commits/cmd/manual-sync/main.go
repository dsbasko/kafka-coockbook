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
	defaultTopic    = "lecture-03-02-commits"
	defaultGroup    = "lecture-03-02-sync"
	defaultLogFile  = "processed-sync.log"
	defaultWorkTime = 200 * time.Millisecond
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик для чтения")
	group := flag.String("group", defaultGroup, "group.id")
	logFile := flag.String("log-file", defaultLogFile, "файл для processed-записей")
	workDelay := flag.Duration("work-delay", defaultWorkTime, "имитация работы — sleep на каждое сообщение")
	crashAfter := flag.Int("crash-after", 0,
		"если > 0, после стольких ОБРАБОТАННЫХ записей делаем os.Exit(1) до commit'а батча")
	fromStart := flag.Bool("from-start", true, "при первом старте группы читать с earliest")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topic:      *topic,
		group:      *group,
		logFile:    *logFile,
		workDelay:  *workDelay,
		crashAfter: *crashAfter,
		fromStart:  *fromStart,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("manual-sync failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic      string
	group      string
	logFile    string
	workDelay  time.Duration
	crashAfter int
	fromStart  bool
}

func run(ctx context.Context, o runOpts) error {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(o.group),
		kgo.ConsumeTopics(o.topic),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-03-02-sync"),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			fmt.Fprintf(os.Stderr, "assigned: %v\n", m)
		}),
	}
	if o.fromStart {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	cl, err := kafka.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	logf, err := os.OpenFile(o.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open log %s: %w", o.logFile, err)
	}
	defer logf.Close()

	fmt.Printf("manual-sync запущен: topic=%q group=%q work-delay=%s crash-after=%d log=%s\n",
		o.topic, o.group, o.workDelay, o.crashAfter, o.logFile)
	fmt.Println("читаем; commit делается ПОСЛЕ полного батча. Ctrl+C — выход.")
	fmt.Println()

	var processed atomic.Int64

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					fmt.Printf("\nостановлен по сигналу. processed=%d.\n", processed.Load())
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

		fmt.Printf("--- батч из %d записей ---\n", len(batch))

		for _, r := range batch {
			time.Sleep(o.workDelay)
			line := fmt.Sprintf("%d,%d,%s,%s\n", r.Partition, r.Offset, string(r.Key), string(r.Value))
			_, _ = logf.WriteString(line)
			n := processed.Add(1)
			fmt.Printf("processed n=%d p=%d off=%d key=%s\n", n, r.Partition, r.Offset, string(r.Key))

			if o.crashAfter > 0 && int(n) == o.crashAfter {
				fmt.Printf("\n=== CRASH SIMULATION в середине батча после %d записей: os.Exit(1) ДО commit'а ===\n", n)
				_ = logf.Sync()
				os.Exit(1)
			}
		}

		commitCtx, commitCancel := context.WithTimeout(ctx, 10*time.Second)
		err := cl.CommitRecords(commitCtx, batch...)
		commitCancel()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("CommitRecords: %w", err)
		}
		fmt.Printf("--- batch committed: %d записей ---\n\n", len(batch))
	}
}
