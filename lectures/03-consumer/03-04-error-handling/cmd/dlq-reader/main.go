package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultDLQ   = "payments-dlq"
	defaultGroup = "lecture-03-04-dlq-reader"
)

var keyHeaders = []string{
	"error.class",
	"error.message",
	"original.topic",
	"original.partition",
	"original.offset",
	"retry.count",
	"dlq.timestamp",
}

func main() {
	logger := log.New()

	dlqTopic := flag.String("dlq", defaultDLQ, "DLQ топик")
	group := flag.String("group", defaultGroup, "group.id")
	fromStart := flag.Bool("from-start", true, "при первом старте группы читать с earliest")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, *dlqTopic, *group, *fromStart); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("dlq-reader failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, dlqTopic, group string, fromStart bool) error {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(dlqTopic),
		kgo.ClientID("lecture-03-04-dlq-reader"),
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

	fmt.Printf("dlq-reader запущен: topic=%q group=%q\n", dlqTopic, group)
	fmt.Println("читаем DLQ; по каждой записи — headers и payload. Ctrl+C — выход.")
	fmt.Println()

	count := 0

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					fmt.Printf("\nостановлен по сигналу. прочитано из DLQ: %d.\n", count)
					return nil
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			count++
			printRecord(count, r)
		})
	}
}

func printRecord(n int, r *kgo.Record) {
	idx := indexHeaders(r.Headers)

	fmt.Printf("=== #%d  p=%d off=%d  key=%s\n", n, r.Partition, r.Offset, string(r.Key))
	for _, name := range keyHeaders {
		if v, ok := idx[name]; ok {
			fmt.Printf("    %-20s = %s\n", name, v)
			delete(idx, name)
		}
	}
	if len(idx) > 0 {
		fmt.Println("    other headers:")
		for k, v := range idx {
			fmt.Printf("      %s = %s\n", k, v)
		}
	}
	fmt.Printf("    payload: %s\n\n", string(r.Value))
}

func indexHeaders(hs []kgo.RecordHeader) map[string]string {
	out := make(map[string]string, len(hs))
	for _, h := range hs {
		out[h.Key] = string(h.Value)
	}
	return out
}
