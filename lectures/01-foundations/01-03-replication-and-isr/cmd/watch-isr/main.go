package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic       = "lecture-01-03-replicated"
	defaultPartitions  = 3
	defaultReplication = 3
	defaultInterval    = 2 * time.Second
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик, который создаём и наблюдаем")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании")
	rf := flag.Int("rf", defaultReplication, "replication factor при создании")
	interval := flag.Duration("interval", defaultInterval, "пауза между опросами metadata")
	once := flag.Bool("once", false, "сделать один опрос и выйти (для тестов)")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, *topic, int32(*partitions), int16(*rf), *interval, *once); err != nil {
		logger.Error("watch-isr failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, topic string, partitions int32, rf int16, interval time.Duration, once bool) error {
	admin, err := kafka.NewAdmin()
	if err != nil {
		return fmt.Errorf("kafka.NewAdmin: %w", err)
	}
	defer admin.Close()

	if err := ensureTopic(ctx, admin, topic, partitions, rf); err != nil {
		return fmt.Errorf("ensure topic: %w", err)
	}

	fmt.Printf("watching ISR for topic=%q every %s (Ctrl+C to stop)\n\n", topic, interval)

	if once {
		return tick(ctx, admin, topic)
	}

	t := time.NewTicker(interval)
	defer t.Stop()

	if err := tick(ctx, admin, topic); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := tick(ctx, admin, topic); err != nil {

				fmt.Fprintf(os.Stderr, "tick failed: %v\n", err)
			}
		}
	}
}

func tick(ctx context.Context, admin *kadm.Client, topic string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	details, err := admin.ListTopics(rpcCtx, topic)
	if err != nil {
		return fmt.Errorf("ListTopics: %w", err)
	}
	td, ok := details[topic]
	if !ok {
		return fmt.Errorf("topic %q отсутствует в metadata-ответе", topic)
	}
	if td.Err != nil {
		return fmt.Errorf("topic %q load error: %w", topic, td.Err)
	}

	printSnapshot(td)
	return nil
}

func ensureTopic(ctx context.Context, admin *kadm.Client, topic string, partitions int32, rf int16) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	resp, err := admin.CreateTopic(rpcCtx, partitions, rf, nil, topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("topic %q создан: partitions=%d rf=%d\n", topic, partitions, rf)
		return nil
	}

	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if errors.Is(cause, kerr.TopicAlreadyExists) {
		fmt.Printf("topic %q уже существует — наблюдаем\n", topic)
		return nil
	}
	return cause
}

func printSnapshot(td kadm.TopicDetail) {
	fmt.Printf("[%s]\n", time.Now().Format("15:04:05"))

	parts := td.Partitions.Sorted()

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PARTITION\tLEADER\tREPLICAS\tISR\tUNDER-REPLICATED")
	for _, p := range parts {
		replicas := append([]int32(nil), p.Replicas...)
		isr := append([]int32(nil), p.ISR...)
		sort.Slice(replicas, func(i, j int) bool { return replicas[i] < replicas[j] })
		sort.Slice(isr, func(i, j int) bool { return isr[i] < isr[j] })

		under := "no"
		if len(p.ISR) < len(p.Replicas) {
			under = fmt.Sprintf("yes (missing %v)", missing(p.Replicas, p.ISR))
		}
		fmt.Fprintf(tw, "%d\t%d\t%v\t%v\t%s\n", p.Partition, p.Leader, replicas, isr, under)
	}
	_ = tw.Flush()
	fmt.Println("---")
}

func missing(replicas, isr []int32) []int32 {
	in := make(map[int32]struct{}, len(isr))
	for _, id := range isr {
		in[id] = struct{}{}
	}
	out := make([]int32, 0, len(replicas)-len(isr))
	for _, id := range replicas {
		if _, ok := in[id]; !ok {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
