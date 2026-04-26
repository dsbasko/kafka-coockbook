// seed — высокопроизводительный загрузчик для топика 03-05.
//
// Кладёт `messages` записей с `keys` различными ключами (round-robin),
// все async через franz-go. По умолчанию идёт без искусственных пауз —
// упирается в брокер и сеть. На локальном sandbox-стенде на ноуте легко
// держит десятки тысяч msg/sec, что хватает для демонстрации роста lag'а
// у sequential-консьюмера (он считает 100 msg/sec при work-delay=10ms).
//
// Используется из Makefile (`seed-fast`).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic    = "lecture-03-05-events"
	defaultMessages = 100_000
	defaultKeys     = 32
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "целевой топик")
	messages := flag.Int("messages", defaultMessages, "сколько записей залить")
	keys := flag.Int("keys", defaultKeys, "сколько различных ключей (round-robin); меньше keys → меньше параллелизма у concurrent-pool")
	flag.Parse()

	if *messages < 1 || *keys < 1 {
		fmt.Fprintln(os.Stderr, "messages и keys должны быть >= 1")
		os.Exit(2)
	}

	ctx, cancel := runctx.New()
	defer cancel()

	if err := run(ctx, *topic, *messages, *keys); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("seed failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, topic string, messages, keys int) error {
	cl, err := kafka.NewClient(
		kgo.ClientID("lecture-03-05-seed"),
		kgo.ProducerLinger(10*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("seed: топик=%q messages=%d keys=%d\n", topic, messages, keys)

	var sent atomic.Int64
	var failed atomic.Int64
	start := time.Now()

	for i := 0; i < messages; i++ {
		if err := ctx.Err(); err != nil {
			break
		}
		k := strconv.Itoa(i % keys)
		v := strconv.Itoa(i)
		rec := &kgo.Record{
			Topic: topic,
			Key:   []byte("k-" + k),
			Value: []byte("v-" + v),
		}
		cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			if err != nil {
				failed.Add(1)
				return
			}
			sent.Add(1)
		})
	}

	if err := cl.Flush(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			return fmt.Errorf("Flush: %w", err)
		}
	}

	elapsed := time.Since(start)
	rate := float64(0)
	if elapsed.Seconds() > 0 {
		rate = float64(sent.Load()) / elapsed.Seconds()
	}
	fmt.Printf("seed: sent=%d failed=%d elapsed=%s rate=%.0f msg/sec\n",
		sent.Load(), failed.Load(), elapsed.Truncate(time.Millisecond), rate)
	return nil
}
