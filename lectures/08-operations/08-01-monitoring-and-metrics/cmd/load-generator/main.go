package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic        = "lecture-08-01-events"
	defaultGroup        = "lecture-08-01-slow"
	defaultRate         = 200
	defaultConsumeDelay = 20 * time.Millisecond
	defaultPayloadKB    = 1
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик")
	group := flag.String("group", defaultGroup, "consumer-group")
	rate := flag.Int("rate", defaultRate, "сколько сообщений в секунду писать (producer)")
	consumeDelay := flag.Duration("consume-delay", defaultConsumeDelay,
		"искусственная задержка обработки на каждое сообщение (consumer)")
	payloadKB := flag.Int("payload-kb", defaultPayloadKB, "размер payload в килобайтах")
	flag.Parse()

	if *rate <= 0 {
		fmt.Fprintln(os.Stderr, "rate должен быть > 0")
		os.Exit(2)
	}

	ctx, cancel := runctx.New()
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	produced := &atomic.Int64{}
	consumed := &atomic.Int64{}

	go func() {
		defer wg.Done()
		if err := runProducer(ctx, *topic, *rate, *payloadKB, produced); err != nil &&
			!errors.Is(err, context.Canceled) {
			logger.Error("producer failed", "err", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := runConsumer(ctx, *topic, *group, *consumeDelay, consumed); err != nil &&
			!errors.Is(err, context.Canceled) {
			logger.Error("consumer failed", "err", err)
		}
	}()

	go reportRates(ctx, produced, consumed)

	wg.Wait()
	fmt.Fprintf(os.Stderr, "\nстопаемся. produced=%d consumed=%d\n",
		produced.Load(), consumed.Load())
}

func runProducer(ctx context.Context, topic string, rate, payloadKB int, produced *atomic.Int64) error {
	cl, err := kafka.NewClient(
		kgo.ClientID("lecture-08-01-load-producer"),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	payload := make([]byte, payloadKB*1024)
	for i := range payload {
		payload[i] = byte('a' + (i % 26))
	}

	interval := time.Second / time.Duration(rate)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var seq int64
	for {
		select {
		case <-ctx.Done():
			flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
			cl.Flush(flushCtx)
			flushCancel()
			return nil
		case <-ticker.C:
			seq++
			rec := &kgo.Record{
				Topic: topic,
				Key:   []byte(fmt.Sprintf("k-%d", seq%32)),
				Value: payload,
			}
			cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
				if err == nil {
					produced.Add(1)
				}
			})
		}
	}
}

func runConsumer(ctx context.Context, topic, group string, delay time.Duration, consumed *atomic.Int64) error {
	cl, err := kafka.NewClient(
		kgo.ClientID("lecture-08-01-load-consumer"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}
		fetches.EachRecord(func(_ *kgo.Record) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			consumed.Add(1)
		})
	}
}

func reportRates(ctx context.Context, produced, consumed *atomic.Int64) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var prevP, prevC int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p := produced.Load()
			c := consumed.Load()
			fmt.Fprintf(os.Stderr,
				"[load] produced=%d (%d/2s)  consumed=%d (%d/2s)  lag~%d\n",
				p, p-prevP, c, c-prevC, p-c,
			)
			prevP, prevC = p, c
		}
	}
}
