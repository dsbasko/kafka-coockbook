package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
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
	defaultTopic      = "lecture-02-05-headers"
	defaultPartitions = 3
	defaultRF         = 3
	defaultMessages   = 5
	defaultGroup      = "lecture-02-05-headers-group"
	defaultService    = "lecture-02-05"
	defaultMsgType    = "order.created.v1"
	defaultRoundtrip  = 8 * time.Second
)

func main() {
	logger := log.New()

	mode := flag.String("mode", "roundtrip", "сценарий: producer | consumer | roundtrip (producer + consumer в одном прогоне)")
	topic := flag.String("topic", defaultTopic, "имя топика")
	messages := flag.Int("messages", defaultMessages, "сколько сообщений написать в режиме producer/roundtrip")
	group := flag.String("group", defaultGroup, "group.id для consumer/roundtrip")
	service := flag.String("source-service", defaultService, "значение header source-service")
	msgType := flag.String("message-type", defaultMsgType, "значение header message-type")
	roundtripWait := flag.Duration("roundtrip-wait", defaultRoundtrip, "сколько ждать сообщений в режиме roundtrip перед выходом")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	var err error
	switch *mode {
	case "producer":
		err = ensureAndProduce(rootCtx, *topic, *messages, *service, *msgType)
	case "consumer":
		err = consume(rootCtx, *topic, *group, 0)
	case "roundtrip":
		err = ensureAndProduce(rootCtx, *topic, *messages, *service, *msgType)
		if err == nil {
			err = consume(rootCtx, *topic, *group, *roundtripWait)
		}
	default:
		logger.Error("unknown mode", "mode", *mode)
		os.Exit(2)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("headers-demo failed", "mode", *mode, "err", err)
		os.Exit(1)
	}
}

func ensureAndProduce(ctx context.Context, topic string, n int, service, msgType string) error {
	admin, err := kafka.NewAdmin()
	if err != nil {
		return fmt.Errorf("kafka.NewAdmin: %w", err)
	}
	defer admin.Close()

	if err := ensureTopic(ctx, admin, topic, defaultPartitions, defaultRF); err != nil {
		return fmt.Errorf("ensure topic %s: %w", topic, err)
	}

	cl, err := kafka.NewClient(kgo.DefaultProduceTopic(topic))
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("[producer] пишем %d сообщений в %q с headers\n", n, topic)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  PART\tOFFSET\tKEY\tCORRELATION-ID\tTRACEPARENT")
	for i := 0; i < n; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		correlationID := newRandomHex(16)
		trace := newTraceparent()
		rec := &kgo.Record{
			Key:   []byte(fmt.Sprintf("order-%d", i+1)),
			Value: []byte(fmt.Sprintf(`{"id":"order-%d","status":"created"}`, i+1)),
			Headers: []kgo.RecordHeader{
				{Key: "traceparent", Value: []byte(trace)},
				{Key: "correlation-id", Value: []byte(correlationID)},
				{Key: "message-type", Value: []byte(msgType)},
				{Key: "source-service", Value: []byte(service)},
			},
		}
		rpcCtx, rpcCancel := context.WithTimeout(ctx, 15*time.Second)
		out := cl.ProduceSync(rpcCtx, rec)
		rpcCancel()
		if err := out.FirstErr(); err != nil {
			_ = tw.Flush()
			return fmt.Errorf("produce: %w", err)
		}
		r := out[0].Record
		fmt.Fprintf(tw, "  %d\t%d\t%s\t%s\t%s\n", r.Partition, r.Offset, string(r.Key), correlationID, trace)
	}
	_ = tw.Flush()
	fmt.Println()
	return nil
}

func consume(ctx context.Context, topic, group string, waitFor time.Duration) error {
	cl, err := kafka.NewClient(
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("[consumer] читаем %q в группе %q\n", topic, group)
	if waitFor > 0 {
		fmt.Printf("[consumer] выйду, если %s не приходит ничего нового\n", waitFor)
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  PART\tOFFSET\tKEY\tHEADERS\tVALUE")

	idleStart := time.Now()
	for {
		pollCtx := ctx
		var pollCancel context.CancelFunc
		if waitFor > 0 {
			pollCtx, pollCancel = context.WithTimeout(ctx, 500*time.Millisecond)
		}
		fetches := cl.PollFetches(pollCtx)
		if pollCancel != nil {
			pollCancel()
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					_ = tw.Flush()
					return nil
				}
				if errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}

		got := 0
		fetches.EachRecord(func(r *kgo.Record) {
			got++
			fmt.Fprintf(tw, "  %d\t%d\t%s\t%s\t%s\n",
				r.Partition, r.Offset,
				string(r.Key),
				formatHeaders(r.Headers),
				string(r.Value),
			)
		})
		_ = tw.Flush()

		if got > 0 {
			idleStart = time.Now()
			continue
		}
		if waitFor > 0 && time.Since(idleStart) >= waitFor {
			fmt.Println()
			fmt.Println("[consumer] ничего нового — выходим")
			return nil
		}
	}
}

func formatHeaders(hs []kgo.RecordHeader) string {
	if len(hs) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(hs))
	byKey := make(map[string]string, len(hs))
	for _, h := range hs {
		keys = append(keys, h.Key)
		byKey[h.Key] = string(h.Value)
	}
	sort.Strings(keys)
	out := ""
	for i, k := range keys {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprintf("%s=%s", k, byKey[k])
	}
	return out
}

func newTraceparent() string {
	traceID := newRandomHex(16)
	spanID := newRandomHex(8)
	return fmt.Sprintf("00-%s-%s-01", traceID, spanID)
}

func newRandomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {

		for i := range b {
			b[i] = byte(time.Now().UnixNano() >> uint(i))
		}
	}
	return hex.EncodeToString(b)
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
		return nil
	}
	return cause
}
