package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopicNonRetriable = "lecture-02-05-non-retriable"
	defaultTopicRetriable    = "lecture-02-05-retriable"
)

func main() {
	logger := log.New()

	mode := flag.String("mode", "non-retriable", "сценарий: non-retriable | retriable")
	topicNR := flag.String("topic-non-retriable", defaultTopicNonRetriable, "топик для non-retriable сценария (max.message.bytes=1024)")
	topicR := flag.String("topic-retriable", defaultTopicRetriable, "топик для retriable сценария (min.insync.replicas=3)")
	payload := flag.Int("payload", 4096, "размер payload в байтах для non-retriable сценария")
	deliveryTimeout := flag.Duration("delivery-timeout", 20*time.Second, "RecordDeliveryTimeout для retriable сценария")
	debug := flag.Bool("debug", true, "kgo.WithLogger(LogLevelDebug) для retriable — видно ретраи на каждой попытке")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	var err error
	switch *mode {
	case "non-retriable":
		err = runNonRetriable(rootCtx, *topicNR, *payload)
	case "retriable":
		err = runRetriable(rootCtx, *topicR, *deliveryTimeout, *debug)
	default:
		logger.Error("unknown mode", "mode", *mode)
		os.Exit(2)
	}
	if err != nil {
		logger.Error("error-classes failed", "mode", *mode, "err", err)
		os.Exit(1)
	}
}

func runNonRetriable(ctx context.Context, topic string, payload int) error {
	admin, err := kafka.NewAdmin()
	if err != nil {
		return fmt.Errorf("kafka.NewAdmin: %w", err)
	}
	defer admin.Close()

	if err := ensureTopicWithMaxBytes(ctx, admin, topic, "1024"); err != nil {
		return fmt.Errorf("ensure topic %s: %w", topic, err)
	}

	cl, err := kafka.NewClient(
		kgo.DefaultProduceTopic(topic),

		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.MaxBufferedRecords(10),

		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	rec := &kgo.Record{
		Key:   []byte("oversized"),
		Value: randomBytes(payload),
	}

	fmt.Printf("[non-retriable] пишем record на %d B в топик %q (на топике max.message.bytes=1024)\n", payload, topic)
	rpcCtx, rpcCancel := context.WithTimeout(ctx, 15*time.Second)
	defer rpcCancel()
	start := time.Now()
	out := cl.ProduceSync(rpcCtx, rec)
	took := time.Since(start)

	pErr := out.FirstErr()
	switch {
	case pErr == nil:
		fmt.Printf("[non-retriable] неожиданно ОК за %s — проверь max.message.bytes на топике (ожидаем MESSAGE_TOO_LARGE)\n", fmtDur(took))
		return errors.New("expected MESSAGE_TOO_LARGE, got success")
	case errors.Is(pErr, kerr.MessageTooLarge):
		fmt.Printf("[non-retriable] получили MESSAGE_TOO_LARGE за %s — это non-retriable, ProduceSync вернул сразу, без ретраев\n", fmtDur(took))
		fmt.Printf("[non-retriable] err: %v\n", pErr)
		return nil
	default:
		fmt.Printf("[non-retriable] непредвиденная ошибка за %s: %v\n", fmtDur(took), pErr)
		return pErr
	}
}

func runRetriable(ctx context.Context, topic string, deliveryTimeout time.Duration, debug bool) error {
	admin, err := kafka.NewAdmin()
	if err != nil {
		return fmt.Errorf("kafka.NewAdmin: %w", err)
	}
	defer admin.Close()

	if err := ensureTopicWithMinISR(ctx, admin, topic, "3"); err != nil {
		return fmt.Errorf("ensure topic %s: %w", topic, err)
	}

	opts := []kgo.Opt{
		kgo.DefaultProduceTopic(topic),
		kgo.RecordDeliveryTimeout(deliveryTimeout),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	if debug {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
	}
	cl, err := kafka.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	rec := &kgo.Record{
		Key:   []byte("retriable"),
		Value: []byte(fmt.Sprintf("acks=all into %s; observe retries in debug log", topic)),
	}

	fmt.Printf("[retriable] пишем 1 record с acks=all в %q (min.insync.replicas=3, delivery-timeout=%s)\n", topic, deliveryTimeout)
	fmt.Printf("[retriable] подсказка: запусти `make kill-broker` в другом терминале до этой команды — тогда ISR=2, NOT_ENOUGH_REPLICAS, видны ретраи\n")
	rpcCtx, rpcCancel := context.WithTimeout(ctx, deliveryTimeout+10*time.Second)
	defer rpcCancel()

	start := time.Now()
	out := cl.ProduceSync(rpcCtx, rec)
	took := time.Since(start)

	pErr := out.FirstErr()
	switch {
	case pErr == nil:
		fmt.Printf("[retriable] доставлено за %s — кластер здоров, ISR=3 (для демки нужно перед запуском убрать одного брокера)\n", fmtDur(took))
		return nil
	case errors.Is(pErr, kgo.ErrRecordTimeout):

		fmt.Printf("[retriable] упёрлись в delivery-timeout после %s ретраев\n", fmtDur(took))
		fmt.Printf("[retriable] err: %v\n", pErr)
		return nil
	case errors.Is(pErr, kerr.NotEnoughReplicas), errors.Is(pErr, kerr.NotEnoughReplicasAfterAppend):
		fmt.Printf("[retriable] NOT_ENOUGH_REPLICAS наружу за %s\n", fmtDur(took))
		fmt.Printf("[retriable] err: %v\n", pErr)
		return nil
	default:
		fmt.Printf("[retriable] другая ошибка за %s: %v\n", fmtDur(took), pErr)
		return pErr
	}
}

func ensureTopicWithMaxBytes(ctx context.Context, admin *kadm.Client, topic string, maxBytes string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	configs := map[string]*string{"max.message.bytes": &maxBytes}
	resp, err := admin.CreateTopic(rpcCtx, 3, 3, configs, topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("topic %q создан: max.message.bytes=%s\n", topic, maxBytes)
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if !errors.Is(cause, kerr.TopicAlreadyExists) {
		return cause
	}

	alterCtx, alterCancel := context.WithTimeout(ctx, 15*time.Second)
	defer alterCancel()
	alterations := []kadm.AlterConfig{
		{Op: kadm.SetConfig, Name: "max.message.bytes", Value: &maxBytes},
	}
	if _, err := admin.AlterTopicConfigs(alterCtx, alterations, topic); err != nil {
		return fmt.Errorf("alter max.message.bytes: %w", err)
	}
	fmt.Printf("topic %q уже существует — установлен max.message.bytes=%s\n", topic, maxBytes)
	return nil
}

func ensureTopicWithMinISR(ctx context.Context, admin *kadm.Client, topic string, minISR string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	configs := map[string]*string{"min.insync.replicas": &minISR}
	resp, err := admin.CreateTopic(rpcCtx, 3, 3, configs, topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("topic %q создан: min.insync.replicas=%s\n", topic, minISR)
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if !errors.Is(cause, kerr.TopicAlreadyExists) {
		return cause
	}

	alterCtx, alterCancel := context.WithTimeout(ctx, 15*time.Second)
	defer alterCancel()
	alterations := []kadm.AlterConfig{
		{Op: kadm.SetConfig, Name: "min.insync.replicas", Value: &minISR},
	}
	if _, err := admin.AlterTopicConfigs(alterCtx, alterations, topic); err != nil {
		return fmt.Errorf("alter min.insync.replicas: %w", err)
	}
	fmt.Printf("topic %q уже существует — установлен min.insync.replicas=%s\n", topic, minISR)
	return nil
}

func randomBytes(n int) []byte {
	if n <= 0 {
		return nil
	}
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		for i := range b {
			b[i] = byte(i)
		}
	}
	return b
}

func fmtDur(d time.Duration) string {
	switch {
	case d == 0:
		return "0"
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d.Microseconds()))
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000.0)
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}
