// multi-strategy — consumer на топик payments с двумя стратегиями
// обработки ошибок:
//
//   - transient (временная ошибка, типа моргнула сеть): повторить тут же,
//     с exponential backoff. Несколько попыток в этом же worker-loop'е.
//     Если за -max-retries попыток успеха нет — отправляем в DLQ.
//   - permanent (битый payload, не парсится, или явно помечено): сразу в DLQ
//     с заголовками error.class / error.message / original.* / retry.count.
//
// Классификация ошибки идёт по типу: handle() возвращает permError для
// «невозвратных» ошибок и обычную error для transient. errors.As различает.
//
// Для имитации:
//   - mode=ok        → handle сразу возвращает nil;
//   - mode=transient → handle падает первые N раз (по in-memory счётчику
//     на (partition, offset)), на (N+1)-й попытке возвращает nil;
//   - mode=permanent → handle всегда возвращает permError;
//   - битый JSON     → permError (poison-pill).
//
// Что важно:
//   - retry-in-place выполняется ВНУТРИ цикла обработки одного record'а,
//     БЕЗ commit'а offset'а посередине. Если процесс упадёт во время
//     retry — на рестарте получим тот же record заново. Это at-least-once.
//   - DLQ-publish + commit — две операции, между ними возможен разрыв.
//     Если упасть после publish, до commit'а — на рестарте отправим в DLQ
//     повторно (см. README про дубли в DLQ).
//   - max-retries специально маленький (3). Большие backoff'ы блокируют
//     poll-loop и могут превысить max.poll.interval.ms — тогда group
//     отбалансируется. Длинный retry — это уже retry-topic с задержкой
//     (модуль 04-04), не in-place.
//
// Запуск: см. Makefile (run-processor / seed / topic-create-all).
package main

import (
	"context"
	"encoding/json"
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
	defaultTopic     = "payments"
	defaultDLQ       = "payments-dlq"
	defaultGroup     = "lecture-03-04-processor"
	defaultRetries   = 3
	defaultBackoff   = 200 * time.Millisecond
	defaultWorkDelay = 50 * time.Millisecond
	transientFails   = 3 // сколько раз handle() даст «transient» прежде чем «исцелиться»
)

// payment — формат входного сообщения. Битый JSON ловится unmarshal'ом
// и тут же классифицируется как permanent (poison-pill).
type payment struct {
	ID     string  `json:"id"`
	Amount float64 `json:"amount,omitempty"`
	Mode   string  `json:"mode"` // ok | transient | permanent
}

// permError — обёртка для «невозвратных» ошибок. Всё, что не permError,
// считается transient (есть смысл повторить).
type permError struct{ msg string }

func (e *permError) Error() string { return e.msg }

func permErrorf(format string, args ...any) error {
	return &permError{msg: fmt.Sprintf(format, args...)}
}

func isPermanent(err error) bool {
	var p *permError
	return errors.As(err, &p)
}

func errClass(err error) string {
	if isPermanent(err) {
		return "permanent"
	}
	return "transient"
}

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "входной топик")
	dlqTopic := flag.String("dlq", defaultDLQ, "DLQ топик")
	group := flag.String("group", defaultGroup, "group.id")
	maxRetries := flag.Int("max-retries", defaultRetries, "сколько раз повторять при transient ошибке (in-place)")
	baseBackoff := flag.Duration("base-backoff", defaultBackoff, "базовый интервал backoff'а; растёт x2 на попытку")
	workDelay := flag.Duration("work-delay", defaultWorkDelay, "имитация работы — sleep на каждое сообщение перед handle()")
	fromStart := flag.Bool("from-start", true, "при первом старте группы читать с earliest")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topic:       *topic,
		dlqTopic:    *dlqTopic,
		group:       *group,
		maxRetries:  *maxRetries,
		baseBackoff: *baseBackoff,
		workDelay:   *workDelay,
		fromStart:   *fromStart,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("multi-strategy failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic       string
	dlqTopic    string
	group       string
	maxRetries  int
	baseBackoff time.Duration
	workDelay   time.Duration
	fromStart   bool
}

type counters struct {
	processed atomic.Int64
	ok        atomic.Int64
	retried   atomic.Int64
	dlqPerm   atomic.Int64
	dlqExh    atomic.Int64
}

func run(ctx context.Context, o runOpts) error {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(o.group),
		kgo.ConsumeTopics(o.topic),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-03-04-processor"),
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

	fmt.Printf("multi-strategy запущен: topic=%q dlq=%q group=%q max-retries=%d base-backoff=%s\n",
		o.topic, o.dlqTopic, o.group, o.maxRetries, o.baseBackoff)
	fmt.Println("transient → in-place retry; permanent / exhausted → DLQ. Ctrl+C — выход.")
	fmt.Println()

	attempts := make(map[string]int) // (partition,offset) → attempts уже потраченные на «исцеление» transient
	c := &counters{}

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					printSummary(c)
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
			n := c.processed.Add(1)
			time.Sleep(o.workDelay)

			if err := processWithRetry(ctx, cl, o, r, attempts, c); err != nil {
				return err
			}

			_ = n
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
		fmt.Printf("--- batch committed: ok=%d retried=%d dlq-perm=%d dlq-exh=%d ---\n\n",
			c.ok.Load(), c.retried.Load(), c.dlqPerm.Load(), c.dlqExh.Load())
	}
}

// processWithRetry — обрабатывает одну запись по правилам:
//   - permanent с первого раза → сразу DLQ;
//   - transient → in-place retry с exponential backoff, до o.maxRetries попыток;
//   - если все попытки выгорели → DLQ как «exhausted».
func processWithRetry(
	ctx context.Context,
	cl *kgo.Client,
	o runOpts,
	r *kgo.Record,
	attempts map[string]int,
	c *counters,
) error {
	key := fmt.Sprintf("%d-%d", r.Partition, r.Offset)

	err := handle(r, attempts, key)
	if err == nil {
		c.ok.Add(1)
		fmt.Printf("OK     p=%d off=%d key=%s\n", r.Partition, r.Offset, string(r.Key))
		delete(attempts, key)
		return nil
	}

	if isPermanent(err) {
		fmt.Printf("PERM   p=%d off=%d key=%s err=%v → DLQ\n", r.Partition, r.Offset, string(r.Key), err)
		c.dlqPerm.Add(1)
		usedAttempts := attempts[key]
		delete(attempts, key)
		return forwardToDLQ(ctx, cl, o.dlqTopic, r, err, usedAttempts)
	}

	// transient → in-place retry с exponential backoff
	for attempt := 1; attempt <= o.maxRetries; attempt++ {
		backoff := o.baseBackoff * (1 << (attempt - 1))
		fmt.Printf("RETRY  p=%d off=%d key=%s attempt=%d backoff=%s err=%v\n",
			r.Partition, r.Offset, string(r.Key), attempt, backoff, err)
		c.retried.Add(1)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		err = handle(r, attempts, key)
		if err == nil {
			fmt.Printf("OK     p=%d off=%d key=%s (после %d retry)\n",
				r.Partition, r.Offset, string(r.Key), attempt)
			c.ok.Add(1)
			delete(attempts, key)
			return nil
		}
		if isPermanent(err) {
			fmt.Printf("PERM   p=%d off=%d key=%s err=%v → DLQ (во время retry)\n",
				r.Partition, r.Offset, string(r.Key), err)
			c.dlqPerm.Add(1)
			usedAttempts := attempts[key]
			delete(attempts, key)
			return forwardToDLQ(ctx, cl, o.dlqTopic, r, err, usedAttempts)
		}
	}

	// исчерпали попытки — в DLQ как exhausted retries
	fmt.Printf("EXH    p=%d off=%d key=%s last-err=%v → DLQ (исчерпали %d retry)\n",
		r.Partition, r.Offset, string(r.Key), err, o.maxRetries)
	c.dlqExh.Add(1)
	usedAttempts := attempts[key]
	delete(attempts, key)
	return forwardToDLQ(ctx, cl, o.dlqTopic, r, fmt.Errorf("exhausted retries: %w", err), usedAttempts)
}

// handle — мок-обработчик платежа.
//
//   - битый JSON          → permError;
//   - mode=ok             → nil;
//   - mode=transient      → permError если что-то странное; иначе обычный error
//     до тех пор пока attempts[key] < transientFails; после — nil;
//   - mode=permanent      → permError;
//   - неизвестный mode    → permError.
//
// Inkrement счётчика attempts[key] происходит ДО возврата ошибки —
// это и обеспечивает «исцеление» через transientFails попыток.
func handle(r *kgo.Record, attempts map[string]int, key string) error {
	var p payment
	if err := json.Unmarshal(r.Value, &p); err != nil {
		return permErrorf("invalid json: %v", err)
	}
	switch p.Mode {
	case "ok":
		return nil
	case "transient":
		attempts[key]++
		if attempts[key] < transientFails {
			return fmt.Errorf("transient downstream blip (attempt %d/%d)",
				attempts[key], transientFails)
		}
		return nil
	case "permanent":
		return permErrorf("payment id=%q rejected by domain rules", p.ID)
	default:
		return permErrorf("unknown mode: %q", p.Mode)
	}
}

// forwardToDLQ — отправляет запись в DLQ с диагностическими headers.
// Топик поведения от исходного: payload идентичный, key тот же,
// добавлены headers, по которым DLQ-reader потом ориентируется.
func forwardToDLQ(
	ctx context.Context,
	cl *kgo.Client,
	dlqTopic string,
	r *kgo.Record,
	cause error,
	attempts int,
) error {
	headers := append([]kgo.RecordHeader(nil), r.Headers...)
	headers = append(headers,
		kgo.RecordHeader{Key: "error.class", Value: []byte(errClass(cause))},
		kgo.RecordHeader{Key: "error.message", Value: []byte(cause.Error())},
		kgo.RecordHeader{Key: "original.topic", Value: []byte(r.Topic)},
		kgo.RecordHeader{Key: "original.partition", Value: []byte(strconv.Itoa(int(r.Partition)))},
		kgo.RecordHeader{Key: "original.offset", Value: []byte(strconv.FormatInt(r.Offset, 10))},
		kgo.RecordHeader{Key: "retry.count", Value: []byte(strconv.Itoa(attempts))},
		kgo.RecordHeader{Key: "dlq.timestamp", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
	)
	rec := &kgo.Record{
		Topic:   dlqTopic,
		Key:     r.Key,
		Value:   r.Value,
		Headers: headers,
	}

	produceCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cl.ProduceSync(produceCtx, rec).FirstErr(); err != nil {
		return fmt.Errorf("DLQ produce: %w", err)
	}
	return nil
}

func printSummary(c *counters) {
	fmt.Printf("\nостановлен по сигналу. processed=%d ok=%d retried=%d dlq-perm=%d dlq-exh=%d.\n",
		c.processed.Load(), c.ok.Load(), c.retried.Load(), c.dlqPerm.Load(), c.dlqExh.Load())
}
