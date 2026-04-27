// processor — consumer с retry-pipeline через отдельные топики с задержкой.
//
// Идея простая. У нас есть основной топик payments и три retry-топика
// payments-retry-30s, payments-retry-5m, payments-retry-1h, плюс
// payments-dlq в конце пайплайна. Один consumer группы lecture-04-04
// слушает все четыре retry/main-топика. Для retry-топиков перед
// обработкой ждёт до record.Timestamp + соответствующий лаг (30s/5m/1h).
//
// Поведение по результату handle():
//
//   - nil                   → commit, идём дальше;
//   - permanent error       → сразу payments-dlq, commit;
//   - transient (любой обычный error) → следующий retry-топик; если уже
//     последний retry — в payments-dlq.
//
// Каждое перемещение по пайплайну форвардит payload + headers, докладывая
// retry.count, error.class, error.message, original.* и retry.last-error.
// Это даёт DLQ-обработчику и replay-CLI всю историю инцидента.
//
// Что важно:
//
//   - sleep до Timestamp + лага блокирует poll-loop. Для лекции это
//     осознанно: pipeline должен быть нагляден. В проде так не делают —
//     либо отдельный поток на каждый retry-топик, либо паузим консьюмер
//     через PauseFetchPartitions; хорошие подробности — в 04-05.
//   - publish в следующий топик и commit входного — две операции, между
//     ними окно для дублей. Ровно та же история, что в 04-03 для outbox.
//   - Last-stop — DLQ. После DLQ retry прекращаются: его читает только
//     dlq-processor (или replay-cli при ручном вмешательстве).
//
// Запуск: см. Makefile (run-processor, seed-with-failures).
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
	defaultMain    = "payments"
	defaultRetry30 = "payments-retry-30s"
	defaultRetry5m = "payments-retry-5m"
	defaultRetry1h = "payments-retry-1h"
	defaultDLQ     = "payments-dlq"
	defaultGroup   = "lecture-04-04-processor"
)

// stage — одна ступень пайплайна. nextTopic пуст для DLQ-ступени;
// delay = 0 для main-ступени, иначе сколько ждать перед обработкой
// (от record.Timestamp).
type stage struct {
	topic     string
	delay     time.Duration
	nextTopic string // пусто = некуда дальше эскалировать (DLQ или последний retry, ведущий в DLQ)
}

// payment — формат входного сообщения. Битый JSON ловится unmarshal'ом
// и тут же классифицируется как permanent (poison-pill).
type payment struct {
	ID     string  `json:"id"`
	Amount float64 `json:"amount,omitempty"`
	Mode   string  `json:"mode"` // ok | transient | permanent
}

// permError — невозвратные ошибки. Всё, что не permError, считается
// transient (есть смысл повторить позже).
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

type counters struct {
	processed atomic.Int64
	ok        atomic.Int64
	escalated atomic.Int64
	dlq       atomic.Int64
}

func main() {
	logger := log.New()

	mainTopic := flag.String("main", defaultMain, "основной входной топик")
	retry30 := flag.String("retry-30s", defaultRetry30, "топик с задержкой 30s")
	retry5m := flag.String("retry-5m", defaultRetry5m, "топик с задержкой 5m")
	retry1h := flag.String("retry-1h", defaultRetry1h, "топик с задержкой 1h")
	dlq := flag.String("dlq", defaultDLQ, "DLQ топик (последняя ступень)")
	group := flag.String("group", defaultGroup, "group.id для всех retry-топиков")

	delay30s := flag.Duration("delay-30s", 30*time.Second, "переопределить задержку для retry-30s (для тестов — например 2s)")
	delay5m := flag.Duration("delay-5m", 5*time.Minute, "переопределить задержку для retry-5m")
	delay1h := flag.Duration("delay-1h", 1*time.Hour, "переопределить задержку для retry-1h")

	fromStart := flag.Bool("from-start", true, "при первом старте группы читать с earliest")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	stages := []stage{
		{topic: *mainTopic, delay: 0, nextTopic: *retry30},
		{topic: *retry30, delay: *delay30s, nextTopic: *retry5m},
		{topic: *retry5m, delay: *delay5m, nextTopic: *retry1h},
		{topic: *retry1h, delay: *delay1h, nextTopic: *dlq},
	}

	if err := run(rootCtx, stages, *dlq, *group, *fromStart); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("processor failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, stages []stage, dlqTopic, group string, fromStart bool) error {
	stageByTopic := make(map[string]stage, len(stages))
	topics := make([]string, 0, len(stages))
	for _, s := range stages {
		stageByTopic[s.topic] = s
		topics = append(topics, s.topic)
	}

	opts := []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-04-04-processor"),
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

	fmt.Printf("processor запущен: group=%q\n", group)
	fmt.Println("ступени пайплайна:")
	for _, s := range stages {
		fmt.Printf("  %-22s delay=%-6s next=%s\n", s.topic, s.delay, s.nextTopic)
	}
	fmt.Printf("  %-22s (terminal — больше не эскалируем)\n", dlqTopic)
	fmt.Println()

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

		for _, r := range batch {
			c.processed.Add(1)
			st, ok := stageByTopic[r.Topic]
			if !ok {
				return fmt.Errorf("неожиданный топик в fetch'е: %q", r.Topic)
			}

			if st.delay > 0 {
				if err := waitUntilDue(ctx, r.Timestamp, st.delay); err != nil {
					return err
				}
			}

			err := handle(r)
			if err == nil {
				c.ok.Add(1)
				fmt.Printf("OK    [%s] p=%d off=%d key=%s\n",
					r.Topic, r.Partition, r.Offset, string(r.Key))
				continue
			}

			if err := forwardOrDLQ(ctx, cl, st, dlqTopic, r, err, c); err != nil {
				return err
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
		fmt.Printf("--- batch committed: ok=%d escalated=%d dlq=%d ---\n\n",
			c.ok.Load(), c.escalated.Load(), c.dlq.Load())
	}
}

// waitUntilDue ждёт, пока (recordTs + delay) не наступит. Если уже наступило —
// возвращается сразу. Прерывается контекстом (SIGINT/SIGTERM).
func waitUntilDue(ctx context.Context, recordTs time.Time, delay time.Duration) error {
	due := recordTs.Add(delay)
	wait := time.Until(due)
	if wait <= 0 {
		return nil
	}
	fmt.Printf("WAIT  due=%s (через %s)\n", due.UTC().Format(time.RFC3339), wait.Truncate(time.Second))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(wait):
		return nil
	}
}

// handle — мок-обработчик платежа, идентичный 03-04.
//
//   - битый JSON     → permError (poison-pill);
//   - mode=ok        → nil;
//   - mode=transient → обычный error (transient — пойдёт на следующую ступень);
//   - mode=permanent → permError (сразу в DLQ, мимо retry).
//
// Под лекцию transient никогда не «исцеляется», чтобы было видно весь пайплайн
// 30s → 5m → 1h → DLQ. На практике transient бывают временными — у нас бы
// прошло после первого-второго ретрая.
func handle(r *kgo.Record) error {
	var p payment
	if err := json.Unmarshal(r.Value, &p); err != nil {
		return permErrorf("invalid json: %v", err)
	}
	switch p.Mode {
	case "ok":
		return nil
	case "transient":
		return fmt.Errorf("transient downstream blip on payment id=%q", p.ID)
	case "permanent":
		return permErrorf("payment id=%q rejected by domain rules", p.ID)
	default:
		return permErrorf("unknown mode: %q", p.Mode)
	}
}

// forwardOrDLQ решает, куда отправить упавший record:
//
//   - permanent → сразу dlqTopic, минуя retry-ступени (поведение 03-04);
//   - transient + есть nextTopic у текущей ступени → в nextTopic;
//   - transient + nextTopic пусто (последняя retry-ступень) → в dlqTopic.
//
// В обоих случаях докладываем headers и коммитим upstream-batch снаружи.
func forwardOrDLQ(
	ctx context.Context,
	cl *kgo.Client,
	st stage,
	dlqTopic string,
	r *kgo.Record,
	cause error,
	c *counters,
) error {
	target := st.nextTopic
	reason := "next-retry"
	if isPermanent(cause) {
		target = dlqTopic
		reason = "permanent"
	} else if target == "" {
		target = dlqTopic
		reason = "exhausted"
	}

	if target == dlqTopic {
		c.dlq.Add(1)
	} else {
		c.escalated.Add(1)
	}

	fmt.Printf("FAIL  [%s] p=%d off=%d key=%s reason=%s err=%v → %s\n",
		r.Topic, r.Partition, r.Offset, string(r.Key), reason, cause, target)

	return forwardWithHeaders(ctx, cl, target, r, cause)
}

// forwardWithHeaders собирает headers по соглашению лекции и шлёт ProduceSync
// в target-топик. Соглашение headers (повторяем 03-04 с дополнениями):
//
//	error.class      → permanent | transient
//	error.message    → последняя ошибка handle()
//	error.timestamp  → когда упало (RFC3339Nano UTC)
//	retry.count      → сколько раз уже эскалировали (0 при первом форварде из main)
//	original.topic   → откуда родом (никогда не меняется при цепочке retry)
//	original.partition / original.offset → где лежал в самом начале
//	previous.topic   → где упало последний раз (полезно для replay)
//
// Если запись уже путешествует по pipeline'у — original.* ставит самый первый
// форвард, последующие лишь читают и оставляют как есть.
func forwardWithHeaders(
	ctx context.Context,
	cl *kgo.Client,
	target string,
	r *kgo.Record,
	cause error,
) error {
	headers := append([]kgo.RecordHeader(nil), r.Headers...)
	idx := indexHeaders(headers)

	prevRetries := 0
	if v, ok := idx["retry.count"]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			prevRetries = n
		}
	}
	nextRetries := prevRetries + 1

	if _, ok := idx["original.topic"]; !ok {
		headers = appendOrReplace(headers, "original.topic", r.Topic)
		headers = appendOrReplace(headers, "original.partition", strconv.Itoa(int(r.Partition)))
		headers = appendOrReplace(headers, "original.offset", strconv.FormatInt(r.Offset, 10))
	}
	headers = appendOrReplace(headers, "previous.topic", r.Topic)
	headers = appendOrReplace(headers, "retry.count", strconv.Itoa(nextRetries))
	headers = appendOrReplace(headers, "error.class", errClass(cause))
	headers = appendOrReplace(headers, "error.message", cause.Error())
	headers = appendOrReplace(headers, "error.timestamp", time.Now().UTC().Format(time.RFC3339Nano))

	rec := &kgo.Record{
		Topic:   target,
		Key:     r.Key,
		Value:   r.Value,
		Headers: headers,
	}

	produceCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := cl.ProduceSync(produceCtx, rec).FirstErr(); err != nil {
		return fmt.Errorf("produce → %s: %w", target, err)
	}
	return nil
}

// appendOrReplace перезаписывает значение header'а с заданным ключом,
// если такой уже есть, иначе добавляет новый. Это нужно, чтобы не плодить
// дубли error.* при многократной эскалации.
func appendOrReplace(hs []kgo.RecordHeader, key, value string) []kgo.RecordHeader {
	for i := range hs {
		if hs[i].Key == key {
			hs[i].Value = []byte(value)
			return hs
		}
	}
	return append(hs, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func indexHeaders(hs []kgo.RecordHeader) map[string]string {
	out := make(map[string]string, len(hs))
	for _, h := range hs {
		out[h.Key] = string(h.Value)
	}
	return out
}

func printSummary(c *counters) {
	fmt.Printf("\nостановлен по сигналу. processed=%d ok=%d escalated=%d dlq=%d.\n",
		c.processed.Load(), c.ok.Load(), c.escalated.Load(), c.dlq.Load())
}
