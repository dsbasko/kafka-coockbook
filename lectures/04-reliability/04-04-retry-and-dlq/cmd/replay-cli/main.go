// replay-cli — переотправка сообщений из DLQ обратно в основной топик.
//
// Сценарий, ради которого этот CLI существует:
//
//   - даунстрим неделю лежал → DLQ накопил тысячу сообщений с
//     error.class=transient;
//   - даунстрим починили; хочется переподать тот же payload в основной
//     pipeline, чтобы не терять заказы;
//   - руками написать «cat dlq | producer» — рискованно: легко переслать
//     лишнего или, наоборот, потерять headers.
//
// Что делает CLI:
//
//   - читает -from-topic (по умолчанию payments-dlq) с -group-suffix
//     (даёт уникальную консьюмер-группу, не пересекаясь с dlq-processor);
//   - фильтрует записи по -since (timestamp >= now() - since) и опционально
//     по -error-class (transient | permanent);
//   - для каждой подходящей перепаковывает в новый record для -to-topic
//     (ключ и payload без изменений; headers сохраняются, retry.count
//     обнуляется, добавляется replay.from-dlq=<dlqOffset>);
//   - идёт ProduceSync, при ошибке возвращает ненулевой код выхода;
//   - завершается, когда либо достиг конца партиций, либо встретил
//     запись старше -since и больше нет «свежих» (см. флаг -idle).
//
// Использование:
//
//	replay -since 1h
//	replay -from-topic payments-dlq -to-topic payments -since 24h \
//	       -error-class transient -dry-run
//
// Что НЕ делает:
//
//   - не удаляет ничего из DLQ. DLQ — append-only, retention отрабатывает
//     по обычным правилам топика (см. 08-02);
//   - не дедуплицирует. Если запустить replay дважды подряд — отправит
//     дважды. На consumer-стороне всё равно нужна идемпотентность.
//
// Запуск: см. Makefile (replay).
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
	defaultFromTopic   = "payments-dlq"
	defaultToTopic     = "payments"
	defaultClientID    = "lecture-04-04-replay"
	defaultGroupPrefix = "lecture-04-04-replay"
)

func main() {
	logger := log.New()

	from := flag.String("from-topic", defaultFromTopic, "источник (DLQ)")
	to := flag.String("to-topic", defaultToTopic, "куда переотправить (обычно основной топик)")
	since := flag.Duration("since", time.Hour, "брать сообщения не старше этого периода (по DLQ-record timestamp)")
	errorClass := flag.String("error-class", "", "фильтр по header error.class (например, transient). Пусто = без фильтра")
	dryRun := flag.Bool("dry-run", false, "ничего не публиковать, только посчитать что отправили бы")
	idle := flag.Duration("idle", 5*time.Second, "после такого периода тишины считаем, что DLQ дочитан и выходим")
	groupSuffix := flag.String("group-suffix", time.Now().UTC().Format("20060102-150405"), "уникальный суффикс группы — чтобы не конфликтовать с dlq-processor и предыдущими replay'ами")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	o := opts{
		fromTopic:   *from,
		toTopic:     *to,
		since:       *since,
		errorClass:  *errorClass,
		dryRun:      *dryRun,
		idle:        *idle,
		groupSuffix: *groupSuffix,
	}

	if err := run(rootCtx, o); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("replay failed", "err", err)
		os.Exit(1)
	}
}

type opts struct {
	fromTopic   string
	toTopic     string
	since       time.Duration
	errorClass  string
	dryRun      bool
	idle        time.Duration
	groupSuffix string
}

type counters struct {
	read       atomic.Int64
	skipped    atomic.Int64
	matched    atomic.Int64
	replayed   atomic.Int64
	failedSend atomic.Int64
}

func run(ctx context.Context, o opts) error {
	cutoff := time.Now().Add(-o.since)
	group := defaultGroupPrefix + "-" + o.groupSuffix

	consumeOpts := []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(o.fromTopic),
		kgo.ClientID(defaultClientID),
		kgo.DisableAutoCommit(),
		// Фильтр по времени работает с earliest: мы должны увидеть и
		// старые записи, чтобы пропустить их по condition `record.Time < cutoff`.
		// Альтернатива — kadm.OffsetsForTimes по cutoff, но для лекции
		// проще-и-понятнее так.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}
	cl, err := kafka.NewClient(consumeOpts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	dryLabel := ""
	if o.dryRun {
		dryLabel = " [DRY-RUN]"
	}
	fmt.Printf("replay%s: %s → %s\n", dryLabel, o.fromTopic, o.toTopic)
	fmt.Printf("           since=%s (cutoff=%s)\n", o.since, cutoff.UTC().Format(time.RFC3339))
	if o.errorClass != "" {
		fmt.Printf("           filter: error.class == %q\n", o.errorClass)
	} else {
		fmt.Printf("           filter: (нет, переотправляем всё подходящее по времени)\n")
	}
	fmt.Printf("           group=%s idle=%s\n\n", group, o.idle)

	c := &counters{}
	idleTimer := time.NewTimer(o.idle)
	defer idleTimer.Stop()

	for {
		// Сбрасываем idle-таймер: каждый успешный poll возвращает данные
		// → значит, дочитываем; пустой fetch → таймер тикает.
		// Реализуем «idle» через PollFetches с коротким poll'ом или select.
		pollCtx, pollCancel := context.WithTimeout(ctx, o.idle)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					if errors.Is(ctx.Err(), context.Canceled) {
						printSummary(c)
						return nil
					}
					// pollCtx истёк — это idle, не реальная отмена.
					printSummary(c)
					fmt.Printf("idle %s — DLQ дочитан, выходим.\n", o.idle)
					return nil
				}
				if errors.Is(e.Err, context.DeadlineExceeded) {
					printSummary(c)
					fmt.Printf("idle %s — DLQ дочитан, выходим.\n", o.idle)
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

		toSend := make([]*kgo.Record, 0, len(batch))
		for _, r := range batch {
			c.read.Add(1)

			if r.Timestamp.Before(cutoff) {
				c.skipped.Add(1)
				continue
			}
			if o.errorClass != "" {
				ec := headerValue(r.Headers, "error.class")
				if ec != o.errorClass {
					c.skipped.Add(1)
					continue
				}
			}
			c.matched.Add(1)

			fmt.Printf("MATCH p=%d off=%d key=%s ts=%s\n",
				r.Partition, r.Offset, string(r.Key),
				r.Timestamp.UTC().Format(time.RFC3339))

			if o.dryRun {
				continue
			}

			toSend = append(toSend, replayRecord(r, o.toTopic))
		}

		if !o.dryRun && len(toSend) > 0 {
			produceCtx, produceCancel := context.WithTimeout(ctx, 30*time.Second)
			results := cl.ProduceSync(produceCtx, toSend...)
			produceCancel()
			var sendErrs int
			for _, res := range results {
				if res.Err != nil {
					c.failedSend.Add(1)
					sendErrs++
					fmt.Fprintf(os.Stderr, "produce error: %v\n", res.Err)
				} else {
					c.replayed.Add(1)
				}
			}
			// Если хоть одна запись не уехала — не двигаем offset, иначе
			// при рестарте мы пропустим её навсегда (она в DLQ останется,
			// но replay-cli уже не вернётся к ней).
			if sendErrs > 0 {
				printSummary(c)
				return fmt.Errorf("%d записей не удалось отправить — offset не закоммичен, можно перезапустить replay", sendErrs)
			}
		}

		// На dry-run мы ничего не публикуем — значит и offset двигать не имеем
		// права. Иначе повторный запуск с тем же -group-suffix начнёт после
		// «просмотренных» записей и реальный replay увидит пустоту.
		if o.dryRun {
			continue
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
	}
}

// replayRecord перепаковывает DLQ-запись в новый record для основного топика.
// Сохраняем все headers, обнуляем retry.count (новая попытка пайплайна
// начинается с нуля), добавляем replay.from-dlq и replay.timestamp —
// чтобы при повторном падении в DLQ было видно, что это replay.
func replayRecord(r *kgo.Record, toTopic string) *kgo.Record {
	headers := append([]kgo.RecordHeader(nil), r.Headers...)
	headers = setHeader(headers, "retry.count", "0")
	headers = setHeader(headers, "replay.from-dlq", r.Topic+"/"+strconv.Itoa(int(r.Partition))+"/"+strconv.FormatInt(r.Offset, 10))
	headers = setHeader(headers, "replay.timestamp", time.Now().UTC().Format(time.RFC3339Nano))
	return &kgo.Record{
		Topic:   toTopic,
		Key:     r.Key,
		Value:   r.Value,
		Headers: headers,
	}
}

func setHeader(hs []kgo.RecordHeader, key, value string) []kgo.RecordHeader {
	for i := range hs {
		if hs[i].Key == key {
			hs[i].Value = []byte(value)
			return hs
		}
	}
	return append(hs, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func headerValue(hs []kgo.RecordHeader, key string) string {
	for _, h := range hs {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func printSummary(c *counters) {
	fmt.Printf("\nrep summary: read=%d skipped=%d matched=%d replayed=%d failed=%d\n",
		c.read.Load(), c.skipped.Load(), c.matched.Load(), c.replayed.Load(), c.failedSend.Load())
}
