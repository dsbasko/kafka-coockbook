// auto-commit — консьюмер с включённым auto-commit и искусственным «крашем»
// в середине обработки. Цель — на пальцах показать, что auto-commit
// коммитит то, что было *получено* через PollFetches, а не то, что было
// реально обработано приложением. Если процесс упал между двумя
// auto-commit'ами — закоммиченный offset не двинулся, и при рестарте
// мы прочитаем те же сообщения снова. Это даёт дубли (at-least-once
// в лучшем случае, иногда хуже — если auto-commit успел сработать,
// но обработка не закончилась).
//
// Что делает программа:
//   1. Подписывается на топик в группе, читает в цикле.
//   2. На каждое сообщение «работает» — спит -work-delay миллисекунд
//      и дописывает запись в processed-auto.log
//      (формат: partition,offset,key,value\n).
//   3. Если -crash-after > 0 — после стольких ОБРАБОТАННЫХ записей делает
//      os.Exit(1) без Close. Это имитирует kill -9 / OOM / упавший контейнер:
//      defer'ы не выполняются, последний buffered auto-commit пропадает.
//   4. Если -crash-after = 0 — корректно завершается по SIGINT (Close
//      сольёт последний commit).
//
// Как смотреть результат:
//   - до запуска: make group-delete-auto    # сбросить committed offset
//   - запуск 1:   make run-auto CRASH=10    # читает 10, пишет лог, падает
//   - запуск 2:   make run-auto CRASH=0     # дочитывает остаток
//   - анализ:     make count-auto           # видно дубликаты по offset'ам
//
// AutoCommitInterval тут специально занижен до 2 секунд, чтобы поведение
// было предсказуемым в учебной демке. Дефолт franz-go = 5 секунд.
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
	defaultGroup    = "lecture-03-02-auto"
	defaultLogFile  = "processed-auto.log"
	defaultWorkTime = 200 * time.Millisecond
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик для чтения")
	group := flag.String("group", defaultGroup, "group.id (по умолчанию свой для каждой стратегии)")
	logFile := flag.String("log-file", defaultLogFile, "файл, в который пишем processed-записи (для подсчёта дублей)")
	workDelay := flag.Duration("work-delay", defaultWorkTime, "имитация работы — sleep на каждое сообщение")
	commitEvery := flag.Duration("commit-interval", 2*time.Second,
		"AutoCommitInterval — как часто фоновая горутина коммитит то, что было получено через PollFetches")
	crashAfter := flag.Int("crash-after", 0,
		"если > 0, после стольких ОБРАБОТАННЫХ записей делаем os.Exit(1) без Close. Имитирует kill -9.")
	fromStart := flag.Bool("from-start", true,
		"при первом старте группы читать с earliest")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topic:       *topic,
		group:       *group,
		logFile:     *logFile,
		workDelay:   *workDelay,
		commitEvery: *commitEvery,
		crashAfter:  *crashAfter,
		fromStart:   *fromStart,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("auto-commit failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic       string
	group       string
	logFile     string
	workDelay   time.Duration
	commitEvery time.Duration
	crashAfter  int
	fromStart   bool
}

func run(ctx context.Context, o runOpts) error {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(o.group),
		kgo.ConsumeTopics(o.topic),
		kgo.AutoCommitInterval(o.commitEvery),
		kgo.ClientID("lecture-03-02-auto"),
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

	fmt.Printf("auto-commit запущен: topic=%q group=%q interval=%s work-delay=%s crash-after=%d log=%s\n",
		o.topic, o.group, o.commitEvery, o.workDelay, o.crashAfter, o.logFile)
	fmt.Println("читаем; Ctrl+C — выход.")
	fmt.Println()

	var processed atomic.Int64

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					fmt.Printf("\nостановлен по сигналу. processed=%d. Close сольёт финальный commit.\n",
						processed.Load())
					return nil
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			time.Sleep(o.workDelay)
			line := fmt.Sprintf("%d,%d,%s,%s\n", r.Partition, r.Offset, string(r.Key), string(r.Value))
			_, _ = logf.WriteString(line)
			n := processed.Add(1)
			fmt.Printf("processed n=%d p=%d off=%d key=%s\n", n, r.Partition, r.Offset, string(r.Key))

			if o.crashAfter > 0 && int(n) == o.crashAfter {
				fmt.Printf("\n=== CRASH SIMULATION после %d записей: os.Exit(1) без Close ===\n", n)
				_ = logf.Sync()
				os.Exit(1)
			}
		})
	}
}
