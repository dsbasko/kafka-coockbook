// manual-async — гибридный режим: AutoCommitMarks + явный MarkCommitRecords
// после каждой обработанной записи. Фоновая горутина auto-commit'ит только
// то, что мы пометили. Дополнительно — пинаем CommitMarkedOffsets вручную
// между батчами, чтобы дать гарантию: «по выходу из батча всё, что я
// пометил, ушло на брокер».
//
// Зачем так:
//   - дешевле, чем sync-commit на каждый батч — фоновый flush идёт
//     параллельно с обработкой следующих сообщений;
//   - честнее, чем чистый auto-commit — коммитятся только те записи,
//     которые приложение реально пометило как «обработана»;
//   - проще, чем самому таскать map[topic]map[partition]offset для
//     CommitOffsetsSync — franz-go держит state внутри.
//
// Что делает программа:
//   1. AutoCommitMarks() + AutoCommitInterval(commit-every) — пометить
//      mark-commit можно, обычный auto-commit на «что было поллено» —
//      выключен.
//   2. Каждый PollFetches возвращает батч. Для каждой записи: спим,
//      пишем в processed-async.log, вызываем MarkCommitRecords.
//   3. После батча зовём CommitMarkedOffsets — синхронный поход на
//      брокер за всё, что было помечено к этому моменту.
//   4. -crash-after симулирует падение между mark и commit. Тут как раз
//      есть интересный «разрыв»: помеченные, но ещё не закоммиченные
//      offset'ы теряются и при рестарте даю те же записи как дубли.
//
// Дубли тут возможны, но окно меньше: только то, что пометили в
// последнем интервале commit-every. С -commit-every=200ms это десятки
// миллисекунд работы, а не весь батч.
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
	defaultGroup    = "lecture-03-02-async"
	defaultLogFile  = "processed-async.log"
	defaultWorkTime = 200 * time.Millisecond
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик для чтения")
	group := flag.String("group", defaultGroup, "group.id")
	logFile := flag.String("log-file", defaultLogFile, "файл для processed-записей")
	workDelay := flag.Duration("work-delay", defaultWorkTime, "имитация работы — sleep на каждое сообщение")
	commitEvery := flag.Duration("commit-every", 500*time.Millisecond,
		"AutoCommitInterval для AutoCommitMarks — фоновый flush mark'нутых offset'ов")
	crashAfter := flag.Int("crash-after", 0,
		"если > 0, после стольких MARK'нутых записей делаем os.Exit(1) до явного CommitMarkedOffsets")
	fromStart := flag.Bool("from-start", true, "при первом старте группы читать с earliest")
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
		logger.Error("manual-async failed", "err", err)
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
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(o.commitEvery),
		kgo.ClientID("lecture-03-02-async"),
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

	fmt.Printf("manual-async запущен: topic=%q group=%q work-delay=%s commit-every=%s crash-after=%d log=%s\n",
		o.topic, o.group, o.workDelay, o.commitEvery, o.crashAfter, o.logFile)
	fmt.Println("читаем; mark на каждой записи, фоновый flush раз в commit-every. Ctrl+C — выход.")
	fmt.Println()

	var processed atomic.Int64

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					// Финальный синхронный commit — вытолкнем то, что
					// успело пометиться, но ещё не было flush'нуто фоном.
					flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
					_ = cl.CommitMarkedOffsets(flushCtx)
					flushCancel()
					fmt.Printf("\nостановлен по сигналу. processed=%d. финальный flush сделан.\n",
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
			cl.MarkCommitRecords(r)
			n := processed.Add(1)
			fmt.Printf("marked n=%d p=%d off=%d key=%s\n", n, r.Partition, r.Offset, string(r.Key))

			if o.crashAfter > 0 && int(n) == o.crashAfter {
				fmt.Printf("\n=== CRASH SIMULATION после %d marks: os.Exit(1) до явного flush ===\n", n)
				_ = logf.Sync()
				os.Exit(1)
			}
		})

		// Между батчами явно пинаем flush. Фоновый коммит и так сработает,
		// но дополнительный пинок гарантирует: на выходе из этой итерации
		// то, что мы успели пометить, гарантированно ушло.
		flushCtx, flushCancel := context.WithTimeout(ctx, 5*time.Second)
		err := cl.CommitMarkedOffsets(flushCtx)
		flushCancel()
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("CommitMarkedOffsets: %w", err)
		}
	}
}
