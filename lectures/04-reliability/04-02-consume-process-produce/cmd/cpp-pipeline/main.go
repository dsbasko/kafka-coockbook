// cpp-pipeline — consume-process-produce пайплайн с exactly-once семантикой
// поверх kgo.GroupTransactSession.
//
// Сценарий: читаем сырые заказы из topic-input, обогащаем (mock lookup —
// добавляем поле "vip" по префиксу ключа и считаем "total"), пишем результат
// в topic-output. Commit offset'а в группе и публикация обогащённой записи —
// одна Kafka-транзакция.
//
// Что показывает демо:
//
//   - между Produce и End случается «крах» (os.Exit(1) с заданной
//     вероятностью). Незавершённая транзакция тайм-аутнется на координаторе
//     и будет отброшена. Записи попали в лог output-топика, но read_committed
//     консьюмер их не увидит — пометки commit нет.
//
//   - после рестарта group consumer вернётся на committed offset,
//     перечитает те же входные записи, выполнит обработку заново и
//     закоммитит транзакцию. На стороне output ровно одна копия каждого
//     уникального ключа.
//
// Запускать через `make run-pipeline` повторно, пока в input не кончатся
// записи. По завершении сравнить downstream count с числом seed'ов: count
// == seed → EOS работает.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTxnID  = "lecture-04-02-pipeline"
	defaultGroup  = "lecture-04-02-pipeline"
	defaultInput  = "cpp-orders"
	defaultOutput = "cpp-orders-enriched"
)

func main() {
	logger := log.New()

	txnID := flag.String("transactional-id", defaultTxnID, "TransactionalID — уникальный per-процесс/per-роль идентификатор")
	group := flag.String("group", defaultGroup, "consumer group")
	input := flag.String("input", defaultInput, "входной топик")
	output := flag.String("output", defaultOutput, "выходной топик (обогащённые записи)")
	crashProb := flag.Float64("crash-prob", 0, "вероятность аварийного os.Exit между Produce и End (0..1)")
	idle := flag.Duration("idle", 8*time.Second, "выйти, если за это время нет новых записей (>5s рекомендуется — group join занимает время)")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		txnID:     *txnID,
		group:     *group,
		input:     *input,
		output:    *output,
		crashProb: *crashProb,
		idle:      *idle,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("cpp-pipeline failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	txnID     string
	group     string
	input     string
	output    string
	crashProb float64
	idle      time.Duration
}

func run(ctx context.Context, o runOpts) error {
	bootstrap := config.EnvOr("KAFKA_BOOTSTRAP", "localhost:19092,localhost:19093,localhost:19094")
	seeds := splitSeeds(bootstrap)
	if len(seeds) == 0 {
		return fmt.Errorf("KAFKA_BOOTSTRAP is empty")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ClientID("lecture-04-02-pipeline"),
		kgo.DialTimeout(5 * time.Second),
		kgo.RequestTimeoutOverhead(10 * time.Second),
		kgo.RetryTimeout(30 * time.Second),

		kgo.TransactionalID(o.txnID),
		kgo.TransactionTimeout(60 * time.Second),

		kgo.ConsumerGroup(o.group),
		kgo.ConsumeTopics(o.input),
		// EOS-чтение: не отдавать записи из ещё не закоммиченных транзакций.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// На первом запуске группы — с начала. Дальше группа сама хранит
		// committed offset и возвращается к нему после рестарта.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}

	sess, err := kgo.NewGroupTransactSession(opts...)
	if err != nil {
		return fmt.Errorf("NewGroupTransactSession: %w", err)
	}
	defer sess.Close()

	fmt.Printf("cpp-pipeline: txn-id=%q group=%q input=%q output=%q crash-prob=%.2f\n",
		o.txnID, o.group, o.input, o.output, o.crashProb)
	fmt.Printf("idle-timeout=%s — после паузы без записей выходим. Запускай повторно, чтобы добрать остаток.\n", o.idle)
	fmt.Println()

	totalIn, totalOut, committedTxns, abortedTxns := 0, 0, 0, 0
	lastSeen := time.Now()

	for {
		if err := ctx.Err(); err != nil {
			break
		}

		pollCtx := ctx
		var pollCancel context.CancelFunc
		if o.idle > 0 {
			pollCtx, pollCancel = context.WithTimeout(ctx, o.idle)
		}
		fetches := sess.PollFetches(pollCtx)
		if pollCancel != nil {
			pollCancel()
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			done := false
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					done = true
					break
				}
				if errors.Is(e.Err, context.DeadlineExceeded) {
					if o.idle > 0 && time.Since(lastSeen) >= o.idle {
						fmt.Println()
						fmt.Printf("idle %s — больше входных записей нет, выходим.\n", o.idle)
						done = true
						break
					}
					continue
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
			if done {
				break
			}
			continue
		}

		if fetches.Empty() {
			continue
		}
		lastSeen = time.Now()

		if err := sess.Begin(); err != nil {
			return fmt.Errorf("Begin: %w", err)
		}

		batchIn, batchOut := 0, 0
		fetches.EachRecord(func(r *kgo.Record) {
			batchIn++

			enriched := enrich(r)

			sess.Produce(ctx, &kgo.Record{
				Topic: o.output,
				Key:   r.Key,
				Value: enriched,
				Headers: []kgo.RecordHeader{
					{Key: "source.topic", Value: []byte(r.Topic)},
					{Key: "source.partition", Value: []byte(fmt.Sprintf("%d", r.Partition))},
					{Key: "source.offset", Value: []byte(fmt.Sprintf("%d", r.Offset))},
				},
			}, func(_ *kgo.Record, err error) {
				if err != nil && !errors.Is(err, kgo.ErrAborting) {
					fmt.Fprintf(os.Stderr, "produce err: %v\n", err)
				}
			})
			batchOut++
		})

		// Принудительно сбрасываем буфер на брокер до решения о crash.
		// Без этого Produce — асинхронный батчинг — может не успеть
		// добраться до лога к моменту os.Exit, и read_uncommitted
		// демонстрация (видны записи аборнутых транзакций) не сработает.
		// End() сделает Flush сам, но нам он нужен ДО возможного краха.
		if err := sess.Client().Flush(ctx); err != nil {
			return fmt.Errorf("flush: %w", err)
		}

		// Симулируем падение pod'а после Flush, но до End. Это самый
		// показательный сценарий: записи уже в логе output, но без
		// commit marker'а — read_committed их не увидит, координатор по
		// таймауту эту транзакцию аборнет.
		if o.crashProb > 0 && rand.Float64() < o.crashProb && batchOut > 0 {
			fmt.Fprintf(os.Stderr, "💥 crash перед End: %d записей уже в логе output, транзакция уйдёт в abort на координаторе\n", batchOut)
			os.Exit(2)
		}

		committed, err := sess.End(ctx, kgo.TryCommit)
		if err != nil {
			return fmt.Errorf("End: %w", err)
		}

		totalIn += batchIn
		if committed {
			committedTxns++
			totalOut += batchOut
			fmt.Printf("[txn #%03d] commit ✓ in=%d out=%d (offsets и produce — атомарно)\n",
				committedTxns+abortedTxns, batchIn, batchOut)
		} else {
			abortedTxns++
			fmt.Printf("[txn #%03d] abort ✗ in=%d out=%d (rebalance/lost — повтор после рестарта)\n",
				committedTxns+abortedTxns, batchIn, batchOut)
		}
	}

	fmt.Println()
	fmt.Println("итог сессии:")
	fmt.Printf("  прочитано записей:   %d\n", totalIn)
	fmt.Printf("  отдано в %-12s %d\n", o.output+":", totalOut)
	fmt.Printf("  commit транзакций:   %d\n", committedTxns)
	fmt.Printf("  abort транзакций:    %d\n", abortedTxns)
	return nil
}

// enrich — mock-обогащение: добавляем поле vip по префиксу key,
// фиксируем processed_at и source. Тяжёлой логики намеренно нет — лекция
// про транзакцию вокруг шага, а не про сам шаг.
func enrich(r *kgo.Record) []byte {
	vip := strings.HasPrefix(string(r.Key), "vip-")
	return fmt.Appendf(nil,
		`{"key":%q,"src_value":%q,"vip":%t,"src_partition":%d,"src_offset":%d,"processed_at":%q}`,
		string(r.Key), string(r.Value), vip, r.Partition, r.Offset,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
}

func splitSeeds(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
