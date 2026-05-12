package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic    = "lecture-03-03-events"
	defaultGroup    = "lecture-03-03-idempotent"
	defaultDSN      = "postgres://lecture:lecture@localhost:15432/lecture_03_03?sslmode=disable"
	defaultWorkTime = 200 * time.Millisecond
)

const insertSQL = `
INSERT INTO messages (topic, partition, "offset", payload, processed_at)
VALUES ($1, $2, $3, $4, NOW())
ON CONFLICT (topic, partition, "offset") DO NOTHING
`

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик для чтения")
	group := flag.String("group", defaultGroup, "group.id")
	workDelay := flag.Duration("work-delay", defaultWorkTime, "имитация работы — sleep на каждое сообщение")
	crashAfter := flag.Int("crash-after", 0,
		"если > 0, после стольких ОБРАБОТАННЫХ записей делаем os.Exit(1) до commit'а offset'а")
	fromStart := flag.Bool("from-start", true, "при первом старте группы читать с earliest")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	dsn := config.EnvOr("DATABASE_URL", defaultDSN)

	if err := run(rootCtx, runOpts{
		topic:      *topic,
		group:      *group,
		dsn:        dsn,
		workDelay:  *workDelay,
		crashAfter: *crashAfter,
		fromStart:  *fromStart,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("idempotent-pg failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic      string
	group      string
	dsn        string
	workDelay  time.Duration
	crashAfter int
	fromStart  bool
}

func run(ctx context.Context, o runOpts) error {
	pool, err := pgxpool.New(ctx, o.dsn)
	if err != nil {
		return fmt.Errorf("pgxpool.New: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("pg ping: %w", err)
	}

	opts := []kgo.Opt{
		kgo.ConsumerGroup(o.group),
		kgo.ConsumeTopics(o.topic),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-03-03-idempotent"),
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

	fmt.Printf("idempotent-pg запущен: topic=%q group=%q work-delay=%s crash-after=%d\n",
		o.topic, o.group, o.workDelay, o.crashAfter)
	fmt.Println("читаем; insert идёт ПЕРЕД commit'ом offset'а. Ctrl+C — выход.")
	fmt.Println()

	var processed atomic.Int64
	var inserted atomic.Int64
	var skipped atomic.Int64

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					fmt.Printf("\nостановлен по сигналу. processed=%d inserted=%d skipped=%d.\n",
						processed.Load(), inserted.Load(), skipped.Load())
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
			time.Sleep(o.workDelay)

			tag, err := pool.Exec(ctx, insertSQL,
				r.Topic, int32(r.Partition), r.Offset, string(r.Value))
			if err != nil {
				return fmt.Errorf("insert p=%d off=%d: %w", r.Partition, r.Offset, err)
			}

			n := processed.Add(1)
			if tag.RowsAffected() == 1 {
				inserted.Add(1)
				fmt.Printf("INSERT n=%d p=%d off=%d key=%s\n", n, r.Partition, r.Offset, string(r.Key))
			} else {
				skipped.Add(1)
				fmt.Printf("DUP    n=%d p=%d off=%d key=%s (уже было — ON CONFLICT)\n",
					n, r.Partition, r.Offset, string(r.Key))
			}

			if o.crashAfter > 0 && int(n) == o.crashAfter {
				fmt.Printf("\n=== CRASH SIMULATION после %d записей: os.Exit(1) ПОСЛЕ insert'а, ДО commit'а ===\n", n)
				fmt.Printf("inserted=%d skipped=%d\n", inserted.Load(), skipped.Load())
				os.Exit(1)
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
		fmt.Printf("--- batch committed: %d записей (inserted=%d skipped=%d total) ---\n\n",
			len(batch), inserted.Load(), skipped.Load())
	}
}
