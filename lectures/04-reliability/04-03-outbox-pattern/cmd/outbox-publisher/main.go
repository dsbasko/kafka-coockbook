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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultDSN = "postgres://lecture:lecture@localhost:15433/lecture_04_03?sslmode=disable"
)

const fetchBatchSQL = `
SELECT id, aggregate_id, topic, payload::text
  FROM outbox
 WHERE published_at IS NULL
 ORDER BY id
 LIMIT $1
 FOR UPDATE SKIP LOCKED
`

const markPublishedSQL = `
UPDATE outbox
   SET published_at = NOW()
 WHERE id = ANY($1::bigint[])
`

func main() {
	logger := log.New()

	batchSize := flag.Int("batch-size", 100, "сколько outbox-записей забирать за итерацию")
	pollInterval := flag.Duration("poll-interval", 500*time.Millisecond, "пауза между итерациями, когда outbox пуст")
	idle := flag.Duration("idle", 8*time.Second, "выйти, если за это время не опубликовано ни одной записи")
	crashAfterProduce := flag.Bool("crash-after-produce", false, "имитация падения: упасть после ProduceSync, ДО UPDATE published_at")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	dsn := config.EnvOr("DATABASE_URL", defaultDSN)

	if err := run(rootCtx, runOpts{
		dsn:               dsn,
		batchSize:         *batchSize,
		pollInterval:      *pollInterval,
		idle:              *idle,
		crashAfterProduce: *crashAfterProduce,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("outbox-publisher failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	dsn               string
	batchSize         int
	pollInterval      time.Duration
	idle              time.Duration
	crashAfterProduce bool
}

type outboxRow struct {
	id          int64
	aggregateID string
	topic       string
	payload     string
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

	cl, err := kafka.NewClient(
		kgo.ClientID("lecture-04-03-publisher"),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("outbox-publisher запущен: batch=%d poll=%s crash-after-produce=%v\n",
		o.batchSize, o.pollInterval, o.crashAfterProduce)
	fmt.Println("забираем неопубликованные записи через FOR UPDATE SKIP LOCKED.")
	fmt.Println()

	var published atomic.Int64
	lastWork := time.Now()

	for {
		if err := ctx.Err(); err != nil {
			fmt.Printf("\nостанов по сигналу. published=%d\n", published.Load())
			return nil
		}

		n, err := publishOnce(ctx, pool, cl, o)
		if err != nil {
			return err
		}

		if n > 0 {
			published.Add(int64(n))
			lastWork = time.Now()
			continue
		}

		if time.Since(lastWork) > o.idle {
			fmt.Printf("\nidle %s — outbox пуст, выходим. published=%d\n", o.idle, published.Load())
			return nil
		}

		select {
		case <-ctx.Done():
		case <-time.After(o.pollInterval):
		}
	}
}

func publishOnce(ctx context.Context, pool *pgxpool.Pool, cl *kgo.Client, o runOpts) (int, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("BeginTx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, fetchBatchSQL, o.batchSize)
	if err != nil {
		return 0, fmt.Errorf("SELECT outbox: %w", err)
	}

	batch := make([]outboxRow, 0, o.batchSize)
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.id, &r.aggregateID, &r.topic, &r.payload); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan: %w", err)
		}
		batch = append(batch, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("rows.Err: %w", err)
	}

	if len(batch) == 0 {
		return 0, nil
	}

	records := make([]*kgo.Record, len(batch))
	for i, r := range batch {
		records[i] = &kgo.Record{
			Topic: r.topic,
			Key:   []byte(r.aggregateID),
			Value: []byte(r.payload),
			Headers: []kgo.RecordHeader{
				{Key: "outbox-id", Value: []byte(strconv.FormatInt(r.id, 10))},
				{Key: "aggregate-id", Value: []byte(r.aggregateID)},
			},
		}
	}

	results := cl.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		return 0, fmt.Errorf("ProduceSync: %w", err)
	}

	for i, r := range batch {
		fmt.Printf("PUB id=%d agg=%s p=%d off=%d\n",
			r.id, r.aggregateID, records[i].Partition, records[i].Offset)
	}

	if o.crashAfterProduce {
		fmt.Fprintf(os.Stderr, "\n=== CRASH SIMULATION после ProduceSync, ДО UPDATE published_at: os.Exit(2) ===\n")
		fmt.Fprintf(os.Stderr, "%d записей улетели в Kafka, но в outbox они всё ещё published_at IS NULL\n", len(batch))
		os.Exit(2)
	}

	ids := make([]int64, len(batch))
	for i, r := range batch {
		ids[i] = r.id
	}

	tag, err := tx.Exec(ctx, markPublishedSQL, ids)
	if err != nil {
		return 0, fmt.Errorf("UPDATE outbox: %w", err)
	}
	if int(tag.RowsAffected()) != len(batch) {
		return 0, fmt.Errorf("UPDATE outbox: ожидали %d, обновили %d", len(batch), tag.RowsAffected())
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("Commit: %w", err)
	}

	return len(batch), nil
}
