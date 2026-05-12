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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultDSN   = "postgres://lecture:lecture@localhost:15433/lecture_04_03?sslmode=disable"
	defaultTopic = "lecture-04-03-orders"
	defaultGroup = "lecture-04-03-orders-consumer"
)

const dedupSQL = `
INSERT INTO processed_outbox_ids (outbox_id)
VALUES ($1)
ON CONFLICT (outbox_id) DO NOTHING
`

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик для чтения")
	group := flag.String("group", defaultGroup, "group.id")
	idle := flag.Duration("idle", 8*time.Second, "выйти, если за это время нет новых записей")
	fromStart := flag.Bool("from-start", true, "при первом старте группы читать с earliest")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	dsn := config.EnvOr("DATABASE_URL", defaultDSN)

	if err := run(rootCtx, runOpts{
		topic:     *topic,
		group:     *group,
		dsn:       dsn,
		idle:      *idle,
		fromStart: *fromStart,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("orders-consumer failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic     string
	group     string
	dsn       string
	idle      time.Duration
	fromStart bool
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
		kgo.ClientID("lecture-04-03-consumer"),
	}
	if o.fromStart {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	cl, err := kafka.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("orders-consumer запущен: topic=%q group=%q idle=%s\n", o.topic, o.group, o.idle)
	fmt.Println("dedup по outbox.id из header'а. Ctrl+C — выход.")
	fmt.Println()

	var processed atomic.Int64
	var inserted atomic.Int64
	var skipped atomic.Int64
	var lastWork = time.Now()

	for {
		pollCtx, pollCancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
					if time.Since(lastWork) > o.idle {
						fmt.Printf("\nidle %s — больше нет записей. processed=%d inserted=%d skipped=%d\n",
							o.idle, processed.Load(), inserted.Load(), skipped.Load())
						return nil
					}
					continue
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}

		batch := make([]*kgo.Record, 0)
		fetches.EachRecord(func(r *kgo.Record) { batch = append(batch, r) })
		if len(batch) == 0 {
			if time.Since(lastWork) > o.idle {
				fmt.Printf("\nidle %s — больше нет записей. processed=%d inserted=%d skipped=%d\n",
					o.idle, processed.Load(), inserted.Load(), skipped.Load())
				return nil
			}
			continue
		}

		lastWork = time.Now()

		for _, r := range batch {
			outboxID, err := outboxIDFrom(r)
			if err != nil {
				return fmt.Errorf("p=%d off=%d: %w", r.Partition, r.Offset, err)
			}

			tag, err := pool.Exec(ctx, dedupSQL, outboxID)
			if err != nil {
				return fmt.Errorf("dedup outbox-id=%d: %w", outboxID, err)
			}

			n := processed.Add(1)
			if tag.RowsAffected() == 1 {
				inserted.Add(1)
				fmt.Printf("INSERT n=%d outbox-id=%d p=%d off=%d key=%s\n",
					n, outboxID, r.Partition, r.Offset, string(r.Key))
			} else {
				skipped.Add(1)
				fmt.Printf("DUP    n=%d outbox-id=%d p=%d off=%d key=%s (уже видели)\n",
					n, outboxID, r.Partition, r.Offset, string(r.Key))
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

func outboxIDFrom(r *kgo.Record) (int64, error) {
	for _, h := range r.Headers {
		if h.Key == "outbox-id" {
			id, err := strconv.ParseInt(string(h.Value), 10, 64)
			if err != nil {
				return 0, fmt.Errorf("плохой outbox-id %q: %w", string(h.Value), err)
			}
			return id, nil
		}
	}
	return 0, errors.New("в записи нет header'а outbox-id")
}
