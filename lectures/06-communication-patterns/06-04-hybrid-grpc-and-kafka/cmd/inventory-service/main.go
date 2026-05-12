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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultDSN   = "postgres://lecture:lecture@localhost:15434/lecture_06_04?sslmode=disable"
	defaultTopic = "lecture-06-04-order-created"
	defaultGroup = "lecture-06-04-inventory"
	consumerName = "inventory"
)

const dedupSQL = `
INSERT INTO processed_events (consumer, outbox_id)
VALUES ($1, $2)
ON CONFLICT (consumer, outbox_id) DO NOTHING
`

const reserveSQL = `
INSERT INTO inventory_reservations (order_id, customer_id, amount_cents)
VALUES ($1, $2, $3)
ON CONFLICT (order_id) DO NOTHING
`

type orderEvent struct {
	ID          string `json:"id"`
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
	TraceID     string `json:"trace_id,omitempty"`
	TenantID    string `json:"tenant_id,omitempty"`
}

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик с order.created")
	group := flag.String("group", defaultGroup, "consumer group для inventory")
	idle := flag.Duration("idle", 0, "выйти после idle без записей (0 — никогда)")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	dsn := config.EnvOr("DATABASE_URL", defaultDSN)

	if err := run(ctx, runOpts{
		topic: *topic,
		group: *group,
		dsn:   dsn,
		idle:  *idle,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("inventory-service failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic string
	group string
	dsn   string
	idle  time.Duration
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
		kgo.ConsumerGroup(o.group),
		kgo.ConsumeTopics(o.topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ClientID("lecture-06-04-inventory"),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("inventory-service запущен: topic=%q group=%q\n", o.topic, o.group)
	fmt.Println("резервирует под order.created в inventory_reservations.")

	var processed atomic.Int64
	var reserved atomic.Int64
	var skipped atomic.Int64
	lastWork := time.Now()

	for {
		pollCtx, pollCancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
					if o.idle > 0 && time.Since(lastWork) > o.idle {
						fmt.Printf("\nidle %s — выходим. processed=%d reserved=%d skipped=%d\n",
							o.idle, processed.Load(), reserved.Load(), skipped.Load())
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
			if o.idle > 0 && time.Since(lastWork) > o.idle {
				fmt.Printf("\nidle %s — выходим. processed=%d reserved=%d skipped=%d\n",
					o.idle, processed.Load(), reserved.Load(), skipped.Load())
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

			var evt orderEvent
			if err := json.Unmarshal(r.Value, &evt); err != nil {
				return fmt.Errorf("unmarshal p=%d off=%d: %w", r.Partition, r.Offset, err)
			}

			var newRow bool
			err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				tag, err := tx.Exec(ctx, dedupSQL, consumerName, outboxID)
				if err != nil {
					return fmt.Errorf("dedup: %w", err)
				}
				if tag.RowsAffected() == 0 {
					return nil
				}
				newRow = true
				if _, err := tx.Exec(ctx, reserveSQL, evt.ID, evt.CustomerID, evt.AmountCents); err != nil {
					return fmt.Errorf("reserve: %w", err)
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("outbox-id=%d order=%s: %w", outboxID, evt.ID, err)
			}
			n := processed.Add(1)
			if !newRow {
				skipped.Add(1)
				fmt.Printf("DUP    n=%d outbox-id=%d p=%d off=%d (already processed by inventory)\n",
					n, outboxID, r.Partition, r.Offset)
				continue
			}
			reserved.Add(1)

			fmt.Printf("RESERVE n=%d order=%s customer=%s amount=%d %s trace=%s\n",
				n, evt.ID, evt.CustomerID, evt.AmountCents, evt.Currency, evt.TraceID)
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
