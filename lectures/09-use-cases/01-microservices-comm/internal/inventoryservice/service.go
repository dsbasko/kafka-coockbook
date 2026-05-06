// Package inventoryservice — consumer на order.created.
//
// Запускается в нескольких копиях с одинаковым Group и разными NodeID.
// Партиции делятся между нодами через consumer group, каждое сообщение
// обрабатывает ровно одна нода. При падении ноды — ребаланс, оставшиеся
// подбирают её партиции.
//
// Гарантии:
//   - at-least-once на стороне Kafka (manual commit после успеха в БД);
//   - dedup по (consumer, outbox_id) + UNIQUE на (order_id) → effectively-once
//     на уровне БД-стейта.
package inventoryservice

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/01-microservices-comm/gen/orders/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
)

const consumerName = "inventory"

const dedupSQL = `
INSERT INTO processed_events (consumer, outbox_id)
VALUES ($1, $2)
ON CONFLICT (consumer, outbox_id) DO NOTHING
`

const reserveSQL = `
INSERT INTO inventory_reservations (order_id, customer_id, amount_cents, reserved_by)
VALUES ($1, $2, $3, $4)
ON CONFLICT (order_id) DO NOTHING
`

type RunOpts struct {
	NodeID string
	Topic  string
	Group  string
	DSN    string
}

func Run(ctx context.Context, o RunOpts) error {
	pool, err := pgxpool.New(ctx, o.DSN)
	if err != nil {
		return fmt.Errorf("pgxpool.New: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("pg ping: %w", err)
	}

	cl, err := kafka.NewClient(
		kgo.ConsumerGroup(o.Group),
		kgo.ConsumeTopics(o.Topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ClientID("usecase-09-01-"+o.NodeID),
		kgo.InstanceID(o.NodeID),
		kgo.SessionTimeout(15*time.Second),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("[%s] inventory-service: topic=%q group=%q\n", o.NodeID, o.Topic, o.Group)

	var processed atomic.Int64
	var reserved atomic.Int64
	var skipped atomic.Int64

	for {
		pollCtx, pollCancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}

		if err := ctx.Err(); err != nil {
			fmt.Printf("[%s] inventory-service остановлен. processed=%d reserved=%d skipped=%d\n",
				o.NodeID, processed.Load(), reserved.Load(), skipped.Load())
			return nil
		}

		batch := make([]*kgo.Record, 0)
		fetches.EachRecord(func(r *kgo.Record) { batch = append(batch, r) })
		if len(batch) == 0 {
			continue
		}

		for _, r := range batch {
			outboxID, err := outboxIDFrom(r)
			if err != nil {
				return fmt.Errorf("p=%d off=%d: %w", r.Partition, r.Offset, err)
			}

			var evt ordersv1.OrderCreated
			if err := proto.Unmarshal(r.Value, &evt); err != nil {
				return fmt.Errorf("unmarshal p=%d off=%d: %w", r.Partition, r.Offset, err)
			}

			// Дедуп-гейт и бизнес-вставка должны быть атомарны: иначе
			// между ними может случиться краш и при рестарте гейт скажет
			// «уже обработано», а резерва так и не будет.
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
				if _, err := tx.Exec(ctx, reserveSQL,
					evt.GetId(), evt.GetCustomerId(), evt.GetAmountCents(), o.NodeID,
				); err != nil {
					return fmt.Errorf("reserve: %w", err)
				}
				return nil
			})
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("outbox-id=%d order=%s: %w", outboxID, evt.GetId(), err)
			}
			processed.Add(1)
			if !newRow {
				skipped.Add(1)
				continue
			}
			reserved.Add(1)
		}

		commitCtx, commitCancel := context.WithTimeout(ctx, 10*time.Second)
		err := cl.CommitRecords(commitCtx, batch...)
		commitCancel()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
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
