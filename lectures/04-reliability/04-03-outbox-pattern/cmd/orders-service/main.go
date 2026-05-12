package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultDSN   = "postgres://lecture:lecture@localhost:15433/lecture_04_03?sslmode=disable"
	defaultTopic = "lecture-04-03-orders"
)

const insertOrderSQL = `
INSERT INTO orders (customer_id, amount)
VALUES ($1, $2)
RETURNING id
`

const insertOutboxSQL = `
INSERT INTO outbox (aggregate_id, topic, payload)
VALUES ($1, $2, $3::jsonb)
RETURNING id
`

func main() {
	logger := log.New()

	count := flag.Int("count", 100, "сколько заказов создать")
	topic := flag.String("topic", defaultTopic, "топик, в который потом publisher отправит события")
	customer := flag.String("customer", "", "если задан — все заказы с этим customer_id (полезно для observability)")
	delay := flag.Duration("delay", 0, "пауза между заказами (для наглядности — 100ms подойдёт)")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	dsn := config.EnvOr("DATABASE_URL", defaultDSN)

	if err := run(rootCtx, runOpts{
		dsn:      dsn,
		count:    *count,
		topic:    *topic,
		customer: *customer,
		delay:    *delay,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("orders-service failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	dsn      string
	count    int
	topic    string
	customer string
	delay    time.Duration
}

type orderEvent struct {
	OrderID    int64   `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	CreatedAt  string  `json:"created_at"`
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

	fmt.Printf("orders-service: создаём %d заказов в БД (topic=%q для outbox)\n", o.count, o.topic)
	fmt.Println("orders + outbox пишутся в одной транзакции на каждый заказ.")
	fmt.Println()

	for i := 1; i <= o.count; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		customerID := o.customer
		if customerID == "" {
			customerID = fmt.Sprintf("cust-%d", rand.IntN(20))
		}
		amount := 100 + rand.Float64()*900

		orderID, outboxID, err := createOrder(ctx, pool, o.topic, customerID, amount)
		if err != nil {
			return fmt.Errorf("заказ %d: %w", i, err)
		}

		fmt.Printf("[%d/%d] order_id=%d outbox_id=%d customer=%s amount=%.2f\n",
			i, o.count, orderID, outboxID, customerID, amount)

		if o.delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(o.delay):
			}
		}
	}

	fmt.Printf("\nготово. %d заказов в orders + %d записей в outbox.\n", o.count, o.count)
	fmt.Println("дальше — make run-publisher (опубликует в Kafka).")
	return nil
}

func createOrder(ctx context.Context, pool *pgxpool.Pool, topic, customerID string, amount float64) (orderID, outboxID int64, err error) {
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, insertOrderSQL, customerID, amount).Scan(&orderID); err != nil {
			return fmt.Errorf("INSERT orders: %w", err)
		}

		evt := orderEvent{
			OrderID:    orderID,
			CustomerID: customerID,
			Amount:     amount,
			Status:     "created",
			CreatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
		}
		payload, err := json.Marshal(evt)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}

		aggregateID := fmt.Sprintf("order-%d", orderID)
		if err := tx.QueryRow(ctx, insertOutboxSQL, aggregateID, topic, string(payload)).Scan(&outboxID); err != nil {
			return fmt.Errorf("INSERT outbox: %w", err)
		}

		return nil
	})
	return
}
