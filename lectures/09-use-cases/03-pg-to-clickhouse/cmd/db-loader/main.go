// db-loader — генератор INSERT/UPDATE/DELETE-нагрузки в Postgres для use case
// 09-03. Параллельно по таблицам users, orders, events. Каждое изменение —
// событие в WAL, которое Debezium читает через replication slot.
//
// Цели: показать, что снепшот покрывает initial-bulk, а live-стриминг — все
// последующие изменения. Числа N (по дефолту 50) подбираются для скорости —
// integration_test использует свой генератор с большими объёмами.
package main

import (
	"context"
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

func main() {
	logger := log.New()

	count := flag.Int("count", 50, "сколько строк создать в каждой таблице")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	dsn := config.EnvOr("PG_DSN",
		"postgres://usecase:usecase@localhost:15442/usecase_09_03?sslmode=disable")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		logger.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := loadAll(ctx, pool, *count); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("loader failed", "err", err)
		os.Exit(1)
	}
}

func loadAll(ctx context.Context, pool *pgxpool.Pool, count int) error {
	startUserID, err := nextID(ctx, pool, "users")
	if err != nil {
		return err
	}
	startOrderID, err := nextID(ctx, pool, "orders")
	if err != nil {
		return err
	}
	startEventID, err := nextID(ctx, pool, "events")
	if err != nil {
		return err
	}

	fmt.Printf("\n=== INSERT %d users / %d orders / %d events ===\n", count, count, count)
	for i := 0; i < count; i++ {
		uid := startUserID + int64(i)
		oid := startOrderID + int64(i)
		eid := startEventID + int64(i)
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `
				INSERT INTO users (id, email, phone, full_name, birth_date, country)
				VALUES ($1, $2, $3, $4, $5, $6)
			`,
				uid,
				fmt.Sprintf("user%d@example.com", uid),
				fmt.Sprintf("+7900%07d", uid%10_000_000),
				fmt.Sprintf("User%d Lastname%d", uid, uid),
				time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC),
				[]string{"RU", "BY", "KZ"}[int(uid)%3],
			); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO orders (id, user_id, amount_cents, currency, status, notes)
				VALUES ($1, $2, $3, 'RUB', 'new', $4)
			`,
				oid, uid, int64(100+rand.IntN(900_000)),
				fmt.Sprintf("private note for user %d", uid),
			); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `
				INSERT INTO events (id, user_id, event_type, ip_address, user_agent)
				VALUES ($1, $2, 'signup', '10.0.0.1', 'Mozilla/5.0 ...')
			`, eid, uid)
			return err
		})
		if err != nil {
			return fmt.Errorf("insert i=%d: %w", i, err)
		}
		if i > 0 && i%50 == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}

	fmt.Println("\n=== UPDATE половины пользователей (status) ===")
	for i := 0; i < count/2; i++ {
		uid := startUserID + int64(rand.IntN(count))
		_, err := pool.Exec(ctx, `
			UPDATE orders SET status = 'paid' WHERE user_id = $1 AND status = 'new'
		`, uid)
		if err != nil {
			return err
		}
	}

	fmt.Println("\n=== DELETE четверти событий ===")
	for i := 0; i < count/4; i++ {
		eid := startEventID + int64(i)
		_, err := pool.Exec(ctx, `DELETE FROM events WHERE id = $1`, eid)
		if err != nil {
			return err
		}
	}

	fmt.Println("\nготово. Debezium увидит каждую операцию и проставит её в cdc.public.*.")
	return nil
}

func nextID(ctx context.Context, pool *pgxpool.Pool, table string) (int64, error) {
	var v int64
	err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) + 1 FROM %s`, table)).Scan(&v)
	return v, err
}
