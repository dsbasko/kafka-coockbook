// db-loader — генератор изменений в Postgres'е для лекции 07-04.
//
// Что делает: вставляет N пользователей, потом обновляет половину из них,
// потом удаляет четверть. Параллельно (по флагу -outbox) кладёт в таблицу
// outbox доменные события в одной транзакции с изменением users — это нужно,
// чтобы посмотреть, как Debezium + EventRouter SMT раскладывают строки
// по топикам events.<aggregate_type>.
//
// Каждое INSERT/UPDATE/DELETE в users порождает событие в WAL'е, которое
// Debezium читает через replication slot и пишет в `cdc.public.users`. По
// умолчанию ключ Kafka-сообщения — primary key (поле id), value — JSON-объект
// со структурой {before, after, op, source, ts_ms}. Колонка op принимает
// 'c' для INSERT, 'u' для UPDATE, 'd' для DELETE, 'r' для строк из snapshot'а.
//
// Запуск:
//
//	make up && make db-init && make connector-create-source
//	make run-loader COUNT=20
//	# в другом терминале:
//	make run-cdc-consumer
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const defaultCount = 20

func main() {
	logger := log.New()

	count := flag.Int("count", defaultCount, "сколько пользователей создать (apex нагрузки)")
	withOutbox := flag.Bool("outbox", true, "класть события в outbox в одной TX с изменениями")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	dsn := config.EnvOr("PG_DSN",
		"postgres://lecture:lecture@localhost:15437/lecture_07_04?sslmode=disable")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		logger.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := run(ctx, pool, *count, *withOutbox); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("loader failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, pool *pgxpool.Pool, count int, withOutbox bool) error {
	fmt.Printf("\n=== INSERT %d пользователей ===\n", count)
	// Стартовать выше текущего MAX(id), иначе повторный запуск ловит PK violation:
	// одного `time.Now().Unix() % 1_000_000` мало, чтобы развести соседние запуски.
	var startID int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(MAX(id), 0) + 1 FROM users`).Scan(&startID); err != nil {
		return fmt.Errorf("seed startID: %w", err)
	}
	ids := make([]int64, 0, count)
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := startID + int64(i)
		ids = append(ids, id)
		email := fmt.Sprintf("user%d@example.com", id)
		fullName := fmt.Sprintf("User %d", id)

		if err := insertUser(ctx, pool, id, email, fullName, withOutbox); err != nil {
			return fmt.Errorf("insert id=%d: %w", id, err)
		}
		fmt.Printf("  INSERT id=%d email=%s\n", id, email)
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("\n=== UPDATE половины пользователей ===\n")
	for i := 0; i < count/2; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := ids[rand.IntN(len(ids))]
		newStatus := []string{"active", "blocked", "suspended", "vip"}[rand.IntN(4)]
		if err := updateUser(ctx, pool, id, newStatus, withOutbox); err != nil {
			return fmt.Errorf("update id=%d: %w", id, err)
		}
		fmt.Printf("  UPDATE id=%d status=%s\n", id, newStatus)
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("\n=== DELETE четверти пользователей ===\n")
	for i := 0; i < count/4; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := ids[i*4%len(ids)]
		if err := deleteUser(ctx, pool, id, withOutbox); err != nil {
			return fmt.Errorf("delete id=%d: %w", id, err)
		}
		fmt.Printf("  DELETE id=%d\n", id)
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("\nготово. Debezium увидит каждую операцию в WAL'е.")
	if withOutbox {
		fmt.Println("В outbox-роутер должны прилететь события в топики events.user.*")
	}
	return nil
}

func insertUser(ctx context.Context, pool *pgxpool.Pool, id int64, email, fullName string, withOutbox bool) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO users (id, email, full_name, status, updated_at)
			VALUES ($1, $2, $3, 'active', NOW())
		`, id, email, fullName)
		if err != nil {
			return err
		}
		if !withOutbox {
			return nil
		}
		payload := fmt.Sprintf(`{"id":%d,"email":%q,"full_name":%q}`, id, email, fullName)
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox (id, aggregate_type, aggregate_id, type, payload)
			VALUES ($1, 'user', $2, 'user.created', $3::jsonb)
		`, uuid.New(), fmt.Sprintf("%d", id), payload)
		return err
	})
}

func updateUser(ctx context.Context, pool *pgxpool.Pool, id int64, status string, withOutbox bool) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE users SET status = $2, updated_at = NOW() WHERE id = $1
		`, id, status)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		if !withOutbox {
			return nil
		}
		payload := fmt.Sprintf(`{"id":%d,"status":%q}`, id, status)
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox (id, aggregate_type, aggregate_id, type, payload)
			VALUES ($1, 'user', $2, 'user.status_changed', $3::jsonb)
		`, uuid.New(), fmt.Sprintf("%d", id), payload)
		return err
	})
}

func deleteUser(ctx context.Context, pool *pgxpool.Pool, id int64, withOutbox bool) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM users WHERE id = $1`, id)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		if !withOutbox {
			return nil
		}
		payload := fmt.Sprintf(`{"id":%d}`, id)
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox (id, aggregate_type, aggregate_id, type, payload)
			VALUES ($1, 'user', $2, 'user.deleted', $3::jsonb)
		`, uuid.New(), fmt.Sprintf("%d", id), payload)
		return err
	})
}
