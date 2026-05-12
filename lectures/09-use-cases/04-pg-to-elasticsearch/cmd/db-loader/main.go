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
	skipMutations := flag.Bool("skip-mutations", false, "не делать UPDATE/DELETE на втором проходе")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	dsn := config.EnvOr("PG_DSN",
		"postgres://usecase:usecase@localhost:15443/usecase_09_04?sslmode=disable")

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		logger.Error("connect postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := loadAll(ctx, pool, *count, *skipMutations); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("loader failed", "err", err)
		os.Exit(1)
	}
}

func loadAll(ctx context.Context, pool *pgxpool.Pool, count int, skipMutations bool) error {
	startProductID, err := nextID(ctx, pool, "products")
	if err != nil {
		return err
	}
	startArticleID, err := nextID(ctx, pool, "articles")
	if err != nil {
		return err
	}
	startUserID, err := nextID(ctx, pool, "users")
	if err != nil {
		return err
	}

	categories := []string{"books", "electronics", "kitchen", "garden", "toys"}
	authors := []string{"alice", "bob", "carol", "dave"}

	fmt.Printf("\n=== INSERT %d products / %d articles / %d users ===\n", count, count, count)
	for i := 0; i < count; i++ {
		pid := startProductID + int64(i)
		aid := startArticleID + int64(i)
		uid := startUserID + int64(i)
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `
				INSERT INTO products (id, sku, name, description, category, price_cents, stock)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
			`,
				pid,
				fmt.Sprintf("SKU-%07d", pid),
				fmt.Sprintf("Product %d %s", pid, randomWord()),
				fmt.Sprintf("Описание товара %d. Подходит для %s. Качество отличное.", pid, categories[int(pid)%len(categories)]),
				categories[int(pid)%len(categories)],
				int64(100+rand.IntN(990_000)),
				rand.IntN(100),
			); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO articles (id, title, body, author, tags, published_at)
				VALUES ($1, $2, $3, $4, $5, $6)
			`,
				aid,
				fmt.Sprintf("Article %d about %s", aid, randomWord()),
				fmt.Sprintf("Полный текст статьи %d. Lorem ipsum dolor sit amet, consectetur adipiscing.", aid),
				authors[int(aid)%len(authors)],
				"news,tech",
				time.Now().UTC(),
			); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `
				INSERT INTO users (id, username, full_name, bio)
				VALUES ($1, $2, $3, $4)
			`,
				uid,
				fmt.Sprintf("user_%d", uid),
				fmt.Sprintf("First%d Last%d", uid, uid),
				fmt.Sprintf("Bio for user %d. Loves Go and Kafka.", uid),
			)
			return err
		})
		if err != nil {
			return fmt.Errorf("insert i=%d: %w", i, err)
		}
		if i > 0 && i%50 == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}

	if skipMutations {
		fmt.Println("\nготово (без UPDATE/DELETE).")
		return nil
	}

	fmt.Println("\n=== UPDATE половины products (name + price) ===")
	for i := 0; i < count/2; i++ {
		pid := startProductID + int64(i)
		_, err := pool.Exec(ctx, `
			UPDATE products SET name = name || ' (updated)', price_cents = price_cents + 100, updated_at = NOW()
			WHERE id = $1
		`, pid)
		if err != nil {
			return err
		}
	}

	fmt.Println("\n=== DELETE четверти articles ===")
	for i := 0; i < count/4; i++ {
		aid := startArticleID + int64(i)
		_, err := pool.Exec(ctx, `DELETE FROM articles WHERE id = $1`, aid)
		if err != nil {
			return err
		}
	}

	fmt.Println("\nготово. Debezium увидит каждую операцию и запишет в search.public.*.")
	return nil
}

func nextID(ctx context.Context, pool *pgxpool.Pool, table string) (int64, error) {
	var v int64
	err := pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT COALESCE(MAX(id), 0) + 1 FROM %s`, table)).Scan(&v)
	return v, err
}

var words = []string{"alpha", "beta", "gamma", "delta", "omega", "sigma", "zeta", "kappa"}

func randomWord() string {
	return words[rand.IntN(len(words))]
}
