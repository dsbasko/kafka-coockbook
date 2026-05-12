package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
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

	esURL := flag.String("es-url", "http://localhost:19200", "Elasticsearch URL")
	alias := flag.String("alias", "products_v1", "ES alias или индекс для поиска")
	pgTable := flag.String("pg-table", "products", "Postgres таблица для сравнения")
	matchField := flag.String("match-field", "name", "поле, по которому делаем match")
	query := flag.String("query", "alpha", "слово для full-text search")
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

	pgCount, err := countPostgres(ctx, pool, *pgTable)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		logger.Error("count pg", "err", err)
		os.Exit(1)
	}

	esCount, err := countES(ctx, *esURL, *alias)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		logger.Error("count es", "err", err)
		os.Exit(1)
	}

	fmt.Printf("\nPostgres %s: %d строк\n", *pgTable, pgCount)
	fmt.Printf("Elasticsearch %s: %d документов\n", *alias, esCount)
	if pgCount != esCount {
		fmt.Printf("\033[33mРАСХОЖДЕНИЕ: %d (PG) vs %d (ES)\033[0m\n", pgCount, esCount)
	} else {
		fmt.Println("счётчики совпали")
	}

	hits, err := matchQuery(ctx, *esURL, *alias, *matchField, *query)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		logger.Error("match query", "err", err)
		os.Exit(1)
	}
	fmt.Printf("\nТоп-5 хитов по %s:\"%s\":\n", *matchField, *query)
	for i, h := range hits {
		fmt.Printf("  %d) id=%v score=%.2f source=%v\n", i+1, h.ID, h.Score, h.Source)
	}
}

func countPostgres(ctx context.Context, pool *pgxpool.Pool, table string) (int64, error) {
	var c int64

	safeTable := pgx.Identifier{table}.Sanitize()
	err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", safeTable)).Scan(&c)
	return c, err
}

func countES(ctx context.Context, esURL, alias string) (int64, error) {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(tctx, "POST",
		fmt.Sprintf("%s/%s/_count", esURL, alias),
		bytes.NewBufferString(`{"query":{"match_all":{}}}`))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("ES count %d: %s", resp.StatusCode, string(body))
	}
	var out struct {
		Count int64 `json:"count"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return 0, err
	}
	return out.Count, nil
}

type hit struct {
	ID     any            `json:"_id"`
	Score  float64        `json:"_score"`
	Source map[string]any `json:"_source"`
}

func matchQuery(ctx context.Context, esURL, alias, field, query string) ([]hit, error) {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	body := fmt.Sprintf(`{"size":5,"query":{"match":{%q:%q}}}`, field, query)
	req, err := http.NewRequestWithContext(tctx, "POST",
		fmt.Sprintf("%s/%s/_search", esURL, alias),
		bytes.NewBufferString(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("ES search %d: %s", resp.StatusCode, string(respBody))
	}
	var out struct {
		Hits struct {
			Hits []hit `json:"hits"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, err
	}
	return out.Hits.Hits, nil
}
