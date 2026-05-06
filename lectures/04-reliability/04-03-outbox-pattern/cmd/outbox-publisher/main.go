// outbox-publisher — поллер, который читает таблицу outbox, шлёт каждую
// неопубликованную запись в Kafka через идемпотентный producer, помечает
// published_at = NOW().
//
// Запрос забора батча:
//
//	SELECT id, aggregate_id, topic, payload
//	  FROM outbox
//	 WHERE published_at IS NULL
//	 ORDER BY id
//	 LIMIT $1
//	 FOR UPDATE SKIP LOCKED;
//
// FOR UPDATE SKIP LOCKED тут — must-have. Без него два publisher'а на одной
// БД толкутся: первый берёт row, второй ждёт по `FOR UPDATE`, всё
// сериализуется в один поток. С `SKIP LOCKED` второй процесс тихо
// пролистывает занятые row'ы и берёт следующие — параллельность есть,
// дублей публикации в нормальной работе нет.
//
// Гарантия — at-least-once. Между Produce и UPDATE published_at есть окно,
// в которое падение процесса даёт ровно один сценарий: запись ушла в Kafka,
// но в БД она всё ещё «не опубликована». На рестарте мы её отправим заново.
// Дубль в Kafka. Идемпотентный producer на уровне franz-go тут не спасает —
// у перезапущенного процесса другой producer-id и sequence number, для
// брокера это новая запись.
//
// Защита от этого дубля живёт на стороне consumer'а — там dedup по outbox.id.
// См. cmd/orders-consumer/main.go.
//
// Флаг -crash-after-produce запускает имитацию падения: после успешного
// ProduceSync, ДО UPDATE — os.Exit(2). Запусти второй раз без флага → второй
// publisher получит ту же outbox-row, отправит её повторно, и в Kafka будет
// два события с одинаковым outbox.id. Consumer должен схлопнуть дубль.
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

// SELECT FOR UPDATE SKIP LOCKED — параллельные publisher'ы не блокируют
// друг друга. Тот, кто первым взял row, видит её под локом до COMMIT;
// второй процесс молча пропускает и идёт дальше.
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

	// Идемпотентный producer (acks=all + ProducerID) — дефолт franz-go.
	// Он защищает от дублей в рамках ОДНОЙ сессии producer'а: ретраи внутри
	// Produce'а не дублируют записи. Между сессиями (рестарт publisher'а)
	// идемпотентность не работает — другой producer-id, другие sequence numbers.
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

// publishOnce — одна итерация поллера: BEGIN, забираем батч под локом,
// шлём в Kafka, помечаем published_at, COMMIT. Между Produce и UPDATE —
// то самое окно, в котором crash порождает дубль (out of band к БД).
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

	// Окно для crash-симуляции. Записи уже в Kafka. Если упасть тут — на
	// рестарте мы заберём их снова, выпустим повторно, и consumer увидит
	// дубль. На уровне БД при rollback'е: published_at остался NULL —
	// то есть запись «не опубликована», хотя физически в Kafka она есть.
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
