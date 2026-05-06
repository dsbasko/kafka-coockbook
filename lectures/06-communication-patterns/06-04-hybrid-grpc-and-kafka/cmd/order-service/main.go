// order-service — write-side гибрида gRPC + Kafka.
//
// Что внутри одного процесса:
//
//  1. gRPC-сервер с CommandService.Create. На каждый Create открывает
//     транзакцию Postgres: INSERT в orders + INSERT в outbox под одним
//     COMMIT. Никакого Produce внутри RPC.
//  2. Outbox-publisher как горутина. Каждые -poll-interval читает
//     неопубликованные записи через FOR UPDATE SKIP LOCKED, шлёт в Kafka
//     через ProduceSync, помечает published_at = NOW().
//
// Почему два узла в одном процессе: лекция про паттерн, а не про деплой.
// В проде publisher часто отдельный — чтобы не масштабировать gRPC API
// и outbox-flow вместе. Тут компактнее: один main.go, обе ответственности.
//
// Что попадает в outbox.payload:
//
//	{
//	  "id":           "<uuid>",
//	  "customer_id":  "...",
//	  "amount_cents": 12300,
//	  "currency":     "EUR",
//	  "status":       "ORDER_STATUS_NEW",
//	  "created_at":   "2026-05-01T12:34:56Z",
//	  "trace_id":     "...",     -- propagation
//	  "tenant_id":    "..."      -- propagation
//	}
//
// trace_id и tenant_id попадают и в payload, и в Kafka headers — чтобы
// downstream-сервисы могли продолжить логи без парсинга тела.
//
// Запуск:
//
//	make up && make db-init && make topic-create
//	make run-order
//	# в другом терминале — grpcurl-create / run-inventory / run-query
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-04-hybrid-grpc-and-kafka/gen/orders/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultDSN   = "postgres://lecture:lecture@localhost:15434/lecture_06_04?sslmode=disable"
	defaultTopic = "lecture-06-04-order-created"
)

const insertOrderSQL = `
INSERT INTO orders (id, customer_id, amount_cents, currency, status)
VALUES ($1, $2, $3, $4, $5)
`

const insertOutboxSQL = `
INSERT INTO outbox (aggregate_id, topic, payload)
VALUES ($1, $2, $3::jsonb)
RETURNING id
`

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

	addr := flag.String("addr", ":50061", "TCP-адрес для gRPC CommandService")
	topic := flag.String("topic", defaultTopic, "топик для events.order.created")
	batchSize := flag.Int("publisher-batch-size", 100, "размер батча outbox publisher'а")
	pollInterval := flag.Duration("publisher-poll-interval", 500*time.Millisecond, "пауза, если outbox пустой")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	dsn := config.EnvOr("DATABASE_URL", defaultDSN)

	if err := run(rootCtx, runOpts{
		grpcAddr:     *addr,
		topic:        *topic,
		dsn:          dsn,
		batchSize:    *batchSize,
		pollInterval: *pollInterval,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("order-service failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	grpcAddr     string
	topic        string
	dsn          string
	batchSize    int
	pollInterval time.Duration
}

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
		kgo.ClientID("lecture-06-04-order-service"),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	lis, err := net.Listen("tcp", o.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", o.grpcAddr, err)
	}

	srv := grpc.NewServer()
	ordersv1.RegisterCommandServiceServer(srv, &commandServer{
		pool:  pool,
		topic: o.topic,
	})
	reflection.Register(srv)

	pubDone := make(chan error, 1)
	go func() {
		pubDone <- runPublisher(ctx, pool, cl, o)
	}()

	srvDone := make(chan error, 1)
	go func() {
		fmt.Printf("order-service: gRPC on %s, outbox publisher → topic=%q\n", o.grpcAddr, o.topic)
		srvDone <- srv.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		fmt.Printf("\nостанов по сигналу: %v\n", ctx.Err())
		srv.GracefulStop()
		<-srvDone
		<-pubDone
		return nil
	case err := <-srvDone:
		return fmt.Errorf("grpc serve: %w", err)
	case err := <-pubDone:
		srv.GracefulStop()
		<-srvDone
		if err == nil {
			return nil
		}
		return fmt.Errorf("publisher: %w", err)
	}
}

// commandServer — gRPC-handler для CommandService. Вся работа с Kafka
// происходит асинхронно в outbox publisher'е, RPC возвращает успех сразу
// после COMMIT'а в БД. Клиент знает: «заказ принят, событие будет позже».
type commandServer struct {
	ordersv1.UnimplementedCommandServiceServer
	pool  *pgxpool.Pool
	topic string
}

func (s *commandServer) Create(ctx context.Context, req *ordersv1.CreateRequest) (*ordersv1.CreateResponse, error) {
	if req.GetCustomerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "customer_id is required")
	}
	if req.GetAmountCents() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount_cents must be > 0")
	}
	if req.GetCurrency() == "" {
		return nil, status.Error(codes.InvalidArgument, "currency is required")
	}

	id := uuid.NewString()
	createdAt := time.Now().UTC()

	evt := orderEvent{
		ID:          id,
		CustomerID:  req.GetCustomerId(),
		AmountCents: req.GetAmountCents(),
		Currency:    req.GetCurrency(),
		Status:      ordersv1.OrderStatus_ORDER_STATUS_NEW.String(),
		CreatedAt:   createdAt.Format(time.RFC3339Nano),
		TraceID:     req.GetTraceId(),
		TenantID:    req.GetTenantId(),
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal event: %v", err)
	}

	var outboxID int64
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, insertOrderSQL,
			id, req.GetCustomerId(), req.GetAmountCents(), req.GetCurrency(),
			ordersv1.OrderStatus_ORDER_STATUS_NEW.String(),
		); err != nil {
			return fmt.Errorf("INSERT orders: %w", err)
		}

		aggregateID := "order-" + id
		if err := tx.QueryRow(ctx, insertOutboxSQL, aggregateID, s.topic, string(payload)).Scan(&outboxID); err != nil {
			return fmt.Errorf("INSERT outbox: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create order tx: %v", err)
	}

	fmt.Printf("CREATE order_id=%s customer=%s amount=%d %s outbox_id=%d\n",
		id, req.GetCustomerId(), req.GetAmountCents(), req.GetCurrency(), outboxID)

	return &ordersv1.CreateResponse{
		Order: &ordersv1.Order{
			Id:          id,
			CustomerId:  req.GetCustomerId(),
			AmountCents: req.GetAmountCents(),
			Currency:    req.GetCurrency(),
			Status:      ordersv1.OrderStatus_ORDER_STATUS_NEW,
			CreatedAt:   timestamppb.New(createdAt),
		},
	}, nil
}

// runPublisher — встроенный outbox-поллер. Та же at-least-once гарантия,
// что и в лекции 04-03: между ProduceSync и UPDATE published_at есть окно,
// в которое крах процесса даёт дубль. Защита — dedup по outbox-id на
// стороне consumer'ов (см. inventory-service / order-query-service).
func runPublisher(ctx context.Context, pool *pgxpool.Pool, cl *kgo.Client, o runOpts) error {
	var published atomic.Int64

	for {
		if err := ctx.Err(); err != nil {
			fmt.Printf("publisher: остановлен, published=%d\n", published.Load())
			return nil
		}

		n, err := publishBatch(ctx, pool, cl, o.batchSize)
		if err != nil {
			return err
		}
		if n > 0 {
			published.Add(int64(n))
			continue
		}

		select {
		case <-ctx.Done():
		case <-time.After(o.pollInterval):
		}
	}
}

type outboxRow struct {
	id          int64
	aggregateID string
	topic       string
	payload     string
}

func publishBatch(ctx context.Context, pool *pgxpool.Pool, cl *kgo.Client, batchSize int) (int, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("BeginTx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, fetchBatchSQL, batchSize)
	if err != nil {
		return 0, fmt.Errorf("SELECT outbox: %w", err)
	}

	batch := make([]outboxRow, 0, batchSize)
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
		var evt orderEvent
		if err := json.Unmarshal([]byte(r.payload), &evt); err != nil {
			return 0, fmt.Errorf("unmarshal outbox row %d: %w", r.id, err)
		}

		records[i] = &kgo.Record{
			Topic: r.topic,
			Key:   []byte(r.aggregateID),
			Value: []byte(r.payload),
			Headers: []kgo.RecordHeader{
				{Key: "outbox-id", Value: []byte(strconv.FormatInt(r.id, 10))},
				{Key: "aggregate-id", Value: []byte(r.aggregateID)},
				{Key: "trace-id", Value: []byte(evt.TraceID)},
				{Key: "tenant-id", Value: []byte(evt.TenantID)},
				{Key: "event-type", Value: []byte("order.created")},
			},
		}
	}

	results := cl.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		return 0, fmt.Errorf("ProduceSync: %w", err)
	}

	for i, r := range batch {
		fmt.Printf("PUBLISH outbox_id=%d agg=%s p=%d off=%d\n",
			r.id, r.aggregateID, records[i].Partition, records[i].Offset)
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
