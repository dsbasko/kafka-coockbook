// Package orderservice — write-side use case 09-01.
//
// Запускается в нескольких копиях с разными RunOpts.GrpcAddr и NodeID. Один
// Postgres, один Kafka, общий outbox. SELECT ... FOR UPDATE SKIP LOCKED не
// даёт двум publisher'ам взять одну строку.
//
// Внутри одного процесса:
//
//  1. gRPC OrderService.Create — пишет orders + outbox в одной транзакции
//     Postgres. Никакого Produce внутри RPC.
//  2. Outbox-publisher горутиной. Каждые PollInterval тянет батч
//     неопубликованных, шлёт в Kafka, помечает published_at.
//
// Гарантия — at-least-once. Между ProduceSync и UPDATE published_at есть
// окно: краш в нём даёт повторную публикацию того же outbox-id при
// рестарте. Защита — dedup на consumer'ах по (consumer, outbox_id).
package orderservice

import (
	"context"
	"errors"
	"fmt"
	"net"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/01-microservices-comm/gen/orders/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
)

const insertOrderSQL = `
INSERT INTO orders (id, customer_id, amount_cents, currency, status)
VALUES ($1, $2, $3, $4, $5)
`

const insertOutboxSQL = `
INSERT INTO outbox (aggregate_id, topic, payload)
VALUES ($1, $2, $3)
RETURNING id
`

const fetchBatchSQL = `
SELECT id, aggregate_id, topic, payload
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

type RunOpts struct {
	GrpcAddr     string
	NodeID       string
	Topic        string
	DSN          string
	BatchSize    int
	PollInterval time.Duration
}

func Run(ctx context.Context, o RunOpts) error {
	if o.BatchSize <= 0 {
		o.BatchSize = 100
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 200 * time.Millisecond
	}

	pool, err := pgxpool.New(ctx, o.DSN)
	if err != nil {
		return fmt.Errorf("pgxpool.New: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("pg ping: %w", err)
	}

	cl, err := kafka.NewClient(
		kgo.ClientID("usecase-09-01-"+o.NodeID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	lis, err := net.Listen("tcp", o.GrpcAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", o.GrpcAddr, err)
	}

	srv := grpc.NewServer()
	ordersv1.RegisterOrderServiceServer(srv, &commandServer{
		pool:   pool,
		topic:  o.Topic,
		nodeID: o.NodeID,
	})
	reflection.Register(srv)

	pubDone := make(chan error, 1)
	go func() {
		pubDone <- runPublisher(ctx, pool, cl, o)
	}()

	srvDone := make(chan error, 1)
	go func() {
		fmt.Printf("[%s] order-service: gRPC on %s, outbox publisher → %q\n",
			o.NodeID, o.GrpcAddr, o.Topic)
		srvDone <- srv.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		fmt.Printf("[%s] остановка по сигналу: %v\n", o.NodeID, ctx.Err())
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

type commandServer struct {
	ordersv1.UnimplementedOrderServiceServer
	pool   *pgxpool.Pool
	topic  string
	nodeID string
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

	evt := &ordersv1.OrderCreated{
		Id:          id,
		CustomerId:  req.GetCustomerId(),
		AmountCents: req.GetAmountCents(),
		Currency:    req.GetCurrency(),
		CreatedAt:   timestamppb.New(createdAt),
		TraceId:     req.GetTraceId(),
		TenantId:    req.GetTenantId(),
	}
	payload, err := proto.Marshal(evt)
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
		if err := tx.QueryRow(ctx, insertOutboxSQL, aggregateID, s.topic, payload).Scan(&outboxID); err != nil {
			return fmt.Errorf("INSERT outbox: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create order tx: %v", err)
	}

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

func runPublisher(ctx context.Context, pool *pgxpool.Pool, cl *kgo.Client, o RunOpts) error {
	var published atomic.Int64

	for {
		if err := ctx.Err(); err != nil {
			fmt.Printf("[%s] publisher остановлен, published=%d\n", o.NodeID, published.Load())
			return nil
		}

		n, err := publishBatch(ctx, pool, cl, o)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		if n > 0 {
			published.Add(int64(n))
			continue
		}

		select {
		case <-ctx.Done():
		case <-time.After(o.PollInterval):
		}
	}
}

type outboxRow struct {
	id          int64
	aggregateID string
	topic       string
	payload     []byte
}

func publishBatch(ctx context.Context, pool *pgxpool.Pool, cl *kgo.Client, o RunOpts) (int, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("BeginTx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, fetchBatchSQL, o.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("SELECT outbox: %w", err)
	}

	batch := make([]outboxRow, 0, o.BatchSize)
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
		var evt ordersv1.OrderCreated
		if err := proto.Unmarshal(r.payload, &evt); err != nil {
			return 0, fmt.Errorf("unmarshal outbox row %d: %w", r.id, err)
		}

		records[i] = &kgo.Record{
			Topic: r.topic,
			Key:   []byte(r.aggregateID),
			Value: r.payload,
			Headers: []kgo.RecordHeader{
				{Key: "outbox-id", Value: []byte(strconv.FormatInt(r.id, 10))},
				{Key: "aggregate-id", Value: []byte(r.aggregateID)},
				{Key: "publisher-node", Value: []byte(o.NodeID)},
				{Key: "trace-id", Value: []byte(evt.GetTraceId())},
				{Key: "tenant-id", Value: []byte(evt.GetTenantId())},
				{Key: "event-type", Value: []byte("order.created")},
				{Key: "content-type", Value: []byte("application/x-protobuf")},
			},
		}
	}

	results := cl.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		return 0, fmt.Errorf("ProduceSync: %w", err)
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
