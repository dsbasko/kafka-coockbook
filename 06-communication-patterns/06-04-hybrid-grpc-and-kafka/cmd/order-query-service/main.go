// order-query-service — read-side гибрида.
//
// Что внутри одного процесса:
//
//  1. gRPC-сервер с QueryService.Get. Читает orders_view из Postgres.
//     Никакого Kafka внутри RPC, никакого write-доступа к orders.
//  2. Projector как горутина. Consumer на топик order.created, на каждое
//     сообщение делает UPSERT в orders_view с dedup'ом по outbox-id.
//
// Лекция демонстрирует CQRS на минимуме:
//   - write-side и read-side — разные API (CommandService vs QueryService),
//     разные сервисы, разные таблицы (orders vs orders_view);
//   - между ними — Kafka как единственная точка соединения;
//   - Get сразу после Create может вернуть NotFound — читай: лекция не врёт
//     про eventual consistency, она показывает её пальцем. Так оно и
//     устроено в проде, пока проектор не догнал лог.
//
// В реальности read-side обычно стоит за CDN или кэшем, отвечает быстрее
// write-side, легко масштабируется по чтению независимо от записи.
//
// Запуск:
//
//	make up && make db-init && make topic-create
//	make run-order
//	make run-query        # в другом терминале
//	make grpcurl-create   # потом
//	make grpcurl-get ID=<uuid>
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"time"

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
	defaultGroup = "lecture-06-04-query-projector"
	consumerName = "query-projector"
)

const dedupSQL = `
INSERT INTO processed_events (consumer, outbox_id)
VALUES ($1, $2)
ON CONFLICT (consumer, outbox_id) DO NOTHING
`

const upsertViewSQL = `
INSERT INTO orders_view (id, customer_id, amount_cents, currency, status, created_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (id) DO UPDATE
   SET customer_id  = EXCLUDED.customer_id,
       amount_cents = EXCLUDED.amount_cents,
       currency     = EXCLUDED.currency,
       status       = EXCLUDED.status,
       projected_at = NOW()
`

const selectViewSQL = `
SELECT id, customer_id, amount_cents, currency, status, created_at
  FROM orders_view
 WHERE id = $1
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

	addr := flag.String("addr", ":50062", "TCP-адрес для gRPC QueryService")
	topic := flag.String("topic", defaultTopic, "топик с order.created")
	group := flag.String("group", defaultGroup, "consumer group для projector'а")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	dsn := config.EnvOr("DATABASE_URL", defaultDSN)

	if err := run(rootCtx, runOpts{
		grpcAddr: *addr,
		topic:    *topic,
		group:    *group,
		dsn:      dsn,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("order-query-service failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	grpcAddr string
	topic    string
	group    string
	dsn      string
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
		kgo.ClientID("lecture-06-04-query-projector"),
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
	ordersv1.RegisterQueryServiceServer(srv, &queryServer{pool: pool})
	reflection.Register(srv)

	projDone := make(chan error, 1)
	go func() {
		projDone <- runProjector(ctx, pool, cl)
	}()

	srvDone := make(chan error, 1)
	go func() {
		fmt.Printf("order-query-service: gRPC on %s, projector group=%q\n", o.grpcAddr, o.group)
		srvDone <- srv.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		fmt.Printf("\nостанов по сигналу: %v\n", ctx.Err())
		srv.GracefulStop()
		<-srvDone
		<-projDone
		return nil
	case err := <-srvDone:
		return fmt.Errorf("grpc serve: %w", err)
	case err := <-projDone:
		srv.GracefulStop()
		<-srvDone
		if err == nil {
			return nil
		}
		return fmt.Errorf("projector: %w", err)
	}
}

type queryServer struct {
	ordersv1.UnimplementedQueryServiceServer
	pool *pgxpool.Pool
}

func (s *queryServer) Get(ctx context.Context, req *ordersv1.GetRequest) (*ordersv1.GetResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	var (
		id, customerID, currency, statusStr string
		amountCents                          int64
		createdAt                            time.Time
	)
	err := s.pool.QueryRow(ctx, selectViewSQL, req.GetId()).Scan(
		&id, &customerID, &amountCents, &currency, &statusStr, &createdAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound,
				"order %q not found in read-store (eventual consistency lag)", req.GetId())
		}
		return nil, status.Errorf(codes.Internal, "select view: %v", err)
	}

	statusEnum, ok := ordersv1.OrderStatus_value[statusStr]
	if !ok {
		statusEnum = int32(ordersv1.OrderStatus_ORDER_STATUS_UNSPECIFIED)
	}

	return &ordersv1.GetResponse{
		Order: &ordersv1.Order{
			Id:          id,
			CustomerId:  customerID,
			AmountCents: amountCents,
			Currency:    currency,
			Status:      ordersv1.OrderStatus(statusEnum),
			CreatedAt:   timestamppb.New(createdAt),
		},
	}, nil
}

// runProjector — тот же at-least-once паттерн, что у inventory: dedup по
// outbox-id, UPSERT в orders_view, manual commit после успеха. Гарантия
// projector'а: на каждый outbox-id orders_view гарантированно содержит
// последнее видимое состояние из payload, дубли не вызывают повторного UPSERT.
func runProjector(ctx context.Context, pool *pgxpool.Pool, cl *kgo.Client) error {
	var processed atomic.Int64
	var projected atomic.Int64
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
			fmt.Printf("projector: остановлен. processed=%d projected=%d skipped=%d\n",
				processed.Load(), projected.Load(), skipped.Load())
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

			var evt orderEvent
			if err := json.Unmarshal(r.Value, &evt); err != nil {
				return fmt.Errorf("unmarshal p=%d off=%d: %w", r.Partition, r.Offset, err)
			}
			createdAt, err := time.Parse(time.RFC3339Nano, evt.CreatedAt)
			if err != nil {
				return fmt.Errorf("parse created_at %q: %w", evt.CreatedAt, err)
			}

			// Дедуп-гейт и UPSERT в orders_view должны быть атомарны: иначе
			// между ними может случиться краш и при рестарте гейт скажет
			// «уже обработано», а view так и не получит запись.
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
				if _, err := tx.Exec(ctx, upsertViewSQL,
					evt.ID, evt.CustomerID, evt.AmountCents, evt.Currency, evt.Status, createdAt,
				); err != nil {
					return fmt.Errorf("upsert view: %w", err)
				}
				return nil
			})
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("outbox-id=%d order=%s: %w", outboxID, evt.ID, err)
			}
			n := processed.Add(1)
			if !newRow {
				skipped.Add(1)
				fmt.Printf("PROJ   DUP n=%d outbox-id=%d p=%d off=%d\n",
					n, outboxID, r.Partition, r.Offset)
				continue
			}
			projected.Add(1)
			fmt.Printf("PROJECT n=%d order=%s customer=%s status=%s\n",
				n, evt.ID, evt.CustomerID, evt.Status)
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
