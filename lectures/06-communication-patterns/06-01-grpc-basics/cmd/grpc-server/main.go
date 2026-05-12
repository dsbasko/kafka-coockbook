package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-01-grpc-basics/gen/orders/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	addr := flag.String("addr", ":50051", "TCP-адрес для gRPC-сервера")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Error("listen", "err", err)
		os.Exit(1)
	}

	srv := grpc.NewServer(
		grpc.UnaryInterceptor(loggingUnaryInterceptor(logger)),
	)

	store := newOrderStore()
	ordersv1.RegisterOrderServiceServer(srv, &orderServer{store: store})

	reflection.Register(srv)

	go func() {
		<-ctx.Done()
		logger.Info("graceful stop", "reason", ctx.Err())
		srv.GracefulStop()
	}()

	logger.Info("grpc-server started", "addr", *addr)
	if err := srv.Serve(lis); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}

type orderStore struct {
	mu     sync.RWMutex
	orders map[string]*ordersv1.Order
}

func newOrderStore() *orderStore {
	return &orderStore{orders: make(map[string]*ordersv1.Order)}
}

func (s *orderStore) put(o *ordersv1.Order) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orders[o.GetId()] = o
}

func (s *orderStore) get(id string) (*ordersv1.Order, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.orders[id]
	return o, ok
}

type orderServer struct {
	ordersv1.UnimplementedOrderServiceServer
	store *orderStore
}

func (s *orderServer) Create(_ context.Context, req *ordersv1.CreateRequest) (*ordersv1.CreateResponse, error) {
	if req.GetCustomerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "customer_id is required")
	}
	if req.GetAmountCents() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "amount_cents must be > 0")
	}
	if req.GetCurrency() == "" {
		return nil, status.Error(codes.InvalidArgument, "currency is required")
	}

	o := &ordersv1.Order{
		Id:          uuid.NewString(),
		CustomerId:  req.GetCustomerId(),
		AmountCents: req.GetAmountCents(),
		Currency:    req.GetCurrency(),
		Status:      ordersv1.OrderStatus_ORDER_STATUS_NEW,
		CreatedAt:   timestamppb.Now(),
	}
	s.store.put(o)
	return &ordersv1.CreateResponse{Order: o}, nil
}

func (s *orderServer) Get(_ context.Context, req *ordersv1.GetRequest) (*ordersv1.GetResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	o, ok := s.store.get(req.GetId())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "order %q not found", req.GetId())
	}
	return &ordersv1.GetResponse{Order: o}, nil
}

func loggingUnaryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		dur := time.Since(start)
		code := status.Code(err).String()

		if err != nil {
			logger.Error("rpc", "method", info.FullMethod, "code", code, "dur", dur, "err", err)
			return resp, err
		}
		logger.Info("rpc", "method", info.FullMethod, "code", code, "dur", dur)
		return resp, nil
	}
}
