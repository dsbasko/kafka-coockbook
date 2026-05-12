package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-01-grpc-basics/gen/orders/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
)

func main() {
	logger := log.New()

	addr := flag.String("addr", "localhost:50051", "адрес gRPC-сервера")
	customerID := flag.String("customer", "cus-001", "customer_id для Create")
	amount := flag.Int64("amount", 1500, "amount_cents для Create")
	currency := flag.String("currency", "USD", "ISO-код валюты")
	timeout := flag.Duration("timeout", 3*time.Second, "deadline на один RPC")
	flag.Parse()

	ctx := context.Background()

	conn, err := grpc.NewClient(
		*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(loggingUnaryClientInterceptor(logger)),
	)
	if err != nil {
		logger.Error("grpc dial", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := ordersv1.NewOrderServiceClient(conn)

	createCtx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	createResp, err := client.Create(createCtx, &ordersv1.CreateRequest{
		CustomerId:  *customerID,
		AmountCents: *amount,
		Currency:    *currency,
	})
	if err != nil {
		printRPCError("Create", err)
		os.Exit(1)
	}
	created := createResp.GetOrder()
	fmt.Printf("created: id=%s customer=%s amount=%d %s status=%s\n",
		created.GetId(),
		created.GetCustomerId(),
		created.GetAmountCents(),
		created.GetCurrency(),
		created.GetStatus(),
	)

	getCtx, cancel2 := context.WithTimeout(ctx, *timeout)
	defer cancel2()
	getResp, err := client.Get(getCtx, &ordersv1.GetRequest{Id: created.GetId()})
	if err != nil {
		printRPCError("Get", err)
		os.Exit(1)
	}
	got := getResp.GetOrder()
	fmt.Printf("got    : id=%s customer=%s amount=%d %s status=%s created_at=%s\n",
		got.GetId(),
		got.GetCustomerId(),
		got.GetAmountCents(),
		got.GetCurrency(),
		got.GetStatus(),
		got.GetCreatedAt().AsTime().Format(time.RFC3339),
	)

	notFoundCtx, cancel3 := context.WithTimeout(ctx, *timeout)
	defer cancel3()
	_, err = client.Get(notFoundCtx, &ordersv1.GetRequest{Id: "no-such-order"})
	if code := status.Code(err); code != codes.NotFound {
		logger.Warn("ожидали NotFound от несуществующего id", "got_code", code, "err", err)
	} else {
		fmt.Printf("notfnd : id=%q -> code=%s (как и ждали)\n", "no-such-order", code)
	}
}

func loggingUnaryClientInterceptor(logger *slog.Logger) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		dur := time.Since(start)
		code := status.Code(err).String()

		if err != nil {
			logger.Warn("rpc", "method", method, "code", code, "dur", dur, "err", err)
			return err
		}
		logger.Info("rpc", "method", method, "code", code, "dur", dur)
		return nil
	}
}

func printRPCError(op string, err error) {
	st, _ := status.FromError(err)
	fmt.Fprintf(os.Stderr, "%s failed: code=%s msg=%s\n", op, st.Code(), st.Message())
}
