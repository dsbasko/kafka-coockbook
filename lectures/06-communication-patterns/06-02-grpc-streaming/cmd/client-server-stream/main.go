package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-02-grpc-streaming/gen/orders/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	addr := flag.String("addr", "localhost:50052", "адрес gRPC-сервера")
	customer := flag.String("customer", "", "customer_id для фильтра (пусто = все)")
	limit := flag.Int("limit", 5, "сколько событий получить (0 = бесконечно)")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		logger.Error("dial", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := ordersv1.NewStreamingServiceClient(conn)

	stream, err := client.Subscribe(ctx, &ordersv1.SubscribeRequest{
		CustomerId: *customer,
		Limit:      int32(*limit),
	})
	if err != nil {
		logger.Error("subscribe rpc start", "err", err)
		os.Exit(1)
	}

	var received int
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			fmt.Printf("stream closed by server, got %d events\n", received)
			return
		}
		if err != nil {
			if code := status.Code(err); code == codes.Canceled || isCtxErr(ctx) {
				fmt.Printf("stream cancelled by client, got %d events\n", received)
				return
			}
			logger.Error("recv", "code", status.Code(err), "err", err)
			os.Exit(1)
		}

		received++
		fmt.Printf("[%d] order_id=%s customer=%s amount=%d at=%s\n",
			received,
			ev.GetOrderId(),
			ev.GetCustomerId(),
			ev.GetAmountCents(),
			ev.GetEmittedAt().AsTime().Format(time.RFC3339),
		)
	}
}

func isCtxErr(ctx context.Context) bool {
	return ctx.Err() != nil
}
