package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-02-grpc-streaming/gen/orders/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	addr := flag.String("addr", "localhost:50052", "адрес gRPC-сервера")
	count := flag.Int("count", 20, "сколько сообщений отправить")
	bad := flag.Int("bad", 3, "из них невалидных (amount=0)")
	pause := flag.Duration("pause", 50*time.Millisecond, "задержка между Send")
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

	stream, err := client.UploadOrders(ctx)
	if err != nil {
		logger.Error("upload rpc start", "err", err)
		os.Exit(1)
	}

	for i := 0; i < *count; i++ {
		input := &ordersv1.OrderInput{
			CustomerId:  fmt.Sprintf("cus-%03d", i%5),
			AmountCents: int64(100 + i*10),
			Currency:    "USD",
		}

		if i < *bad {
			input.AmountCents = 0
		}
		if err := stream.Send(input); err != nil {
			logger.Error("send", "i", i, "err", err)
			os.Exit(1)
		}
		fmt.Printf("sent #%d customer=%s amount=%d\n", i, input.CustomerId, input.AmountCents)
		time.Sleep(*pause)
	}

	summary, err := stream.CloseAndRecv()
	if err != nil {
		logger.Error("close and recv", "code", status.Code(err), "err", err)
		os.Exit(1)
	}

	fmt.Println("---")
	fmt.Printf("summary: received=%d accepted=%d rejected=%d total_cents=%d\n",
		summary.GetReceived(),
		summary.GetAccepted(),
		summary.GetRejected(),
		summary.GetTotalCents(),
	)
}
