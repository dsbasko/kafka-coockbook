// client-bidi — клиент к Chat (bidi-stream).
//
// Bidi — это два независимых потока поверх одного HTTP/2 stream'а.
// Поэтому в клиенте два goroutine'а: один Send'ит сообщения с интервалом,
// второй Recv'ит echo. Координация — через контекст и канал done.
//
// Сценарий: шлём -count сообщений, каждое с -interval, в фоне читаем echo.
// Сервер throttling'ит ответы (по умолчанию 200ms задержка), поэтому при
// маленьком interval видно, что Send и Recv не синхронные — клиент успеет
// зашить три-четыре message'а раньше, чем сервер начнёт отвечать.
//
// Запуск: см. Makefile (`make run-chat`).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-02-grpc-streaming/gen/orders/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	addr := flag.String("addr", "localhost:50052", "адрес gRPC-сервера")
	from := flag.String("from", "client-1", "имя отправителя")
	count := flag.Int("count", 5, "сколько сообщений отправить")
	interval := flag.Duration("interval", 50*time.Millisecond, "интервал между Send")
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

	stream, err := client.Chat(ctx)
	if err != nil {
		logger.Error("chat rpc start", "err", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readEchoes(ctx, stream)
	}()

	for i := 0; i < *count; i++ {
		msg := &ordersv1.ChatMessage{
			From:   *from,
			Text:   fmt.Sprintf("ping %d", i+1),
			SentAt: timestamppb.Now(),
		}
		if err := stream.Send(msg); err != nil {
			logger.Error("send", "i", i, "err", err)
			break
		}
		fmt.Printf("> sent: %s\n", msg.GetText())
		time.Sleep(*interval)
	}

	// Закрываем send-сторону — сервер увидит io.EOF в Recv и завершит
	// свою сторону. Дальше ждём, пока read-горутина дочитает оставшиеся
	// echo и тоже завершится.
	if err := stream.CloseSend(); err != nil {
		logger.Warn("close send", "err", err)
	}
	wg.Wait()
}

func readEchoes(ctx context.Context, stream grpc.BidiStreamingClient[ordersv1.ChatMessage, ordersv1.ChatMessage]) {
	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			fmt.Println("< stream closed by server")
			return
		}
		if err != nil {
			if code := status.Code(err); code == codes.Canceled || ctx.Err() != nil {
				fmt.Println("< stream cancelled")
				return
			}
			fmt.Fprintf(os.Stderr, "recv error: %v\n", err)
			return
		}
		fmt.Printf("< echo from %s: %s\n", m.GetFrom(), m.GetText())
	}
}
