package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-02-grpc-streaming/gen/orders/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	addr := flag.String("addr", ":50052", "TCP-адрес для gRPC-сервера")
	tickInterval := flag.Duration("tick", time.Second, "интервал между OrderEvent в Subscribe")
	echoDelay := flag.Duration("echo-delay", 200*time.Millisecond, "throttling задержка для echo в Chat")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Error("listen", "err", err)
		os.Exit(1)
	}

	srv := grpc.NewServer()
	ordersv1.RegisterStreamingServiceServer(srv, &streamingServer{
		logger:       logger,
		tickInterval: *tickInterval,
		echoDelay:    *echoDelay,
	})
	reflection.Register(srv)

	go func() {
		<-ctx.Done()
		logger.Info("graceful stop", "reason", ctx.Err())
		srv.GracefulStop()
	}()

	logger.Info("streaming-server started", "addr", *addr, "tick", *tickInterval, "echo_delay", *echoDelay)
	if err := srv.Serve(lis); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}

type streamingServer struct {
	ordersv1.UnimplementedStreamingServiceServer
	logger       *slog.Logger
	tickInterval time.Duration
	echoDelay    time.Duration
}

func (s *streamingServer) Subscribe(req *ordersv1.SubscribeRequest, stream grpc.ServerStreamingServer[ordersv1.OrderEvent]) error {
	s.logger.Info("subscribe started", "customer_id", req.GetCustomerId(), "limit", req.GetLimit())

	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	var sent int32
	for {
		select {
		case <-stream.Context().Done():
			s.logger.Info("subscribe ended: client gone", "sent", sent, "err", stream.Context().Err())
			return nil

		case t := <-ticker.C:
			ev := mockOrderEvent(req.GetCustomerId(), t)
			if err := stream.Send(ev); err != nil {
				s.logger.Warn("subscribe send failed", "sent", sent, "err", err)
				return err
			}
			sent++
			if req.GetLimit() > 0 && sent >= req.GetLimit() {
				s.logger.Info("subscribe ended: limit reached", "sent", sent)
				return nil
			}
		}
	}
}

func (s *streamingServer) UploadOrders(stream grpc.ClientStreamingServer[ordersv1.OrderInput, ordersv1.UploadSummary]) error {
	var summary ordersv1.UploadSummary
	for {
		in, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			s.logger.Info("upload ended", "received", summary.Received, "accepted", summary.Accepted, "rejected", summary.Rejected)
			return stream.SendAndClose(&summary)
		}
		if err != nil {
			s.logger.Warn("upload recv failed", "err", err)
			return err
		}

		summary.Received++
		if in.GetAmountCents() <= 0 || in.GetCustomerId() == "" {
			summary.Rejected++
			continue
		}
		summary.Accepted++
		summary.TotalCents += in.GetAmountCents()
	}
}

func (s *streamingServer) Chat(stream grpc.BidiStreamingServer[ordersv1.ChatMessage, ordersv1.ChatMessage]) error {
	type incoming struct {
		msg *ordersv1.ChatMessage
		err error
	}
	in := make(chan incoming, 8)

	go func() {
		for {
			m, err := stream.Recv()
			in <- incoming{msg: m, err: err}
			if err != nil {
				close(in)
				return
			}
		}
	}()

	for {
		select {
		case <-stream.Context().Done():
			s.logger.Info("chat ended: ctx done", "err", stream.Context().Err())
			return nil

		case ev, ok := <-in:
			if !ok {
				return nil
			}
			if errors.Is(ev.err, io.EOF) {
				s.logger.Info("chat ended: client closed send")
				return nil
			}
			if ev.err != nil {
				s.logger.Warn("chat recv failed", "err", ev.err)
				return ev.err
			}

			time.Sleep(s.echoDelay)

			reply := &ordersv1.ChatMessage{
				From:   "server",
				Text:   fmt.Sprintf("echo: %s", ev.msg.GetText()),
				SentAt: timestamppb.Now(),
			}
			if err := stream.Send(reply); err != nil {
				s.logger.Warn("chat send failed", "err", err)
				return err
			}
		}
	}
}

func mockOrderEvent(filterCustomer string, now time.Time) *ordersv1.OrderEvent {
	customer := filterCustomer
	if customer == "" {
		customer = fmt.Sprintf("cus-%03d", rand.IntN(100))
	}
	return &ordersv1.OrderEvent{
		OrderId:     fmt.Sprintf("ord-%d", now.UnixMilli()),
		CustomerId:  customer,
		AmountCents: int64(100 + rand.IntN(9900)),
		EmittedAt:   timestamppb.New(now),
	}
}
