package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	usersv1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-03-sync-vs-async/gen/users/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	addr := flag.String("addr", ":50061", "TCP-адрес для gRPC-приёмника")
	name := flag.String("name", "", "имя приёмника для логов; если пусто, берётся LISTENER_NAME или addr")
	flag.Parse()

	if *name == "" {
		*name = config.EnvOr("LISTENER_NAME", *addr)
	}

	ctx, cancel := runctx.New()
	defer cancel()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Error("listen", "err", err)
		os.Exit(1)
	}

	srv := grpc.NewServer()
	usersv1.RegisterUserEventServiceServer(srv, &listenerServer{name: *name})
	reflection.Register(srv)

	go func() {
		<-ctx.Done()
		logger.Info("graceful stop", "name", *name, "reason", ctx.Err())
		srv.GracefulStop()
	}()

	logger.Info("grpc-listener started", "name", *name, "addr", *addr)
	if err := srv.Serve(lis); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}

type listenerServer struct {
	usersv1.UnimplementedUserEventServiceServer
	name string
}

func (s *listenerServer) Notify(_ context.Context, req *usersv1.NotifyRequest) (*usersv1.NotifyResponse, error) {
	ev := req.GetEvent()
	if ev.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	fmt.Printf("[%s] got user_id=%s email=%s country=%s\n",
		s.name, ev.GetUserId(), ev.GetEmail(), ev.GetCountry())

	return &usersv1.NotifyResponse{Accepted: true}, nil
}
