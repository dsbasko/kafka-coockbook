package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"time"

	"github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/01-microservices-comm/internal/orderservice"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultDSN   = "postgres://usecase:usecase@localhost:15440/usecase_09_01?sslmode=disable"
	defaultTopic = "usecase-09-01-order-created"
)

func main() {
	logger := log.New()

	addr := flag.String("addr", ":50081", "TCP-адрес для gRPC OrderService")
	nodeID := flag.String("node-id", "order-1", "идентификатор ноды (для логов и трассировки)")
	topic := flag.String("topic", defaultTopic, "топик для order.created")
	batchSize := flag.Int("publisher-batch-size", 100, "размер батча outbox publisher'а")
	pollInterval := flag.Duration("publisher-poll-interval", 200*time.Millisecond, "пауза, если outbox пустой")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	dsn := config.EnvOr("DATABASE_URL", defaultDSN)

	if err := orderservice.Run(ctx, orderservice.RunOpts{
		GrpcAddr:     *addr,
		NodeID:       *nodeID,
		Topic:        *topic,
		DSN:          dsn,
		BatchSize:    *batchSize,
		PollInterval: *pollInterval,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("order-service failed", "err", err, "node", *nodeID)
		os.Exit(1)
	}
}
