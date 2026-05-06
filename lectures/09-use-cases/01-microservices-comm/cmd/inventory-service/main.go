package main

import (
	"context"
	"errors"
	"flag"
	"os"

	"github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/01-microservices-comm/internal/inventoryservice"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultDSN   = "postgres://usecase:usecase@localhost:15440/usecase_09_01?sslmode=disable"
	defaultTopic = "usecase-09-01-order-created"
	defaultGroup = "usecase-09-01-inventory"
)

func main() {
	logger := log.New()

	nodeID := flag.String("node-id", "inventory-1", "идентификатор ноды (в reserved_by)")
	topic := flag.String("topic", defaultTopic, "топик с order.created")
	group := flag.String("group", defaultGroup, "consumer group")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	dsn := config.EnvOr("DATABASE_URL", defaultDSN)

	if err := inventoryservice.Run(ctx, inventoryservice.RunOpts{
		NodeID: *nodeID,
		Topic:  *topic,
		Group:  *group,
		DSN:    dsn,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("inventory-service failed", "err", err, "node", *nodeID)
		os.Exit(1)
	}
}
