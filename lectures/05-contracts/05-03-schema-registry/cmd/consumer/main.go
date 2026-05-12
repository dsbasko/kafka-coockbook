package main

import (
	"flag"
	"fmt"
	"os"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/05-contracts/05-03-schema-registry/gen/orders/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	kafkactl "github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	topic := flag.String("topic", "lecture-05-03-orders-sr", "топик для чтения")
	group := flag.String("group", "lecture-05-03-consumer", "consumer group")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	srURL := config.EnvOr("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	srCl, err := sr.NewClient(sr.URLs(srURL))
	if err != nil {
		logger.Error("sr client", "err", err)
		os.Exit(1)
	}

	serde := sr.NewSerde()
	var registered sync.Map

	cl, err := kafkactl.NewClient(
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		logger.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	logger.Info("consumer started", "topic", *topic, "group", *group, "sr", srURL)

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			logger.Info("consumer stopping", "reason", ctx.Err())
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				logger.Error("fetch", "topic", e.Topic, "partition", e.Partition, "err", e.Err)
			}
			os.Exit(1)
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			id, _, err := serde.DecodeID(rec.Value)
			if err != nil {
				logger.Error("decode id", "err", err, "partition", rec.Partition, "offset", rec.Offset)
				return
			}

			if _, ok := registered.Load(id); !ok {
				schema, err := srCl.SchemaByID(ctx, id)
				if err != nil {
					logger.Error("sr fetch", "id", id, "err", err)
					return
				}
				logger.Info("registering schema id", "id", id, "type", schema.Type.String())
				serde.Register(
					id,
					&ordersv1.Order{},
					sr.DecodeFn(func(b []byte, v any) error {
						return proto.Unmarshal(b, v.(*ordersv1.Order))
					}),
					sr.Index(0),
				)
				registered.Store(id, struct{}{})
			}

			var order ordersv1.Order
			if err := serde.Decode(rec.Value, &order); err != nil {
				logger.Error("serde decode", "err", err, "partition", rec.Partition, "offset", rec.Offset)
				return
			}
			printOrder(rec, id, &order)
		})
	}
}

func printOrder(rec *kgo.Record, schemaID int, o *ordersv1.Order) {
	fmt.Printf("--- %s/%d@%d key=%s schema_id=%d ---\n",
		rec.Topic, rec.Partition, rec.Offset, string(rec.Key), schemaID)
	fmt.Printf("  id           = %s\n", o.GetId())
	fmt.Printf("  customer_id  = %s\n", o.GetCustomerId())
	fmt.Printf("  amount_cents = %d %s\n", o.GetAmountCents(), o.GetCurrency())
	fmt.Printf("  status       = %s\n", o.GetStatus().String())
	if ts := o.GetCreatedAt(); ts != nil {
		fmt.Printf("  created_at   = %s\n", ts.AsTime().Format("2006-01-02 15:04:05Z07:00"))
	}
	fmt.Printf("  items        = %d\n", len(o.GetItems()))
	for i, it := range o.GetItems() {
		fmt.Printf("    [%d] sku=%s qty=%d price=%d\n", i, it.GetSku(), it.GetQuantity(), it.GetPriceCents())
	}
}
