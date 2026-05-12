package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/05-contracts/05-02-protobuf-in-go/gen/orders/v1"
	kafkactl "github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	topic := flag.String("topic", "lecture-05-02-orders-proto", "топик для чтения")
	group := flag.String("group", "lecture-05-02-consumer", "consumer group")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

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

	logger.Info("consumer started", "topic", *topic, "group", *group)

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
			var order ordersv1.Order
			if err := proto.Unmarshal(rec.Value, &order); err != nil {
				logger.Error("proto unmarshal",
					"err", err,
					"partition", rec.Partition,
					"offset", rec.Offset,
				)
				return
			}
			printOrder(rec, &order)
		})
	}
}

func printOrder(rec *kgo.Record, o *ordersv1.Order) {
	fmt.Printf("--- %s/%d@%d key=%s ---\n", rec.Topic, rec.Partition, rec.Offset, string(rec.Key))
	fmt.Printf("  id           = %s\n", o.GetId())
	fmt.Printf("  customer_id  = %s\n", o.GetCustomerId())
	fmt.Printf("  amount_cents = %d %s\n", o.GetAmountCents(), o.GetCurrency())
	fmt.Printf("  status       = %s\n", o.GetStatus().String())
	if ts := o.GetCreatedAt(); ts != nil {
		fmt.Printf("  created_at   = %s\n", ts.AsTime().Format("2006-01-02 15:04:05Z07:00"))
	}
	if d := o.GetReservationTtl(); d != nil {
		fmt.Printf("  reservation  = %s\n", d.AsDuration())
	}
	if note := o.GetNote(); note != "" {
		fmt.Printf("  note         = %s\n", note)
	}
	fmt.Printf("  items        = %d\n", len(o.GetItems()))
	for i, it := range o.GetItems() {
		fmt.Printf("    [%d] sku=%s qty=%d price=%d\n", i, it.GetSku(), it.GetQuantity(), it.GetPriceCents())
	}
}
