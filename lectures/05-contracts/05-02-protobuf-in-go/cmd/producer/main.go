package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/05-contracts/05-02-protobuf-in-go/gen/orders/v1"
	kafkactl "github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	topic := flag.String("topic", "lecture-05-02-orders-proto", "топик для записи")
	count := flag.Int("count", 10, "сколько заказов произвести")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	cl, err := kafkactl.NewClient(
		kgo.DefaultProduceTopic(*topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		logger.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	for i := 0; i < *count; i++ {
		order := mockOrder(i)

		payload, err := proto.Marshal(order)
		if err != nil {
			logger.Error("proto marshal", "err", err)
			os.Exit(1)
		}

		rec := &kgo.Record{
			Topic: *topic,
			Key:   []byte(order.GetId()),
			Value: payload,
			Headers: []kgo.RecordHeader{
				{Key: "content-type", Value: []byte("application/x-protobuf")},
				{Key: "schema", Value: []byte("orders.v1.Order")},
			},
		}

		res := cl.ProduceSync(ctx, rec)
		if err := res.FirstErr(); err != nil {
			logger.Error("produce", "err", err)
			os.Exit(1)
		}

		out := res[0].Record
		fmt.Printf("ok  id=%s status=%s items=%d bytes=%d -> %s/%d@%d\n",
			order.GetId(),
			order.GetStatus().String(),
			len(order.GetItems()),
			len(payload),
			out.Topic, out.Partition, out.Offset,
		)
	}

	if err := cl.Flush(ctx); err != nil {
		logger.Error("flush", "err", err)
		os.Exit(1)
	}
	logger.Info("producer done", "count", *count, "topic", *topic)
}

func mockOrder(i int) *ordersv1.Order {
	statuses := []ordersv1.OrderStatus{
		ordersv1.OrderStatus_ORDER_STATUS_CREATED,
		ordersv1.OrderStatus_ORDER_STATUS_PAID,
		ordersv1.OrderStatus_ORDER_STATUS_SHIPPED,
	}
	status := statuses[i%len(statuses)]

	items := make([]*ordersv1.OrderItem, 0, 1+i%3)
	for j := 0; j <= i%3; j++ {
		items = append(items, &ordersv1.OrderItem{
			Sku:        fmt.Sprintf("sku-%03d", rand.IntN(1000)),
			Quantity:   int32(1 + rand.IntN(5)),
			PriceCents: int64(100 + rand.IntN(9000)),
		})
	}

	note := fmt.Sprintf("order #%d auto-generated", i)

	return &ordersv1.Order{
		Id:             fmt.Sprintf("ord-%05d", i),
		CustomerId:     fmt.Sprintf("cus-%03d", rand.IntN(100)),
		AmountCents:    int64(1000 + rand.IntN(50000)),
		Currency:       "USD",
		CreatedAtUnix:  time.Now().Unix(),
		Items:          items,
		Status:         status,
		CreatedAt:      timestamppb.Now(),
		ReservationTtl: durationpb.New(15 * time.Minute),
		Note:           &note,
	}
}
