// producer — пишет N заказов в Kafka, используя Confluent wire format
// через franz-go sr.Serde.
//
// Что показывает лекция 05-03:
//
//   - sr.Client регистрирует Protobuf-схему в Schema Registry под
//     subject'ом "<topic>-value" (TopicNameStrategy);
//   - sr.Serde знает schema_id и Encode-функцию (proto.Marshal); она
//     сама добавляет magic byte (0) + 4 байта big-endian schema_id +
//     обязательный для protobuf message-index перед payload'ом;
//   - получившиеся байты — это Confluent wire format. Любой клиент,
//     который умеет SR (Java, Python, kcat -s value=s -r ...), такие
//     сообщения прочитает не зная заранее, какая схема использовалась.
//
// Запуск: см. Makefile.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/05-contracts/05-03-schema-registry/gen/orders/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	kafkactl "github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const orderProtoSchema = `syntax = "proto3";
package orders.v1;
import "google/protobuf/timestamp.proto";

enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  ORDER_STATUS_CREATED = 1;
  ORDER_STATUS_PAID = 2;
  ORDER_STATUS_SHIPPED = 3;
  ORDER_STATUS_DELIVERED = 4;
  ORDER_STATUS_CANCELLED = 5;
}

message OrderItem {
  string sku = 1;
  int32 quantity = 2;
  int64 price_cents = 3;
}

message Order {
  string id = 1;
  string customer_id = 2;
  int64 amount_cents = 3;
  string currency = 4;
  repeated OrderItem items = 5;
  OrderStatus status = 6;
  google.protobuf.Timestamp created_at = 7;
}
`

func main() {
	logger := log.New()

	topic := flag.String("topic", "lecture-05-03-orders-sr", "топик для записи")
	count := flag.Int("count", 10, "сколько заказов произвести")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	srURL := config.EnvOr("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	subject := *topic + "-value"

	id, err := registerSchema(ctx, srURL, subject)
	if err != nil {
		logger.Error("register schema", "err", err)
		os.Exit(1)
	}
	logger.Info("schema registered", "subject", subject, "id", id)

	serde := sr.NewSerde()
	serde.Register(
		id,
		&ordersv1.Order{},
		sr.EncodeFn(func(v any) ([]byte, error) {
			return proto.Marshal(v.(*ordersv1.Order))
		}),
		sr.Index(0),
	)

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

		payload, err := serde.Encode(order)
		if err != nil {
			logger.Error("serde encode", "err", err)
			os.Exit(1)
		}

		rec := &kgo.Record{
			Topic: *topic,
			Key:   []byte(order.GetId()),
			Value: payload,
		}

		res := cl.ProduceSync(ctx, rec)
		if err := res.FirstErr(); err != nil {
			logger.Error("produce", "err", err)
			os.Exit(1)
		}

		out := res[0].Record
		fmt.Printf("ok  id=%s status=%-25s magic=0x%02x schema_id=%d bytes=%d -> %s/%d@%d\n",
			order.GetId(),
			order.GetStatus().String(),
			payload[0],
			int(payload[1])<<24|int(payload[2])<<16|int(payload[3])<<8|int(payload[4]),
			len(payload),
			out.Topic, out.Partition, out.Offset,
		)
	}

	if err := cl.Flush(ctx); err != nil {
		logger.Error("flush", "err", err)
		os.Exit(1)
	}
	logger.Info("producer done", "count", *count, "topic", *topic, "schema_id", id)
}

// registerSchema публикует Protobuf-схему в SR под нужным subject'ом и
// возвращает globally-unique schema_id. Если схема уже зарегистрирована
// под этим subject'ом — SR вернёт тот же id, без создания новой версии.
func registerSchema(ctx context.Context, url, subject string) (int, error) {
	cl, err := sr.NewClient(sr.URLs(url))
	if err != nil {
		return 0, fmt.Errorf("sr client: %w", err)
	}

	ss, err := cl.CreateSchema(ctx, subject, sr.Schema{
		Schema: orderProtoSchema,
		Type:   sr.TypeProtobuf,
	})
	if err != nil {
		return 0, fmt.Errorf("create schema: %w", err)
	}
	return ss.ID, nil
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

	return &ordersv1.Order{
		Id:          fmt.Sprintf("ord-%05d", i),
		CustomerId:  fmt.Sprintf("cus-%03d", rand.IntN(100)),
		AmountCents: int64(1000 + rand.IntN(50000)),
		Currency:    "USD",
		Items:       items,
		Status:      status,
		CreatedAt:   timestamppb.New(time.Now()),
	}
}
