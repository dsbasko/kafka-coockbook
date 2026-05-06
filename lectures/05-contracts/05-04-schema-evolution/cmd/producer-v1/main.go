// producer-v1 — пишет Order'ы по схеме v1 (id, customer_id, amount_cents)
// в топик `lecture-05-04-orders-v1`, регистрирует свою схему в subject
// `<topic>-value`.
//
// Это базовая ситуация «всё началось с v1»: один producer, один
// consumer, одна схема. Дальше из неё будет вырастать v3 (см.
// producer-v3); здесь специально показано, что v1 — это нормальный,
// валидный, законченный контракт сам по себе.
//
// Запуск: см. Makefile (`make run-producer-v1`).
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/proto"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/05-contracts/05-04-schema-evolution/gen/orders/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	kafkactl "github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const orderProtoSchemaV1 = `syntax = "proto3";
package orders.v1;

message Order {
  string id = 1;
  string customer_id = 2;
  int64 amount_cents = 3;
}
`

func main() {
	logger := log.New()

	topic := flag.String("topic", "lecture-05-04-orders-v1", "топик для записи")
	count := flag.Int("count", 5, "сколько заказов произвести")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	srURL := config.EnvOr("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	subject := *topic + "-value"

	id, err := registerSchemaV1(ctx, srURL, subject)
	if err != nil {
		logger.Error("register schema", "err", err)
		os.Exit(1)
	}
	logger.Info("schema registered", "subject", subject, "version", "v1", "id", id)

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
		o := mockOrderV1(i)
		payload, err := serde.Encode(o)
		if err != nil {
			logger.Error("serde encode", "err", err)
			os.Exit(1)
		}
		rec := &kgo.Record{
			Topic: *topic,
			Key:   []byte(o.GetId()),
			Value: payload,
		}
		res := cl.ProduceSync(ctx, rec)
		if err := res.FirstErr(); err != nil {
			logger.Error("produce", "err", err)
			os.Exit(1)
		}
		out := res[0].Record
		fmt.Printf("v1 ok  id=%s amount=%d schema_id=%d -> %s/%d@%d\n",
			o.GetId(), o.GetAmountCents(), id,
			out.Topic, out.Partition, out.Offset,
		)
	}

	if err := cl.Flush(ctx); err != nil {
		logger.Error("flush", "err", err)
		os.Exit(1)
	}
	logger.Info("producer-v1 done", "count", *count, "topic", *topic)
}

func registerSchemaV1(ctx context.Context, url, subject string) (int, error) {
	cl, err := sr.NewClient(sr.URLs(url))
	if err != nil {
		return 0, fmt.Errorf("sr client: %w", err)
	}
	ss, err := cl.CreateSchema(ctx, subject, sr.Schema{
		Schema: orderProtoSchemaV1,
		Type:   sr.TypeProtobuf,
	})
	if err != nil {
		return 0, fmt.Errorf("create schema: %w", err)
	}
	return ss.ID, nil
}

func mockOrderV1(i int) *ordersv1.Order {
	return &ordersv1.Order{
		Id:          fmt.Sprintf("ord-v1-%05d", i),
		CustomerId:  fmt.Sprintf("cus-%03d", rand.IntN(100)),
		AmountCents: int64(1000 + rand.IntN(50000)),
	}
}
