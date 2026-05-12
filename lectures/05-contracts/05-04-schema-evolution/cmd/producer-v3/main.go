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

	ordersv3 "github.com/dsbasko/kafka-sandbox/lectures/05-contracts/05-04-schema-evolution/gen/orders/v3"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	kafkactl "github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const orderProtoSchemaV3 = `syntax = "proto3";
package orders.v3;

message Address {
  string country = 1;
  string city = 2;
  string street = 3;
}

message Order {
  string id = 1;
  string customer_id = 2;
  int64 amount_cents = 3;
  string currency = 4;
  Address shipping_address = 5;
}
`

func main() {
	logger := log.New()

	topic := flag.String("topic", "lecture-05-04-orders-v3", "топик для записи")
	count := flag.Int("count", 5, "сколько заказов произвести")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	srURL := config.EnvOr("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	subject := *topic + "-value"

	id, err := registerSchemaV3(ctx, srURL, subject)
	if err != nil {
		logger.Error("register schema", "err", err)
		os.Exit(1)
	}
	logger.Info("schema registered", "subject", subject, "version", "v3", "id", id)

	serde := sr.NewSerde()
	serde.Register(
		id,
		&ordersv3.Order{},
		sr.EncodeFn(func(v any) ([]byte, error) {
			return proto.Marshal(v.(*ordersv3.Order))
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
		o := mockOrderV3(i)
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
		fmt.Printf("v3 ok  id=%s amount=%d %s addr=%s/%s schema_id=%d -> %s/%d@%d\n",
			o.GetId(), o.GetAmountCents(), o.GetCurrency(),
			o.GetShippingAddress().GetCountry(), o.GetShippingAddress().GetCity(),
			id, out.Topic, out.Partition, out.Offset,
		)
	}

	if err := cl.Flush(ctx); err != nil {
		logger.Error("flush", "err", err)
		os.Exit(1)
	}
	logger.Info("producer-v3 done", "count", *count, "topic", *topic)
}

func registerSchemaV3(ctx context.Context, url, subject string) (int, error) {
	cl, err := sr.NewClient(sr.URLs(url))
	if err != nil {
		return 0, fmt.Errorf("sr client: %w", err)
	}
	ss, err := cl.CreateSchema(ctx, subject, sr.Schema{
		Schema: orderProtoSchemaV3,
		Type:   sr.TypeProtobuf,
	})
	if err != nil {
		return 0, fmt.Errorf("create schema: %w", err)
	}
	return ss.ID, nil
}

var (
	cities    = []string{"Tashkent", "Samarkand", "Bukhara", "Almaty", "Bishkek"}
	countries = []string{"UZ", "KZ", "KG"}
)

func mockOrderV3(i int) *ordersv3.Order {
	return &ordersv3.Order{
		Id:          fmt.Sprintf("ord-v3-%05d", i),
		CustomerId:  fmt.Sprintf("cus-%03d", rand.IntN(100)),
		AmountCents: int64(1000 + rand.IntN(50000)),
		Currency:    "USD",
		ShippingAddress: &ordersv3.Address{
			Country: countries[rand.IntN(len(countries))],
			City:    cities[rand.IntN(len(cities))],
			Street:  fmt.Sprintf("Some str. %d", 1+rand.IntN(200)),
		},
	}
}
