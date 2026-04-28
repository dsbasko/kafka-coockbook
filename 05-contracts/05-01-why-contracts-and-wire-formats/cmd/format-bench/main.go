// format-bench — записывает один и тот же набор Order'ов в три топика,
// сериализуя их JSON / Avro / Protobuf, и потом печатает таблицу с
// размерами на диске.
//
// Что показывает лекция 05-01:
//
//   - один и тот же Order, та же модель, три разных wire-формата;
//   - JSON через encoding/json — тут схема живёт только в head'е автора;
//   - Avro через hamba/avro — schema-driven, .avsc парсится в рантайме;
//   - Protobuf через protowire — тут мы НЕ генерируем код, а вручную
//     раскладываем поля по wire-формату по тегам из proto/order.proto.
//     Полноценный buf-pipeline — в 05-02; в 05-01 показываем, что
//     protobuf-байты — это просто структурированный wire-формат.
//
// После записи зовём kadm.DescribeAllLogDirs и считаем размер каждого
// топика на диске. Реплики суммируются по всем брокерам — это и есть то,
// что Kafka хранит на самом деле.
//
// Запуск: см. Makefile.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protowire"

	kafkactl "github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

// Order — модель в Go. Поля повторяют proto/order.proto и avro/order.avsc.
type Order struct {
	ID            string      `json:"id" avro:"id"`
	CustomerID    string      `json:"customer_id" avro:"customer_id"`
	AmountCents   int64       `json:"amount_cents" avro:"amount_cents"`
	Currency      string      `json:"currency" avro:"currency"`
	CreatedAtUnix int64       `json:"created_at_unix" avro:"created_at_unix"`
	Items         []OrderItem `json:"items" avro:"items"`
}

type OrderItem struct {
	SKU        string `json:"sku" avro:"sku"`
	Quantity   int32  `json:"quantity" avro:"quantity"`
	PriceCents int64  `json:"price_cents" avro:"price_cents"`
}

// avroSchemaJSON — содержимое avro/order.avsc одной строкой. На прод-проектах
// её обычно грузят из файла или из Schema Registry; для лекции встроено,
// чтобы бинарь был самодостаточным.
const avroSchemaJSON = `{
	"type": "record",
	"name": "Order",
	"namespace": "orders.v1",
	"fields": [
		{ "name": "id",              "type": "string" },
		{ "name": "customer_id",     "type": "string" },
		{ "name": "amount_cents",    "type": "long"   },
		{ "name": "currency",        "type": "string" },
		{ "name": "created_at_unix", "type": "long"   },
		{
			"name": "items",
			"type": {
				"type": "array",
				"items": {
					"type": "record",
					"name": "OrderItem",
					"fields": [
						{ "name": "sku",         "type": "string" },
						{ "name": "quantity",    "type": "int"    },
						{ "name": "price_cents", "type": "long"   }
					]
				}
			}
		}
	]
}`

func main() {
	logger := log.New()

	count := flag.Int("count", 100_000, "сколько Order'ов записать в каждый топик")
	itemsPerOrder := flag.Int("items", 3, "сколько OrderItem в каждом Order'е")
	jsonTopic := flag.String("json-topic", "lecture-05-01-orders-json", "топик для JSON-варианта")
	avroTopic := flag.String("avro-topic", "lecture-05-01-orders-avro", "топик для Avro-варианта")
	protoTopic := flag.String("proto-topic", "lecture-05-01-orders-proto", "топик для Protobuf-варианта")
	skipDescribe := flag.Bool("skip-describe", false, "не запрашивать DescribeLogDirs (например, если на стенде нет прав)")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	cl, err := kafkactl.NewClient()
	if err != nil {
		logger.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)

	avroSchema, err := avro.Parse(avroSchemaJSON)
	if err != nil {
		logger.Error("parse avro schema", "err", err)
		os.Exit(1)
	}

	// Один и тот же набор Order'ов уйдёт во все три топика — это критично
	// для честного сравнения форматов.
	orders := generateOrders(*count, *itemsPerOrder)

	// Чистый прогон каждый раз — пересоздаём топики.
	for _, t := range []string{*jsonTopic, *avroTopic, *protoTopic} {
		if err := recreateTopic(ctx, adm, t); err != nil {
			logger.Error("recreate topic", "topic", t, "err", err)
			os.Exit(1)
		}
	}

	type stat struct {
		name        string
		topic       string
		bytesOnWire int64
		duration    time.Duration
	}
	stats := []stat{
		{name: "JSON", topic: *jsonTopic},
		{name: "Avro", topic: *avroTopic},
		{name: "Protobuf", topic: *protoTopic},
	}

	encoders := []func(*Order) ([]byte, error){
		encodeJSON,
		func(o *Order) ([]byte, error) { return avro.Marshal(avroSchema, o) },
		encodeProto,
	}

	for i := range stats {
		logger.Info("encoding+publishing", "format", stats[i].name, "count", *count, "topic", stats[i].topic)
		t0 := time.Now()
		bytes, err := publishAll(ctx, cl, stats[i].topic, orders, encoders[i])
		if err != nil {
			logger.Error("publish", "format", stats[i].name, "err", err)
			os.Exit(1)
		}
		stats[i].bytesOnWire = bytes
		stats[i].duration = time.Since(t0)
	}

	// Дать брокеру дописать сегменты на диск перед DescribeLogDirs.
	time.Sleep(2 * time.Second)

	fmt.Println()
	fmt.Println("=== payload bytes (только value, без headers/key/overhead Kafka) ===")
	fmt.Printf("%-10s %-40s %14s %12s %14s\n", "format", "topic", "payload", "avg/rec", "elapsed")
	for _, s := range stats {
		avg := float64(s.bytesOnWire) / float64(*count)
		fmt.Printf("%-10s %-40s %14d %12.2f %14s\n", s.name, s.topic, s.bytesOnWire, avg, s.duration.Round(time.Millisecond))
	}

	if *skipDescribe {
		return
	}

	sizes, err := topicDiskSizes(ctx, adm, []string{*jsonTopic, *avroTopic, *protoTopic})
	if err != nil {
		logger.Warn("DescribeLogDirs не отработал — payload bytes выше", "err", err)
		return
	}

	fmt.Println()
	fmt.Println("=== on-disk bytes (kadm.DescribeAllLogDirs, сумма по всем репликам) ===")
	fmt.Printf("%-40s %14s\n", "topic", "bytes")
	for _, t := range []string{*jsonTopic, *avroTopic, *protoTopic} {
		fmt.Printf("%-40s %14d\n", t, sizes[t])
	}
}

func generateOrders(count, items int) []*Order {
	rng := rand.New(rand.NewPCG(42, 1337))
	currencies := []string{"USD", "EUR", "RUB", "BTC"}
	out := make([]*Order, 0, count)
	for i := 0; i < count; i++ {
		o := &Order{
			ID:            fmt.Sprintf("ord-%010d", i),
			CustomerID:    fmt.Sprintf("cust-%06d", rng.IntN(1_000_000)),
			AmountCents:   int64(rng.IntN(100_000_00)),
			Currency:      currencies[rng.IntN(len(currencies))],
			CreatedAtUnix: time.Now().Unix() - int64(rng.IntN(30*24*3600)),
			Items:         make([]OrderItem, items),
		}
		for j := 0; j < items; j++ {
			o.Items[j] = OrderItem{
				SKU:        fmt.Sprintf("sku-%05d", rng.IntN(50_000)),
				Quantity:   int32(1 + rng.IntN(5)),
				PriceCents: int64(rng.IntN(50_000)),
			}
		}
		out = append(out, o)
	}
	return out
}

func publishAll(ctx context.Context, cl *kgo.Client, topic string, orders []*Order, encode func(*Order) ([]byte, error)) (int64, error) {
	const batch = 500
	var total int64

	records := make([]*kgo.Record, 0, batch)
	flush := func() error {
		if len(records) == 0 {
			return nil
		}
		results := cl.ProduceSync(ctx, records...)
		for _, r := range results {
			if r.Err != nil {
				return fmt.Errorf("produce: %w", r.Err)
			}
		}
		records = records[:0]
		return nil
	}

	for i, o := range orders {
		payload, err := encode(o)
		if err != nil {
			return 0, fmt.Errorf("encode order #%d: %w", i, err)
		}
		total += int64(len(payload))
		records = append(records, &kgo.Record{Topic: topic, Key: []byte(o.ID), Value: payload})
		if len(records) >= batch {
			if err := flush(); err != nil {
				return 0, err
			}
		}
	}
	return total, flush()
}

func encodeJSON(o *Order) ([]byte, error) {
	return json.Marshal(o)
}

// encodeProto — раскладывает Order в protobuf wire-формат вручную, по тегам
// из proto/order.proto. Никакого код-гена. Видно, как в реальности устроен
// бинарный protobuf: tag (field_number << 3 | wire_type), потом value.
func encodeProto(o *Order) ([]byte, error) {
	var buf []byte
	buf = appendString(buf, 1, o.ID)
	buf = appendString(buf, 2, o.CustomerID)
	buf = appendInt64(buf, 3, o.AmountCents)
	buf = appendString(buf, 4, o.Currency)
	buf = appendInt64(buf, 5, o.CreatedAtUnix)
	for i := range o.Items {
		item := encodeOrderItem(&o.Items[i])
		buf = protowire.AppendTag(buf, 6, protowire.BytesType)
		buf = protowire.AppendBytes(buf, item)
	}
	return buf, nil
}

func encodeOrderItem(it *OrderItem) []byte {
	var buf []byte
	buf = appendString(buf, 1, it.SKU)
	buf = appendInt32(buf, 2, it.Quantity)
	buf = appendInt64(buf, 3, it.PriceCents)
	return buf
}

func appendString(buf []byte, fieldNum protowire.Number, v string) []byte {
	if v == "" {
		return buf // proto3: пустые строки на wire не пишутся
	}
	buf = protowire.AppendTag(buf, fieldNum, protowire.BytesType)
	return protowire.AppendString(buf, v)
}

func appendInt64(buf []byte, fieldNum protowire.Number, v int64) []byte {
	if v == 0 {
		return buf
	}
	buf = protowire.AppendTag(buf, fieldNum, protowire.VarintType)
	return protowire.AppendVarint(buf, uint64(v))
}

func appendInt32(buf []byte, fieldNum protowire.Number, v int32) []byte {
	if v == 0 {
		return buf
	}
	buf = protowire.AppendTag(buf, fieldNum, protowire.VarintType)
	return protowire.AppendVarint(buf, uint64(int64(v)))
}

// recreateTopic удаляет топик (если есть) и создаёт заново с 1 партицией.
// Одна партиция — чтобы DescribeLogDirs давал чистое число без размазывания
// по партициям.
func recreateTopic(ctx context.Context, adm *kadm.Client, topic string) error {
	resp, err := adm.DeleteTopic(ctx, topic)
	if err == nil && resp.Err != nil {
		// Ошибка из самого ответа — обычно UnknownTopicOrPartition, нас устраивает.
		if !strings.Contains(resp.Err.Error(), "UNKNOWN_TOPIC_OR_PARTITION") {
			return fmt.Errorf("DeleteTopic %s: %w", topic, resp.Err)
		}
	}
	// Дать controller'у обработать удаление.
	time.Sleep(800 * time.Millisecond)

	cresp, err := adm.CreateTopic(ctx, 1, 3, nil, topic)
	if err != nil {
		return fmt.Errorf("CreateTopic %s: %w", topic, err)
	}
	if cresp.Err != nil && !strings.Contains(cresp.Err.Error(), "TOPIC_ALREADY_EXISTS") {
		return fmt.Errorf("CreateTopic %s: %w", topic, cresp.Err)
	}
	return nil
}

// topicDiskSizes собирает суммарный размер на диске для каждого топика по всем
// репликам. На стенде RF=3, так что итог делится примерно на 3, чтобы получить
// размер «полезной нагрузки на одну реплику».
func topicDiskSizes(ctx context.Context, adm *kadm.Client, topics []string) (map[string]int64, error) {
	want := make(map[string]struct{}, len(topics))
	out := make(map[string]int64, len(topics))
	for _, t := range topics {
		want[t] = struct{}{}
		out[t] = 0
	}

	dirs, err := adm.DescribeAllLogDirs(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("DescribeAllLogDirs: %w", err)
	}

	dirs.Each(func(d kadm.DescribedLogDir) {
		if d.Err != nil {
			return
		}
		d.Topics.Each(func(p kadm.DescribedLogDirPartition) {
			if _, ok := want[p.Topic]; !ok {
				return
			}
			out[p.Topic] += p.Size
		})
	})

	return out, nil
}
