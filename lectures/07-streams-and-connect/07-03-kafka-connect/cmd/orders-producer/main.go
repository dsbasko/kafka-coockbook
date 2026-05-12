package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic       = "lecture-07-03-orders"
	defaultPartitions  = 3
	defaultReplication = 3
	defaultCount       = 100
	defaultStartID     = 1
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик, в который пишем заказы")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании")
	rf := flag.Int("rf", defaultReplication, "replication factor при создании")
	count := flag.Int("count", defaultCount, "сколько сообщений отправить")
	startID := flag.Int("start-id", defaultStartID, "первый id заказа (повторный запуск с тем же id => UPSERT)")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topic:      *topic,
		partitions: int32(*partitions),
		rf:         int16(*rf),
		count:      *count,
		startID:    int64(*startID),
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("orders-producer failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic      string
	partitions int32
	rf         int16
	count      int
	startID    int64
}

type orderEnvelope struct {
	Schema  connectSchema `json:"schema"`
	Payload orderPayload  `json:"payload"`
}

type orderPayload struct {
	ID         int64   `json:"id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	CreatedAt  string  `json:"created_at"`
}

type connectSchema struct {
	Type     string         `json:"type"`
	Optional bool           `json:"optional"`
	Name     string         `json:"name"`
	Fields   []connectField `json:"fields"`
}

type connectField struct {
	Field    string `json:"field"`
	Type     string `json:"type"`
	Optional bool   `json:"optional"`
}

func ordersSchema() connectSchema {
	return connectSchema{
		Type:     "struct",
		Optional: false,
		Name:     "lecture_07_03.orders",
		Fields: []connectField{
			{Field: "id", Type: "int64", Optional: false},
			{Field: "customer_id", Type: "string", Optional: false},
			{Field: "amount", Type: "double", Optional: false},
			{Field: "status", Type: "string", Optional: false},
			{Field: "created_at", Type: "string", Optional: false},
		},
	}
}

func run(ctx context.Context, o runOpts) error {
	cl, err := kafka.NewClient()
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)

	if err := ensureTopic(ctx, admin, o); err != nil {
		return fmt.Errorf("ensure topic: %w", err)
	}

	fmt.Printf("пишем %d заказов в %q (start-id=%d). Sink сам делает UPSERT по id.\n\n",
		o.count, o.topic, o.startID)

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "N\tID\tCUSTOMER\tAMOUNT\tPARTITION\tOFFSET")

	schema := ordersSchema()

	for i := 0; i < o.count; i++ {
		if err := ctx.Err(); err != nil {
			_ = tw.Flush()
			return err
		}

		id := o.startID + int64(i)
		customerID := fmt.Sprintf("cust-%d", rand.IntN(50))
		amount := 100 + rand.Float64()*900

		envelope := orderEnvelope{
			Schema: schema,
			Payload: orderPayload{
				ID:         id,
				CustomerID: customerID,
				Amount:     amount,
				Status:     "created",
				CreatedAt:  time.Now().UTC().Format(time.RFC3339Nano),
			},
		}
		valueBytes, err := json.Marshal(envelope)
		if err != nil {
			_ = tw.Flush()
			return fmt.Errorf("marshal envelope: %w", err)
		}

		rec := &kgo.Record{
			Topic: o.topic,
			Key:   []byte(fmt.Sprintf("%d", id)),
			Value: valueBytes,
		}

		rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
		res := cl.ProduceSync(rpcCtx, rec)
		rpcCancel()
		if err := res.FirstErr(); err != nil {
			_ = tw.Flush()
			return fmt.Errorf("produce %d: %w", i, err)
		}
		got := res[0].Record
		fmt.Fprintf(tw, "%d\t%d\t%s\t%.2f\t%d\t%d\n",
			i+1, id, customerID, amount, got.Partition, got.Offset)
	}
	_ = tw.Flush()

	fmt.Println()
	fmt.Println("готово. Дальше: make db-count — должно совпасть с числом уникальных id.")
	return nil
}

func ensureTopic(ctx context.Context, admin *kadm.Client, o runOpts) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	resp, err := admin.CreateTopic(rpcCtx, o.partitions, o.rf, nil, o.topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("topic %q создан: partitions=%d rf=%d\n\n", o.topic, o.partitions, o.rf)
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if errors.Is(cause, kerr.TopicAlreadyExists) {
		fmt.Printf("topic %q уже существует — пишем как есть\n\n", o.topic)
		return nil
	}
	return cause
}
