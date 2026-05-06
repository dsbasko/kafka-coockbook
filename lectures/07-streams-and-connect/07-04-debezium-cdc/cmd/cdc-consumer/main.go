// cdc-consumer — читает CDC-события Debezium из топика `cdc.public.users`
// и печатает их в человекочитаемом виде.
//
// Формат Debezium event'а (JsonConverter без schemas.enable):
//
//	{
//	  "before": { "id": 42, "email": "...", "status": "active", ... } | null,
//	  "after":  { "id": 42, "email": "...", "status": "blocked", ... } | null,
//	  "source": { "version": "...", "connector": "...", "ts_ms": ..., "lsn": ... },
//	  "op":     "c" | "u" | "d" | "r" | "t",
//	  "ts_ms":  1714723200000
//	}
//
// op:
//   - "c" — INSERT (create)
//   - "u" — UPDATE (before и after оба заполнены, потому что REPLICA IDENTITY FULL)
//   - "d" — DELETE (after = null)
//   - "r" — read (snapshot, начальное наполнение)
//   - "t" — truncate
//
// Tombstone — отдельный record с value=null после "d", специально для compact'а.
// Параметр tombstones.on.delete=true в connector-конфиге включает их.
//
// Параллельно подписываемся на outbox-роутер: для каждого aggregate_type
// создаётся свой топик events.<type>. Подписываемся по wildcard через
// regex-форму ConsumerGroup'а с topics=events.*. Это удобный паттерн,
// когда заранее не знаешь все aggregate-типы.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	cdcTopic     = "cdc.public.users"
	defaultGroup = "lecture-07-04-cdc-consumer"
	// Один регэксп покрывает и cdc-топик users, и весь набор outbox-топиков
	// events.<aggregate_type>. ConsumeRegex включает интерпретацию строк
	// в ConsumeTopics как регулярных выражений.
	subscribeRegex = `^cdc\.public\.users$|^events\..+$`
)

func main() {
	logger := log.New()

	ctx, cancel := runctx.New()
	defer cancel()

	cl, err := kafka.NewClient(
		kgo.ConsumerGroup(defaultGroup),
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics(subscribeRegex),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		logger.Error("kafka.NewClient", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	fmt.Printf("слушаю по регэкспу %q (group=%s)\n\n",
		subscribeRegex, defaultGroup)

	if err := loop(ctx, cl); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("consume failed", "err", err)
		os.Exit(1)
	}
}

func loop(ctx context.Context, cl *kgo.Client) error {
	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return e.Err
				}
				fmt.Fprintf(os.Stderr, "fetch error topic=%s partition=%d: %v\n",
					e.Topic, e.Partition, e.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			handle(r)
		})
	}
}

func handle(r *kgo.Record) {
	if strings.HasPrefix(r.Topic, "events.") {
		printOutbox(r)
		return
	}
	printDebezium(r)
}

// debeziumEnvelope покрывает только то, что нам нужно для печати; остальные
// поля молча игнорируются.
type debeziumEnvelope struct {
	Before map[string]any `json:"before"`
	After  map[string]any `json:"after"`
	Op     string         `json:"op"`
	Source map[string]any `json:"source"`
	TsMs   int64          `json:"ts_ms"`
}

func printDebezium(r *kgo.Record) {
	if r.Value == nil {
		fmt.Printf("[CDC tombstone] topic=%s partition=%d offset=%d key=%s\n",
			r.Topic, r.Partition, r.Offset, string(r.Key))
		return
	}
	var env debeziumEnvelope
	if err := json.Unmarshal(r.Value, &env); err != nil {
		fmt.Printf("[CDC parse error] topic=%s offset=%d: %v\nraw=%s\n",
			r.Topic, r.Offset, err, string(r.Value))
		return
	}
	opLabel := opName(env.Op)
	fmt.Printf("[CDC %s] topic=%s partition=%d offset=%d key=%s\n",
		opLabel, r.Topic, r.Partition, r.Offset, string(r.Key))
	if env.Before != nil {
		fmt.Printf("  before: %s\n", compactJSON(env.Before))
	}
	if env.After != nil {
		fmt.Printf("  after:  %s\n", compactJSON(env.After))
	}
	if lsn, ok := env.Source["lsn"]; ok {
		fmt.Printf("  source: ts_ms=%d lsn=%v\n", env.TsMs, lsn)
	}
	fmt.Println()
}

func printOutbox(r *kgo.Record) {
	fmt.Printf("[OUTBOX] topic=%s partition=%d offset=%d key=%s\n",
		r.Topic, r.Partition, r.Offset, string(r.Key))
	for _, h := range r.Headers {
		fmt.Printf("  header %s = %s\n", h.Key, string(h.Value))
	}
	if r.Value == nil {
		fmt.Println("  value: <null>")
	} else {
		fmt.Printf("  value: %s\n", string(r.Value))
	}
	fmt.Println()
}

func opName(op string) string {
	switch op {
	case "c":
		return "INSERT"
	case "u":
		return "UPDATE"
	case "d":
		return "DELETE"
	case "r":
		return "SNAPSHOT"
	case "t":
		return "TRUNCATE"
	default:
		return "op=" + op
	}
}

func compactJSON(m map[string]any) string {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Sprintf("%v", m)
	}
	return string(b)
}
