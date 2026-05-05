//go:build integration

// Дополнительный integration-тест уровня pipeline-component: проверяет,
// что Go-часть use case'а (anonymizer) корректно трансформирует Debezium-
// событие в clean-сообщение для analytics-топика. Не требует Kafka Connect
// и плагинов — мы сами публикуем JSON в формате Debezium в cdc.public.*
// и читаем результат из analytics.*.
//
// Этот тест дополняет TestEndToEnd_PgToClickHouse: тот покрывает полный E2E
// (Postgres → Debezium → Anonymizer → ClickHouse Sink → ClickHouse) и
// требует kafka-connect с установленными плагинами. Этот же тест работает
// везде, где доступна только Kafka, и валидирует ровно то, что мы пишем
// своими руками — anonymizer.
package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/03-pg-to-clickhouse/internal/anonymizer"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
)

const (
	anonymizerOnlyGroup = "usecase-09-03-it-anonymizer-only"
	verifierGroup       = "usecase-09-03-it-verifier"
)

func TestAnonymizer_GoPipeline(t *testing.T) {
	root, rootCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer rootCancel()

	bootstrap := config.EnvOr("KAFKA_BOOTSTRAP", defaultBootstrap)
	if err := pingKafka(root, bootstrap); err != nil {
		t.Skipf("Kafka недоступна: %v", err)
	}

	if err := recreateTopics(root, bootstrap, allTopics); err != nil {
		t.Fatalf("recreate topics: %v", err)
	}
	if err := resetConsumerGroups(root, bootstrap, []string{anonymizerOnlyGroup, verifierGroup}); err != nil {
		t.Fatalf("reset cg: %v", err)
	}

	cfg, err := anonymizer.LoadConfig("../anonymize.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	// Поднимаем anonymizer в горутине.
	anonCtx, anonCancel := context.WithCancel(root)
	anonErr := new(atomic.Value)
	anonDone := make(chan struct{})
	go func() {
		defer close(anonDone)
		err := anonymizer.Run(anonCtx, anonymizer.RunOpts{
			NodeID:      "anon-only",
			Bootstrap:   splitCSV(bootstrap),
			Group:       anonymizerOnlyGroup,
			SourceRegex: `^cdc\.public\.(users|orders|events)$`,
			Config:      cfg,
			FromStart:   true,
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			anonErr.Store(err)
		}
	}()
	defer func() {
		anonCancel()
		<-anonDone
		if v := anonErr.Load(); v != nil {
			t.Errorf("anonymizer finished with error: %v", v)
		}
	}()

	// Шлём в cdc.public.users событие в формате Debezium.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(splitCSV(bootstrap)...),
		kgo.ClientID("usecase-09-03-it-emit"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		t.Fatalf("emit client: %v", err)
	}
	defer cl.Close()

	emitDebeziumUser(t, cl, root, "c", 1, map[string]any{
		"id":         float64(1),
		"email":      "alice@example.com",
		"phone":      "+79001234567",
		"full_name":  "Alice Wonderland",
		"birth_date": "1990-01-01",
		"country":    "RU",
	}, 100)
	emitDebeziumUser(t, cl, root, "u", 2, map[string]any{
		"id":         float64(2),
		"email":      "bob@example.com",
		"phone":      "+79007654321",
		"full_name":  "Bob TheBuilder",
		"birth_date": "1985-05-05",
		"country":    "BY",
	}, 200)
	emitDebeziumOrder(t, cl, root, "c", 1, map[string]any{
		"id":           float64(1),
		"user_id":      float64(1),
		"amount_cents": float64(50000),
		"currency":     "RUB",
		"status":       "paid",
		"notes":        "private note about user",
		"created_at":   "2026-05-04T10:00:00Z",
	}, 300)
	emitDebeziumEvent(t, cl, root, "d", 7, map[string]any{
		"id":         float64(7),
		"user_id":    float64(99),
		"event_type": "logout",
		"ip_address": "10.0.0.7",
		"user_agent": "Chrome",
	}, 400)

	// Читаем analytics-топики, собираем по 1 сообщению на каждый из трёх.
	verifier, err := kgo.NewClient(
		kgo.SeedBrokers(splitCSV(bootstrap)...),
		kgo.ClientID("usecase-09-03-it-verify"),
		kgo.ConsumerGroup(verifierGroup),
		kgo.ConsumeTopics(analyticsTopics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("verifier client: %v", err)
	}
	defer verifier.Close()

	// Ожидаем 2 user-события (c+u), 1 order, 1 event-delete.
	expected := map[string]int{
		"analytics.users":  2,
		"analytics.orders": 1,
		"analytics.events": 1,
	}
	deadline := time.Now().Add(45 * time.Second)
	got := make(map[string][]map[string]any)

	allCollected := func() bool {
		for topic, n := range expected {
			if len(got[topic]) < n {
				return false
			}
		}
		return true
	}

	for time.Now().Before(deadline) && !allCollected() {
		pollCtx, cancel := context.WithTimeout(root, 3*time.Second)
		fetches := verifier.PollFetches(pollCtx)
		cancel()
		fetches.EachRecord(func(r *kgo.Record) {
			var payload map[string]any
			if err := json.Unmarshal(r.Value, &payload); err == nil {
				got[r.Topic] = append(got[r.Topic], payload)
			}
		})
	}

	for topic, want := range expected {
		if len(got[topic]) < want {
			t.Fatalf("за 45s по %s собрали %d из %d сообщений", topic, len(got[topic]), want)
		}
	}

	// users: оба события (id=1 INSERT, id=2 UPDATE) должны быть с
	// hex-hash email/phone, truncated full_name, без birth_date, _deleted=0.
	for _, u := range got["analytics.users"] {
		if h, _ := u["email"].(string); len(h) != 64 {
			t.Errorf("expected sha256 hex email, got %q (len=%d)", h, len(h))
		}
		if h, _ := u["phone"].(string); len(h) != 64 {
			t.Errorf("expected sha256 hex phone, got %q", h)
		}
		if name, _ := u["full_name"].(string); !strings.HasSuffix(name, ".") {
			t.Errorf("expected truncated full_name, got %q", name)
		}
		if _, ok := u["birth_date"]; ok {
			t.Errorf("birth_date должен быть дропнут, но он есть: %v", u["birth_date"])
		}
		if d, _ := u["_deleted"]; toUint(d) != 0 {
			t.Errorf("expected _deleted=0 for INSERT/UPDATE, got %v", d)
		}
		if l, _ := u["cdc_lsn"]; toUint(l) == 0 {
			t.Errorf("cdc_lsn должен быть >0, got %v", l)
		}
	}

	// orders: notes дропнут.
	orders := got["analytics.orders"][0]
	if _, ok := orders["notes"]; ok {
		t.Errorf("orders.notes должен быть дропнут: %v", orders["notes"])
	}
	if s, _ := orders["status"].(string); s != "paid" {
		t.Errorf("orders.status passthrough: got %q", s)
	}

	// events: ip_address и user_agent дропнуты, _deleted=1 (op=d).
	ev := got["analytics.events"][0]
	if _, ok := ev["ip_address"]; ok {
		t.Errorf("events.ip_address должен быть дропнут")
	}
	if _, ok := ev["user_agent"]; ok {
		t.Errorf("events.user_agent должен быть дропнут")
	}
	if d, _ := ev["_deleted"]; toUint(d) != 1 {
		t.Errorf("expected _deleted=1 for DELETE, got %v", d)
	}
	if et, _ := ev["event_type"].(string); et != "logout" {
		t.Errorf("events.event_type passthrough: got %q", et)
	}
}

func emitDebeziumUser(t *testing.T, cl *kgo.Client, ctx context.Context, op string, key int, after map[string]any, lsn float64) {
	t.Helper()
	emit(t, cl, ctx, "cdc.public.users", op, key, after, lsn)
}

func emitDebeziumOrder(t *testing.T, cl *kgo.Client, ctx context.Context, op string, key int, after map[string]any, lsn float64) {
	t.Helper()
	emit(t, cl, ctx, "cdc.public.orders", op, key, after, lsn)
}

func emitDebeziumEvent(t *testing.T, cl *kgo.Client, ctx context.Context, op string, key int, before map[string]any, lsn float64) {
	t.Helper()
	envelope := map[string]any{
		"before": before,
		"after":  nil,
		"op":     "d",
		"source": map[string]any{"lsn": lsn, "ts_ms": float64(time.Now().UnixMilli())},
		"ts_ms":  time.Now().UnixMilli(),
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	r := &kgo.Record{
		Topic: "cdc.public.events",
		Key:   []byte(fmt.Sprintf("%d", key)),
		Value: body,
	}
	if op != "d" {
		t.Fatalf("emitDebeziumEvent предполагает op=d, got %q", op)
	}
	res := cl.ProduceSync(ctx, r)
	if err := res.FirstErr(); err != nil {
		t.Fatalf("produce events: %v", err)
	}
}

func emit(t *testing.T, cl *kgo.Client, ctx context.Context, topic, op string, key int, fields map[string]any, lsn float64) {
	t.Helper()
	envelope := map[string]any{
		"op":     op,
		"source": map[string]any{"lsn": lsn, "ts_ms": float64(time.Now().UnixMilli())},
		"ts_ms":  time.Now().UnixMilli(),
	}
	switch op {
	case "c", "r", "u":
		envelope["after"] = fields
	case "d":
		envelope["before"] = fields
	}
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	res := cl.ProduceSync(ctx, &kgo.Record{
		Topic: topic,
		Key:   []byte(fmt.Sprintf("%d", key)),
		Value: body,
	})
	if err := res.FirstErr(); err != nil {
		t.Fatalf("produce %s: %v", topic, err)
	}
}

func toUint(v any) uint64 {
	switch n := v.(type) {
	case float64:
		return uint64(n)
	case int:
		return uint64(n)
	case int64:
		return uint64(n)
	case uint64:
		return n
	}
	return 0
}

// Подавляем "imported and not used" для kadm если внезапно перестанет
// использоваться: kadm нужен в pingKafka и recreateTopics.
var _ = kadm.NewClient
