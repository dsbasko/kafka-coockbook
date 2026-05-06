package anonymizer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// RunOpts описывает запуск anonymizer-консьюмера.
type RunOpts struct {
	NodeID      string
	Bootstrap   []string
	Group       string
	SourceRegex string // напр. `^cdc\.public\.(users|orders|events)$`
	Config      *Config
	FromStart   bool   // для тестов: читать с начала топика
	Logger      *slog.Logger
}

// Run — главная петля. Читает Debezium-события, применяет правила и пишет в
// analytics.<table>. Один и тот же producer client используется для всех
// outgoing-сообщений (idempotent + acks=all включены по дефолту franz-go).
//
// Семантика at-least-once: коммитим offset после batch'а доставленных
// сообщений. Дубль из Postgres'а на стороне ClickHouse схлопнется через
// ReplacingMergeTree(cdc_lsn).
func Run(ctx context.Context, opts RunOpts) error {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Config == nil {
		return errors.New("anonymizer: nil Config")
	}
	if len(opts.Bootstrap) == 0 {
		return errors.New("anonymizer: empty bootstrap")
	}
	if opts.SourceRegex == "" {
		return errors.New("anonymizer: empty SourceRegex")
	}

	consumerOpts := []kgo.Opt{
		kgo.SeedBrokers(opts.Bootstrap...),
		kgo.ClientID("usecase-09-03-anonymizer-" + opts.NodeID),
		kgo.ConsumerGroup(opts.Group),
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics(opts.SourceRegex),
		kgo.DisableAutoCommit(),
	}
	if opts.FromStart {
		consumerOpts = append(consumerOpts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}
	cl, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		return fmt.Errorf("anonymizer consumer: %w", err)
	}
	defer cl.Close()

	producerOpts := []kgo.Opt{
		kgo.SeedBrokers(opts.Bootstrap...),
		kgo.ClientID("usecase-09-03-anonymizer-prod-" + opts.NodeID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	prod, err := kgo.NewClient(producerOpts...)
	if err != nil {
		return fmt.Errorf("anonymizer producer: %w", err)
	}
	defer prod.Close()

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return e.Err
				}
				opts.Logger.Warn("fetch error", "topic", e.Topic, "partition", e.Partition, "err", e.Err)
			}
		}
		if fetches.NumRecords() == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			continue
		}

		var outRecords []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			rec, err := transform(r, opts.Config)
			if err != nil {
				opts.Logger.Warn("transform skipped",
					"topic", r.Topic, "offset", r.Offset, "err", err)
				return
			}
			if rec == nil {
				return
			}
			outRecords = append(outRecords, rec)
		})
		if len(outRecords) > 0 {
			res := prod.ProduceSync(ctx, outRecords...)
			if firstErr := res.FirstErr(); firstErr != nil {
				return fmt.Errorf("produce analytics: %w", firstErr)
			}
		}

		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
	}
}

// debeziumEnvelope — поле value Debezium event'а в формате JsonConverter
// без schemas.enable. Покрывает только то, что нам нужно.
type debeziumEnvelope struct {
	Before map[string]any `json:"before"`
	After  map[string]any `json:"after"`
	Op     string         `json:"op"`
	Source map[string]any `json:"source"`
	TsMs   int64          `json:"ts_ms"`
}

// transform — pure-функция: берёт Debezium-record и возвращает запись в
// analytics-топик (или nil, если событие нужно пропустить, например
// tombstone после DELETE — у нас тип _deleted=1 уже отправлен с before-кадра,
// дублировать не надо).
func transform(r *kgo.Record, cfg *Config) (*kgo.Record, error) {
	if r.Value == nil {
		// Tombstone: один из вариантов — пропустить, потому что _deleted=1
		// уже отправлен из шага d (см. ниже). Это согласуется с логикой
		// ReplacingMergeTree, которой нужен только row с большим cdc_lsn.
		return nil, nil
	}
	table := tableNameFromTopic(r.Topic)
	if table == "" {
		return nil, fmt.Errorf("cannot derive table from topic %q", r.Topic)
	}
	if cfg.TargetTopic(table) == "" {
		return nil, nil // table не описана в YAML — пропускаем без ошибки
	}

	var env debeziumEnvelope
	if err := json.Unmarshal(r.Value, &env); err != nil {
		return nil, fmt.Errorf("parse envelope: %w", err)
	}

	// Для DELETE берём before и помечаем _deleted=1; для INSERT/UPDATE/READ —
	// after с _deleted=0. Если after нет (странный кейс), пропускаем.
	var raw map[string]any
	deleted := uint8(0)
	switch env.Op {
	case "d":
		raw = env.Before
		deleted = 1
	case "c", "u", "r":
		raw = env.After
	default:
		// 't' (truncate) и неизвестные операции пропускаем
		return nil, nil
	}
	if raw == nil {
		return nil, nil
	}

	cleaned, ok := cfg.Apply(table, raw)
	if !ok {
		return nil, nil
	}

	// Метаданные для аналитического слоя.
	cleaned["cdc_lsn"] = lsnFromSource(env.Source)
	cleaned["_deleted"] = deleted

	out, err := json.Marshal(cleaned)
	if err != nil {
		return nil, fmt.Errorf("marshal cleaned: %w", err)
	}

	return &kgo.Record{
		Topic: cfg.TargetTopic(table),
		Key:   r.Key, // PK как ключ — порядок per-id сохраняется
		Value: out,
	}, nil
}

// tableNameFromTopic: cdc.public.users → users.
func tableNameFromTopic(topic string) string {
	const prefix = "cdc.public."
	if !strings.HasPrefix(topic, prefix) {
		return ""
	}
	return topic[len(prefix):]
}

// lsnFromSource: Debezium кладёт source.lsn как float64 (из JSON-числа).
// Берём как UInt64, fallback'аясь на ts_ms если lsn нет.
func lsnFromSource(src map[string]any) uint64 {
	if v, ok := src["lsn"]; ok {
		switch n := v.(type) {
		case float64:
			return uint64(n)
		case int64:
			return uint64(n)
		case json.Number:
			if u, err := strconv.ParseUint(n.String(), 10, 64); err == nil {
				return u
			}
		case string:
			if u, err := strconv.ParseUint(n, 10, 64); err == nil {
				return u
			}
		}
	}
	if v, ok := src["ts_ms"]; ok {
		if f, ok := v.(float64); ok {
			return uint64(f)
		}
	}
	return 0
}
