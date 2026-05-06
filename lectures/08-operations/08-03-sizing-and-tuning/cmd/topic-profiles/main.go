// topic-profiles — три топика с разными профилями нагрузки.
//
// Идея лекции 08-03 — увидеть, что «правильный» topic-config это не
// набор магических чисел, а отражение профиля нагрузки. Программа
// создаёт три топика с разными политиками (CDC / metrics / events) и
// печатает их финальный конфиг через DescribeTopicConfigs — рядом стоят
// cleanup.policy, retention, segment, compression, min.insync.replicas
// и max.message.bytes. Сравнивая профили, видно, как одни и те же ручки
// крутят в разные стороны под разный сценарий.
//
// Профили:
//   - cdc      — compact + min.ISR=2 + retention=-1 + zstd
//                long-lived state (Debezium CDC, конфигурации, профили).
//   - metrics  — delete + retention=24h + segment.ms=10m + lz4
//                короткоживущие точки, объём важнее длительности.
//   - events   — delete + retention=7d  + segment.ms=1d + lz4
//                бизнес-события с rebroadcast окном в неделю.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

// shownConfigs — ключи, которые мы вытаскиваем из DescribeTopicConfigs
// и печатаем рядом для сравнения. Список выбран по разделам README:
// cleanup-политика, retention, segment, compression, надёжность,
// размер сообщения, поведение при отказе, тип таймстампа.
var shownConfigs = []string{
	"cleanup.policy",
	"retention.ms",
	"retention.bytes",
	"segment.ms",
	"segment.bytes",
	"min.insync.replicas",
	"max.message.bytes",
	"compression.type",
	"unclean.leader.election.enable",
	"message.timestamp.type",
}

type profile struct {
	name      string
	topic     string
	parts     int32
	rf        int16
	configs   map[string]*string
	rationale string
}

func profiles(prefix string) []profile {
	return []profile{
		{
			name:  "cdc",
			topic: prefix + "-cdc",
			parts: 6,
			rf:    3,
			configs: map[string]*string{
				"cleanup.policy":                 kadm.StringPtr("compact"),
				"retention.ms":                   kadm.StringPtr("-1"),
				"min.insync.replicas":            kadm.StringPtr("2"),
				"compression.type":               kadm.StringPtr("zstd"),
				"max.message.bytes":              kadm.StringPtr("2097152"),
				"min.cleanable.dirty.ratio":      kadm.StringPtr("0.1"),
				"unclean.leader.election.enable": kadm.StringPtr("false"),
				"message.timestamp.type":         kadm.StringPtr("CreateTime"),
			},
			rationale: "long-lived state по ключу: compact + retention=-1 + zstd, min.ISR=2 на случай падения брокера",
		},
		{
			name:  "metrics",
			topic: prefix + "-metrics",
			parts: 12,
			rf:    3,
			configs: map[string]*string{
				"cleanup.policy":                 kadm.StringPtr("delete"),
				"retention.ms":                   kadm.StringPtr("86400000"),
				"segment.ms":                     kadm.StringPtr("600000"),
				"segment.bytes":                  kadm.StringPtr("134217728"),
				"min.insync.replicas":            kadm.StringPtr("2"),
				"compression.type":               kadm.StringPtr("lz4"),
				"max.message.bytes":              kadm.StringPtr("1048576"),
				"unclean.leader.election.enable": kadm.StringPtr("false"),
				"message.timestamp.type":         kadm.StringPtr("LogAppendTime"),
			},
			rationale: "короткие сэмплы метрик: TTL=24h, segment 10 мин для быстрой ротации, lz4 для дешёвого сжатия",
		},
		{
			name:  "events",
			topic: prefix + "-events",
			parts: 12,
			rf:    3,
			configs: map[string]*string{
				"cleanup.policy":                 kadm.StringPtr("delete"),
				"retention.ms":                   kadm.StringPtr("604800000"),
				"segment.ms":                     kadm.StringPtr("86400000"),
				"min.insync.replicas":            kadm.StringPtr("2"),
				"compression.type":               kadm.StringPtr("lz4"),
				"max.message.bytes":              kadm.StringPtr("1048576"),
				"unclean.leader.election.enable": kadm.StringPtr("false"),
				"message.timestamp.type":         kadm.StringPtr("CreateTime"),
			},
			rationale: "бизнес-события с replay в течение недели: retention=7d, segment=1d, lz4",
		},
	}
}

func main() {
	logger := log.New()

	prefix := flag.String("prefix", "lecture-08-03", "префикс имени для трёх топиков (cdc/metrics/events)")
	recreate := flag.Bool("recreate", false, "пересоздать топики (DELETE → CREATE) — детерминированный вывод")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	if err := run(ctx, *prefix, *recreate); err != nil {
		logger.Error("topic-profiles failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, prefix string, recreate bool) error {
	cl, err := kafka.NewClient()
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)

	prof := profiles(prefix)

	if recreate {
		for _, p := range prof {
			if err := dropTopic(ctx, admin, p.topic); err != nil {
				return fmt.Errorf("drop %s: %w", p.topic, err)
			}
		}
	}

	for _, p := range prof {
		if err := ensureTopic(ctx, admin, p); err != nil {
			return fmt.Errorf("ensure %s: %w", p.topic, err)
		}
	}

	fmt.Println()
	fmt.Println("=== DescribeTopicConfigs: фактический конфиг каждого профиля ===")
	if err := describeAll(ctx, admin, prof); err != nil {
		return fmt.Errorf("describe: %w", err)
	}
	return nil
}

func ensureTopic(ctx context.Context, admin *kadm.Client, p profile) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	resp, err := admin.CreateTopic(rpcCtx, p.parts, p.rf, p.configs, p.topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("[%s] topic %q создан (partitions=%d, rf=%d) — %s\n",
			p.name, p.topic, p.parts, p.rf, p.rationale)
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if !errors.Is(cause, kerr.TopicAlreadyExists) {
		return cause
	}

	fmt.Printf("[%s] topic %q уже есть — подгоняем конфиг (AlterTopicConfigs)\n", p.name, p.topic)
	alters := make([]kadm.AlterConfig, 0, len(p.configs))
	for k, v := range p.configs {
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: v})
	}
	alterResp, err := admin.AlterTopicConfigs(rpcCtx, alters, p.topic)
	if err != nil {
		return fmt.Errorf("AlterTopicConfigs: %w", err)
	}
	for _, r := range alterResp {
		if r.Err != nil {
			return fmt.Errorf("alter %s: %w", r.Name, r.Err)
		}
	}
	return nil
}

func dropTopic(ctx context.Context, admin *kadm.Client, topic string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resp, err := admin.DeleteTopic(rpcCtx, topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("topic %q удалён (recreate=true)\n", topic)
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if errors.Is(cause, kerr.UnknownTopicOrPartition) {
		return nil
	}
	return cause
}

// describeAll достаёт DescribeTopicConfigs одним запросом и печатает
// табличку: одна колонка на профиль, одна строка на каждую важную ручку.
// Пустые ячейки означают «дефолт брокера, явно не выставлено».
func describeAll(ctx context.Context, admin *kadm.Client, prof []profile) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	names := make([]string, 0, len(prof))
	for _, p := range prof {
		names = append(names, p.topic)
	}

	// Свежесозданный топик иногда не успевает попасть на все брокеры до
	// первого DescribeConfigs. Поэтому при UNKNOWN_TOPIC_OR_PARTITION для
	// одного из имён ждём короткий хвост и повторяем — обычно одного-двух
	// заходов хватает.
	rcs, err := describeWithRetry(rpcCtx, admin, names)
	if err != nil {
		return err
	}

	// topic -> key -> value
	values := make(map[string]map[string]string, len(prof))
	for _, rc := range rcs {
		if rc.Err != nil {
			fmt.Fprintf(os.Stderr, "describe %s: %v\n", rc.Name, rc.Err)
			continue
		}
		m := make(map[string]string, len(rc.Configs))
		for _, c := range rc.Configs {
			if c.Value != nil {
				m[c.Key] = *c.Value
			}
		}
		values[rc.Name] = m
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	header := []string{"CONFIG"}
	for _, p := range prof {
		header = append(header, strings.ToUpper(p.name))
	}
	fmt.Fprintln(tw, strings.Join(header, "\t"))
	fmt.Fprintln(tw, strings.Repeat("-\t", len(header)))

	for _, key := range shownConfigs {
		row := []string{key}
		for _, p := range prof {
			row = append(row, formatVal(key, values[p.topic][key]))
		}
		fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	fmt.Println()
	fmt.Println("=== summary по партициям и replication factor ===")
	if err := describePartitions(rpcCtx, admin, prof); err != nil {
		return err
	}
	return nil
}

func describeWithRetry(ctx context.Context, admin *kadm.Client, names []string) (kadm.ResourceConfigs, error) {
	const maxAttempts = 5
	delay := 500 * time.Millisecond
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		rcs, err := admin.DescribeTopicConfigs(ctx, names...)
		if err != nil {
			return nil, err
		}
		retryNeeded := false
		for _, rc := range rcs {
			if errors.Is(rc.Err, kerr.UnknownTopicOrPartition) {
				retryNeeded = true
				break
			}
		}
		if !retryNeeded || attempt == maxAttempts {
			return rcs, nil
		}
		select {
		case <-ctx.Done():
			return rcs, nil
		case <-time.After(delay):
		}
		delay *= 2
	}
	return nil, fmt.Errorf("describe: max attempts reached")
}

// describePartitions печатает количество партиций и replication factor.
// ListTopics — отдельный вызов, потому что DescribeConfigs возвращает
// только конфиги, не layout.
func describePartitions(ctx context.Context, admin *kadm.Client, prof []profile) error {
	names := make([]string, 0, len(prof))
	for _, p := range prof {
		names = append(names, p.topic)
	}
	td, err := admin.ListTopics(ctx, names...)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TOPIC\tPARTITIONS\tREPLICATION_FACTOR")
	sorted := append([]profile(nil), prof...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].name < sorted[j].name })
	for _, p := range sorted {
		t, ok := td[p.topic]
		if !ok || t.Err != nil {
			fmt.Fprintf(tw, "%s\t?\t?\n", p.topic)
			continue
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\n", p.topic, len(t.Partitions), t.Partitions.NumReplicas())
	}
	return tw.Flush()
}

// formatVal делает значения чуть читабельнее: переводит миллисекунды
// retention.ms / segment.ms в человекочитаемое (1d / 7d / 24h), не теряя
// «сырое» значение в скобках. -1 для retention.ms означает «без TTL».
func formatVal(key, raw string) string {
	if raw == "" {
		return "—"
	}
	switch key {
	case "retention.ms", "segment.ms":
		if raw == "-1" {
			return "-1 (без TTL)"
		}
		if d, ok := parseMillis(raw); ok {
			return fmt.Sprintf("%s (%s)", humanDuration(d), raw)
		}
	case "retention.bytes":
		if raw == "-1" {
			return "-1 (без лимита)"
		}
		if b, ok := parseBytes(raw); ok {
			return fmt.Sprintf("%s (%s)", humanBytes(b), raw)
		}
	case "segment.bytes", "max.message.bytes":
		if b, ok := parseBytes(raw); ok {
			return fmt.Sprintf("%s (%s)", humanBytes(b), raw)
		}
	}
	return raw
}

func parseMillis(s string) (time.Duration, bool) {
	var ms int64
	if _, err := fmt.Sscanf(s, "%d", &ms); err != nil {
		return 0, false
	}
	if ms < 0 {
		return 0, false
	}
	return time.Duration(ms) * time.Millisecond, true
}

func parseBytes(s string) (int64, bool) {
	var b int64
	if _, err := fmt.Sscanf(s, "%d", &b); err != nil {
		return 0, false
	}
	return b, true
}

func humanDuration(d time.Duration) string {
	switch {
	case d >= 24*time.Hour && d%(24*time.Hour) == 0:
		return fmt.Sprintf("%dd", int64(d/(24*time.Hour)))
	case d >= time.Hour && d%time.Hour == 0:
		return fmt.Sprintf("%dh", int64(d/time.Hour))
	case d >= time.Minute && d%time.Minute == 0:
		return fmt.Sprintf("%dm", int64(d/time.Minute))
	case d >= time.Second && d%time.Second == 0:
		return fmt.Sprintf("%ds", int64(d/time.Second))
	default:
		return d.String()
	}
}

func humanBytes(b int64) string {
	const (
		_  = iota
		kb = 1 << (10 * iota)
		mb
		gb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.0f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.0f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
