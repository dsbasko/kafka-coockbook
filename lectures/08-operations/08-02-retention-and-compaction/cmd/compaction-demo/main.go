package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
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
	defaultTopic         = "lecture-08-02-users-state"
	defaultKeys          = 1000
	defaultUpdates       = 100_000
	defaultTombstoneKeys = 100
	defaultWait          = 30 * time.Second
	defaultSegmentMs     = 5_000
	defaultDirtyRatio    = "0.001"
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик с cleanup.policy=compact")
	keys := flag.Int("keys", defaultKeys, "сколько уникальных ключей")
	updates := flag.Int("updates", defaultUpdates, "сколько всего записей записать (по ключам — round-robin)")
	tombstones := flag.Bool("tombstones", true, "после первой компакции написать tombstone'ы и проверить эффект")
	tombstoneKeys := flag.Int("tombstone-keys", defaultTombstoneKeys, "сколько ключей удалить через tombstone")
	wait := flag.Duration("wait", defaultWait, "пауза, чтобы компактор брокера успел поработать")
	recreate := flag.Bool("recreate", true, "удалить топик перед стартом (детерминированный размер)")
	flag.Parse()

	if *keys <= 0 || *updates <= 0 {
		fmt.Fprintln(os.Stderr, "keys и updates должны быть > 0")
		os.Exit(2)
	}
	if *tombstoneKeys > *keys {
		*tombstoneKeys = *keys
	}

	ctx, cancel := runctx.New()
	defer cancel()

	if err := run(ctx, runOpts{
		topic:         *topic,
		keys:          *keys,
		updates:       *updates,
		tombstones:    *tombstones,
		tombstoneKeys: *tombstoneKeys,
		wait:          *wait,
		recreate:      *recreate,
	}); err != nil {
		logger.Error("compaction-demo failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic         string
	keys          int
	updates       int
	tombstones    bool
	tombstoneKeys int
	wait          time.Duration
	recreate      bool
}

func run(ctx context.Context, o runOpts) error {
	cl, err := kafka.NewClient()
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)

	if o.recreate {
		if err := dropTopic(ctx, admin, o.topic); err != nil {
			return fmt.Errorf("drop topic: %w", err)
		}
	}

	if err := ensureCompactTopic(ctx, admin, o.topic); err != nil {
		return fmt.Errorf("ensure topic: %w", err)
	}

	fmt.Printf("=== STEP 1: пишем %d обновлений на %d ключей в %q ===\n",
		o.updates, o.keys, o.topic)
	if err := produceUpdates(ctx, cl, o.topic, o.keys, o.updates); err != nil {
		return fmt.Errorf("produce updates: %w", err)
	}

	if err := snapshot(ctx, admin, o.topic, "после записи"); err != nil {
		return fmt.Errorf("snapshot before compaction: %w", err)
	}

	fmt.Printf("\n=== STEP 2: ждём %s, чтобы compactor сжал закрытые сегменты ===\n", o.wait)
	if err := waitWithHeartbeats(ctx, cl, o.topic, o.wait); err != nil {
		return fmt.Errorf("wait: %w", err)
	}

	if err := snapshot(ctx, admin, o.topic, "после первой компакции"); err != nil {
		return fmt.Errorf("snapshot after compaction: %w", err)
	}

	if !o.tombstones {
		return nil
	}

	fmt.Printf("\n=== STEP 3: пишем %d tombstone'ов (Value=nil) ===\n", o.tombstoneKeys)
	if err := produceTombstones(ctx, cl, o.topic, o.tombstoneKeys); err != nil {
		return fmt.Errorf("produce tombstones: %w", err)
	}

	fmt.Printf("\n=== STEP 4: ждём %s — tombstone должен пройти через compactor ===\n", o.wait)
	if err := waitWithHeartbeats(ctx, cl, o.topic, o.wait); err != nil {
		return fmt.Errorf("wait tombstones: %w", err)
	}

	if err := snapshot(ctx, admin, o.topic, "после tombstone'ов"); err != nil {
		return fmt.Errorf("snapshot after tombstones: %w", err)
	}

	expected := o.keys - o.tombstoneKeys
	fmt.Printf("\n=== STEP 5: считаем уникальные ключи в логе (ожидаем ~%d) ===\n", expected)
	if err := countDistinctKeys(ctx, o.topic); err != nil {
		return fmt.Errorf("count distinct keys: %w", err)
	}
	return nil
}

func ensureCompactTopic(ctx context.Context, admin *kadm.Client, topic string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	configs := compactionConfig()

	resp, err := admin.CreateTopic(rpcCtx, 1, 3, configs, topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("topic %q создан с compaction-конфигом\n", topic)
		printConfigSummary(configs)
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if !errors.Is(cause, kerr.TopicAlreadyExists) {
		return cause
	}

	fmt.Printf("topic %q уже существует — подгоняем compaction-конфиг\n", topic)
	alters := make([]kadm.AlterConfig, 0, len(configs))
	for k, v := range configs {
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: v})
	}
	alterResp, err := admin.AlterTopicConfigs(rpcCtx, alters, topic)
	if err != nil {
		return fmt.Errorf("AlterTopicConfigs: %w", err)
	}
	for _, r := range alterResp {
		if r.Err != nil {
			return fmt.Errorf("alter %s: %w", r.Name, r.Err)
		}
	}
	printConfigSummary(configs)
	return nil
}

func compactionConfig() map[string]*string {
	return map[string]*string{
		"cleanup.policy":            kadm.StringPtr("compact"),
		"segment.ms":                kadm.StringPtr(strconv.Itoa(defaultSegmentMs)),
		"segment.bytes":             kadm.StringPtr("1048576"),
		"min.cleanable.dirty.ratio": kadm.StringPtr(defaultDirtyRatio),
		"min.compaction.lag.ms":     kadm.StringPtr("0"),
		"max.compaction.lag.ms":     kadm.StringPtr("10000"),
		"delete.retention.ms":       kadm.StringPtr("5000"),
		"min.insync.replicas":       kadm.StringPtr("2"),
	}
}

func printConfigSummary(configs map[string]*string) {
	keys := make([]string, 0, len(configs))
	for k := range configs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, k := range keys {
		fmt.Fprintf(tw, "  %s\t= %s\n", k, *configs[k])
	}
	_ = tw.Flush()
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

func produceUpdates(ctx context.Context, cl *kgo.Client, topic string, keys, updates int) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	for i := 0; i < updates; i++ {
		k := i % keys
		rec := &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("user-%05d", k)),
			Value: []byte(fmt.Sprintf(`{"v":%d,"ts":%d}`, i, time.Now().UnixMilli())),
		}
		cl.Produce(rpcCtx, rec, nil)
		if i > 0 && i%10000 == 0 {
			fmt.Printf("  produced %d/%d\n", i, updates)
		}
	}
	if err := cl.Flush(rpcCtx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	fmt.Printf("  produced %d/%d (готово)\n", updates, updates)
	return nil
}

func produceTombstones(ctx context.Context, cl *kgo.Client, topic string, n int) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		rec := &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("user-%05d", i)),
			Value: nil,
		}
		if err := cl.ProduceSync(rpcCtx, rec).FirstErr(); err != nil {
			return fmt.Errorf("tombstone %d: %w", i, err)
		}
	}
	fmt.Printf("  tombstones=%d записаны\n", n)
	return nil
}

func waitWithHeartbeats(ctx context.Context, cl *kgo.Client, topic string, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	hb := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			if time.Now().After(deadline) {
				return nil
			}
			hb++
			rec := &kgo.Record{
				Topic: topic,
				Key:   []byte(fmt.Sprintf("__heartbeat-%d", hb)),
				Value: []byte("hb"),
			}
			rpcCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			if err := cl.ProduceSync(rpcCtx, rec).FirstErr(); err != nil {
				cancel()
				fmt.Fprintf(os.Stderr, "heartbeat %d: %v\n", hb, err)
				continue
			}
			cancel()
		}
	}
}

func snapshot(ctx context.Context, admin *kadm.Client, topic, label string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	starts, err := admin.ListStartOffsets(rpcCtx, topic)
	if err != nil {
		return fmt.Errorf("ListStartOffsets: %w", err)
	}
	ends, err := admin.ListEndOffsets(rpcCtx, topic)
	if err != nil {
		return fmt.Errorf("ListEndOffsets: %w", err)
	}
	size, err := topicSize(rpcCtx, admin, topic)
	if err != nil {
		return fmt.Errorf("topicSize: %w", err)
	}

	fmt.Printf("\n[%s] %s\n", label, time.Now().Format("15:04:05"))
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PARTITION\tEARLIEST\tLATEST\tRECORDS_IN_LOG")
	starts.Each(func(s kadm.ListedOffset) {
		if s.Err != nil {
			return
		}
		var latest int64 = -1
		if eo, ok := ends.Lookup(topic, s.Partition); ok && eo.Err == nil {
			latest = eo.Offset
		}
		fmt.Fprintf(tw, "%d\t%d\t%d\t%d\n", s.Partition, s.Offset, latest, latest-s.Offset)
	})
	_ = tw.Flush()
	fmt.Printf("size on disk (одна реплика, все партиции): %d bytes (%.1f KB)\n",
		size, float64(size)/1024.0)
	return nil
}

func topicSize(ctx context.Context, admin *kadm.Client, topic string) (int64, error) {
	all, err := admin.DescribeAllLogDirs(ctx, nil)
	if err != nil {
		return 0, err
	}
	seen := make(map[int32]bool)
	var size int64
	all.Each(func(d kadm.DescribedLogDir) {
		d.Topics.Each(func(p kadm.DescribedLogDirPartition) {
			if p.Topic != topic {
				return
			}
			if seen[p.Partition] {
				return
			}
			seen[p.Partition] = true
			size += p.Size
		})
	})
	return size, nil
}

func countDistinctKeys(ctx context.Context, topic string) error {
	cl, err := kafka.NewClient(
		kgo.ClientID("lecture-08-02-counter"),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	endsAdmin := kadm.NewClient(cl)
	ends, err := endsAdmin.ListEndOffsets(rpcCtx, topic)
	if err != nil {
		return fmt.Errorf("ListEndOffsets: %w", err)
	}
	var target int64
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err == nil {
			target += o.Offset
		}
	})

	keys := make(map[string]struct{}, 8192)
	tombstones := 0
	heartbeats := 0
	var read int64
	deadline := time.Now().Add(20 * time.Second)
	for read < target && time.Now().Before(deadline) {
		fetches := cl.PollFetches(rpcCtx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
					read = target
					break
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			read++
			k := string(r.Key)
			if len(k) > 2 && k[:2] == "__" {
				heartbeats++
				return
			}
			if r.Value == nil {
				tombstones++
				delete(keys, k)
				return
			}
			keys[k] = struct{}{}
		})
	}
	fmt.Printf("прочитано записей: %d\n", read)
	fmt.Printf("уникальных user-ключей в финальном логе: %d\n", len(keys))
	fmt.Printf("встречено tombstone'ов: %d\n", tombstones)
	fmt.Printf("встречено heartbeat-ключей: %d\n", heartbeats)
	return nil
}
