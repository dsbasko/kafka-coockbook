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
	defaultTopic     = "lecture-08-02-events"
	defaultRetention = 60 * time.Second
	defaultSegment   = 10 * time.Second
	defaultRate      = 5
	defaultPoll      = 5 * time.Second
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик с cleanup.policy=delete")
	retention := flag.Duration("retention", defaultRetention, "retention.ms")
	segment := flag.Duration("segment", defaultSegment, "segment.ms")
	rate := flag.Int("rate", defaultRate, "сообщений в секунду в фоновом продьюсере")
	poll := flag.Duration("poll", defaultPoll, "интервал опроса earliest/latest")
	recreate := flag.Bool("recreate", true, "удалить топик перед стартом")
	flag.Parse()

	if *rate <= 0 {
		fmt.Fprintln(os.Stderr, "rate должен быть > 0")
		os.Exit(2)
	}

	ctx, cancel := runctx.New()
	defer cancel()

	if err := run(ctx, runOpts{
		topic:     *topic,
		retention: *retention,
		segment:   *segment,
		rate:      *rate,
		poll:      *poll,
		recreate:  *recreate,
	}); err != nil {
		logger.Error("retention-demo failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic     string
	retention time.Duration
	segment   time.Duration
	rate      int
	poll      time.Duration
	recreate  bool
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
	if err := ensureRetentionTopic(ctx, admin, o.topic, o.retention, o.segment); err != nil {
		return fmt.Errorf("ensure topic: %w", err)
	}

	fmt.Printf("\nфоновый продьюсер: %d msg/sec в %q\n", o.rate, o.topic)
	fmt.Printf("опрос offset'ов раз в %s. Ctrl+C — выход.\n\n", o.poll)
	fmt.Println("ожидание: первые ~" + o.retention.String() + " после старта earliest=0,")
	fmt.Println("дальше старые сегменты начинают удаляться, earliest ползёт вверх к latest.")

	go produceLoop(ctx, cl, o.topic, o.rate)

	t := time.NewTicker(o.poll)
	defer t.Stop()

	if err := tick(ctx, admin, o.topic); err != nil {
		fmt.Fprintf(os.Stderr, "tick: %v\n", err)
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := tick(ctx, admin, o.topic); err != nil {
				fmt.Fprintf(os.Stderr, "tick: %v\n", err)
			}
		}
	}
}

func ensureRetentionTopic(ctx context.Context, admin *kadm.Client, topic string, retention, segment time.Duration) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	configs := map[string]*string{
		"cleanup.policy":      kadm.StringPtr("delete"),
		"retention.ms":        kadm.StringPtr(strconv.FormatInt(retention.Milliseconds(), 10)),
		"segment.ms":          kadm.StringPtr(strconv.FormatInt(segment.Milliseconds(), 10)),
		"min.insync.replicas": kadm.StringPtr("2"),
	}
	resp, err := admin.CreateTopic(rpcCtx, 1, 3, configs, topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("topic %q создан\n", topic)
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
	fmt.Printf("topic %q уже есть — подгоняем конфиг\n", topic)
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

func produceLoop(ctx context.Context, cl *kgo.Client, topic string, rate int) {
	interval := time.Second / time.Duration(rate)
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte('a' + (i % 26))
	}
	var seq int64
	for {
		select {
		case <-ctx.Done():
			cl.Flush(context.Background())
			return
		case <-t.C:
			seq++
			rec := &kgo.Record{
				Topic: topic,
				Key:   []byte(fmt.Sprintf("k-%d", seq%16)),
				Value: payload,
			}
			cl.Produce(ctx, rec, nil)
		}
	}
}

func tick(ctx context.Context, admin *kadm.Client, topic string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
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

	fmt.Printf("[%s]\n", time.Now().Format("15:04:05"))
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PARTITION\tEARLIEST\tLATEST\tRETAINED")
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
	fmt.Printf("size on disk (одна реплика): %d bytes (%.1f KB)\n---\n",
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
