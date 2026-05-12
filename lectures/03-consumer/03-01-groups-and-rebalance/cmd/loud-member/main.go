package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic = "lecture-03-01-groups"
	defaultGroup = "lecture-03-01-loud"
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик, на который подписываемся (6 партиций)")
	group := flag.String("group", defaultGroup, "group.id для всех копий")
	strategy := flag.String("strategy", "cooperative-sticky", "стратегия балансировки: sticky | cooperative-sticky")
	heartbeat := flag.Duration("heartbeat", 3*time.Second, "период heartbeat внутри группы (kgo.HeartbeatInterval)")
	sessionTimeout := flag.Duration("session-timeout", 30*time.Second, "session.timeout.ms — после стольких пропущенных heartbeat'ов брокер исключит члена")
	flag.Parse()

	memberID := config.EnvOr("MEMBER_ID", "1")

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topic:          *topic,
		group:          *group,
		memberID:       memberID,
		strategy:       *strategy,
		heartbeat:      *heartbeat,
		sessionTimeout: *sessionTimeout,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("loud-member failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic          string
	group          string
	memberID       string
	strategy       string
	heartbeat      time.Duration
	sessionTimeout time.Duration
}

func run(ctx context.Context, o runOpts) error {
	balancer, err := pickBalancer(o.strategy)
	if err != nil {
		return err
	}

	opts := []kgo.Opt{
		kgo.ConsumerGroup(o.group),
		kgo.ConsumeTopics(o.topic),
		kgo.Balancers(balancer),
		kgo.HeartbeatInterval(o.heartbeat),
		kgo.SessionTimeout(o.sessionTimeout),
		kgo.ClientID(fmt.Sprintf("lecture-03-01-loud-%s", o.memberID)),
		kgo.InstanceID(fmt.Sprintf("loud-member-%s", o.memberID)),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			printEvent(o.memberID, "ASSIGNED", m)
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			printEvent(o.memberID, "REVOKED", m)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			printEvent(o.memberID, "LOST", m)
		}),
	}

	cl, err := kafka.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("[member=%s] стартую: topic=%q group=%q strategy=%s heartbeat=%s session-timeout=%s\n",
		o.memberID, o.topic, o.group, o.strategy, o.heartbeat, o.sessionTimeout)
	fmt.Printf("[member=%s] жду ребалансов и записей. Ctrl+C — выход.\n\n", o.memberID)

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					fmt.Printf("\n[member=%s] остановлен по сигналу.\n", o.memberID)
					return nil
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			fmt.Printf("[member=%s] p=%d off=%d key=%s value=%s\n",
				o.memberID, r.Partition, r.Offset, string(r.Key), string(r.Value),
			)
		})
	}
}

func pickBalancer(name string) (kgo.GroupBalancer, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "sticky":
		return kgo.StickyBalancer(), nil
	case "cooperative-sticky", "cooperative", "coop":
		return kgo.CooperativeStickyBalancer(), nil
	default:
		return nil, fmt.Errorf("unknown strategy %q (поддерживаем sticky | cooperative-sticky)", name)
	}
}

func printEvent(memberID, kind string, m map[string][]int32) {
	now := time.Now().Format("15:04:05.000")
	if len(m) == 0 {
		fmt.Printf("[member=%s] %s %s: <ничего>\n", memberID, now, kind)
		return
	}
	topics := make([]string, 0, len(m))
	for t := range m {
		topics = append(topics, t)
	}
	sort.Strings(topics)

	parts := make([]string, 0, len(topics))
	for _, t := range topics {
		ps := append([]int32(nil), m[t]...)
		sort.Slice(ps, func(i, j int) bool { return ps[i] < ps[j] })
		parts = append(parts, fmt.Sprintf("%s=%v", t, ps))
	}
	fmt.Printf("[member=%s] %s %s: %s\n", memberID, now, kind, strings.Join(parts, " "))
}
