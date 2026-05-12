package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopics = "tx-orders,tx-payments,tx-shipments"
	defaultGroup  = "lecture-04-01-rc"
)

func main() {
	logger := log.New()

	topicsCSV := flag.String("topics", defaultTopics, "топики через запятую")
	group := flag.String("group", defaultGroup, "consumer group")
	isolation := flag.String("isolation", "committed", "committed | uncommitted")
	count := flag.Int("count", 0, "выйти после N прочитанных записей (0 — без лимита)")
	idle := flag.Duration("idle", 0, "выйти, если за это время не пришло ни одной записи (0 — без таймаута)")
	fromStart := flag.Bool("from-start", true, "при первом старте группы читать с earliest")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topics:    splitCSV(*topicsCSV),
		group:     *group,
		isolation: *isolation,
		count:     *count,
		idle:      *idle,
		fromStart: *fromStart,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("read-committed failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topics    []string
	group     string
	isolation string
	count     int
	idle      time.Duration
	fromStart bool
}

func run(ctx context.Context, o runOpts) error {
	level, label, err := pickIsolation(o.isolation)
	if err != nil {
		return err
	}

	opts := []kgo.Opt{
		kgo.ConsumerGroup(o.group),
		kgo.ConsumeTopics(o.topics...),
		kgo.FetchIsolationLevel(level),
		kgo.ClientID("lecture-04-01-rc"),
		kgo.DisableAutoCommit(),
	}
	if o.fromStart {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	cl, err := kafka.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("read-committed: topics=%v group=%q isolation=%s\n", o.topics, o.group, label)
	if o.count > 0 {
		fmt.Printf("выйдет после %d прочитанных.\n", o.count)
	}
	if o.idle > 0 {
		fmt.Printf("выйдет, если %s нет новых.\n", o.idle)
	}
	fmt.Println()

	perTopic := make(map[string]int, len(o.topics))
	total := 0
	lastSeen := time.Now()

	for {
		if err := ctx.Err(); err != nil {
			break
		}

		pollCtx := ctx
		var pollCancel context.CancelFunc
		if o.idle > 0 {
			pollCtx, pollCancel = context.WithTimeout(ctx, o.idle)
		}
		fetches := cl.PollFetches(pollCtx)
		if pollCancel != nil {
			pollCancel()
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					goto done
				}
				if errors.Is(e.Err, context.DeadlineExceeded) {
					if o.idle > 0 && time.Since(lastSeen) >= o.idle {
						fmt.Println()
						fmt.Println("idle timeout — больше записей не пришло.")
						goto done
					}
					continue
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}

		batchHadRecords := false
		fetches.EachRecord(func(r *kgo.Record) {
			batchHadRecords = true
			perTopic[r.Topic]++
			total++
			fmt.Printf("[%-13s p%d o%-6d] key=%-3s value=%s\n",
				r.Topic, r.Partition, r.Offset, string(r.Key), string(r.Value))
			if o.count > 0 && total >= o.count {

			}
		})
		if batchHadRecords {
			lastSeen = time.Now()
		}
		if o.count > 0 && total >= o.count {
			break
		}
	}

done:
	fmt.Println()
	fmt.Println("итого по топикам:")
	for _, t := range o.topics {
		fmt.Printf("  %-13s %d\n", t, perTopic[t])
	}
	fmt.Printf("  total         %d\n", total)
	return nil
}

func pickIsolation(s string) (kgo.IsolationLevel, string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "committed", "read_committed", "rc":
		return kgo.ReadCommitted(), "read_committed", nil
	case "uncommitted", "read_uncommitted", "ru":
		return kgo.ReadUncommitted(), "read_uncommitted", nil
	default:
		return kgo.IsolationLevel{}, "", fmt.Errorf("unknown -isolation=%q (committed|uncommitted)", s)
	}
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
