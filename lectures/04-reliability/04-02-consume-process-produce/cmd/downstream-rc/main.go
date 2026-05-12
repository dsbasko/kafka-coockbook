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
	defaultTopic = "cpp-orders-enriched"
	defaultGroup = "lecture-04-02-downstream"
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "выходной топик пайплайна")
	group := flag.String("group", defaultGroup, "consumer group для downstream")
	isolation := flag.String("isolation", "committed", "committed | uncommitted")
	idle := flag.Duration("idle", 8*time.Second, "выйти, если за это время нет новых записей (>5s рекомендуется — первое подключение к группе занимает несколько секунд)")
	verbose := flag.Bool("verbose", false, "печатать каждую запись (по умолчанию только итоги)")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topic:     *topic,
		group:     *group,
		isolation: *isolation,
		idle:      *idle,
		verbose:   *verbose,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("downstream-rc failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic     string
	group     string
	isolation string
	idle      time.Duration
	verbose   bool
}

func run(ctx context.Context, o runOpts) error {
	level, label, err := pickIsolation(o.isolation)
	if err != nil {
		return err
	}

	cl, err := kafka.NewClient(
		kgo.ConsumerGroup(o.group),
		kgo.ConsumeTopics(o.topic),
		kgo.FetchIsolationLevel(level),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ClientID("lecture-04-02-downstream"),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("downstream-rc: topic=%q group=%q isolation=%s idle=%s\n", o.topic, o.group, label, o.idle)
	fmt.Println()

	total := 0
	uniqueKeys := make(map[string]int)
	dupKeys := 0
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
			done := false
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					done = true
					break
				}
				if errors.Is(e.Err, context.DeadlineExceeded) {
					if o.idle > 0 && time.Since(lastSeen) >= o.idle {
						done = true
						break
					}
					continue
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
			if done {
				break
			}
			continue
		}

		batchHad := false
		fetches.EachRecord(func(r *kgo.Record) {
			batchHad = true
			total++
			key := string(r.Key)
			prev := uniqueKeys[key]
			uniqueKeys[key] = prev + 1
			if prev > 0 {
				dupKeys++
			}
			if o.verbose {
				fmt.Printf("[p%d o%-6d] key=%s value=%s\n", r.Partition, r.Offset, key, string(r.Value))
			}
		})
		if batchHad {
			lastSeen = time.Now()
		}
	}

	fmt.Println()
	fmt.Println("итог downstream:")
	fmt.Printf("  всего записей:    %d\n", total)
	fmt.Printf("  уникальных ключей: %d\n", len(uniqueKeys))
	fmt.Printf("  дублей по ключу:  %d\n", dupKeys)
	fmt.Println()
	if dupKeys == 0 {
		fmt.Println("✓ EOS работает: каждый ключ встретился ровно один раз.")
	} else {
		fmt.Println("⚠ дубли — это либо isolation=uncommitted (видны записи аборнутых транзакций),")
		fmt.Println("  либо реально нарушенный EOS (баг в пайплайне или конфиге).")
	}
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
