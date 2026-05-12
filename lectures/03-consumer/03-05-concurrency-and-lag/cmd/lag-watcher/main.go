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

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultGroup    = "lecture-03-05-sequential"
	defaultInterval = 2 * time.Second
)

func main() {
	logger := log.New()

	group := flag.String("group", defaultGroup, "group.id для опроса (sequential или pool)")
	interval := flag.Duration("interval", defaultInterval, "период опроса")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, *group, *interval); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("lag-watcher failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, group string, interval time.Duration) error {
	admin, err := kafka.NewAdmin()
	if err != nil {
		return fmt.Errorf("kafka.NewAdmin: %w", err)
	}
	defer admin.Close()

	fmt.Printf("lag-watcher запущен: group=%q interval=%s\n", group, interval)
	fmt.Println("раз в interval — снапшот lag'а через kadm.Lag. Ctrl+C — выход.")
	fmt.Println()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if err := tick(ctx, admin, group); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\nостановлен по сигналу.")
			return nil
		case <-ticker.C:
			if err := tick(ctx, admin, group); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				fmt.Fprintf(os.Stderr, "tick error: %v\n", err)
			}
		}
	}
}

func tick(ctx context.Context, admin *kadm.Client, group string) error {
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	lags, err := admin.Lag(reqCtx, group)
	if err != nil {
		return fmt.Errorf("admin.Lag: %w", err)
	}

	now := time.Now().Format("15:04:05")
	dl, ok := lags[group]
	if !ok {
		fmt.Printf("[%s] group=%q: не описана (ещё не существует?)\n", now, group)
		return nil
	}
	if err := dl.Error(); err != nil {
		fmt.Printf("[%s] group=%q: ошибка опроса: %v\n", now, group, err)
		return nil
	}

	if dl.Lag.IsEmpty() {
		fmt.Printf("[%s] group=%q state=%s: партиций пока нет (consumer'ы не подключены или не закоммитили)\n",
			now, group, dl.State)
		return nil
	}

	rows := dl.Lag.Sorted()
	parts := make([]string, 0, len(rows))
	var total int64
	for _, m := range rows {
		if m.Lag > 0 {
			total += m.Lag
		}
		parts = append(parts, fmt.Sprintf("%s/%d=%d (commit=%d end=%d)",
			m.Topic, m.Partition, m.Lag, m.Commit.At, m.End.Offset,
		))
	}
	sort.Strings(parts)
	fmt.Printf("[%s] group=%q state=%s total-lag=%d\n  %s\n",
		now, group, dl.State, total, strings.Join(parts, "\n  "),
	)
	return nil
}
