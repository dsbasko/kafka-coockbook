package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTxnID = "lecture-04-01-zombie"
	defaultTopic = "tx-orders"
)

func main() {
	logger := log.New()

	txnID := flag.String("transactional-id", defaultTxnID, "общий TransactionalID — один и тот же у обоих процессов")
	topic := flag.String("topic", defaultTopic, "куда писать (любой существующий топик из транзакционного набора)")
	role := flag.String("role", "first", "first или second — для меток в логе")
	interval := flag.Duration("interval", 1*time.Second, "пауза между транзакциями")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		txnID:    *txnID,
		topic:    *topic,
		role:     *role,
		interval: *interval,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("zombie-fence failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	txnID    string
	topic    string
	role     string
	interval time.Duration
}

func run(ctx context.Context, o runOpts) error {
	cl, err := kafka.NewClient(
		kgo.TransactionalID(o.txnID),
		kgo.TransactionTimeout(60*time.Second),
		kgo.ClientID("lecture-04-01-zombie-"+o.role),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("zombie-fence [%s]: txn-id=%q topic=%q\n", o.role, o.txnID, o.topic)
	fmt.Println("каждую секунду — одна транзакция с одной записью.")
	fmt.Println("когда стартует второй процесс с тем же txn-id, этот получит InvalidProducerEpoch.")
	fmt.Println()

	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()

	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		attempt++
		err := oneTxn(ctx, cl, o, attempt)
		if err == nil {
			fmt.Printf("[%s #%03d] commit ✓\n", o.role, attempt)
			continue
		}
		if isFenced(err) {
			fmt.Printf("[%s #%03d] FENCED — другой процесс с тем же txn-id перехватил epoch.\n", o.role, attempt)
			fmt.Printf("        реальная ошибка: %v\n", err)
			fmt.Println("        этот процесс становится zombie — никаких commit'ов больше быть не может.")
			return nil
		}
		return fmt.Errorf("attempt %d: %w", attempt, err)
	}
}

func oneTxn(ctx context.Context, cl *kgo.Client, o runOpts, attempt int) error {
	if err := cl.BeginTransaction(); err != nil {
		return fmt.Errorf("BeginTransaction: %w", err)
	}

	results := cl.ProduceSync(ctx, &kgo.Record{
		Topic: o.topic,
		Key:   []byte(o.role),
		Value: []byte(fmt.Sprintf(`{"role":%q,"attempt":%d}`, o.role, attempt)),
	})
	if produceErr := results.FirstErr(); produceErr != nil {

		_ = cl.EndTransaction(ctx, kgo.TryAbort)
		return produceErr
	}

	return cl.EndTransaction(ctx, kgo.TryCommit)
}

func isFenced(err error) bool {
	return errors.Is(err, kerr.ProducerFenced) ||
		errors.Is(err, kerr.InvalidProducerEpoch)
}
