package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTxnID    = "lecture-04-01-tx-producer"
	topicOrders     = "tx-orders"
	topicPayments   = "tx-payments"
	topicShipments  = "tx-shipments"
	defaultAttempts = 10
	defaultCommitP  = 0.7
)

func main() {
	logger := log.New()

	txnID := flag.String("transactional-id", defaultTxnID, "TransactionalID — уникальный per-процесс идентификатор")
	attempts := flag.Int("attempts", defaultAttempts, "сколько транзакций пытаться")
	commitProb := flag.Float64("commit-prob", defaultCommitP, "вероятность commit (иначе abort) на каждой транзакции")
	pause := flag.Duration("pause", 200*time.Millisecond, "пауза между попытками")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		txnID:      *txnID,
		attempts:   *attempts,
		commitProb: *commitProb,
		pause:      *pause,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("transactional-producer failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	txnID      string
	attempts   int
	commitProb float64
	pause      time.Duration
}

func run(ctx context.Context, o runOpts) error {
	cl, err := kafka.NewClient(
		kgo.TransactionalID(o.txnID),

		kgo.TransactionTimeout(60*time.Second),
		kgo.ClientID("lecture-04-01-tx-producer"),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("transactional-producer: txn-id=%q attempts=%d commit-prob=%.2f\n",
		o.txnID, o.attempts, o.commitProb)
	fmt.Printf("topics: %s, %s, %s\n", topicOrders, topicPayments, topicShipments)
	fmt.Println()

	beforeOrders, beforePayments, beforeShipments, err := readEndOffsets(ctx, cl)
	if err != nil {
		return fmt.Errorf("baseline offsets: %w", err)
	}

	committed, aborted := 0, 0
	for i := 0; i < o.attempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		ok, err := tryOnce(ctx, cl, i, o.commitProb)
		if err != nil {
			return fmt.Errorf("attempt %d: %w", i, err)
		}
		if ok {
			committed++
			fmt.Printf("[#%02d] commit ✓\n", i)
		} else {
			aborted++
			fmt.Printf("[#%02d] abort ✗\n", i)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(o.pause):
		}
	}

	afterOrders, afterPayments, afterShipments, err := readEndOffsets(ctx, cl)
	if err != nil {
		return fmt.Errorf("final offsets: %w", err)
	}

	addedOrders := afterOrders - beforeOrders
	addedPayments := afterPayments - beforePayments
	addedShipments := afterShipments - beforeShipments

	fmt.Println()
	fmt.Println("итог:")
	fmt.Printf("  attempts:  %d\n", o.attempts)
	fmt.Printf("  committed: %d   aborted: %d\n", committed, aborted)
	fmt.Println()
	fmt.Println("дельта end-offset (включая control records от транзакций):")
	fmt.Printf("  %-13s +%d\n", topicOrders, addedOrders)
	fmt.Printf("  %-13s +%d\n", topicPayments, addedPayments)
	fmt.Printf("  %-13s +%d\n", topicShipments, addedShipments)
	fmt.Println()
	fmt.Println("read_committed увидит ровно по committed записей в каждом топике.")
	fmt.Println("read_uncommitted увидит committed+aborted (но не control records).")
	return nil
}

func tryOnce(ctx context.Context, cl *kgo.Client, attempt int, commitProb float64) (bool, error) {
	if err := cl.BeginTransaction(); err != nil {
		return false, fmt.Errorf("BeginTransaction: %w", err)
	}

	orderID := strconv.Itoa(attempt)
	produceErr := produceTriple(ctx, cl, orderID)

	wantCommit := rand.Float64() < commitProb

	if produceErr != nil {
		fmt.Fprintf(os.Stderr, "[#%02d] produce error → forcing abort: %v\n", attempt, produceErr)
		wantCommit = false
	}

	commit := kgo.TryAbort
	if wantCommit {
		commit = kgo.TryCommit
	}

	if err := cl.EndTransaction(ctx, commit); err != nil {
		return false, fmt.Errorf("EndTransaction(%v): %w", commit, err)
	}
	return wantCommit, nil
}

func produceTriple(ctx context.Context, cl *kgo.Client, orderID string) error {
	results := cl.ProduceSync(ctx,
		&kgo.Record{
			Topic: topicOrders,
			Key:   []byte(orderID),
			Value: []byte(fmt.Sprintf(`{"order_id":%q,"status":"created"}`, orderID)),
		},
		&kgo.Record{
			Topic: topicPayments,
			Key:   []byte(orderID),
			Value: []byte(fmt.Sprintf(`{"order_id":%q,"amount":1000}`, orderID)),
		},
		&kgo.Record{
			Topic: topicShipments,
			Key:   []byte(orderID),
			Value: []byte(fmt.Sprintf(`{"order_id":%q,"address":"warehouse-7"}`, orderID)),
		},
	)
	return results.FirstErr()
}

func readEndOffsets(ctx context.Context, cl *kgo.Client) (orders, payments, shipments int64, err error) {
	adm := kadm.NewClient(cl)
	offsets, err := adm.ListEndOffsets(ctx, topicOrders, topicPayments, topicShipments)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("ListEndOffsets: %w", err)
	}
	if perr := offsets.Error(); perr != nil {
		return 0, 0, 0, fmt.Errorf("ListEndOffsets partial: %w", perr)
	}
	return sumEnd(offsets, topicOrders), sumEnd(offsets, topicPayments), sumEnd(offsets, topicShipments), nil
}

func sumEnd(offsets kadm.ListedOffsets, topic string) int64 {
	var total int64
	for _, p := range offsets[topic] {
		total += p.Offset
	}
	return total
}
