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

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/types/known/timestamppb"

	sagav1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-05-saga-choreography/gen/saga/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-05-saga-choreography/internal/sagaio"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	mode := flag.String("mode", "choreo", "choreo | orch")
	failRateStr := flag.String("fail-rate", "", "вероятность сорвать AUTHORIZE [0..1]; пустое значение — взять из FAIL_RATE")
	flag.Parse()

	failRate := parseFailRate(*failRateStr, config.EnvOr("FAIL_RATE", "0"))

	ctx, cancel := runctx.New()
	defer cancel()

	switch *mode {
	case "choreo":
		if err := runChoreo(ctx, failRate); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("payment-service choreo", "err", err)
			os.Exit(1)
		}
	case "orch":
		if err := runOrch(ctx, failRate); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("payment-service orch", "err", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "mode must be choreo or orch")
		os.Exit(2)
	}
}

func parseFailRate(flagVal, envVal string) float64 {
	src := flagVal
	if src == "" {
		src = envVal
	}
	v, err := strconv.ParseFloat(src, 64)
	if err != nil || v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func shouldFail(rate float64) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	return rand.Float64() < rate
}

func runChoreo(ctx context.Context, failRate float64) error {
	cl, err := kafka.NewClient(
		kgo.ConsumerGroup("lecture-06-05-payment-choreo"),
		kgo.ConsumeTopics(
			sagaio.TopicChoreoOrderRequested,
			sagaio.TopicChoreoInventoryFailed,
			sagaio.TopicChoreoInventoryReleased,
		),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-06-05-payment-choreo"),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("payment-service[choreo] запущен. fail-rate=%.2f\n", failRate)

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		pollCtx, pc := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(pollCtx)
		pc()

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}

		batch := make([]*kgo.Record, 0)
		fetches.EachRecord(func(r *kgo.Record) { batch = append(batch, r) })
		for _, r := range batch {
			if err := handleChoreo(ctx, cl, r, failRate); err != nil {
				return err
			}
		}
		if len(batch) == 0 {
			continue
		}
		cctx, cc := context.WithTimeout(ctx, 10*time.Second)
		if err := cl.CommitRecords(cctx, batch...); err != nil && !errors.Is(err, context.Canceled) {
			cc()
			return fmt.Errorf("CommitRecords: %w", err)
		}
		cc()
	}
}

func handleChoreo(ctx context.Context, cl *kgo.Client, r *kgo.Record, failRate float64) error {
	switch r.Topic {
	case sagaio.TopicChoreoOrderRequested:
		var evt sagav1.OrderRequested
		if err := sagaio.Unmarshal(r, &evt); err != nil {
			return err
		}
		now := timestamppb.New(time.Now().UTC())
		if shouldFail(failRate) {
			fmt.Printf("AUTHORIZE saga=%s FAIL (fail-rate)\n", sagaio.Short(evt.GetSagaId()))
			return sagaio.Produce(ctx, cl, sagaio.TopicChoreoPaymentFailed, evt.GetSagaId(),
				&sagav1.PaymentFailed{
					SagaId:     evt.GetSagaId(),
					Reason:     "card declined",
					OccurredAt: now,
				})
		}
		paymentID := "pay-" + uuid.NewString()[:8]
		fmt.Printf("AUTHORIZE saga=%s OK payment_id=%s\n", sagaio.Short(evt.GetSagaId()), paymentID)
		return sagaio.Produce(ctx, cl, sagaio.TopicChoreoPaymentCompleted, evt.GetSagaId(),
			&sagav1.PaymentCompleted{
				SagaId:      evt.GetSagaId(),
				PaymentId:   paymentID,
				AmountCents: evt.GetAmountCents(),
				Currency:    evt.GetCurrency(),
				OccurredAt:  now,
			})

	case sagaio.TopicChoreoInventoryFailed:
		var evt sagav1.InventoryFailed
		if err := sagaio.Unmarshal(r, &evt); err != nil {
			return err
		}

		fmt.Printf("REFUND    saga=%s reason=inventory:%s\n", sagaio.Short(evt.GetSagaId()), evt.GetReason())
		return sagaio.Produce(ctx, cl, sagaio.TopicChoreoPaymentRefunded, evt.GetSagaId(),
			&sagav1.PaymentRefunded{
				SagaId:     evt.GetSagaId(),
				Reason:     "compensate-after-inventory-failed: " + evt.GetReason(),
				OccurredAt: timestamppb.New(time.Now().UTC()),
			})

	case sagaio.TopicChoreoInventoryReleased:
		var evt sagav1.InventoryReleased
		if err := sagaio.Unmarshal(r, &evt); err != nil {
			return err
		}

		fmt.Printf("REFUND    saga=%s reason=shipment-cascade\n", sagaio.Short(evt.GetSagaId()))
		return sagaio.Produce(ctx, cl, sagaio.TopicChoreoPaymentRefunded, evt.GetSagaId(),
			&sagav1.PaymentRefunded{
				SagaId:     evt.GetSagaId(),
				Reason:     "compensate-after-shipment-failed",
				OccurredAt: timestamppb.New(time.Now().UTC()),
			})
	}
	return nil
}

func runOrch(ctx context.Context, failRate float64) error {
	cl, err := kafka.NewClient(
		kgo.ConsumerGroup("lecture-06-05-payment-orch"),
		kgo.ConsumeTopics(sagaio.TopicOrchPaymentCmd),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-06-05-payment-orch"),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("payment-service[orch] запущен. fail-rate=%.2f\n", failRate)

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		pollCtx, pc := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(pollCtx)
		pc()

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				return fmt.Errorf("fetch: %w", e.Err)
			}
		}

		batch := make([]*kgo.Record, 0)
		fetches.EachRecord(func(r *kgo.Record) { batch = append(batch, r) })
		for _, r := range batch {
			var cmd sagav1.PaymentCommand
			if err := sagaio.Unmarshal(r, &cmd); err != nil {
				return err
			}
			reply := &sagav1.PaymentReply{
				SagaId: cmd.GetSagaId(),
				Action: cmd.GetAction(),
			}
			switch cmd.GetAction() {
			case sagav1.PaymentAction_PAYMENT_ACTION_AUTHORIZE:
				if shouldFail(failRate) {
					reply.Ok = false
					reply.Reason = "card declined"
					fmt.Printf("AUTHORIZE saga=%s FAIL\n", sagaio.Short(cmd.GetSagaId()))
				} else {
					reply.Ok = true
					reply.PaymentId = "pay-" + uuid.NewString()[:8]
					fmt.Printf("AUTHORIZE saga=%s OK payment_id=%s\n", sagaio.Short(cmd.GetSagaId()), reply.PaymentId)
				}
			case sagav1.PaymentAction_PAYMENT_ACTION_REFUND:
				reply.Ok = true
				reply.PaymentId = "ref-" + uuid.NewString()[:8]
				fmt.Printf("REFUND    saga=%s OK\n", sagaio.Short(cmd.GetSagaId()))
			default:
				reply.Ok = false
				reply.Reason = "unknown action"
			}
			if err := sagaio.Produce(ctx, cl, sagaio.TopicOrchPaymentReply, cmd.GetSagaId(), reply); err != nil {
				return err
			}
		}

		if len(batch) == 0 {
			continue
		}
		cctx, cc := context.WithTimeout(ctx, 10*time.Second)
		if err := cl.CommitRecords(cctx, batch...); err != nil && !errors.Is(err, context.Canceled) {
			cc()
			return fmt.Errorf("CommitRecords: %w", err)
		}
		cc()
	}
}
