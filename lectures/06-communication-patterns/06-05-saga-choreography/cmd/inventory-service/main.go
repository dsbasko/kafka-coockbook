// inventory-service — резервирует и компенсирует резерв.
//
// choreo:
//
//	payment.completed → inventory.reserved | inventory.failed
//	shipment.failed   → inventory.released
//
// orch: subscribed to inventory-cmd, отвечает в inventory-reply.
// RESERVE может срываться через FAIL_RATE; RELEASE — компенсация, всегда ok.
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
	failRateStr := flag.String("fail-rate", "", "вероятность сорвать RESERVE [0..1]; пустое — из FAIL_RATE")
	flag.Parse()

	failRate := parseRate(*failRateStr, config.EnvOr("FAIL_RATE", "0"))

	ctx, cancel := runctx.New()
	defer cancel()

	switch *mode {
	case "choreo":
		if err := runChoreo(ctx, failRate); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("inventory-service choreo", "err", err)
			os.Exit(1)
		}
	case "orch":
		if err := runOrch(ctx, failRate); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("inventory-service orch", "err", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "mode must be choreo or orch")
		os.Exit(2)
	}
}

func parseRate(flagVal, envVal string) float64 {
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
		kgo.ConsumerGroup("lecture-06-05-inventory-choreo"),
		kgo.ConsumeTopics(
			sagaio.TopicChoreoPaymentCompleted,
			sagaio.TopicChoreoShipmentFailed,
		),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-06-05-inventory-choreo"),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("inventory-service[choreo] запущен. fail-rate=%.2f\n", failRate)

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
	case sagaio.TopicChoreoPaymentCompleted:
		var evt sagav1.PaymentCompleted
		if err := sagaio.Unmarshal(r, &evt); err != nil {
			return err
		}
		now := timestamppb.New(time.Now().UTC())
		if shouldFail(failRate) {
			fmt.Printf("RESERVE   saga=%s FAIL (fail-rate)\n", sagaio.Short(evt.GetSagaId()))
			return sagaio.Produce(ctx, cl, sagaio.TopicChoreoInventoryFailed, evt.GetSagaId(),
				&sagav1.InventoryFailed{
					SagaId:     evt.GetSagaId(),
					Reason:     "out of stock",
					OccurredAt: now,
				})
		}
		reservationID := "res-" + uuid.NewString()[:8]
		fmt.Printf("RESERVE   saga=%s OK reservation_id=%s\n", sagaio.Short(evt.GetSagaId()), reservationID)
		return sagaio.Produce(ctx, cl, sagaio.TopicChoreoInventoryReserved, evt.GetSagaId(),
			&sagav1.InventoryReserved{
				SagaId:        evt.GetSagaId(),
				ReservationId: reservationID,
				OccurredAt:    now,
			})

	case sagaio.TopicChoreoShipmentFailed:
		var evt sagav1.ShipmentFailed
		if err := sagaio.Unmarshal(r, &evt); err != nil {
			return err
		}
		// Резерв был, теперь надо вернуть. Компенсация всегда ok.
		fmt.Printf("RELEASE   saga=%s reason=%s\n", sagaio.Short(evt.GetSagaId()), evt.GetReason())
		return sagaio.Produce(ctx, cl, sagaio.TopicChoreoInventoryReleased, evt.GetSagaId(),
			&sagav1.InventoryReleased{
				SagaId:        evt.GetSagaId(),
				ReservationId: "",
				Reason:        "compensate-after-shipment-failed: " + evt.GetReason(),
				OccurredAt:    timestamppb.New(time.Now().UTC()),
			})
	}
	return nil
}

func runOrch(ctx context.Context, failRate float64) error {
	cl, err := kafka.NewClient(
		kgo.ConsumerGroup("lecture-06-05-inventory-orch"),
		kgo.ConsumeTopics(sagaio.TopicOrchInventoryCmd),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-06-05-inventory-orch"),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("inventory-service[orch] запущен. fail-rate=%.2f\n", failRate)

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
			var cmd sagav1.InventoryCommand
			if err := sagaio.Unmarshal(r, &cmd); err != nil {
				return err
			}
			reply := &sagav1.InventoryReply{
				SagaId: cmd.GetSagaId(),
				Action: cmd.GetAction(),
			}
			switch cmd.GetAction() {
			case sagav1.InventoryAction_INVENTORY_ACTION_RESERVE:
				if shouldFail(failRate) {
					reply.Ok = false
					reply.Reason = "out of stock"
					fmt.Printf("RESERVE   saga=%s FAIL\n", sagaio.Short(cmd.GetSagaId()))
				} else {
					reply.Ok = true
					reply.ReservationId = "res-" + uuid.NewString()[:8]
					fmt.Printf("RESERVE   saga=%s OK reservation_id=%s\n", sagaio.Short(cmd.GetSagaId()), reply.ReservationId)
				}
			case sagav1.InventoryAction_INVENTORY_ACTION_RELEASE:
				reply.Ok = true
				fmt.Printf("RELEASE   saga=%s OK\n", sagaio.Short(cmd.GetSagaId()))
			default:
				reply.Ok = false
				reply.Reason = "unknown action"
			}
			if err := sagaio.Produce(ctx, cl, sagaio.TopicOrchInventoryReply, cmd.GetSagaId(), reply); err != nil {
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
