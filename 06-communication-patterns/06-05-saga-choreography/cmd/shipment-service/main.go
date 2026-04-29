// shipment-service — последний шаг саги.
//
// choreo:
//
//	inventory.reserved → shipment.scheduled | shipment.failed
//
// orch: subscribed to shipment-cmd, отвечает в shipment-reply.
//
// shipment.failed запускает каскад компенсаций: inventory отпускает резерв,
// payment рефаундит — это то, что показывает make chaos-fail-shipment.
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
	failRateStr := flag.String("fail-rate", "", "вероятность сорвать SCHEDULE [0..1]; пустое — из FAIL_RATE")
	flag.Parse()

	failRate := parseRate(*failRateStr, config.EnvOr("FAIL_RATE", "0"))

	ctx, cancel := runctx.New()
	defer cancel()

	switch *mode {
	case "choreo":
		if err := runChoreo(ctx, failRate); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("shipment-service choreo", "err", err)
			os.Exit(1)
		}
	case "orch":
		if err := runOrch(ctx, failRate); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("shipment-service orch", "err", err)
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
		kgo.ConsumerGroup("lecture-06-05-shipment-choreo"),
		kgo.ConsumeTopics(sagaio.TopicChoreoInventoryReserved),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-06-05-shipment-choreo"),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("shipment-service[choreo] запущен. fail-rate=%.2f\n", failRate)

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
			var evt sagav1.InventoryReserved
			if err := sagaio.Unmarshal(r, &evt); err != nil {
				return err
			}
			now := timestamppb.New(time.Now().UTC())
			if shouldFail(failRate) {
				fmt.Printf("SCHEDULE  saga=%s FAIL (fail-rate)\n", sagaio.Short(evt.GetSagaId()))
				if err := sagaio.Produce(ctx, cl, sagaio.TopicChoreoShipmentFailed, evt.GetSagaId(),
					&sagav1.ShipmentFailed{
						SagaId:     evt.GetSagaId(),
						Reason:     "no courier capacity",
						OccurredAt: now,
					}); err != nil {
					return err
				}
				continue
			}
			shipmentID := "ship-" + uuid.NewString()[:8]
			fmt.Printf("SCHEDULE  saga=%s OK shipment_id=%s\n", sagaio.Short(evt.GetSagaId()), shipmentID)
			if err := sagaio.Produce(ctx, cl, sagaio.TopicChoreoShipmentScheduled, evt.GetSagaId(),
				&sagav1.ShipmentScheduled{
					SagaId:     evt.GetSagaId(),
					ShipmentId: shipmentID,
					OccurredAt: now,
				}); err != nil {
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

func runOrch(ctx context.Context, failRate float64) error {
	cl, err := kafka.NewClient(
		kgo.ConsumerGroup("lecture-06-05-shipment-orch"),
		kgo.ConsumeTopics(sagaio.TopicOrchShipmentCmd),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-06-05-shipment-orch"),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("shipment-service[orch] запущен. fail-rate=%.2f\n", failRate)

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
			var cmd sagav1.ShipmentCommand
			if err := sagaio.Unmarshal(r, &cmd); err != nil {
				return err
			}
			reply := &sagav1.ShipmentReply{
				SagaId: cmd.GetSagaId(),
				Action: cmd.GetAction(),
			}
			if cmd.GetAction() == sagav1.ShipmentAction_SHIPMENT_ACTION_SCHEDULE {
				if shouldFail(failRate) {
					reply.Ok = false
					reply.Reason = "no courier capacity"
					fmt.Printf("SCHEDULE  saga=%s FAIL\n", sagaio.Short(cmd.GetSagaId()))
				} else {
					reply.Ok = true
					reply.ShipmentId = "ship-" + uuid.NewString()[:8]
					fmt.Printf("SCHEDULE  saga=%s OK shipment_id=%s\n", sagaio.Short(cmd.GetSagaId()), reply.ShipmentId)
				}
			} else {
				reply.Ok = false
				reply.Reason = "unknown action"
			}
			if err := sagaio.Produce(ctx, cl, sagaio.TopicOrchShipmentReply, cmd.GetSagaId(), reply); err != nil {
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
