// order-service-choreo — наблюдатель за сагой в choreography-варианте.
//
// В choreography у саги нет одного «хозяина», который видит все шаги. Этот
// сервис собирает воедино только потому, что подписан на все 9 событий
// саги; в реальности так делает не сервис, а отдельный observability-pipeline.
// В лекции один процесс печатает таймлайн, чтобы было видно, что
// происходит, без копания в нескольких терминалах.
//
// Что делает:
//
//   - subscribe ко всем choreo-топикам (order.requested, payment.*,
//     inventory.*, shipment.*) с собственной consumer-группой;
//   - в памяти держит { saga_id → []событий с временем } и при каждом
//     событии печатает обновлённый таймлайн;
//   - помечает сагу как SUCCESS на shipment-scheduled и FAILED на
//     payment.failed / inventory.failed (без compensation) или после
//     payment.refunded (с compensation).
//
// Запускать только в choreography. Для orchestration наблюдателя нет —
// там состояние и так центрально, в saga_state.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	sagav1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-05-saga-choreography/gen/saga/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-05-saga-choreography/internal/sagaio"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const defaultGroup = "lecture-06-05-order-choreo"

type sagaTimeline struct {
	mu     sync.Mutex
	events map[string][]string
}

func newTimeline() *sagaTimeline {
	return &sagaTimeline{events: map[string][]string{}}
}

func (t *sagaTimeline) record(sagaID, event string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events[sagaID] = append(t.events[sagaID], event)
	fmt.Printf("SAGA %s | %s\n", sagaio.Short(sagaID), event)
	if isTerminal(event) {
		fmt.Printf("       chain: %s\n", chain(t.events[sagaID]))
	}
}

func isTerminal(e string) bool {
	switch e {
	case "shipment.scheduled", "payment.failed", "payment.refunded":
		return true
	}
	return false
}

func chain(evts []string) string {
	out := ""
	for i, e := range evts {
		if i > 0 {
			out += " → "
		}
		out += e
	}
	return out
}

func main() {
	logger := log.New()

	group := flag.String("group", defaultGroup, "consumer group")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	cl, err := kafka.NewClient(
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(
			sagaio.TopicChoreoOrderRequested,
			sagaio.TopicChoreoPaymentCompleted,
			sagaio.TopicChoreoPaymentFailed,
			sagaio.TopicChoreoPaymentRefunded,
			sagaio.TopicChoreoInventoryReserved,
			sagaio.TopicChoreoInventoryFailed,
			sagaio.TopicChoreoInventoryReleased,
			sagaio.TopicChoreoShipmentScheduled,
			sagaio.TopicChoreoShipmentFailed,
		),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-06-05-order-choreo"),
	)
	if err != nil {
		logger.Error("kafka.NewClient", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	tl := newTimeline()
	fmt.Printf("order-service-choreo: подписан на %d choreo-топиков, group=%s\n", 9, *group)
	fmt.Println("ждём события — запусти place-order чтобы стартовать сагу.")

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		pollCtx, pollCancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				logger.Error("fetch", "topic", e.Topic, "p", e.Partition, "err", e.Err)
			}
		}

		batch := make([]*kgo.Record, 0)
		fetches.EachRecord(func(r *kgo.Record) {
			batch = append(batch, r)
			handle(r, tl)
		})
		if len(batch) == 0 {
			continue
		}
		commitCtx, commitCancel := context.WithTimeout(ctx, 10*time.Second)
		if err := cl.CommitRecords(commitCtx, batch...); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("CommitRecords", "err", err)
		}
		commitCancel()
	}
}

func handle(r *kgo.Record, tl *sagaTimeline) {
	type withSaga interface {
		GetSagaId() string
	}

	var msg proto.Message
	var event string
	switch r.Topic {
	case sagaio.TopicChoreoOrderRequested:
		msg, event = &sagav1.OrderRequested{}, "order.requested"
	case sagaio.TopicChoreoPaymentCompleted:
		msg, event = &sagav1.PaymentCompleted{}, "payment.completed"
	case sagaio.TopicChoreoPaymentFailed:
		msg, event = &sagav1.PaymentFailed{}, "payment.failed"
	case sagaio.TopicChoreoPaymentRefunded:
		msg, event = &sagav1.PaymentRefunded{}, "payment.refunded"
	case sagaio.TopicChoreoInventoryReserved:
		msg, event = &sagav1.InventoryReserved{}, "inventory.reserved"
	case sagaio.TopicChoreoInventoryFailed:
		msg, event = &sagav1.InventoryFailed{}, "inventory.failed"
	case sagaio.TopicChoreoInventoryReleased:
		msg, event = &sagav1.InventoryReleased{}, "inventory.released"
	case sagaio.TopicChoreoShipmentScheduled:
		msg, event = &sagav1.ShipmentScheduled{}, "shipment.scheduled"
	case sagaio.TopicChoreoShipmentFailed:
		msg, event = &sagav1.ShipmentFailed{}, "shipment.failed"
	default:
		return
	}
	if err := sagaio.Unmarshal(r, msg); err != nil {
		fmt.Fprintf(os.Stderr, "unmarshal %s: %v\n", r.Topic, err)
		return
	}
	id := msg.(withSaga).GetSagaId()
	tl.record(id, event)
}
