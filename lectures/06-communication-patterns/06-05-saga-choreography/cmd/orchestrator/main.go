// orchestrator — центральная state-machine саги в orchestration-варианте.
//
// Подписан на 4 топика: place-order и три *-reply. Каждое сообщение — это
// перевод саги в следующее состояние. saga_state в Postgres хранит
// current_step, status и последнее событие; шаги перевода:
//
//	(none) + place-order            → AWAITING_PAYMENT, отправляет payment.AUTHORIZE
//	AWAITING_PAYMENT + payment.ok   → AWAITING_INVENTORY, отправляет inventory.RESERVE
//	AWAITING_PAYMENT + payment.fail → DONE/FAILED
//	AWAITING_INVENTORY + inv.ok     → AWAITING_SHIPMENT, отправляет shipment.SCHEDULE
//	AWAITING_INVENTORY + inv.fail   → COMPENSATING_PAYMENT, отправляет payment.REFUND
//	AWAITING_SHIPMENT + ship.ok     → DONE/SUCCESS
//	AWAITING_SHIPMENT + ship.fail   → COMPENSATING_INVENTORY, отправляет inventory.RELEASE
//	COMPENSATING_INVENTORY + inv reply → COMPENSATING_PAYMENT, отправляет payment.REFUND
//	COMPENSATING_PAYMENT + payment reply → DONE/FAILED
//
// Что мы упрощаем относительно production: UPDATE saga_state и Produce
// next-cmd идут двумя последовательными вызовами без транзакционного
// outbox'а. Если оркестратор крашнется ровно между UPDATE и Produce —
// сага зависнет. В production это закрывается outbox'ом или EOS-транзакцией
// (см. лекции 04-01 и 04-03). Здесь намеренно проще, чтобы фокус был на
// state-машине, а не на инфраструктуре.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	sagav1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-05-saga-choreography/gen/saga/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-05-saga-choreography/internal/sagaio"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultDSN = "postgres://lecture:lecture@localhost:15435/lecture_06_05?sslmode=disable"

	stepAwaitingPayment       = "AWAITING_PAYMENT"
	stepAwaitingInventory     = "AWAITING_INVENTORY"
	stepAwaitingShipment      = "AWAITING_SHIPMENT"
	stepCompensatingInventory = "COMPENSATING_INVENTORY"
	stepCompensatingPayment   = "COMPENSATING_PAYMENT"
	stepDone                  = "DONE"

	statusRunning = "RUNNING"
	statusSuccess = "SUCCESS"
	statusFailed  = "FAILED"
)

const insertSagaSQL = `
INSERT INTO saga_state (saga_id, customer_id, amount_cents, currency, current_step, status, last_event)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (saga_id) DO NOTHING
RETURNING saga_id
`

const selectSagaSQL = `
SELECT customer_id, amount_cents, currency, current_step, status, last_event,
       payment_id, reservation_id, shipment_id, failure_reason
  FROM saga_state
 WHERE saga_id = $1
`

const updateSagaSQL = `
UPDATE saga_state
   SET current_step = $2,
       status       = $3,
       last_event   = $4,
       payment_id   = COALESCE($5, payment_id),
       reservation_id = COALESCE($6, reservation_id),
       shipment_id  = COALESCE($7, shipment_id),
       failure_reason = COALESCE($8, failure_reason),
       updated_at   = NOW()
 WHERE saga_id = $1
`

type sagaRow struct {
	customerID  string
	amountCents int64
	currency    string
	step        string
	status      string
	lastEvent   string
	paymentID   string
	reservID    string
	shipmentID  string
	failure     string
}

func main() {
	logger := log.New()
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	dsn := config.EnvOr("DATABASE_URL", defaultDSN)

	if err := run(ctx, dsn); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("orchestrator", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, dsn string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("pgxpool.New: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("pg ping: %w", err)
	}

	cl, err := kafka.NewClient(
		kgo.ConsumerGroup("lecture-06-05-orchestrator"),
		kgo.ConsumeTopics(
			sagaio.TopicOrchPlaceOrder,
			sagaio.TopicOrchPaymentReply,
			sagaio.TopicOrchInventoryReply,
			sagaio.TopicOrchShipmentReply,
		),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-06-05-orchestrator"),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Println("orchestrator: подписан на place-order + три *-reply, ждём входящих саг...")

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
			if err := dispatch(ctx, pool, cl, r); err != nil {
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

func dispatch(ctx context.Context, pool *pgxpool.Pool, cl *kgo.Client, r *kgo.Record) error {
	switch r.Topic {
	case sagaio.TopicOrchPlaceOrder:
		return onPlaceOrder(ctx, pool, cl, r)
	case sagaio.TopicOrchPaymentReply:
		return onPaymentReply(ctx, pool, cl, r)
	case sagaio.TopicOrchInventoryReply:
		return onInventoryReply(ctx, pool, cl, r)
	case sagaio.TopicOrchShipmentReply:
		return onShipmentReply(ctx, pool, cl, r)
	}
	return nil
}

func onPlaceOrder(ctx context.Context, pool *pgxpool.Pool, cl *kgo.Client, r *kgo.Record) error {
	var po sagav1.PlaceOrder
	if err := sagaio.Unmarshal(r, &po); err != nil {
		return err
	}

	var got string
	err := pool.QueryRow(ctx, insertSagaSQL,
		po.GetSagaId(), po.GetCustomerId(), po.GetAmountCents(), po.GetCurrency(),
		stepAwaitingPayment, statusRunning, "place-order",
	).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		// сага уже существует — это retry place-order'а, игнорируем
		fmt.Printf("PLACE     saga=%s already-exists, skip\n", short(po.GetSagaId()))
		return nil
	}
	if err != nil {
		return fmt.Errorf("INSERT saga_state: %w", err)
	}

	fmt.Printf("PLACE     saga=%s customer=%s amount=%d %s → AWAITING_PAYMENT\n",
		short(po.GetSagaId()), po.GetCustomerId(), po.GetAmountCents(), po.GetCurrency())

	return sagaio.Produce(ctx, cl, sagaio.TopicOrchPaymentCmd, po.GetSagaId(),
		&sagav1.PaymentCommand{
			SagaId:      po.GetSagaId(),
			Action:      sagav1.PaymentAction_PAYMENT_ACTION_AUTHORIZE,
			CustomerId:  po.GetCustomerId(),
			AmountCents: po.GetAmountCents(),
			Currency:    po.GetCurrency(),
		})
}

func onPaymentReply(ctx context.Context, pool *pgxpool.Pool, cl *kgo.Client, r *kgo.Record) error {
	var rep sagav1.PaymentReply
	if err := sagaio.Unmarshal(r, &rep); err != nil {
		return err
	}

	row, err := loadSaga(ctx, pool, rep.GetSagaId())
	if err != nil {
		return err
	}
	if row == nil {
		fmt.Printf("WARN      saga=%s payment-reply без saga_state, skip\n", short(rep.GetSagaId()))
		return nil
	}

	switch rep.GetAction() {
	case sagav1.PaymentAction_PAYMENT_ACTION_AUTHORIZE:
		if rep.GetOk() {
			pid := rep.GetPaymentId()
			if err := updateSaga(ctx, pool, rep.GetSagaId(),
				stepAwaitingInventory, statusRunning, "payment.authorized",
				&pid, nil, nil, nil); err != nil {
				return err
			}
			fmt.Printf("PAYMENT.OK saga=%s payment_id=%s → AWAITING_INVENTORY\n", short(rep.GetSagaId()), pid)
			return sagaio.Produce(ctx, cl, sagaio.TopicOrchInventoryCmd, rep.GetSagaId(),
				&sagav1.InventoryCommand{
					SagaId:      rep.GetSagaId(),
					Action:      sagav1.InventoryAction_INVENTORY_ACTION_RESERVE,
					CustomerId:  row.customerID,
					AmountCents: row.amountCents,
				})
		}
		// payment.AUTHORIZE failed — компенсаций не нужно, ничего не делалось
		reason := rep.GetReason()
		if err := updateSaga(ctx, pool, rep.GetSagaId(),
			stepDone, statusFailed, "payment.failed", nil, nil, nil, &reason); err != nil {
			return err
		}
		fmt.Printf("PAYMENT.FAIL saga=%s reason=%q → DONE/FAILED\n", short(rep.GetSagaId()), reason)
		return nil

	case sagav1.PaymentAction_PAYMENT_ACTION_REFUND:
		// REFUND приходит в фазе компенсации. Это финальный шаг — что бы ни
		// ответил сервис refund'а, оркестратор переводит сагу в FAILED;
		// если рефанд не удался, дальше начинается ручной разбор.
		failure := row.failure
		if !rep.GetOk() {
			failure = "refund-failed: " + rep.GetReason()
		}
		if err := updateSaga(ctx, pool, rep.GetSagaId(),
			stepDone, statusFailed, "payment.refunded", nil, nil, nil, &failure); err != nil {
			return err
		}
		fmt.Printf("PAYMENT.REFUNDED saga=%s → DONE/FAILED reason=%q\n", short(rep.GetSagaId()), failure)
		return nil
	}
	return nil
}

func onInventoryReply(ctx context.Context, pool *pgxpool.Pool, cl *kgo.Client, r *kgo.Record) error {
	var rep sagav1.InventoryReply
	if err := sagaio.Unmarshal(r, &rep); err != nil {
		return err
	}
	row, err := loadSaga(ctx, pool, rep.GetSagaId())
	if err != nil {
		return err
	}
	if row == nil {
		fmt.Printf("WARN      saga=%s inventory-reply без saga_state, skip\n", short(rep.GetSagaId()))
		return nil
	}

	switch rep.GetAction() {
	case sagav1.InventoryAction_INVENTORY_ACTION_RESERVE:
		if rep.GetOk() {
			rid := rep.GetReservationId()
			if err := updateSaga(ctx, pool, rep.GetSagaId(),
				stepAwaitingShipment, statusRunning, "inventory.reserved",
				nil, &rid, nil, nil); err != nil {
				return err
			}
			fmt.Printf("INVENT.OK saga=%s reservation_id=%s → AWAITING_SHIPMENT\n", short(rep.GetSagaId()), rid)
			return sagaio.Produce(ctx, cl, sagaio.TopicOrchShipmentCmd, rep.GetSagaId(),
				&sagav1.ShipmentCommand{
					SagaId:     rep.GetSagaId(),
					Action:     sagav1.ShipmentAction_SHIPMENT_ACTION_SCHEDULE,
					CustomerId: row.customerID,
				})
		}
		// inventory.RESERVE failed → надо рефанднуть payment
		reason := "inventory-failed: " + rep.GetReason()
		if err := updateSaga(ctx, pool, rep.GetSagaId(),
			stepCompensatingPayment, statusRunning, "inventory.failed", nil, nil, nil, &reason); err != nil {
			return err
		}
		fmt.Printf("INVENT.FAIL saga=%s reason=%q → COMPENSATING_PAYMENT\n", short(rep.GetSagaId()), rep.GetReason())
		return sagaio.Produce(ctx, cl, sagaio.TopicOrchPaymentCmd, rep.GetSagaId(),
			&sagav1.PaymentCommand{
				SagaId:      rep.GetSagaId(),
				Action:      sagav1.PaymentAction_PAYMENT_ACTION_REFUND,
				CustomerId:  row.customerID,
				AmountCents: row.amountCents,
				Currency:    row.currency,
			})

	case sagav1.InventoryAction_INVENTORY_ACTION_RELEASE:
		// RELEASE — компенсация после shipment.failed. Дальше — payment refund.
		if err := updateSaga(ctx, pool, rep.GetSagaId(),
			stepCompensatingPayment, statusRunning, "inventory.released", nil, nil, nil, nil); err != nil {
			return err
		}
		fmt.Printf("INVENT.RELEASED saga=%s → COMPENSATING_PAYMENT\n", short(rep.GetSagaId()))
		return sagaio.Produce(ctx, cl, sagaio.TopicOrchPaymentCmd, rep.GetSagaId(),
			&sagav1.PaymentCommand{
				SagaId:      rep.GetSagaId(),
				Action:      sagav1.PaymentAction_PAYMENT_ACTION_REFUND,
				CustomerId:  row.customerID,
				AmountCents: row.amountCents,
				Currency:    row.currency,
			})
	}
	return nil
}

func onShipmentReply(ctx context.Context, pool *pgxpool.Pool, cl *kgo.Client, r *kgo.Record) error {
	var rep sagav1.ShipmentReply
	if err := sagaio.Unmarshal(r, &rep); err != nil {
		return err
	}
	row, err := loadSaga(ctx, pool, rep.GetSagaId())
	if err != nil {
		return err
	}
	if row == nil {
		fmt.Printf("WARN      saga=%s shipment-reply без saga_state, skip\n", short(rep.GetSagaId()))
		return nil
	}

	if rep.GetOk() {
		sid := rep.GetShipmentId()
		if err := updateSaga(ctx, pool, rep.GetSagaId(),
			stepDone, statusSuccess, "shipment.scheduled", nil, nil, &sid, nil); err != nil {
			return err
		}
		fmt.Printf("SHIP.OK   saga=%s shipment_id=%s → DONE/SUCCESS\n", short(rep.GetSagaId()), sid)
		return nil
	}
	// shipment.SCHEDULE failed → надо отпустить резерв, потом рефанд
	reason := "shipment-failed: " + rep.GetReason()
	if err := updateSaga(ctx, pool, rep.GetSagaId(),
		stepCompensatingInventory, statusRunning, "shipment.failed", nil, nil, nil, &reason); err != nil {
		return err
	}
	fmt.Printf("SHIP.FAIL saga=%s reason=%q → COMPENSATING_INVENTORY\n", short(rep.GetSagaId()), rep.GetReason())
	return sagaio.Produce(ctx, cl, sagaio.TopicOrchInventoryCmd, rep.GetSagaId(),
		&sagav1.InventoryCommand{
			SagaId:      rep.GetSagaId(),
			Action:      sagav1.InventoryAction_INVENTORY_ACTION_RELEASE,
			CustomerId:  row.customerID,
			AmountCents: row.amountCents,
		})
}

func loadSaga(ctx context.Context, pool *pgxpool.Pool, sagaID string) (*sagaRow, error) {
	var row sagaRow
	var paymentID, reservID, shipmentID, failure *string
	err := pool.QueryRow(ctx, selectSagaSQL, sagaID).Scan(
		&row.customerID, &row.amountCents, &row.currency,
		&row.step, &row.status, &row.lastEvent,
		&paymentID, &reservID, &shipmentID, &failure,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("SELECT saga_state: %w", err)
	}
	if paymentID != nil {
		row.paymentID = *paymentID
	}
	if reservID != nil {
		row.reservID = *reservID
	}
	if shipmentID != nil {
		row.shipmentID = *shipmentID
	}
	if failure != nil {
		row.failure = *failure
	}
	return &row, nil
}

func updateSaga(ctx context.Context, pool *pgxpool.Pool, sagaID, step, status, lastEvent string,
	paymentID, reservID, shipmentID, failure *string) error {
	_, err := pool.Exec(ctx, updateSagaSQL, sagaID, step, status, lastEvent,
		paymentID, reservID, shipmentID, failure)
	if err != nil {
		return fmt.Errorf("UPDATE saga_state: %w", err)
	}
	return nil
}

func short(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}
