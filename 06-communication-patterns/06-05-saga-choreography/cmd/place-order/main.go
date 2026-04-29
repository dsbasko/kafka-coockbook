// place-order — триггер саги, общий для обоих вариантов. Публикует в
// нужный топик одно или несколько сообщений и выходит. Это не «сервис»,
// а тестовая ручка: имитирует входящий запрос «оформить заказ».
//
// Запуск:
//
//	go run ./cmd/place-order -mode=choreo -count=3
//	go run ./cmd/place-order -mode=orch   -count=3
//
// В choreo-варианте публикуется OrderRequested в saga-choreo.order-requested,
// первый звено цепочки. В orch-варианте — PlaceOrder в saga-orch.place-order,
// его подбирает оркестратор.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	sagav1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-05-saga-choreography/gen/saga/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-05-saga-choreography/internal/sagaio"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	mode := flag.String("mode", "choreo", "choreo | orch")
	count := flag.Int("count", 1, "сколько саг запустить")
	flag.Parse()

	if *count <= 0 {
		fmt.Fprintln(os.Stderr, "count must be > 0")
		os.Exit(2)
	}
	if *mode != "choreo" && *mode != "orch" {
		fmt.Fprintln(os.Stderr, "mode must be choreo or orch")
		os.Exit(2)
	}

	ctx, cancel := runctx.New()
	defer cancel()

	cl, err := kafka.NewClient()
	if err != nil {
		logger.Error("kafka.NewClient", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	for i := 0; i < *count; i++ {
		sagaID := uuid.NewString()
		customer := fmt.Sprintf("cust-%04d", rand.IntN(10000))
		amount := int64(1000 + rand.IntN(9000))
		now := time.Now().UTC()

		var topic string
		var msg proto.Message
		switch *mode {
		case "choreo":
			topic = sagaio.TopicChoreoOrderRequested
			msg = &sagav1.OrderRequested{
				SagaId:      sagaID,
				CustomerId:  customer,
				AmountCents: amount,
				Currency:    "EUR",
				OccurredAt:  timestamppb.New(now),
			}
		case "orch":
			topic = sagaio.TopicOrchPlaceOrder
			msg = &sagav1.PlaceOrder{
				SagaId:      sagaID,
				CustomerId:  customer,
				AmountCents: amount,
				Currency:    "EUR",
			}
		}

		if err := sagaio.Produce(ctx, cl, topic, sagaID, msg); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			logger.Error("produce trigger", "err", err)
			os.Exit(1)
		}
		fmt.Printf("PLACED saga=%s mode=%s customer=%s amount=%d EUR topic=%s\n",
			sagaID, *mode, customer, amount, topic)
	}
}
