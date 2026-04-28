// consumer-v1 — читает указанный топик так, будто в мире существует
// только Order схемы v1 (id, customer_id, amount_cents).
//
// По умолчанию подписан на `lecture-05-04-orders-v3` — туда пишет
// producer-v3 сообщения со всеми пятью полями. Это и есть демонстрация:
//
//   - consumer-v1 не зовёт SR и не использует schema_id для разбора.
//     Он срезает первые 5 байт Confluent wire format'а плюс protobuf
//     message-index, остаток подаёт в proto.Unmarshal в *v1.Order;
//   - тэги 4 (currency) и 5 (shipping_address) уходят в unknown
//     fields структуры — это и есть forward compatibility Protobuf'а
//     на wire-уровне;
//   - rolling deployment: новый producer выкатился, старые consumer'ы
//     ничего не знают про новые поля и не падают.
//
// Можно переключить на `lecture-05-04-orders-v1` — там лежат сообщения
// от producer-v1, читаются они идентично, но без unknown fields.
//
// Запуск: см. Makefile (`make run-consumer-v1`).
package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/05-contracts/05-04-schema-evolution/gen/orders/v1"
	kafkactl "github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	topic := flag.String("topic", "lecture-05-04-orders-v3", "топик для чтения")
	group := flag.String("group", "lecture-05-04-consumer-v1", "consumer group")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	cl, err := kafkactl.NewClient(
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		logger.Error("kafka client", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	logger.Info("consumer-v1 started", "topic", *topic, "group", *group)

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			logger.Info("consumer stopping", "reason", ctx.Err())
			return
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				logger.Error("fetch", "topic", e.Topic, "partition", e.Partition, "err", e.Err)
			}
			os.Exit(1)
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			schemaID, payload, err := stripWireFormatHeader(rec.Value)
			if err != nil {
				logger.Error("strip header", "err", err, "partition", rec.Partition, "offset", rec.Offset)
				return
			}

			var order ordersv1.Order
			if err := proto.Unmarshal(payload, &order); err != nil {
				logger.Error("unmarshal v1", "err", err, "partition", rec.Partition, "offset", rec.Offset)
				return
			}
			printOrder(rec, schemaID, &order)
		})
	}
}

// stripWireFormatHeader разбирает Confluent wire format вручную:
//
//	[0]      magic byte (всегда 0)
//	[1..4]   schema_id big-endian
//	[5..]    для protobuf — message-index varint(0) или массив, потом payload
//
// На уровне consumer-v1 нам не нужен schema_id для разбора (мы намеренно
// притворяемся старым клиентом), но удобно вывести его в лог — видно,
// что одни и те же байты приходили под разными schema_id.
func stripWireFormatHeader(raw []byte) (int, []byte, error) {
	if len(raw) < 5 {
		return 0, nil, errors.New("payload too short for confluent wire format")
	}
	if raw[0] != 0 {
		return 0, nil, fmt.Errorf("magic byte = %d, ожидали 0", raw[0])
	}
	id := int(binary.BigEndian.Uint32(raw[1:5]))

	rest := raw[5:]
	idx, n, err := readMessageIndex(rest)
	if err != nil {
		return 0, nil, fmt.Errorf("message index: %w", err)
	}
	if idx != 0 && idx != -1 {
		// Для одного top-level message в файле SR обычно пишет 0
		// (один varint). Многосообщенные файлы и nested сюда не
		// доехали — лекция этим случаем не занимается.
		return 0, nil, fmt.Errorf("неподдержанный message-index: %d", idx)
	}
	return id, rest[n:], nil
}

// readMessageIndex читает либо одиночный varint(0) (большинство случаев),
// либо length-префикс + N varint'ов (Confluent формат). Возвращает первый
// индекс и сколько байт занял заголовок.
func readMessageIndex(b []byte) (int, int, error) {
	if len(b) == 0 {
		return 0, 0, errors.New("empty payload")
	}
	v, n := binary.Varint(b)
	if n <= 0 {
		return 0, 0, errors.New("invalid varint")
	}
	if v == 0 {
		return 0, n, nil
	}
	consumed := n
	idx, n2 := binary.Varint(b[consumed:])
	if n2 <= 0 {
		return 0, 0, errors.New("invalid first index")
	}
	consumed += n2
	for i := int64(1); i < v; i++ {
		_, nn := binary.Varint(b[consumed:])
		if nn <= 0 {
			return 0, 0, errors.New("invalid extra index")
		}
		consumed += nn
	}
	return int(idx), consumed, nil
}

func printOrder(rec *kgo.Record, schemaID int, o *ordersv1.Order) {
	unknown := len(o.ProtoReflect().GetUnknown())
	fmt.Printf("--- %s/%d@%d key=%s schema_id=%d ---\n",
		rec.Topic, rec.Partition, rec.Offset, string(rec.Key), schemaID)
	fmt.Printf("  id           = %s\n", o.GetId())
	fmt.Printf("  customer_id  = %s\n", o.GetCustomerId())
	fmt.Printf("  amount_cents = %d\n", o.GetAmountCents())
	if unknown > 0 {
		fmt.Printf("  unknown      = %d bytes (поля v3, которые v1 не знает)\n", unknown)
	}
}
