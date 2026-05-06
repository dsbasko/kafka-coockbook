package sagaio

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// Produce маршалит protobuf и шлёт в Kafka синхронно. Ключ — saga_id, чтобы
// все события одной саги ложились в одну партицию и обрабатывались строго
// по порядку каждым consumer'ом. Без этого compensation мог бы прийти
// раньше прямого события.
func Produce(ctx context.Context, cl *kgo.Client, topic, sagaID string, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("proto.Marshal %T: %w", msg, err)
	}
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(sagaID),
		Value: payload,
		Headers: []kgo.RecordHeader{
			{Key: "saga-id", Value: []byte(sagaID)},
			{Key: "event-type", Value: []byte(fmt.Sprintf("%T", msg))},
		},
	}
	res := cl.ProduceSync(ctx, rec)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("ProduceSync %s: %w", topic, err)
	}
	return nil
}

// Unmarshal — обёртка над proto.Unmarshal с подсказкой, что произошла
// ошибка на конкретной паре topic+offset. Это нужно при отладке: иначе
// в логах висит «proto: cannot parse» без контекста.
func Unmarshal(r *kgo.Record, msg proto.Message) error {
	if err := proto.Unmarshal(r.Value, msg); err != nil {
		return fmt.Errorf("Unmarshal %s p=%d off=%d: %w", r.Topic, r.Partition, r.Offset, err)
	}
	return nil
}

// Short — безопасный prefix saga_id для логов. Без guard'а пустой/короткий
// id, прилетевший из чужого/частично заполненного сообщения, валит сервис
// `slice bounds out of range` и блокирует консьюмер на повторе offset'а.
func Short(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}
