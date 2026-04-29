// kafka-broadcast — sender в асинхронной схеме.
//
// Делает РОВНО ОДНУ вещь: пишет UserSignedUp в топик user-events.
// Никакого списка получателей здесь нет и быть не должно — кто и
// зачем будет читать это сообщение, sender'а вообще не касается.
//
// Что важно увидеть в этой реализации (это и обсуждается в README):
//
//   - sender НЕ знает про получателей. Добавить нового — поднять ещё
//     одну consumer-группу, sender не меняется;
//   - latency броадкаста = только produce, дальше события доставляются
//     получателям независимо. Один медленный получатель никак не
//     тормозит ни sender'а, ни остальных получателей;
//   - топик — durable. Сообщение лежит до retention.ms; получатель
//     может упасть, перезапуститься, replay'нуть — sender об этом
//     не узнает;
//   - порядок гарантирован per-partition; ключ user_id даёт «события
//     одного пользователя — в одну партицию», что важно если
//     получатель захочет stateful-обработку.
//
// Запуск: см. Makefile (`make run-kafka-broadcast`).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	usersv1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-03-sync-vs-async/gen/users/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic       = "lecture-06-03-user-events"
	defaultPartitions  = 3
	defaultReplication = 3
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик, в который пишем события")
	users := flag.Int("users", 5, "сколько UserSignedUp событий записать")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании топика")
	rf := flag.Int("rf", defaultReplication, "replication factor при создании топика")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	cl, err := kafka.NewClient()
	if err != nil {
		logger.Error("kafka.NewClient", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	admin := kadm.NewClient(cl)
	if err := ensureTopic(ctx, admin, *topic, int32(*partitions), int16(*rf)); err != nil {
		logger.Error("ensure topic", "err", err)
		os.Exit(1)
	}

	fmt.Printf("пишем %d событий в топик %q\n\n", *users, *topic)

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "N\tUSER_ID\tEMAIL\tPARTITION\tOFFSET")

	for i := 0; i < *users; i++ {
		if ctx.Err() != nil {
			_ = tw.Flush()
			return
		}
		ev := mockUser(i)
		payload, err := proto.Marshal(ev)
		if err != nil {
			logger.Error("marshal", "err", err)
			os.Exit(1)
		}
		rec := &kgo.Record{
			Topic: *topic,
			// Ключ = user_id. Все события одного пользователя лягут в
			// одну партицию — это даст порядок per-user если кому-то
			// из получателей этот порядок нужен.
			Key:   []byte(ev.GetUserId()),
			Value: payload,
		}

		rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
		res := cl.ProduceSync(rpcCtx, rec)
		rpcCancel()
		if err := res.FirstErr(); err != nil {
			_ = tw.Flush()
			logger.Error("produce", "i", i, "err", err)
			os.Exit(1)
		}
		got := res[0].Record
		fmt.Fprintf(tw, "%d\t%s\t%s\t%d\t%d\n",
			i+1, ev.GetUserId(), ev.GetEmail(), got.Partition, got.Offset)
	}
	_ = tw.Flush()

	fmt.Println()
	fmt.Println("готово. Никто не дёрнут синхронно — события лежат в топике, ")
	fmt.Println("читать их будут запущенные kafka-listener'ы (в каждой группе — копия).")
}

func ensureTopic(ctx context.Context, admin *kadm.Client, topic string, partitions int32, rf int16) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	resp, err := admin.CreateTopic(rpcCtx, partitions, rf, nil, topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("topic %q создан: partitions=%d rf=%d\n\n", topic, partitions, rf)
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if errors.Is(cause, kerr.TopicAlreadyExists) {
		fmt.Printf("topic %q уже существует — пишем как есть\n\n", topic)
		return nil
	}
	return cause
}

func mockUser(i int) *usersv1.UserSignedUp {
	countries := []string{"KZ", "UZ", "GE", "AM", "RU"}
	return &usersv1.UserSignedUp{
		UserId:     uuid.NewString(),
		Email:      fmt.Sprintf("user-%d@example.com", i),
		Country:    countries[i%len(countries)],
		SignedUpAt: timestamppb.Now(),
	}
}
