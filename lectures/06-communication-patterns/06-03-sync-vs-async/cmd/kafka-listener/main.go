// kafka-listener — приёмник UserSignedUp в асинхронной схеме.
//
// Запускается под именем своей consumer-группы. ВАЖНО: имя группы
// определяет «кто получит событие». Sender один раз пишет в топик
// user-events, дальше каждая отдельная группа независимо читает все
// сообщения. Аналог трёх gRPC-приёмников из синхронного варианта —
// три копии этого процесса с РАЗНЫМИ -group (например, analytics,
// notifications, billing). Если два процесса запустить с одной и
// той же группой — они поделят партиции между собой (это другой
// сценарий, scale-out внутри одного логического получателя; см.
// лекцию 03-01).
//
// Sender ничего про этот listener не знает. Чтобы добавить ещё одного
// «получателя» — поднимаем новый процесс с новым -group, никаких
// изменений в sender. Это и есть decoupling.
//
// Запуск: см. Makefile (`make run-kafka-listeners-3`).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	usersv1 "github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-03-sync-vs-async/gen/users/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const defaultTopic = "lecture-06-03-user-events"

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик с UserSignedUp событиями")
	group := flag.String("group", "", "consumer group; если пусто, берётся LISTENER_GROUP")
	fromStart := flag.Bool("from-start", true,
		"при первом старте группы читать с earliest")
	flag.Parse()

	if *group == "" {
		*group = config.EnvOr("LISTENER_GROUP", "")
	}
	if *group == "" {
		fmt.Fprintln(os.Stderr, "no group; use -group=<name> или env LISTENER_GROUP")
		os.Exit(2)
	}

	ctx, cancel := runctx.New()
	defer cancel()

	if err := run(ctx, *topic, *group, *fromStart); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("kafka-listener failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, topic, group string, fromStart bool) error {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ClientID(fmt.Sprintf("lecture-06-03-listener-%s", group)),
	}
	if fromStart {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	cl, err := kafka.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("listener запущен: topic=%q group=%q\n", topic, group)
	fmt.Println("читаем; Ctrl+C — выход.")
	fmt.Println()

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			var ev usersv1.UserSignedUp
			if err := proto.Unmarshal(r.Value, &ev); err != nil {
				fmt.Printf("[%s] partition=%d offset=%d UNMARSHAL FAIL err=%v\n",
					group, r.Partition, r.Offset, err)
				return
			}
			fmt.Printf("[%s] partition=%d offset=%d user_id=%s email=%s country=%s\n",
				group, r.Partition, r.Offset,
				ev.GetUserId(), ev.GetEmail(), ev.GetCountry())
		})
	}
}
