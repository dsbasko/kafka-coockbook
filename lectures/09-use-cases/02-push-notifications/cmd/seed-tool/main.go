package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	notificationsv1 "github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/02-push-notifications/gen/notifications/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	count := flag.Int("count", 100, "сколько уведомлений отправить")
	topic := flag.String("topic", "usecase-09-02-notification-events", "входной топик")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	cl, err := kafka.NewClient(
		kgo.ClientID("usecase-09-02-seed"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kafka.NewClient: %v\n", err)
		os.Exit(1)
	}
	defer cl.Close()

	channels := []notificationsv1.Channel{
		notificationsv1.Channel_CHANNEL_FIREBASE,
		notificationsv1.Channel_CHANNEL_APNS,
		notificationsv1.Channel_CHANNEL_WEBHOOK,
	}

	now := time.Now().UTC()
	records := make([]*kgo.Record, 0, *count)
	for i := 0; i < *count; i++ {
		ch := channels[i%len(channels)]
		notif := &notificationsv1.Notification{
			Id:          fmt.Sprintf("seed-%06d", i),
			UserId:      fmt.Sprintf("user-%d", i%37),
			Channel:     ch,
			Title:       "Seed Notification",
			Body:        fmt.Sprintf("seeded message %d", i),
			PayloadJson: `{"seeded":true}`,
			TraceId:     fmt.Sprintf("seed-trace-%d", i),
			CreatedAt:   timestamppb.New(now),
		}
		payload, err := proto.Marshal(notif)
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal: %v\n", err)
			os.Exit(1)
		}
		records = append(records, &kgo.Record{
			Topic: *topic,
			Key:   []byte(notif.GetUserId()),
			Value: payload,
		})
	}

	produceCtx, pcancel := context.WithTimeout(ctx, 60*time.Second)
	defer pcancel()
	if err := cl.ProduceSync(produceCtx, records...).FirstErr(); err != nil {
		fmt.Fprintf(os.Stderr, "ProduceSync: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("seeded %d уведомлений в %s\n", *count, *topic)
}
