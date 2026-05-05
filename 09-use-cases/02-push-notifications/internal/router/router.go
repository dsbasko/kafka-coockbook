// Package router — notification-events → notification-{channel}.
//
// Один consumer на входе. Достаёт Notification из payload, читает поле
// channel и форвардит record в канал-специфичный топик. Дальше каждым
// каналом занимается свой sender со своим retry-пайплайном.
//
// Гарантия — at-least-once: ProduceSync на канал, потом CommitRecords.
// Между ProduceSync и Commit окно для дублей; защита у sender'а через
// processed_events (dedup по notification_id).
//
// Записи с CHANNEL_UNSPECIFIED или неизвестным каналом форвардятся в
// router-DLQ (RouterDLQ), а не молча отбрасываются — иначе кривой
// продьюсер (proto3-дефолт = UNSPECIFIED) терял бы данные.
package router

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	notificationsv1 "github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/02-push-notifications/gen/notifications/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
)

type RunOpts struct {
	NodeID       string
	SourceTopic  string
	Group        string
	FirebaseDest string
	APNsDest     string
	WebhookDest  string
	// RouterDLQ — топик, куда уходят записи с UNSPECIFIED/неизвестным
	// каналом. Обязателен: иначе бы пришлось молча дропать data.
	RouterDLQ string
}

func Run(ctx context.Context, o RunOpts) error {
	if o.RouterDLQ == "" {
		return fmt.Errorf("RouterDLQ topic is required")
	}

	cl, err := kafka.NewClient(
		kgo.ConsumerGroup(o.Group),
		kgo.ConsumeTopics(o.SourceTopic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ClientID("usecase-09-02-router-"+o.NodeID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.SessionTimeout(15*time.Second),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("[%s] notification-router: source=%q group=%q\n", o.NodeID, o.SourceTopic, o.Group)

	var routed atomic.Int64
	var dlqed atomic.Int64

	for {
		pollCtx, pollCancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}

		if err := ctx.Err(); err != nil {
			fmt.Printf("[%s] router остановлен. routed=%d dlqed=%d\n", o.NodeID, routed.Load(), dlqed.Load())
			return nil
		}

		batch := make([]*kgo.Record, 0)
		fetches.EachRecord(func(r *kgo.Record) { batch = append(batch, r) })
		if len(batch) == 0 {
			continue
		}

		out := make([]*kgo.Record, 0, len(batch))
		var batchRouted, batchDLQed int64
		for _, r := range batch {
			var n notificationsv1.Notification
			if err := proto.Unmarshal(r.Value, &n); err != nil {
				return fmt.Errorf("unmarshal p=%d off=%d: %w", r.Partition, r.Offset, err)
			}

			dest := destinationFor(n.GetChannel(), o)
			channel := n.GetChannel().String()
			if dest == "" {
				dest = o.RouterDLQ
				batchDLQed++
				fmt.Printf("[%s] router-DLQ: channel=%q id=%q p=%d off=%d\n",
					o.NodeID, channel, n.GetId(), r.Partition, r.Offset)
			} else {
				batchRouted++
			}

			out = append(out, &kgo.Record{
				Topic:   dest,
				Key:     r.Key,
				Value:   r.Value,
				Headers: appendRouterHeaders(r.Headers, o.NodeID, channel),
			})
		}

		if len(out) > 0 {
			results := cl.ProduceSync(ctx, out...)
			if err := results.FirstErr(); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("ProduceSync: %w", err)
			}
			routed.Add(batchRouted)
			dlqed.Add(batchDLQed)
		}

		commitCtx, commitCancel := context.WithTimeout(ctx, 10*time.Second)
		err := cl.CommitRecords(commitCtx, batch...)
		commitCancel()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("CommitRecords: %w", err)
		}
	}
}

func destinationFor(c notificationsv1.Channel, o RunOpts) string {
	switch c {
	case notificationsv1.Channel_CHANNEL_FIREBASE:
		return o.FirebaseDest
	case notificationsv1.Channel_CHANNEL_APNS:
		return o.APNsDest
	case notificationsv1.Channel_CHANNEL_WEBHOOK:
		return o.WebhookDest
	default:
		return ""
	}
}

func appendRouterHeaders(in []kgo.RecordHeader, nodeID, channel string) []kgo.RecordHeader {
	out := append([]kgo.RecordHeader(nil), in...)
	out = append(out,
		kgo.RecordHeader{Key: "router-node", Value: []byte(nodeID)},
		kgo.RecordHeader{Key: "channel", Value: []byte(channel)},
	)
	return out
}
