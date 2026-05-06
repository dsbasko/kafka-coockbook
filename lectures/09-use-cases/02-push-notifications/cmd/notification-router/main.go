// notification-router — единый consumer на topic notification-events.
//
// Достаёт Notification из payload, читает поле channel и форвардит record
// в notification-{firebase,apns,webhook}. Дальше каждый канал обслуживает
// свой sender со своим retry-пайплайном и mock-внешним сервисом.
package main

import (
	"context"
	"errors"
	"flag"
	"os"

	"github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/02-push-notifications/internal/router"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	nodeID := flag.String("node-id", "router-1", "идентификатор ноды")
	source := flag.String("source", "usecase-09-02-notification-events", "входной топик")
	group := flag.String("group", "usecase-09-02-router", "consumer group")
	firebase := flag.String("firebase", "usecase-09-02-notification-firebase", "топик firebase")
	apns := flag.String("apns", "usecase-09-02-notification-apns", "топик apns")
	webhook := flag.String("webhook", "usecase-09-02-notification-webhook", "топик webhook")
	routerDLQ := flag.String("router-dlq", "usecase-09-02-notification-events-dlq", "DLQ для записей с UNSPECIFIED/неизвестным каналом")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	if err := router.Run(ctx, router.RunOpts{
		NodeID:       *nodeID,
		SourceTopic:  *source,
		Group:        *group,
		FirebaseDest: *firebase,
		APNsDest:     *apns,
		WebhookDest:  *webhook,
		RouterDLQ:    *routerDLQ,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("notification-router failed", "err", err)
		os.Exit(1)
	}
}
