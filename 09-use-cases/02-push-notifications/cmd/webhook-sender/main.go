// webhook-sender — sender для канала webhook. Тонкая обёртка над sender.Main.
package main

import "github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/02-push-notifications/internal/sender"

func main() {
	sender.Main(sender.CmdDefaults{
		Channel:      sender.Channel("webhook"),
		NodeID:       "webhook-1",
		Target:       "http://localhost:8093/send",
		HMACSecret:   "usecase-09-02-webhook",
		MainTopic:    "usecase-09-02-notification-webhook",
		Retry30Topic: "usecase-09-02-notification-webhook-retry-30s",
		Retry5mTopic: "usecase-09-02-notification-webhook-retry-5m",
		DLQTopic:     "usecase-09-02-notification-webhook-dlq",
		Group:        "usecase-09-02-webhook-sender",
		DLQGroup:     "usecase-09-02-webhook-dlq",
	})
}
