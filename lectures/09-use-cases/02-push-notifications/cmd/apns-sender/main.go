package main

import "github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/02-push-notifications/internal/sender"

func main() {
	sender.Main(sender.CmdDefaults{
		Channel:      sender.Channel("apns"),
		NodeID:       "apns-1",
		Target:       "http://localhost:8092/send",
		HMACSecret:   "usecase-09-02-apns",
		MainTopic:    "usecase-09-02-notification-apns",
		Retry30Topic: "usecase-09-02-notification-apns-retry-30s",
		Retry5mTopic: "usecase-09-02-notification-apns-retry-5m",
		DLQTopic:     "usecase-09-02-notification-apns-dlq",
		Group:        "usecase-09-02-apns-sender",
		DLQGroup:     "usecase-09-02-apns-dlq",
	})
}
