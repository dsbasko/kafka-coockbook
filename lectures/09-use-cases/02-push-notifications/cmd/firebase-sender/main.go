package main

import "github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/02-push-notifications/internal/sender"

func main() {
	sender.Main(sender.CmdDefaults{
		Channel:      sender.Channel("firebase"),
		NodeID:       "firebase-1",
		Target:       "http://localhost:8091/send",
		HMACSecret:   "usecase-09-02-firebase",
		MainTopic:    "usecase-09-02-notification-firebase",
		Retry30Topic: "usecase-09-02-notification-firebase-retry-30s",
		Retry5mTopic: "usecase-09-02-notification-firebase-retry-5m",
		DLQTopic:     "usecase-09-02-notification-firebase-dlq",
		Group:        "usecase-09-02-firebase-sender",
		DLQGroup:     "usecase-09-02-firebase-dlq",
	})
}
