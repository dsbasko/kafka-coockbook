package sender

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

// CmdDefaults — конфиг-«профиль» канала. Значения, специфичные для firebase
// vs apns vs webhook (имя канала, порт mock'а, дефолтные имена топиков и
// группы) задаются именно здесь — соответствующий cmd/main.go их подхватывает.
type CmdDefaults struct {
	Channel      Channel
	NodeID       string
	Target       string
	HMACSecret   string
	MainTopic    string
	Retry30Topic string
	Retry5mTopic string
	DLQTopic     string
	Group        string
	DLQGroup     string
}

// Main — точка входа sender'а. Парсит флаги и зовёт Run или RunDLQ
// в зависимости от режима. У каждого канала свой набор дефолтов через d.
func Main(d CmdDefaults) {
	logger := log.New()

	mode := flag.String("mode", "deliver", "deliver — основной sender, dlq — consumer DLQ-топика для записи истории")
	nodeID := flag.String("node-id", d.NodeID, "идентификатор ноды")
	target := flag.String("target", d.Target, "URL HTTP-mock'а получателя")
	hmacSecret := flag.String("hmac-secret", d.HMACSecret, "ключ для X-Signature")
	mainTopic := flag.String("main-topic", d.MainTopic, "входной топик канала")
	retry30Topic := flag.String("retry-30s-topic", d.Retry30Topic, "топик первой ступени retry")
	retry5mTopic := flag.String("retry-5m-topic", d.Retry5mTopic, "топик второй ступени retry")
	dlqTopic := flag.String("dlq-topic", d.DLQTopic, "DLQ-топик")
	group := flag.String("group", d.Group, "consumer group для main + retry")
	dlqGroup := flag.String("dlq-group", d.DLQGroup, "consumer group для DLQ-режима")

	delay30 := flag.Duration("delay-30s", 30*time.Second, "переопределить задержку retry-30s (для тестов)")
	delay5m := flag.Duration("delay-5m", 5*time.Minute, "переопределить задержку retry-5m (для тестов)")

	httpTimeout := flag.Duration("http-timeout", 3*time.Second, "таймаут одного HTTP запроса")
	maxAttempts := flag.Int("max-attempts", 3, "максимум HTTP-попыток на ступени")
	initialBackoff := flag.Duration("initial-backoff", 100*time.Millisecond, "стартовый backoff")
	maxBackoff := flag.Duration("max-backoff", 2*time.Second, "верхний потолок backoff'а")
	cbTrip := flag.Uint("cb-trip-after", 5, "сколько подряд неуспехов до перехода CB в Open")
	cbOpenTimeout := flag.Duration("cb-open-timeout", 5*time.Second, "сколько CB сидит в Open")
	fromStart := flag.Bool("from-start", false, "при первом запуске группы читать с earliest")
	flag.Parse()

	dsn := config.EnvOr("DATABASE_URL", "postgres://usecase:usecase@localhost:15441/usecase_09_02?sslmode=disable")

	ctx, cancel := runctx.New()
	defer cancel()

	switch *mode {
	case "deliver":
		stages := []Stage{
			{Topic: *mainTopic, Delay: 0, NextTopic: *retry30Topic},
			{Topic: *retry30Topic, Delay: *delay30, NextTopic: *retry5mTopic},
			{Topic: *retry5mTopic, Delay: *delay5m, NextTopic: ""},
		}
		err := Run(ctx, RunOpts{
			NodeID:         *nodeID,
			Channel:        d.Channel,
			Stages:         stages,
			DLQTopic:       *dlqTopic,
			Group:          *group,
			Target:         *target,
			HMACSecret:     *hmacSecret,
			HTTPTimeout:    *httpTimeout,
			MaxAttempts:    *maxAttempts,
			InitialBackoff: *initialBackoff,
			MaxBackoff:     *maxBackoff,
			CBTripAfter:    *cbTrip,
			CBOpenTimeout:  *cbOpenTimeout,
			DSN:            dsn,
			FromStart:      *fromStart,
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("sender failed", "channel", d.Channel, "err", err)
			os.Exit(1)
		}
	case "dlq":
		err := RunDLQ(ctx, DLQOpts{
			NodeID:    *nodeID,
			Channel:   d.Channel,
			DLQTopic:  *dlqTopic,
			Group:     *dlqGroup,
			DSN:       dsn,
			FromStart: *fromStart,
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("dlq consumer failed", "channel", d.Channel, "err", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown -mode=%s, ожидаем deliver|dlq\n", *mode)
		os.Exit(2)
	}
}
