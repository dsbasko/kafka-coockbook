//go:build integration

// Integration test для use case 09-02: end-to-end push-notifications.
//
// Что делает:
//
//   - запускает 3 mock-приёмника (firebase/apns/webhook) через httptest.Server,
//     с FAIL_RATE_503=0.3 — треть попыток падает с 503 (transient);
//   - запускает 1 ноду notification-router в горутине;
//   - запускает по 1 sender'у на каждый канал в режиме deliver и по 1 на DLQ;
//   - продьюсит N=200 уведомлений в notification-events со случайным каналом;
//   - ждёт, пока в notifications_log накопится N строк суммарно по всем
//     каналам (delivered + dlq);
//   - проверяет, что delivered > 0, что DLQ может быть >0 (это ок при
//     fail rate'е), что один и тот же notification_id не появился дважды;
//   - запускает мини-replay: берёт DLQ-записи (если есть), переотправляет
//     в main-топик соответствующего канала и ждёт, что они уйдут в delivered
//     при FAIL_RATE_503=0 (mock'и переключаются на ноль фейлов через flag).
//
// Тест требует:
//
//   - Kafka стенд (kafka-1/2/3) из корневого docker-compose.yml;
//   - Postgres из docker-compose.override.yml (порт 15441) с db/init.sql.
//
// Build tag `integration` нужен, чтобы `go test ./...` не дёргал внешние ресурсы.
// Запуск: `go test -tags=integration ./test/...` или `make test-integration`.
package integration_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	notificationsv1 "github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/02-push-notifications/gen/notifications/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/02-push-notifications/internal/mockserver"
	"github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/02-push-notifications/internal/router"
	"github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/02-push-notifications/internal/sender"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
)

const (
	dsn              = "postgres://usecase:usecase@localhost:15441/usecase_09_02?sslmode=disable"
	defaultBootstrap = "localhost:19092,localhost:19093,localhost:19094"

	sourceTopic    = "usecase-09-02-it-notification-events"
	routerDLQTopic = "usecase-09-02-it-notification-events-dlq"
	firebaseTopic  = "usecase-09-02-it-notification-firebase"
	apnsTopic      = "usecase-09-02-it-notification-apns"
	webhookTopic   = "usecase-09-02-it-notification-webhook"
	firebaseRetry30 = "usecase-09-02-it-notification-firebase-retry-30s"
	firebaseRetry5m = "usecase-09-02-it-notification-firebase-retry-5m"
	firebaseDLQ    = "usecase-09-02-it-notification-firebase-dlq"
	apnsRetry30    = "usecase-09-02-it-notification-apns-retry-30s"
	apnsRetry5m    = "usecase-09-02-it-notification-apns-retry-5m"
	apnsDLQ        = "usecase-09-02-it-notification-apns-dlq"
	webhookRetry30 = "usecase-09-02-it-notification-webhook-retry-30s"
	webhookRetry5m = "usecase-09-02-it-notification-webhook-retry-5m"
	webhookDLQ     = "usecase-09-02-it-notification-webhook-dlq"

	routerGroup       = "usecase-09-02-it-router"
	firebaseGroup     = "usecase-09-02-it-firebase-sender"
	apnsGroup         = "usecase-09-02-it-apns-sender"
	webhookGroup      = "usecase-09-02-it-webhook-sender"
	firebaseDLQGroup  = "usecase-09-02-it-firebase-dlq"
	apnsDLQGroup      = "usecase-09-02-it-apns-dlq"
	webhookDLQGroup   = "usecase-09-02-it-webhook-dlq"
)

func TestEndToEnd_PushNotifications(t *testing.T) {
	const totalNotifications = 200

	root, rootCancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer rootCancel()

	if err := pingPostgres(root); err != nil {
		t.Skipf("Postgres недоступен (make up && make db-init): %v", err)
	}
	bootstrap := config.EnvOr("KAFKA_BOOTSTRAP", defaultBootstrap)
	if err := pingKafka(root, bootstrap); err != nil {
		t.Skipf("Kafka недоступна (стенд из корневого docker-compose.yml): %v", err)
	}

	if err := truncateTables(root); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	allTopics := []string{
		sourceTopic, routerDLQTopic,
		firebaseTopic, firebaseRetry30, firebaseRetry5m, firebaseDLQ,
		apnsTopic, apnsRetry30, apnsRetry5m, apnsDLQ,
		webhookTopic, webhookRetry30, webhookRetry5m, webhookDLQ,
	}
	if err := recreateTopics(root, bootstrap, allTopics); err != nil {
		t.Fatalf("recreate topics: %v", err)
	}
	if err := resetConsumerGroups(root, bootstrap, []string{
		routerGroup,
		firebaseGroup, apnsGroup, webhookGroup,
		firebaseDLQGroup, apnsDLQGroup, webhookDLQGroup,
	}); err != nil {
		t.Fatalf("reset consumer groups: %v", err)
	}

	// Mock-сервисы. Используем httptest.Server, чтобы не зависеть от docker.
	// Через atomic-конфиг можем менять fail_rate в середине теста.
	fbCfg := &mockConfig{}
	apnsCfg := &mockConfig{}
	whCfg := &mockConfig{}
	// Высокий fail rate, чтобы хотя бы часть сообщений исчерпала retry-ступени
	// и попала в DLQ — нам нужно валидировать replay-флоу.
	fbCfg.set(0.7, 0.0, 5)
	apnsCfg.set(0.7, 0.0, 5)
	whCfg.set(0.7, 0.0, 5)

	fbStats := &mockserver.Stats{}
	apnsStats := &mockserver.Stats{}
	whStats := &mockserver.Stats{}

	// liveMockHandler перечитывает mockConfig атомарно при каждом запросе,
	// так что смена fail rate через set() видна сразу без перевешивания
	// http.Handler'а (которое гонится с in-flight запросами).
	fbSrv := httptest.NewServer(liveMockHandler(fbCfg, fbStats))
	apnsSrv := httptest.NewServer(liveMockHandler(apnsCfg, apnsStats))
	whSrv := httptest.NewServer(liveMockHandler(whCfg, whStats))
	defer fbSrv.Close()
	defer apnsSrv.Close()
	defer whSrv.Close()

	// Сервисы. Каждой ноде свой контекст — сможем убить и подсмотреть recovery.
	var wg sync.WaitGroup
	type node struct {
		name   string
		cancel context.CancelFunc
		err    *atomic.Value
		done   chan struct{}
	}
	nodes := []*node{}

	startRouter := func() *node {
		ctx, cancel := context.WithCancel(root)
		errBox := new(atomic.Value)
		done := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(done)
			err := router.Run(ctx, router.RunOpts{
				NodeID:       "it-router",
				SourceTopic:  sourceTopic,
				Group:        routerGroup,
				FirebaseDest: firebaseTopic,
				APNsDest:     apnsTopic,
				WebhookDest:  webhookTopic,
				RouterDLQ:    routerDLQTopic,
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				errBox.Store(err)
			}
		}()
		return &node{name: "router", cancel: cancel, err: errBox, done: done}
	}
	startSender := func(name string, ch sender.Channel, target, mainTopic, retry30, retry5m, dlq, group string) *node {
		ctx, cancel := context.WithCancel(root)
		errBox := new(atomic.Value)
		done := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(done)
			err := sender.Run(ctx, sender.RunOpts{
				NodeID:  name,
				Channel: ch,
				Stages: []sender.Stage{
					{Topic: mainTopic, Delay: 0, NextTopic: retry30},
					{Topic: retry30, Delay: 200 * time.Millisecond, NextTopic: retry5m},
					{Topic: retry5m, Delay: 500 * time.Millisecond, NextTopic: ""},
				},
				DLQTopic:       dlq,
				Group:          group,
				Target:         target + "/send",
				HMACSecret:     "it-secret-" + string(ch),
				HTTPTimeout:    2 * time.Second,
				MaxAttempts:    1,
				InitialBackoff: 50 * time.Millisecond,
				MaxBackoff:     200 * time.Millisecond,
				CBTripAfter:    20,
				CBOpenTimeout:  500 * time.Millisecond,
				DSN:            dsn,
				FromStart:      true,
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				errBox.Store(err)
			}
		}()
		return &node{name: name, cancel: cancel, err: errBox, done: done}
	}
	startDLQ := func(name string, ch sender.Channel, dlqTopic, group string) *node {
		ctx, cancel := context.WithCancel(root)
		errBox := new(atomic.Value)
		done := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(done)
			err := sender.RunDLQ(ctx, sender.DLQOpts{
				NodeID:    name,
				Channel:   ch,
				DLQTopic:  dlqTopic,
				Group:     group,
				DSN:       dsn,
				FromStart: true,
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				errBox.Store(err)
			}
		}()
		return &node{name: name, cancel: cancel, err: errBox, done: done}
	}

	rNode := startRouter()
	fbNode := startSender("firebase-1", "firebase", fbSrv.URL, firebaseTopic, firebaseRetry30, firebaseRetry5m, firebaseDLQ, firebaseGroup)
	apnsNode := startSender("apns-1", "apns", apnsSrv.URL, apnsTopic, apnsRetry30, apnsRetry5m, apnsDLQ, apnsGroup)
	whNode := startSender("webhook-1", "webhook", whSrv.URL, webhookTopic, webhookRetry30, webhookRetry5m, webhookDLQ, webhookGroup)
	fbDLQ := startDLQ("firebase-dlq-1", "firebase", firebaseDLQ, firebaseDLQGroup)
	apnsDLQNode := startDLQ("apns-dlq-1", "apns", apnsDLQ, apnsDLQGroup)
	whDLQ := startDLQ("webhook-dlq-1", "webhook", webhookDLQ, webhookDLQGroup)
	nodes = append(nodes, rNode, fbNode, apnsNode, whNode, fbDLQ, apnsDLQNode, whDLQ)

	t.Logf("льём %d уведомлений в %s", totalNotifications, sourceTopic)
	if err := produceNotifications(root, bootstrap, totalNotifications); err != nil {
		t.Fatalf("produce notifications: %v", err)
	}

	// Регрессия на router-DLQ: пара записей с CHANNEL_UNSPECIFIED — router
	// должен сроутить их в routerDLQTopic, а не молча дропнуть. На senders
	// они не попадают, поэтому в notifications_log тоже не появятся и счётчик
	// delivered+dlq=200 это не сломает.
	const routerDLQExpect = 3
	if err := produceUnspecifiedNotifications(root, bootstrap, routerDLQExpect); err != nil {
		t.Fatalf("produce UNSPECIFIED notifications: %v", err)
	}

	// Ждём, пока notifications_log заполнится по всем notification_id.
	// Сначала с включёнными failures: возможны записи в DLQ.
	deadline := time.Now().Add(90 * time.Second)
	var lastSnap historySnapshot
	for {
		snap, err := historyByStatus(root)
		if err != nil {
			t.Fatalf("history snapshot: %v", err)
		}
		lastSnap = snap
		t.Logf("history: delivered=%d dlq=%d (target=%d)", snap.delivered, snap.dlq, totalNotifications)
		if snap.delivered+snap.dlq >= totalNotifications {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("eventual consistency не достигнута за 90s: %+v", snap)
		}
		time.Sleep(500 * time.Millisecond)
	}

	if lastSnap.delivered == 0 {
		t.Fatalf("за 90s ни одно уведомление не было доставлено: %+v", lastSnap)
	}

	if err := assertNoDuplicates(root); err != nil {
		t.Fatalf("duplicates check: %v", err)
	}

	if got, err := countTopicRecords(root, bootstrap, routerDLQTopic, routerDLQExpect, 30*time.Second); err != nil {
		t.Fatalf("router-DLQ check: %v", err)
	} else if got != routerDLQExpect {
		t.Fatalf("router-DLQ: ожидали %d UNSPECIFIED записей, получили %d", routerDLQExpect, got)
	}

	t.Logf("разбивка по каналам: %v", channelStatusBreakdown(root, t))

	// Replay. Если в DLQ кто-то остался — сбрасываем fail rate, читаем DLQ
	// и переотправляем в main-топики каналов с новым notification_id'ом
	// (replay = реактивная отправка, не повторное использование того же id —
	// тогда dedup-гейт пропустит). Чтобы тест был стабильным, имитируем
	// идемпотентный replay: для каждого dlq-id мы в логе видим status='dlq';
	// после replay появляется ещё одна запись с тем же id, но dedup'нем
	// её через PRIMARY KEY (notification_id, channel). Поэтому для replay
	// генерим новый id с префиксом replay-, и проверяем что delivered растёт.
	if lastSnap.dlq > 0 {
		// Сначала переключаем mock'и на ноль фейлов: иначе CB не успеет
		// закрыться обратно, а при replay'е новые id'шники снова уйдут
		// на failure и попадут в DLQ. liveMockHandler читает значения
		// атомарно при каждом запросе, поэтому повторно перевешивать
		// handler не нужно.
		t.Logf("replay: переключаем mock'и на 0%% fail")
		fbCfg.set(0.0, 0.0, 5)
		apnsCfg.set(0.0, 0.0, 5)
		whCfg.set(0.0, 0.0, 5)

		// Ждём, пока pipeline стабилизируется (in-flight retry-5m дойдут).
		stableDeadline := time.Now().Add(15 * time.Second)
		var prev historySnapshot
		stableTicks := 0
		for time.Now().Before(stableDeadline) {
			s, err := historyByStatus(root)
			if err != nil {
				t.Fatalf("stabilization snapshot: %v", err)
			}
			if s == prev {
				stableTicks++
				if stableTicks >= 3 {
					break
				}
			} else {
				stableTicks = 0
				prev = s
			}
			time.Sleep(500 * time.Millisecond)
		}

		baseline, err := historyByStatus(root)
		if err != nil {
			t.Fatalf("baseline: %v", err)
		}
		t.Logf("baseline before replay: delivered=%d dlq=%d", baseline.delivered, baseline.dlq)

		replayed, err := replayDLQ(root, bootstrap)
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
		t.Logf("replay: переотправлено %d сообщений", replayed)
		if replayed == 0 {
			t.Fatalf("replay не прочитал ни одной DLQ-записи (ожидали ~%d)", baseline.dlq)
		}

		// Ожидаем, что больше половины replays успешно доставятся
		// (даём слабину на in-flight CB и race condition'ы между replay
		// и доеданием очередей senders'ом).
		threshold := baseline.delivered + replayed/2
		deadline = time.Now().Add(60 * time.Second)
		for {
			snap, err := historyByStatus(root)
			if err != nil {
				t.Fatalf("history after replay: %v", err)
			}
			if snap.delivered >= threshold {
				t.Logf("replay завершился: delivered=%d (ожидали ≥%d), dlq=%d",
					snap.delivered, threshold, snap.dlq)
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("replay не сошёлся за 60s: %+v (expected delivered ≥ %d)", snap, threshold)
			}
			time.Sleep(500 * time.Millisecond)
		}
	} else {
		t.Logf("DLQ пусто — replay не нужен. Это валидно: fail rate был не настолько большим, чтобы исчерпать ступени.")
	}

	for _, n := range nodes {
		n.cancel()
	}
	wg.Wait()
	for _, n := range nodes {
		if v := n.err.Load(); v != nil {
			t.Errorf("%s завершился с ошибкой: %v", n.name, v)
		}
	}
}

type historySnapshot struct {
	delivered int
	dlq       int
}

func historyByStatus(ctx context.Context) (historySnapshot, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return historySnapshot{}, err
	}
	defer pool.Close()

	var s historySnapshot
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications_log WHERE status='delivered'`).Scan(&s.delivered); err != nil {
		return s, err
	}
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM notifications_log WHERE status='dlq'`).Scan(&s.dlq); err != nil {
		return s, err
	}
	return s, nil
}

func channelStatusBreakdown(ctx context.Context, t *testing.T) string {
	t.Helper()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Sprintf("err: %v", err)
	}
	defer pool.Close()
	rows, err := pool.Query(ctx,
		`SELECT channel, status, COUNT(*) FROM notifications_log GROUP BY channel, status ORDER BY channel, status`)
	if err != nil {
		return fmt.Sprintf("err: %v", err)
	}
	defer rows.Close()
	var parts []string
	for rows.Next() {
		var ch, st string
		var n int
		if err := rows.Scan(&ch, &st, &n); err != nil {
			return fmt.Sprintf("err: %v", err)
		}
		parts = append(parts, fmt.Sprintf("%s/%s=%d", ch, st, n))
	}
	return strings.Join(parts, " ")
}

func assertNoDuplicates(ctx context.Context) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	var dup int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
		    SELECT notification_id, channel, COUNT(*) c
		    FROM notifications_log
		    GROUP BY notification_id, channel
		    HAVING COUNT(*) > 1
		) t`).Scan(&dup)
	if err != nil {
		return err
	}
	if dup > 0 {
		return fmt.Errorf("найдено %d дублей в notifications_log (по notification_id+channel)", dup)
	}
	return nil
}

func produceNotifications(ctx context.Context, bootstrap string, n int) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(splitCSV(bootstrap)...),
		kgo.ClientID("usecase-09-02-it-producer"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	channels := []notificationsv1.Channel{
		notificationsv1.Channel_CHANNEL_FIREBASE,
		notificationsv1.Channel_CHANNEL_APNS,
		notificationsv1.Channel_CHANNEL_WEBHOOK,
	}

	records := make([]*kgo.Record, 0, n)
	now := time.Now().UTC()
	for i := 0; i < n; i++ {
		ch := channels[i%len(channels)]
		notif := &notificationsv1.Notification{
			Id:          fmt.Sprintf("notif-%04d", i),
			UserId:      fmt.Sprintf("user-%d", i%37),
			Channel:     ch,
			Title:       "It Works",
			Body:        fmt.Sprintf("hello %d", i),
			PayloadJson: `{"deeplink":"app://x"}`,
			TraceId:     fmt.Sprintf("trace-%d", i),
			CreatedAt:   timestamppb.New(now),
		}
		payload, err := proto.Marshal(notif)
		if err != nil {
			return err
		}
		records = append(records, &kgo.Record{
			Topic: sourceTopic,
			Key:   []byte(notif.GetUserId()),
			Value: payload,
		})
	}

	tctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	results := cl.ProduceSync(tctx, records...)
	return results.FirstErr()
}

// produceUnspecifiedNotifications льёт N записей с channel=CHANNEL_UNSPECIFIED.
// Используется как регрессия на routing: router должен переложить такие записи
// в routerDLQTopic (а не молча отбросить), иначе кривой продьюсер с proto3-дефолтом
// потеряет данные.
func produceUnspecifiedNotifications(ctx context.Context, bootstrap string, n int) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(splitCSV(bootstrap)...),
		kgo.ClientID("usecase-09-02-it-producer-unspec"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	now := time.Now().UTC()
	records := make([]*kgo.Record, 0, n)
	for i := 0; i < n; i++ {
		notif := &notificationsv1.Notification{
			Id:        fmt.Sprintf("notif-unspec-%04d", i),
			UserId:    fmt.Sprintf("user-unspec-%d", i),
			Channel:   notificationsv1.Channel_CHANNEL_UNSPECIFIED,
			Title:     "It Works",
			Body:      fmt.Sprintf("unspec %d", i),
			TraceId:   fmt.Sprintf("trace-unspec-%d", i),
			CreatedAt: timestamppb.New(now),
		}
		payload, err := proto.Marshal(notif)
		if err != nil {
			return err
		}
		records = append(records, &kgo.Record{
			Topic: sourceTopic,
			Key:   []byte(notif.GetUserId()),
			Value: payload,
		})
	}

	tctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	results := cl.ProduceSync(tctx, records...)
	return results.FirstErr()
}

// countTopicRecords читает топик с начала через одноразовую consumer-группу
// и считает поступившие записи, пока не наберётся expect или не выйдет timeout.
// Используется для проверки, что router сложил UNSPECIFIED-записи в router-DLQ.
func countTopicRecords(ctx context.Context, bootstrap, topic string, expect int, timeout time.Duration) (int, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(splitCSV(bootstrap)...),
		kgo.ClientID("usecase-09-02-it-router-dlq-reader"),
		kgo.ConsumerGroup(fmt.Sprintf("usecase-09-02-it-router-dlq-cg-%d", time.Now().UnixNano())),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return 0, err
	}
	defer cl.Close()

	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	got := 0
	for got < expect {
		pollCtx, pollCancel := context.WithTimeout(tctx, 2*time.Second)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				return got, fmt.Errorf("fetch %s: %w", topic, e.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			got++
		})
		if tctx.Err() != nil {
			break
		}
	}
	return got, nil
}

func replayDLQ(ctx context.Context, bootstrap string) (int, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(splitCSV(bootstrap)...),
		kgo.ClientID("usecase-09-02-it-replay"),
		kgo.ConsumerGroup("usecase-09-02-it-replay-cg"),
		kgo.ConsumeTopics(firebaseDLQ, apnsDLQ, webhookDLQ),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return 0, err
	}
	defer cl.Close()

	produceCl, err := kgo.NewClient(
		kgo.SeedBrokers(splitCSV(bootstrap)...),
		kgo.ClientID("usecase-09-02-it-replay-producer"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return 0, err
	}
	defer produceCl.Close()

	deadline := time.Now().Add(15 * time.Second)
	count := 0

	for time.Now().Before(deadline) {
		pollCtx, pollCancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) || errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				return count, fmt.Errorf("fetch DLQ: %w", e.Err)
			}
		}

		got := 0
		fetches.EachRecord(func(r *kgo.Record) {
			got++
			var notif notificationsv1.Notification
			if err := proto.Unmarshal(r.Value, &notif); err != nil {
				return
			}
			notif.Id = "replay-" + notif.GetId()
			payload, err := proto.Marshal(&notif)
			if err != nil {
				return
			}
			target := mainForDLQ(r.Topic)
			if target == "" {
				return
			}
			res := produceCl.ProduceSync(ctx, &kgo.Record{
				Topic:   target,
				Key:     []byte(notif.GetUserId()),
				Value:   payload,
				Headers: []kgo.RecordHeader{{Key: "replay", Value: []byte("1")}},
			})
			if res.FirstErr() == nil {
				count++
			}
		})

		if got == 0 && count > 0 {
			break
		}
	}

	return count, nil
}

func mainForDLQ(dlqTopic string) string {
	switch dlqTopic {
	case firebaseDLQ:
		return firebaseTopic
	case apnsDLQ:
		return apnsTopic
	case webhookDLQ:
		return webhookTopic
	}
	return ""
}

// mockConfig — обёртка над atomic-полем, чтобы тест мог менять fail rate
// в середине прогона (для replay-фазы). Хендлер ниже читает значения
// атомарно при каждом запросе, поэтому смена через set() видна сразу
// без пересоздания http.Handler'а.
type mockConfig struct {
	fail503     atomic.Value // float64
	failTimeout atomic.Value // float64
	latencyMS   atomic.Int64
}

func (m *mockConfig) set(f503, fto float64, latency int) {
	m.fail503.Store(f503)
	m.failTimeout.Store(fto)
	m.latencyMS.Store(int64(latency))
}

// liveMockHandler возвращает http.Handler, который при каждом запросе
// читает текущие значения mockConfig через atomic. Альтернатива —
// перевешивать httptest.Server.Config.Handler — небезопасна: поле Handler
// у http.Server обычное, не atomic, и присваивание гонится с in-flight
// запросами (поймал бы `go test -race`).
func liveMockHandler(m *mockConfig, stats *mockserver.Stats) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := mockserver.Config{
			Fail503:     loadFloat(&m.fail503),
			FailTimeout: loadFloat(&m.failTimeout),
			LatencyMS:   int(m.latencyMS.Load()),
		}
		mockserver.Handler(cfg, stats).ServeHTTP(w, r)
	})
}

func loadFloat(v *atomic.Value) float64 {
	if x := v.Load(); x != nil {
		if f, ok := x.(float64); ok {
			return f
		}
	}
	return 0
}

func truncateTables(ctx context.Context) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	_, err = pool.Exec(ctx, `TRUNCATE TABLE notifications_log, processed_events RESTART IDENTITY`)
	return err
}

func pingPostgres(ctx context.Context) error {
	tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	pool, err := pgxpool.New(tctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	return pool.Ping(tctx)
}

func pingKafka(ctx context.Context, bootstrap string) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(splitCSV(bootstrap)...), kgo.DialTimeout(2*time.Second))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	if _, err := adm.ListBrokers(tctx); err != nil {
		return err
	}
	return nil
}

func recreateTopics(ctx context.Context, bootstrap string, topics []string) error {
	tctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(splitCSV(bootstrap)...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	_, _ = adm.DeleteTopics(tctx, topics...)
	time.Sleep(800 * time.Millisecond)
	for _, topic := range topics {
		_, err := adm.CreateTopic(tctx, 3, 3, nil, topic)
		if err != nil && !errors.Is(err, kadm.ErrEmpty) {
			time.Sleep(500 * time.Millisecond)
			_, retryErr := adm.CreateTopic(tctx, 3, 3, nil, topic)
			if retryErr != nil {
				if strings.Contains(retryErr.Error(), "already exists") {
					continue
				}
				return fmt.Errorf("create topic %s: %w (retry: %v)", topic, err, retryErr)
			}
		}
	}
	return nil
}

func resetConsumerGroups(ctx context.Context, bootstrap string, groups []string) error {
	tctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(splitCSV(bootstrap)...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	for _, g := range groups {
		_, _ = adm.DeleteGroup(tctx, g)
	}
	return nil
}

func splitCSV(s string) []string {
	out := []string{}
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
