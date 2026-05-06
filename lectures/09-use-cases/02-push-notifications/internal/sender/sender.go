// Package sender — общая логика канал-специфичных отправителей
// (firebase-sender, apns-sender, webhook-sender).
//
// Один экземпляр sender'а:
//
//   - подписан на main-топик канала и его retry-ступени
//     (notification-{channel}, -{channel}-retry-30s, -{channel}-retry-5m);
//   - на retry-ступенях ждёт до record.Timestamp+stage.Delay перед обработкой;
//   - вызывает HTTP delivery с CB (sony/gobreaker) и backoff+jitter, HMAC
//     подпись тела через X-Signature, Idempotency-Key из notification_id;
//   - на успехе пишет notifications_log(status='delivered', ...) +
//     processed_events(consumer, notification_id) в одной транзакции;
//   - на transient-ошибке форвардит в следующую retry-ступень или DLQ;
//   - на permanent-ошибке (4xx кроме 408/429) сразу в DLQ;
//   - DLQ — терминальная ступень; sender читает DLQ-топик отдельной
//     consumer group `*-dlq` и пишет notifications_log(status='dlq',...)
//     с last_error из header'а.
//
// Шаблон retry-пайплайна повторяет 04-04. CB + HMAC — повторяет 04-05.
// Dedup'ируем через processed_events так же, как 09-01: PRIMARY KEY
// (consumer, notification_id) + INSERT ON CONFLICT DO NOTHING.
package sender

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sony/gobreaker/v2"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	notificationsv1 "github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/02-push-notifications/gen/notifications/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
)

const insertHistorySQL = `
INSERT INTO notifications_log (notification_id, channel, user_id, status, attempts, sent_by, last_error)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (notification_id, channel) DO NOTHING
`

const dedupSQL = `
INSERT INTO processed_events (consumer, notification_id)
VALUES ($1, $2)
ON CONFLICT (consumer, notification_id) DO NOTHING
`

// Stage — одна ступень retry-пайплайна. Sender знает все ступени канала
// и форвардит between ними по результату HTTP delivery.
type Stage struct {
	Topic     string
	Delay     time.Duration
	NextTopic string // пусто = эскалируем в DLQ
}

type Channel string

type RunOpts struct {
	NodeID    string
	Channel   Channel
	Stages    []Stage
	DLQTopic  string
	Group     string

	Target         string // HTTP endpoint mock'а (например http://localhost:8091/send)
	HMACSecret     string
	HTTPTimeout    time.Duration
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration

	CBTripAfter   uint
	CBOpenTimeout time.Duration

	DSN string

	// FromStart=true → первый запуск группы читает с earliest. Для интеграционных
	// тестов это критично — иначе при пересоздании группы старые сообщения теряются.
	FromStart bool
}

func defaults(o *RunOpts) {
	if o.HTTPTimeout == 0 {
		o.HTTPTimeout = 3 * time.Second
	}
	if o.MaxAttempts == 0 {
		o.MaxAttempts = 3
	}
	if o.InitialBackoff == 0 {
		o.InitialBackoff = 100 * time.Millisecond
	}
	if o.MaxBackoff == 0 {
		o.MaxBackoff = 2 * time.Second
	}
	if o.CBTripAfter == 0 {
		o.CBTripAfter = 5
	}
	if o.CBOpenTimeout == 0 {
		o.CBOpenTimeout = 5 * time.Second
	}
}

func Run(ctx context.Context, o RunOpts) error {
	defaults(&o)

	pool, err := pgxpool.New(ctx, o.DSN)
	if err != nil {
		return fmt.Errorf("pgxpool.New: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("pg ping: %w", err)
	}

	stageByTopic := make(map[string]Stage, len(o.Stages))
	topics := make([]string, 0, len(o.Stages))
	for _, s := range o.Stages {
		stageByTopic[s.Topic] = s
		topics = append(topics, s.Topic)
	}

	opts := []kgo.Opt{
		kgo.ConsumerGroup(o.Group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.ClientID("usecase-09-02-" + string(o.Channel) + "-sender-" + o.NodeID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.SessionTimeout(15 * time.Second),
	}
	if o.FromStart {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	cl, err := kafka.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	s := &runner{
		opts:       o,
		stageByTop: stageByTopic,
		pool:       pool,
		cl:         cl,
		httpClient: newHTTPClient(o.HTTPTimeout),
	}
	s.cb = gobreaker.NewCircuitBreaker[deliveryResult](gobreaker.Settings{
		Name:        string(o.Channel) + "-sender-cb",
		MaxRequests: 1,
		Timeout:     o.CBOpenTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= uint32(o.CBTripAfter)
		},
		// CB должен реагировать на падение upstream'а (transport / 5xx / 408 / 429),
		// а не на бизнес-валидацию. errPermanent (4xx, кроме 408/429) — здоровая
		// реакция здорового сервиса на кривой payload, считаем такие ответы успехом.
		IsSuccessful: func(err error) bool {
			return err == nil || errors.Is(err, errPermanent)
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			fmt.Printf("[%s] CB %s: %s → %s\n", o.NodeID, name, from, to)
		},
	})

	fmt.Printf("[%s] %s-sender: stages=%d group=%q target=%s\n",
		o.NodeID, o.Channel, len(o.Stages), o.Group, o.Target)

	return s.run(ctx)
}

type runner struct {
	opts       RunOpts
	stageByTop map[string]Stage
	pool       *pgxpool.Pool
	cl         *kgo.Client
	httpClient *http.Client
	cb         *gobreaker.CircuitBreaker[deliveryResult]

	delivered atomic.Int64
	escalated atomic.Int64
	dlq       atomic.Int64
}

type deliveryResult struct {
	statusCode int
	attempts   int
}

var errPermanent = errors.New("permanent http failure")

func (s *runner) run(ctx context.Context) error {
	for {
		pollCtx, pollCancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := s.cl.PollFetches(pollCtx)
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
			fmt.Printf("[%s] %s-sender остановлен. delivered=%d escalated=%d dlq=%d\n",
				s.opts.NodeID, s.opts.Channel, s.delivered.Load(), s.escalated.Load(), s.dlq.Load())
			return nil
		}

		batch := make([]*kgo.Record, 0)
		fetches.EachRecord(func(r *kgo.Record) { batch = append(batch, r) })
		if len(batch) == 0 {
			continue
		}

		toCommit := make([]*kgo.Record, 0, len(batch))
	processBatch:
		for i, r := range batch {
			st, ok := s.stageByTop[r.Topic]
			if !ok {
				return fmt.Errorf("неожиданный топик: %q", r.Topic)
			}

			if st.Delay > 0 {
				if err := waitUntilDue(ctx, r.Timestamp, st.Delay); err != nil {
					return err
				}
			}

			_, err := s.handle(ctx, r)
			switch {
			case err == nil:
				s.delivered.Add(1)
				toCommit = append(toCommit, r)
			case errors.Is(err, errPermanent):
				if ferr := s.forward(ctx, st, r, err, true); ferr != nil {
					return ferr
				}
				toCommit = append(toCommit, r)
			case errors.Is(err, gobreaker.ErrOpenState), errors.Is(err, gobreaker.ErrTooManyRequests):
				// CB открыт — перематываем cursor для всех необработанных записей
				// текущего batch (включая записи на других партициях, идущие после
				// CB-rejected). Иначе их offsets уже advance'нуты в franz-go и они
				// потеряются до rebalance.
				rewinds := make(map[string]map[int32]kgo.EpochOffset)
				for _, rem := range batch[i:] {
					tp, ok := rewinds[rem.Topic]
					if !ok {
						tp = make(map[int32]kgo.EpochOffset)
						rewinds[rem.Topic] = tp
					}
					existing, has := tp[rem.Partition]
					if !has || rem.Offset < existing.Offset {
						tp[rem.Partition] = kgo.EpochOffset{
							Epoch:  rem.LeaderEpoch,
							Offset: rem.Offset,
						}
					}
				}
				s.cl.SetOffsets(rewinds)
				fmt.Printf("[%s] CB rejected key=%s — rewind %s/%d to offset %d (+ остальные партиции batch)\n",
					s.opts.NodeID, string(r.Key), r.Topic, r.Partition, r.Offset)
				break processBatch
			default:
				if ferr := s.forward(ctx, st, r, err, false); ferr != nil {
					return ferr
				}
				toCommit = append(toCommit, r)
			}
		}

		if len(toCommit) > 0 {
			commitCtx, ccancel := context.WithTimeout(ctx, 10*time.Second)
			err := s.cl.CommitRecords(commitCtx, toCommit...)
			ccancel()
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("CommitRecords: %w", err)
			}
		}
	}
}

// handle — bus delivery: HTTP вызов с retry-внутри одного CB-execute.
// Внутри CB.Execute() — backoff с jitter + классификация HTTP кодов.
//
// На успехе пишем notifications_log + processed_events в одной транзакции
// с dedup-гейтом. Ровно та же история, что у 09-01: гейт и бизнес-вставка
// должны быть атомарны.
func (s *runner) handle(ctx context.Context, r *kgo.Record) (deliveryResult, error) {
	var n notificationsv1.Notification
	if err := proto.Unmarshal(r.Value, &n); err != nil {
		// poison-pill: невалидный protobuf — сразу в DLQ.
		return deliveryResult{}, fmt.Errorf("unmarshal: %w: %w", err, errPermanent)
	}

	result, err := s.cb.Execute(func() (deliveryResult, error) {
		return s.deliverWithRetries(ctx, &n)
	})
	if err != nil {
		return result, err
	}

	// Успешная доставка — пишем в БД.
	dbErr := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		consumer := string(s.opts.Channel) + "-sender"
		tag, err := tx.Exec(ctx, dedupSQL, consumer, n.GetId())
		if err != nil {
			return fmt.Errorf("dedup: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		_, err = tx.Exec(ctx, insertHistorySQL,
			n.GetId(), string(s.opts.Channel), n.GetUserId(),
			"delivered", result.attempts, s.opts.NodeID, "")
		return err
	})
	if dbErr != nil {
		return result, fmt.Errorf("history: %w", dbErr)
	}
	return result, nil
}

func (s *runner) deliverWithRetries(ctx context.Context, n *notificationsv1.Notification) (deliveryResult, error) {
	backoff := s.opts.InitialBackoff
	var lastErr error
	var lastStatus int
	for attempt := 1; attempt <= s.opts.MaxAttempts; attempt++ {
		status, err := s.send(ctx, n)
		lastStatus = status
		if err == nil {
			return deliveryResult{statusCode: status, attempts: attempt}, nil
		}
		if errors.Is(err, errPermanent) {
			return deliveryResult{statusCode: status, attempts: attempt}, err
		}
		lastErr = err
		if attempt == s.opts.MaxAttempts {
			break
		}
		var sleep time.Duration
		if backoff > 0 {
			sleep = time.Duration(rand.Int64N(int64(backoff)))
		}
		select {
		case <-ctx.Done():
			return deliveryResult{statusCode: status, attempts: attempt}, ctx.Err()
		case <-time.After(sleep):
		}
		backoff *= 2
		if backoff > s.opts.MaxBackoff {
			backoff = s.opts.MaxBackoff
		}
	}
	return deliveryResult{statusCode: lastStatus, attempts: s.opts.MaxAttempts},
		fmt.Errorf("retries exhausted: %w", lastErr)
}

// send — один HTTP-запрос с HMAC и Idempotency-Key.
// Idempotency-Key = notification_id, чтобы receiver видел повторы как один запрос.
// X-Signature = HMAC-SHA256(secret, body).
func (s *runner) send(ctx context.Context, n *notificationsv1.Notification) (int, error) {
	body, err := proto.Marshal(n)
	if err != nil {
		return 0, fmt.Errorf("%w: %w", err, errPermanent)
	}
	mac := hmac.New(sha256.New, []byte(s.opts.HMACSecret))
	mac.Write(body)
	sig := hex.EncodeToString(mac.Sum(nil))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.opts.Target, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Idempotency-Key", n.GetId())
	req.Header.Set("X-Signature", sig)
	req.Header.Set("X-Channel", string(s.opts.Channel))
	req.Header.Set("X-Trace-Id", n.GetTraceId())

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return 0, classifyTransport(err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return resp.StatusCode, nil
	case resp.StatusCode == http.StatusRequestTimeout,
		resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode >= 500:
		return resp.StatusCode, fmt.Errorf("retriable status %d", resp.StatusCode)
	default:
		return resp.StatusCode, fmt.Errorf("status %d: %w", resp.StatusCode, errPermanent)
	}
}

func classifyTransport(err error) error {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return fmt.Errorf("transport: %w", err)
	}
	return fmt.Errorf("http: %w", err)
}

// forward — escалация на следующую ступень или DLQ. permanent сразу в DLQ.
func (s *runner) forward(ctx context.Context, st Stage, r *kgo.Record, cause error, permanent bool) error {
	target := st.NextTopic
	reason := "next-retry"
	if permanent {
		target = s.opts.DLQTopic
		reason = "permanent"
	} else if target == "" {
		target = s.opts.DLQTopic
		reason = "exhausted"
	}

	if target == s.opts.DLQTopic {
		s.dlq.Add(1)
	} else {
		s.escalated.Add(1)
	}

	headers := append([]kgo.RecordHeader(nil), r.Headers...)
	idx := indexHeaders(headers)
	prevRetries := 0
	if v, ok := idx["retry.count"]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			prevRetries = n
		}
	}
	if _, ok := idx["original.topic"]; !ok {
		headers = appendOrReplace(headers, "original.topic", r.Topic)
	}
	headers = appendOrReplace(headers, "previous.topic", r.Topic)
	headers = appendOrReplace(headers, "retry.count", strconv.Itoa(prevRetries+1))
	headers = appendOrReplace(headers, "error.class", boolStr(permanent, "permanent", "transient"))
	headers = appendOrReplace(headers, "error.message", cause.Error())
	headers = appendOrReplace(headers, "error.timestamp", time.Now().UTC().Format(time.RFC3339Nano))
	headers = appendOrReplace(headers, "forward.reason", reason)

	rec := &kgo.Record{
		Topic:   target,
		Key:     r.Key,
		Value:   r.Value,
		Headers: headers,
	}

	produceCtx, pcancel := context.WithTimeout(ctx, 10*time.Second)
	defer pcancel()
	if err := s.cl.ProduceSync(produceCtx, rec).FirstErr(); err != nil {
		return fmt.Errorf("produce → %s: %w", target, err)
	}
	return nil
}

func waitUntilDue(ctx context.Context, recordTs time.Time, delay time.Duration) error {
	due := recordTs.Add(delay)
	wait := time.Until(due)
	if wait <= 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(wait):
		return nil
	}
}

func indexHeaders(hs []kgo.RecordHeader) map[string]string {
	out := make(map[string]string, len(hs))
	for _, h := range hs {
		out[h.Key] = string(h.Value)
	}
	return out
}

func appendOrReplace(hs []kgo.RecordHeader, key, value string) []kgo.RecordHeader {
	for i := range hs {
		if hs[i].Key == key {
			hs[i].Value = []byte(value)
			return hs
		}
	}
	return append(hs, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func boolStr(b bool, t, f string) string {
	if b {
		return t
	}
	return f
}

func newHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          50,
			IdleConnTimeout:       30 * time.Second,
			TLSHandshakeTimeout:   3 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}

// RunDLQ — отдельный consumer на DLQ-топик. Пишет notifications_log
// со status='dlq' и last_error из header'а. Этот процесс — последняя
// возможность увидеть, что произошло с уведомлением, и (при желании)
// сделать manual replay через replay-cli.
type DLQOpts struct {
	NodeID   string
	Channel  Channel
	DLQTopic string
	Group    string
	DSN      string

	FromStart bool
}

func RunDLQ(ctx context.Context, o DLQOpts) error {
	pool, err := pgxpool.New(ctx, o.DSN)
	if err != nil {
		return fmt.Errorf("pgxpool.New: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("pg ping: %w", err)
	}

	opts := []kgo.Opt{
		kgo.ConsumerGroup(o.Group),
		kgo.ConsumeTopics(o.DLQTopic),
		kgo.DisableAutoCommit(),
		kgo.ClientID("usecase-09-02-" + string(o.Channel) + "-dlq-" + o.NodeID),
		kgo.SessionTimeout(15 * time.Second),
	}
	if o.FromStart {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	cl, err := kafka.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("[%s] %s-dlq: topic=%q group=%q\n", o.NodeID, o.Channel, o.DLQTopic, o.Group)

	consumer := string(o.Channel) + "-dlq"

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
			return nil
		}

		batch := make([]*kgo.Record, 0)
		fetches.EachRecord(func(r *kgo.Record) { batch = append(batch, r) })
		if len(batch) == 0 {
			continue
		}

		for _, r := range batch {
			var n notificationsv1.Notification
			if err := proto.Unmarshal(r.Value, &n); err != nil {
				// Невалидный proto в DLQ — лог в notifications_log невозможен.
				// Логируем для observability и идём дальше (poison-pill коммитится в общем батче).
				fmt.Printf("[%s] DLQ poison-pill %s/%d@%d: %v\n",
					o.NodeID, r.Topic, r.Partition, r.Offset, err)
				continue
			}
			idx := indexHeaders(r.Headers)
			lastErr := idx["error.message"]
			retryCount := 1
			if v, ok := idx["retry.count"]; ok {
				if cnt, err := strconv.Atoi(v); err == nil {
					retryCount = cnt
				}
			}

			err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				tag, err := tx.Exec(ctx, dedupSQL, consumer, n.GetId())
				if err != nil {
					return fmt.Errorf("dedup: %w", err)
				}
				if tag.RowsAffected() == 0 {
					return nil
				}
				_, err = tx.Exec(ctx, insertHistorySQL,
					n.GetId(), string(o.Channel), n.GetUserId(),
					"dlq", retryCount, o.NodeID, lastErr)
				return err
			})
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("history dlq: %w", err)
			}
		}

		commitCtx, ccancel := context.WithTimeout(ctx, 10*time.Second)
		err := cl.CommitRecords(commitCtx, batch...)
		ccancel()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("CommitRecords: %w", err)
		}
	}
}
