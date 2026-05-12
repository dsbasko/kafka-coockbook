package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/sony/gobreaker/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

func main() {
	logger := log.New()

	topic := flag.String("topic", "notifications", "входной топик с уведомлениями")
	group := flag.String("group", "lecture-04-05-courier", "consumer group.id")
	target := flag.String("target", "http://localhost:8090/deliver", "URL webhook'а получателя")
	hmacSecret := flag.String("hmac-secret", "lecture-04-05", "ключ для X-Signature (HMAC-SHA256)")
	httpTimeout := flag.Duration("http-timeout", 3*time.Second, "таймаут одного HTTP-запроса")
	maxAttempts := flag.Int("max-attempts", 4, "максимум попыток доставки на сообщение")
	initialBackoff := flag.Duration("initial-backoff", 200*time.Millisecond, "стартовый интервал между retry'ями")
	maxBackoff := flag.Duration("max-backoff", 5*time.Second, "верхний потолок backoff'а")
	cbConsecutive := flag.Uint("cb-trip-after", 5, "сколько подряд неуспехов до перехода CB в Open")
	cbOpenTimeout := flag.Duration("cb-open-timeout", 15*time.Second, "сколько CB сидит в Open до перехода в Half-Open")
	pauseAfter := flag.Duration("pause-after", 10*time.Second, "сколько CB должен пробыть в Open, чтобы паузим партиции")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	c := &courier{
		logger:         logger,
		target:         *target,
		hmacKey:        []byte(*hmacSecret),
		httpClient:     newHTTPClient(*httpTimeout),
		topics:         []string{*topic},
		maxAttempts:    *maxAttempts,
		initialBackoff: *initialBackoff,
		maxBackoff:     *maxBackoff,
		pauseAfter:     *pauseAfter,
	}

	c.cb = gobreaker.NewCircuitBreaker[deliveryResult](gobreaker.Settings{
		Name:        "courier-webhook",
		MaxRequests: 1,
		Timeout:     *cbOpenTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= uint32(*cbConsecutive)
		},
		OnStateChange: c.onStateChange,
	})

	cl, err := kafka.NewClient(
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-04-05-courier"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		logger.Error("kafka.NewClient", "err", err)
		os.Exit(1)
	}
	defer cl.Close()
	c.cl = cl

	if err := c.run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("courier failed", "err", err)
		os.Exit(1)
	}

	c.printSummary()
}

type courier struct {
	logger     *slog.Logger
	cl         *kgo.Client
	httpClient *http.Client
	cb         *gobreaker.CircuitBreaker[deliveryResult]

	target  string
	hmacKey []byte
	topics  []string

	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	pauseAfter     time.Duration

	openSince atomic.Int64
	paused    atomic.Bool

	delivered  atomic.Int64
	failed     atomic.Int64
	dropped4xx atomic.Int64
}

type deliveryResult struct {
	statusCode int
	attempts   int
}

func (c *courier) run(ctx context.Context) error {
	c.logger.Info("courier started", "topics", c.topics, "target", c.target)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		c.maybePauseOnLongOpen()

		pollCtx, pollCancel := context.WithTimeout(ctx, time.Second)
		fetches := c.cl.PollFetches(pollCtx)
		pollCancel()
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return nil
				}
				if errors.Is(e.Err, context.DeadlineExceeded) {
					continue
				}
				c.logger.Error("fetch error", "topic", e.Topic, "partition", e.Partition, "err", e.Err)
			}
			continue
		}

		batch := make([]*kgo.Record, 0)
		fetches.EachRecord(func(r *kgo.Record) { batch = append(batch, r) })

		toCommit := make([]*kgo.Record, 0, len(batch))
	processBatch:
		for _, r := range batch {
			outcome, err := c.deliver(ctx, r)
			switch {
			case err == nil:
				c.delivered.Add(1)
				c.logger.Info("delivered",
					"key", string(r.Key),
					"partition", r.Partition,
					"offset", r.Offset,
					"attempts", outcome.attempts,
					"status", outcome.statusCode)
				toCommit = append(toCommit, r)
			case errors.Is(err, errPermanent):
				c.dropped4xx.Add(1)
				c.logger.Warn("permanent — committing without delivery",
					"key", string(r.Key),
					"offset", r.Offset,
					"status", outcome.statusCode)
				toCommit = append(toCommit, r)
			case errors.Is(err, gobreaker.ErrOpenState), errors.Is(err, gobreaker.ErrTooManyRequests):
				c.failed.Add(1)
				c.logger.Warn("CB rejected — record stays uncommitted",
					"key", string(r.Key),
					"offset", r.Offset,
					"err", err)

				break processBatch
			default:
				c.failed.Add(1)
				c.logger.Warn("transient — record stays uncommitted",
					"key", string(r.Key),
					"offset", r.Offset,
					"attempts", outcome.attempts,
					"err", err)
				break processBatch
			}
		}

		if len(toCommit) > 0 {
			commitCtx, ccancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.cl.CommitRecords(commitCtx, toCommit...)
			ccancel()
			if err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("commit: %w", err)
			}
		}
	}
}

func (c *courier) deliver(ctx context.Context, r *kgo.Record) (deliveryResult, error) {
	return c.cb.Execute(func() (deliveryResult, error) {
		return c.deliverWithRetries(ctx, r)
	})
}

func (c *courier) deliverWithRetries(ctx context.Context, r *kgo.Record) (deliveryResult, error) {
	backoff := c.initialBackoff
	var lastErr error
	var lastStatus int

	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		status, err := c.send(ctx, r)
		lastStatus = status
		if err == nil {
			return deliveryResult{statusCode: status, attempts: attempt}, nil
		}
		if errors.Is(err, errPermanent) {
			return deliveryResult{statusCode: status, attempts: attempt}, err
		}
		lastErr = err

		if attempt == c.maxAttempts {
			break
		}

		var sleep time.Duration
		if backoff > 0 {
			sleep = time.Duration(rand.Int64N(int64(backoff)))
		}
		c.logger.Info("retry",
			"key", string(r.Key),
			"attempt", attempt,
			"status", status,
			"sleep", sleep,
			"err", err)

		select {
		case <-ctx.Done():
			return deliveryResult{statusCode: status, attempts: attempt}, ctx.Err()
		case <-time.After(sleep):
		}

		backoff *= 2
		if backoff > c.maxBackoff {
			backoff = c.maxBackoff
		}
	}

	return deliveryResult{statusCode: lastStatus, attempts: c.maxAttempts},
		fmt.Errorf("retries exhausted: %w", lastErr)
}

func (c *courier) send(ctx context.Context, r *kgo.Record) (int, error) {
	body := r.Value
	mac := hmac.New(sha256.New, c.hmacKey)
	mac.Write(body)
	signature := hex.EncodeToString(mac.Sum(nil))

	idem := fmt.Sprintf("%s:%d:%d", r.Topic, r.Partition, r.Offset)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.target, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idem)
	req.Header.Set("X-Signature", signature)
	req.Header.Set("X-Origin-Topic", r.Topic)
	req.Header.Set("X-Origin-Key", string(r.Key))

	resp, err := c.httpClient.Do(req)
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

var errPermanent = errors.New("permanent http failure")

func classifyTransport(err error) error {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return fmt.Errorf("transport: %w", err)
	}
	return fmt.Errorf("http: %w", err)
}

func (c *courier) onStateChange(name string, from, to gobreaker.State) {
	c.logger.Warn("CB state change", "name", name, "from", from, "to", to)
	switch to {
	case gobreaker.StateOpen:
		c.openSince.Store(time.Now().UnixNano())
	case gobreaker.StateHalfOpen, gobreaker.StateClosed:
		c.openSince.Store(0)
		if c.paused.Swap(false) {
			c.cl.ResumeFetchTopics(c.topics...)
			c.logger.Info("partitions resumed", "topics", c.topics)
		}
	}
}

func (c *courier) maybePauseOnLongOpen() {
	since := c.openSince.Load()
	if since == 0 {
		return
	}
	if time.Since(time.Unix(0, since)) < c.pauseAfter {
		return
	}
	if c.paused.CompareAndSwap(false, true) {
		c.cl.PauseFetchTopics(c.topics...)
		c.logger.Warn("partitions paused — CB stayed Open too long",
			"topics", c.topics,
			"open_for", time.Since(time.Unix(0, since)).Truncate(time.Second))
	}
	_ = c.cb.State()
}

func (c *courier) printSummary() {
	fmt.Fprintf(os.Stderr,
		"\nостановлен. delivered=%d failed=%d dropped_4xx=%d cb_state=%s\n",
		c.delivered.Load(), c.failed.Load(), c.dropped4xx.Load(), c.cb.State())
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
