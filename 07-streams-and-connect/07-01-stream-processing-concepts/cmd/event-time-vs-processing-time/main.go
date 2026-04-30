// event-time-vs-processing-time — демо к лекции 07-01.
//
// Один бинарь с двумя ролями:
//
//   -role=events     — продьюсер. Каждые `rate` пишет одно событие в топик.
//                      В header'е "event-time" лежит unix-nano «оригинального»
//                      времени события — оно отстаёт от wall-clock'а от 0 до
//                      `event-lag-max` секунд (равномерно). С вероятностью
//                      `late-prob` событие помечается «поздним»: лаг
//                      случайно из [late-lag-min, late-lag-max].
//
//   -role=aggregator — консьюмер. Считает count событий по 1-минутным
//                      tumbling-окнам в двух системах координат:
//                      (а) по event-time (header), (б) по processing-time
//                      (wall-clock в момент чтения). Раз в `print` печатает
//                      обе таблицы side-by-side — видно, что одни и те же
//                      сообщения попадают в разные минутные ведра.
//
// Топик создаётся идемпотентно при любом старте (партиций=3, RF=3).
// Останавливается по SIGINT/SIGTERM (runctx.New).
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic         = "lecture-07-01-events"
	defaultPartitions    = 3
	defaultReplication   = 3
	defaultGroup         = "lecture-07-01-aggregator"
	defaultRate          = 50 * time.Millisecond
	defaultLateProb      = 0.10
	defaultEventLagMax   = 60 * time.Second
	defaultLateLagMin    = 90 * time.Second
	defaultLateLagMax    = 240 * time.Second
	defaultPrintInterval = 5 * time.Second
	defaultWindow        = 1 * time.Minute
	defaultPrintWindows  = 8
	defaultUserCardinality = 50
)

func main() {
	logger := log.New()

	role := flag.String("role", "events", "роль процесса: events или aggregator")
	topic := flag.String("topic", defaultTopic, "Kafka topic")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании")
	rf := flag.Int("rf", defaultReplication, "replication factor при создании")

	rate := flag.Duration("rate", defaultRate, "пауза между событиями (events role)")
	lateProb := flag.Float64("late-prob", defaultLateProb, "вероятность позднего события 0..1 (events role)")
	eventLagMax := flag.Duration("event-lag-max", defaultEventLagMax, "верхняя граница нормального event-time лага (events role)")
	lateLagMin := flag.Duration("late-lag-min", defaultLateLagMin, "нижняя граница лага у поздних событий (events role)")
	lateLagMax := flag.Duration("late-lag-max", defaultLateLagMax, "верхняя граница лага у поздних событий (events role)")
	users := flag.Int("users", defaultUserCardinality, "сколько разных user-ключей используем (events role, должно быть > 0)")

	group := flag.String("group", defaultGroup, "consumer group (aggregator role)")
	printInterval := flag.Duration("print", defaultPrintInterval, "период печати агрегата (aggregator role)")
	window := flag.Duration("window", defaultWindow, "размер tumbling-окна (aggregator role)")
	printWindows := flag.Int("print-windows", defaultPrintWindows, "сколько последних окон выводить (aggregator role)")

	flag.Parse()

	if *users <= 0 {
		logger.Error("users must be > 0", "users", *users)
		os.Exit(2)
	}

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := ensureTopic(rootCtx, *topic, int32(*partitions), int16(*rf)); err != nil {
		logger.Error("ensure topic failed", "err", err)
		os.Exit(1)
	}

	switch *role {
	case "events":
		err := runProducer(rootCtx, producerOpts{
			topic:       *topic,
			rate:        *rate,
			lateProb:    *lateProb,
			eventLagMax: *eventLagMax,
			lateLagMin:  *lateLagMin,
			lateLagMax:  *lateLagMax,
			users:       *users,
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("producer failed", "err", err)
			os.Exit(1)
		}
	case "aggregator":
		err := runAggregator(rootCtx, aggregatorOpts{
			topic:         *topic,
			group:         *group,
			window:        *window,
			printInterval: *printInterval,
			printWindows:  *printWindows,
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("aggregator failed", "err", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown role %q (ожидается events|aggregator)\n", *role)
		os.Exit(2)
	}
}

func ensureTopic(ctx context.Context, topic string, partitions int32, rf int16) error {
	cl, err := kafka.NewClient()
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)

	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	resp, err := admin.CreateTopic(rpcCtx, partitions, rf, nil, topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("topic %q создан: partitions=%d rf=%d\n", topic, partitions, rf)
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if errors.Is(cause, kerr.TopicAlreadyExists) {
		return nil
	}
	return cause
}

type producerOpts struct {
	topic       string
	rate        time.Duration
	lateProb    float64
	eventLagMax time.Duration
	lateLagMin  time.Duration
	lateLagMax  time.Duration
	users       int
}

type eventPayload struct {
	UserID string  `json:"user_id"`
	Amount float64 `json:"amount"`
}

func runProducer(ctx context.Context, o producerOpts) error {
	cl, err := kafka.NewClient()
	if err != nil {
		return err
	}
	defer cl.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	tk := time.NewTicker(o.rate)
	defer tk.Stop()

	fmt.Printf("producer started: topic=%q rate=%s late-prob=%.2f event-lag-max=%s late-lag=[%s..%s]\n",
		o.topic, o.rate, o.lateProb, o.eventLagMax, o.lateLagMin, o.lateLagMax)

	count := 0
	lateCount := 0
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\nproducer stopped: sent=%d late=%d\n", count, lateCount)
			return nil
		case now := <-tk.C:
			lag := time.Duration(rng.Int63n(int64(o.eventLagMax) + 1))
			late := false
			if rng.Float64() < o.lateProb && o.lateLagMax > o.lateLagMin {
				lag = o.lateLagMin + time.Duration(rng.Int63n(int64(o.lateLagMax-o.lateLagMin)+1))
				late = true
			}
			eventTime := now.Add(-lag)

			payload := eventPayload{
				UserID: fmt.Sprintf("u-%02d", rng.Intn(o.users)),
				Amount: float64(rng.Intn(10000)) / 100,
			}
			body, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("json.Marshal: %w", err)
			}

			headers := []kgo.RecordHeader{
				{Key: "event-time", Value: encodeUnixNano(eventTime)},
			}
			if late {
				headers = append(headers, kgo.RecordHeader{Key: "late", Value: []byte("1")})
			}

			rec := &kgo.Record{
				Topic:   o.topic,
				Key:     []byte(payload.UserID),
				Value:   body,
				Headers: headers,
			}
			rpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = cl.ProduceSync(rpcCtx, rec).FirstErr()
			cancel()
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("ProduceSync: %w", err)
			}
			count++
			if late {
				lateCount++
			}
			if count%50 == 0 {
				fmt.Printf("sent=%d late=%d (last lag=%s, last user=%s)\n",
					count, lateCount, lag.Round(time.Second), payload.UserID)
			}
		}
	}
}

func encodeUnixNano(t time.Time) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(t.UnixNano()))
	return b
}

func decodeUnixNano(b []byte) (time.Time, bool) {
	if len(b) != 8 {
		return time.Time{}, false
	}
	ns := int64(binary.BigEndian.Uint64(b))
	return time.Unix(0, ns), true
}

type aggregatorOpts struct {
	topic         string
	group         string
	window        time.Duration
	printInterval time.Duration
	printWindows  int
}

type aggregator struct {
	mu           sync.Mutex
	window       time.Duration
	byEventTime  map[time.Time]int
	byProcessing map[time.Time]int
	totalRecords int
	lateCount    int
	missingHdr   int
}

func newAggregator(window time.Duration) *aggregator {
	return &aggregator{
		window:       window,
		byEventTime:  map[time.Time]int{},
		byProcessing: map[time.Time]int{},
	}
}

func (a *aggregator) add(eventTime, processingTime time.Time, late, missing bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.byEventTime[eventTime.Truncate(a.window)]++
	a.byProcessing[processingTime.Truncate(a.window)]++
	a.totalRecords++
	if late {
		a.lateCount++
	}
	if missing {
		a.missingHdr++
	}
}

type windowRow struct {
	start time.Time
	count int
}

func (a *aggregator) snapshot(maxWindows int) (event, processing []windowRow, total, late, missing int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	event = lastN(a.byEventTime, maxWindows)
	processing = lastN(a.byProcessing, maxWindows)
	total = a.totalRecords
	late = a.lateCount
	missing = a.missingHdr
	return
}

func lastN(m map[time.Time]int, n int) []windowRow {
	rows := make([]windowRow, 0, len(m))
	for k, v := range m {
		rows = append(rows, windowRow{start: k, count: v})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].start.Before(rows[j].start) })
	if n > 0 && len(rows) > n {
		rows = rows[len(rows)-n:]
	}
	return rows
}

func runAggregator(ctx context.Context, o aggregatorOpts) error {
	cl, err := kafka.NewClient(
		kgo.ConsumerGroup(o.group),
		kgo.ConsumeTopics(o.topic),
		// Новые группы стартуют с конца — старые тестовые сообщения не интересны;
		// видим только то, что producer пишет прямо сейчас.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		// Демо без commit'ов — состояние живёт в памяти aggregator'а, после
		// рестарта окна всё равно начнутся заново.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	agg := newAggregator(o.window)

	go printLoop(ctx, agg, o.printInterval, o.printWindows)

	fmt.Printf("aggregator started: topic=%q group=%q window=%s print=%s\n",
		o.topic, o.group, o.window, o.printInterval)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return nil
				}
				fmt.Fprintf(os.Stderr, "fetch err on %s/%d: %v\n", e.Topic, e.Partition, e.Err)
			}
		}
		fetches.EachRecord(func(rec *kgo.Record) {
			processingTime := time.Now()
			eventTime := processingTime
			missing := true
			late := false
			for _, h := range rec.Headers {
				switch h.Key {
				case "event-time":
					if t, ok := decodeUnixNano(h.Value); ok {
						eventTime = t
						missing = false
					}
				case "late":
					late = string(h.Value) == "1"
				}
			}
			agg.add(eventTime, processingTime, late, missing)
		})
	}
}

func printLoop(ctx context.Context, agg *aggregator, interval time.Duration, maxWindows int) {
	tk := time.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			eventRows, procRows, total, late, missing := agg.snapshot(maxWindows)
			if total == 0 {
				fmt.Println("[aggregator] нет записей пока — ждём producer")
				continue
			}
			fmt.Printf("\n[%s]  total=%d  late=%d  no-header=%d\n",
				time.Now().Format("15:04:05"), total, late, missing)

			tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "WINDOW\tBY EVENT-TIME\tBY PROCESSING-TIME\tDIFF")
			for _, w := range mergeWindows(eventRows, procRows) {
				ec := lookupCount(eventRows, w)
				pc := lookupCount(procRows, w)
				fmt.Fprintf(tw, "%s\t%s\t%s\t%+d\n",
					w.Format("15:04"), formatCount(ec), formatCount(pc), ec-pc)
			}
			_ = tw.Flush()
			fmt.Println("---")
		}
	}
}

func mergeWindows(a, b []windowRow) []time.Time {
	set := map[time.Time]struct{}{}
	for _, r := range a {
		set[r.start] = struct{}{}
	}
	for _, r := range b {
		set[r.start] = struct{}{}
	}
	out := make([]time.Time, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Before(out[j]) })
	return out
}

func lookupCount(rows []windowRow, w time.Time) int {
	for _, r := range rows {
		if r.start.Equal(w) {
			return r.count
		}
	}
	return 0
}

func formatCount(n int) string {
	if n == 0 {
		return "."
	}
	return strconv.Itoa(n)
}
