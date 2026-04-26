// bench-acks параллельно гоняет три продьюсера с acks=0/1/all и сравнивает
// latency P50/P99/P99.9 и throughput. Видны две вещи: acks=0 быстрее всего и
// без гарантий, acks=all дороже на круг и устойчив к падению брокера ровно до
// порога min.insync.replicas.
//
// Каждый режим пишет в свой топик (`<prefix>-<mode>`), все три топика созданы
// идемпотентно с min.insync.replicas=3 на RF=3 — это важно для демки
// kill-broker: упадёт kafka-2, ISR падает до 2, для acks=all включаются
// NotEnoughReplicas, для acks=0/1 ничего не меняется.
//
// Запись идёт через ProduceSync на каждый отдельный record. Это даёт честную
// per-record latency (а не «время отдачи целого батча»). Throughput тут
// получается ниже теоретического максимума — у клиента всегда не больше
// одного сообщения в полёте, — зато P50/P99 показывают, сколько занимает
// один полный round-trip с конкретными acks.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
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
	defaultTopicPrefix = "lecture-02-02-acks"
	defaultPartitions  = 3
	defaultReplication = 3
	defaultMessages    = 1000
	defaultPayload     = 1024
	defaultMinISR      = "3"
)

func main() {
	logger := log.New()

	topicPrefix := flag.String("topic-prefix", defaultTopicPrefix, "префикс топика; полное имя — <prefix>-<mode>")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании")
	rf := flag.Int("rf", defaultReplication, "replication factor при создании")
	messages := flag.Int("messages", defaultMessages, "сообщений на каждый режим acks")
	payload := flag.Int("payload", defaultPayload, "размер payload в байтах")
	timeout := flag.Duration("timeout", 60*time.Second, "верхний потолок на каждый режим (после — отмена ctx)")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topicPrefix: *topicPrefix,
		partitions:  int32(*partitions),
		rf:          int16(*rf),
		messages:    *messages,
		payload:     *payload,
		timeout:     *timeout,
	}); err != nil {
		logger.Error("bench-acks failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topicPrefix string
	partitions  int32
	rf          int16
	messages    int
	payload     int
	timeout     time.Duration
}

type ackMode struct {
	name      string // отображаемое имя режима
	topicTail string // суффикс к topicPrefix
	opts      []kgo.Opt
}

func run(ctx context.Context, o runOpts) error {
	// RecordDeliveryTimeout — общий потолок на полную доставку одного record'а:
	// при kill-broker'е acks=all иначе ретраит NotEnoughReplicas вечно (а
	// rpcCtx ловит DEADLINE_EXCEEDED раньше брокерского ответа). С 5 секундами
	// успех укладывается всегда, а под degraded ISR клиент честно отдаёт
	// NOT_ENOUGH_REPLICAS наружу.
	deliveryTimeout := kgo.RecordDeliveryTimeout(5 * time.Second)
	modes := []ackMode{
		{"acks=0", "0", []kgo.Opt{kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite(), deliveryTimeout}},
		{"acks=1", "1", []kgo.Opt{kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite(), deliveryTimeout}},
		{"acks=all", "all", []kgo.Opt{deliveryTimeout}}, // дефолт franz-go: idempotent + AllISRAcks
	}

	admin, err := kafka.NewAdmin()
	if err != nil {
		return fmt.Errorf("kafka.NewAdmin: %w", err)
	}
	defer admin.Close()

	for _, m := range modes {
		topic := fmt.Sprintf("%s-%s", o.topicPrefix, m.topicTail)
		if err := ensureTopic(ctx, admin, topic, o.partitions, o.rf, defaultMinISR); err != nil {
			return fmt.Errorf("ensure topic %s: %w", topic, err)
		}
	}

	payload := makePayload(o.payload)
	fmt.Printf("параллельно пишем %d сообщений по %d B на каждый режим acks (partitions=%d, rf=%d, min.insync.replicas=%s)\n\n",
		o.messages, o.payload, o.partitions, o.rf, defaultMinISR)

	results := make([]benchResult, len(modes))
	var wg sync.WaitGroup
	for i, m := range modes {
		wg.Add(1)
		go func(i int, m ackMode) {
			defer wg.Done()
			topic := fmt.Sprintf("%s-%s", o.topicPrefix, m.topicTail)
			runCtx, runCancel := context.WithTimeout(ctx, o.timeout)
			defer runCancel()
			results[i] = runMode(runCtx, m, topic, o.messages, payload)
		}(i, m)
	}
	wg.Wait()

	fmt.Println("результаты:")
	printTable(os.Stdout, results)

	fmt.Println()
	fmt.Println("сверка с end offsets из лога:")
	for _, m := range modes {
		topic := fmt.Sprintf("%s-%s", o.topicPrefix, m.topicTail)
		fmt.Printf("[%s] %s\n", m.name, topic)
		if err := printEndOffsets(ctx, admin, topic); err != nil {
			fmt.Fprintf(os.Stderr, "  list offsets failed: %v\n", err)
		}
	}

	for _, r := range results {
		if len(r.errs) == 0 {
			continue
		}
		fmt.Println()
		fmt.Printf("[%s] классы ошибок:\n", r.name)
		printErrTable(os.Stdout, r.errs)
	}
	return nil
}

// benchResult копит latency и счётчики по одному режиму acks.
// latencies накапливаем только для успешных produce'ов — иначе таймаутные
// ретраи смазывают P99.
type benchResult struct {
	name      string
	sent      int64
	failed    int64
	elapsed   time.Duration
	latencies []time.Duration
	errs      map[string]int64
}

// runMode синхронно гоняет msgs сообщений через ProduceSync, фиксирует
// latency каждого отдельного round-trip'а. Без параллелизма внутри одного
// режима — мы хотим честную per-record latency, не throughput-batch'инг.
func runMode(ctx context.Context, m ackMode, topic string, msgs int, payload []byte) benchResult {
	res := benchResult{name: m.name, errs: make(map[string]int64)}

	cl, err := kafka.NewClient(m.opts...)
	if err != nil {
		res.errs[fmt.Sprintf("client init: %s", err.Error())] = 1
		return res
	}
	defer cl.Close()

	res.latencies = make([]time.Duration, 0, msgs)

	start := time.Now()
	for i := 0; i < msgs; i++ {
		if err := ctx.Err(); err != nil {
			break
		}
		rec := &kgo.Record{Topic: topic, Value: payload}
		rpcCtx, rpcCancel := context.WithTimeout(ctx, 15*time.Second)
		sendAt := time.Now()
		out := cl.ProduceSync(rpcCtx, rec)
		took := time.Since(sendAt)
		rpcCancel()

		if err := out.FirstErr(); err != nil {
			res.failed++
			res.errs[classifyErr(err)]++
			continue
		}
		res.sent++
		res.latencies = append(res.latencies, took)
	}
	res.elapsed = time.Since(start)
	return res
}

// makePayload — фиксированный набор байт. Содержимое не важно, важна длина.
func makePayload(n int) []byte {
	if n <= 0 {
		return nil
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + (i % 26))
	}
	return b
}

func classifyErr(err error) string {
	if err == nil {
		return ""
	}
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return ke.Message // например, NOT_ENOUGH_REPLICAS, REQUEST_TIMED_OUT
	}
	if errors.Is(err, context.Canceled) {
		return "CONTEXT_CANCELED"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "DEADLINE_EXCEEDED"
	}
	return err.Error()
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func printTable(w *os.File, results []benchResult) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "MODE\tSENT\tFAILED\tELAPSED\tTHROUGHPUT\tP50\tP99\tP99.9\tMAX")
	for _, r := range results {
		sorted := make([]time.Duration, len(r.latencies))
		copy(sorted, r.latencies)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

		var throughput float64
		if r.elapsed > 0 {
			throughput = float64(r.sent) / r.elapsed.Seconds()
		}
		var maxLat time.Duration
		if n := len(sorted); n > 0 {
			maxLat = sorted[n-1]
		}

		fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t%.0f msg/s\t%s\t%s\t%s\t%s\n",
			r.name,
			r.sent,
			r.failed,
			fmtDur(r.elapsed),
			throughput,
			fmtDur(percentile(sorted, 0.50)),
			fmtDur(percentile(sorted, 0.99)),
			fmtDur(percentile(sorted, 0.999)),
			fmtDur(maxLat),
		)
	}
	_ = tw.Flush()
}

func printErrTable(w *os.File, errs map[string]int64) {
	keys := make([]string, 0, len(errs))
	for k := range errs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ERROR\tCOUNT")
	for _, k := range keys {
		fmt.Fprintf(tw, "%s\t%d\n", k, errs[k])
	}
	_ = tw.Flush()
}

// fmtDur округляет до читаемых единиц: миллисекунды для коротких latency,
// секунды — для elapsed.
func fmtDur(d time.Duration) string {
	switch {
	case d == 0:
		return "0"
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d.Microseconds()))
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000.0)
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

// ensureTopic создаёт топик с min.insync.replicas=3. Если топик уже есть —
// alter-конфигом приводит к нужному значению, чтобы повторные запуски работали
// детерминированно.
func ensureTopic(ctx context.Context, admin *kadm.Client, topic string, partitions int32, rf int16, minISR string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	configs := map[string]*string{"min.insync.replicas": &minISR}
	resp, err := admin.CreateTopic(rpcCtx, partitions, rf, configs, topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("topic %q создан: partitions=%d rf=%d min.insync.replicas=%s\n", topic, partitions, rf, minISR)
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if !errors.Is(cause, kerr.TopicAlreadyExists) {
		return cause
	}

	alterCtx, alterCancel := context.WithTimeout(ctx, 15*time.Second)
	defer alterCancel()
	alterations := []kadm.AlterConfig{
		{Op: kadm.SetConfig, Name: "min.insync.replicas", Value: &minISR},
	}
	if _, err := admin.AlterTopicConfigs(alterCtx, alterations, topic); err != nil {
		return fmt.Errorf("alter min.insync.replicas: %w", err)
	}
	fmt.Printf("topic %q уже существует — установлен min.insync.replicas=%s\n", topic, minISR)
	return nil
}

func printEndOffsets(ctx context.Context, admin *kadm.Client, topic string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ends, err := admin.ListEndOffsets(rpcCtx, topic)
	if err != nil {
		return fmt.Errorf("ListEndOffsets: %w", err)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  PARTITION\tLATEST")
	var total int64
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			return
		}
		fmt.Fprintf(tw, "  %d\t%d\n", o.Partition, o.Offset)
		total += o.Offset
	})
	fmt.Fprintf(tw, "  TOTAL\t%d\n", total)
	return tw.Flush()
}
