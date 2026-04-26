// bench гоняет матрицу (linger × compression) и сравнивает throughput,
// latency P50/P99/P99.9 и размер на диске. Каждая комбинация пишется в свой
// топик `<prefix>-l<linger>-<comp>`, иначе размеры на диске смешаются.
//
// Запись идёт асинхронно через cl.Produce + per-record callback. Это
// принципиально: ProduceSync на каждый record блокирует клиента и убивает
// батчинг — мы тогда измерим не «эффект от linger», а пинг до брокера.
// Latency мерим как «время от cl.Produce до callback», throughput — как
// `messages / cl.Flush_elapsed`. Размер берём из kadm.DescribeAllLogDirs
// после прогона.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
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
	defaultTopicPrefix = "lecture-02-04-batching"
	defaultPartitions  = 3
	defaultReplication = 3
	defaultMessages    = 100_000
	defaultPayload     = 1024
)

func main() {
	logger := log.New()

	topicPrefix := flag.String("topic-prefix", defaultTopicPrefix, "префикс; полное имя топика — <prefix>-l<lingerMs>-<comp>")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании")
	rf := flag.Int("rf", defaultReplication, "replication factor")
	messages := flag.Int("messages", defaultMessages, "сообщений в каждом прогоне (по умолчанию 100000)")
	payload := flag.Int("payload", defaultPayload, "размер payload в байтах (по умолчанию 1024)")
	timeout := flag.Duration("timeout", 5*time.Minute, "верхний потолок на один прогон")
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
		logger.Error("bench failed", "err", err)
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

// scenario — одна точка матрицы: какой linger, какой кодек, как назвать
// суффикс топика и как выглядеть в таблице.
type scenario struct {
	name      string
	topicTail string
	linger    time.Duration
	codec     kgo.CompressionCodec
}

func run(ctx context.Context, o runOpts) error {
	// Полезные комбинации: 0/5/50ms × none/lz4/zstd. Snappy и gzip оставлены
	// за кадром — snappy ведёт себя ~как lz4 по соотношению скорость/ratio,
	// gzip медленнее и в проде используется реже остальных.
	lingers := []struct {
		name string
		dur  time.Duration
	}{
		{"0ms", 0},
		{"5ms", 5 * time.Millisecond},
		{"50ms", 50 * time.Millisecond},
	}
	codecs := []struct {
		name  string
		codec kgo.CompressionCodec
	}{
		{"none", kgo.NoCompression()},
		{"lz4", kgo.Lz4Compression()},
		{"zstd", kgo.ZstdCompression()},
	}

	var scenarios []scenario
	for _, l := range lingers {
		for _, c := range codecs {
			scenarios = append(scenarios, scenario{
				name:      fmt.Sprintf("linger=%-4s compression=%s", l.name, c.name),
				topicTail: fmt.Sprintf("l%s-%s", l.name, c.name),
				linger:    l.dur,
				codec:     c.codec,
			})
		}
	}

	admin, err := kafka.NewAdmin()
	if err != nil {
		return fmt.Errorf("kafka.NewAdmin: %w", err)
	}
	defer admin.Close()

	// Все топики создаём заранее — DescribeLogDirs в конце прогона видит сразу
	// всё, и у каждого прогона стартовая позиция партиции одинакова (нам
	// важно сравнивать дельту, а не абсолют).
	topics := make([]string, len(scenarios))
	for i, s := range scenarios {
		topics[i] = fmt.Sprintf("%s-%s", o.topicPrefix, s.topicTail)
		if err := ensureTopic(ctx, admin, topics[i], o.partitions, o.rf); err != nil {
			return fmt.Errorf("ensure topic %s: %w", topics[i], err)
		}
	}

	// Генерим messages уникальных JSON-шаблонов размером ~payload. JSON
	// выбран намеренно: на нём кодеки показывают реалистичный ratio
	// (структура сжимается, случайные id/payload — нет). Переиспользовать
	// один и тот же блок нельзя — иначе zstd/lz4 закодировали бы его как
	// «повтор N раз», ratio поплыл бы в небо.
	payloads, err := makeJSONPayloads(o.messages, o.payload)
	if err != nil {
		return fmt.Errorf("makeJSONPayloads: %w", err)
	}

	fmt.Printf("матрица %d сценариев, %d JSON-сообщений по ~%d B, partitions=%d, rf=%d\n\n",
		len(scenarios), o.messages, o.payload, o.partitions, o.rf)

	// Снимок размеров до прогона — чтобы повторные запуски не накапливались.
	sizesBefore, err := topicSizes(ctx, admin, topics)
	if err != nil {
		return fmt.Errorf("DescribeLogDirs before: %w", err)
	}

	results := make([]benchResult, len(scenarios))
	for i, s := range scenarios {
		runCtx, runCancel := context.WithTimeout(ctx, o.timeout)
		fmt.Printf("[%d/%d] %s ... ", i+1, len(scenarios), s.name)
		results[i] = runScenario(runCtx, s, topics[i], payloads)
		runCancel()
		fmt.Printf("sent=%d failed=%d elapsed=%s\n",
			results[i].sent, results[i].failed, results[i].elapsed.Round(time.Millisecond))
	}

	// Брокеру нужна доля секунды, чтобы досинхронизировать сегменты после
	// последнего fetch ответа: без задержки бывает, что Size возвращается на
	// 1–2 батча меньше реального.
	time.Sleep(500 * time.Millisecond)
	sizesAfter, err := topicSizes(ctx, admin, topics)
	if err != nil {
		return fmt.Errorf("DescribeLogDirs after: %w", err)
	}

	for i := range results {
		results[i].topic = topics[i]
		results[i].deltaBytes = sizesAfter[topics[i]] - sizesBefore[topics[i]]
	}

	fmt.Println()
	fmt.Println("итоги:")
	printResults(os.Stdout, results)

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

// benchResult копит latency и счётчики по одному сценарию.
type benchResult struct {
	name       string
	topic      string
	sent       int64
	failed     int64
	elapsed    time.Duration
	latencies  []time.Duration
	errs       map[string]int64
	deltaBytes int64
}

// runScenario гоняет один сценарий: создаёт клиент с нужными опциями,
// async-производит len(payloads) сообщений, ждёт Flush, собирает результаты.
func runScenario(ctx context.Context, s scenario, topic string, payloads [][]byte) benchResult {
	msgs := len(payloads)
	res := benchResult{name: s.name, errs: make(map[string]int64)}

	opts := []kgo.Opt{
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerLinger(s.linger),
		kgo.ProducerBatchCompression(s.codec),
		// Достаточно большой батч и буфер, чтобы linger реально успел
		// набрать сообщений: на 100k×1KB при дефолтных 10000 буферных
		// записей мы упирались бы не в linger, а в backpressure.
		kgo.ProducerBatchMaxBytes(1 << 20), // 1 MiB
		kgo.MaxBufferedRecords(200_000),
	}

	cl, err := kafka.NewClient(opts...)
	if err != nil {
		res.errs[fmt.Sprintf("client init: %s", err.Error())] = 1
		return res
	}
	defer cl.Close()

	res.latencies = make([]time.Duration, msgs)

	var (
		mu      sync.Mutex
		idx     atomic.Int64
		failed  atomic.Int64
		errsLoc = make(map[string]int64)
	)

	start := time.Now()
	for i := 0; i < msgs; i++ {
		if err := ctx.Err(); err != nil {
			break
		}
		rec := &kgo.Record{Value: payloads[i]}
		sendAt := time.Now()
		cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			took := time.Since(sendAt)
			if err != nil {
				failed.Add(1)
				cls := classifyErr(err)
				mu.Lock()
				errsLoc[cls]++
				mu.Unlock()
				return
			}
			// Слот в latencies резервируем только для успешных, иначе
			// failed-callback'и оставляли бы нули в массиве и сбивали
			// перцентили вниз.
			i := idx.Add(1) - 1
			if i < int64(len(res.latencies)) {
				res.latencies[i] = took
			}
		})
	}

	flushCtx, flushCancel := context.WithTimeout(ctx, 2*time.Minute)
	_ = cl.Flush(flushCtx)
	flushCancel()
	res.elapsed = time.Since(start)

	res.sent = idx.Load()
	res.failed = failed.Load()
	res.errs = errsLoc

	// Подрезаем latencies до фактически записанных (на случай ранней отмены).
	res.latencies = res.latencies[:idx.Load()]
	return res
}

// makeJSONPayloads генерит count уникальных JSON-сообщений размером
// примерно targetSize байт. Структура — фиксированная (поля повторяются),
// id и payload — случайные. Это даёт реалистичный compression-ratio:
// структура жмётся (zstd/lz4 видят повторяющиеся ключи), random-часть нет.
// Если бы мы переиспользовали один буфер, ratio был бы фиктивным.
func makeJSONPayloads(count, targetSize int) ([][]byte, error) {
	out := make([][]byte, count)
	for i := 0; i < count; i++ {
		b, err := buildJSONRecord(i, targetSize)
		if err != nil {
			return nil, err
		}
		out[i] = b
	}
	return out, nil
}

// buildJSONRecord собирает JSON-запись фиксированной структуры. Поле
// `payload` дополняется hex-байтами до приблизительного размера.
func buildJSONRecord(seq, targetSize int) ([]byte, error) {
	// Структурные накладные расходы: фиксированные ключи плюс id/ts.
	// Остаток до targetSize забиваем random hex'ом (2 hex char = 1 byte).
	header := fmt.Sprintf(
		`{"seq":%d,"id":"`, seq,
	)
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return nil, err
	}
	id := fmt.Sprintf("%x", idBytes)
	tail := fmt.Sprintf(`","ts":%d,"event":"order.created","payload":"`, time.Now().UnixNano())
	closing := `"}`

	used := len(header) + len(id) + len(tail) + len(closing)
	pad := targetSize - used
	if pad < 0 {
		pad = 0
	}
	// hex-кодирование: pad байт hex'а = pad/2 случайных байт.
	rawN := (pad + 1) / 2
	raw := make([]byte, rawN)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	hexPad := fmt.Sprintf("%x", raw)
	if len(hexPad) > pad {
		hexPad = hexPad[:pad]
	}

	buf := make([]byte, 0, used+pad)
	buf = append(buf, header...)
	buf = append(buf, id...)
	buf = append(buf, tail...)
	buf = append(buf, hexPad...)
	buf = append(buf, closing...)
	return buf, nil
}

func classifyErr(err error) string {
	if err == nil {
		return ""
	}
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return ke.Message
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

func printResults(w *os.File, results []benchResult) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SCENARIO\tSENT\tFAILED\tELAPSED\tTHROUGHPUT\tP50\tP99\tP99.9\tDISK")
	for _, r := range results {
		sorted := make([]time.Duration, len(r.latencies))
		copy(sorted, r.latencies)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

		var throughput float64
		if r.elapsed > 0 {
			throughput = float64(r.sent) / r.elapsed.Seconds()
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
			fmtBytes(r.deltaBytes),
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

func fmtBytes(b int64) string {
	const (
		_  = iota
		KB = 1 << (10 * iota)
		MB
		GB
	)
	switch {
	case b <= 0:
		return "0"
	case b < KB:
		return fmt.Sprintf("%dB", b)
	case b < MB:
		return fmt.Sprintf("%.1fKB", float64(b)/float64(KB))
	case b < GB:
		return fmt.Sprintf("%.1fMB", float64(b)/float64(MB))
	default:
		return fmt.Sprintf("%.2fGB", float64(b)/float64(GB))
	}
}

func ensureTopic(ctx context.Context, admin *kadm.Client, topic string, partitions int32, rf int16) error {
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

// topicSizes суммирует размер партиций по каждому топику через
// DescribeAllLogDirs. На rf=3 у каждого партиционного логфайла три реплики;
// мы считаем строго по одной реплике на партицию (первая найденная),
// чтобы сравнение сценариев читалось как «сколько байт на диске у одного
// брокера», а не «суммарно у всего кластера».
func topicSizes(ctx context.Context, admin *kadm.Client, topics []string) (map[string]int64, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	wanted := make(map[string]bool, len(topics))
	for _, t := range topics {
		wanted[t] = true
	}

	// Передаём nil — у API «nil = все log dirs»: это надёжнее, чем
	// TopicsSet с пустым списком партиций (брокеры разных версий по-разному
	// интерпретируют пустой partitions[]). Лишние топики просто отфильтруем.
	all, err := admin.DescribeAllLogDirs(rpcCtx, nil)
	if err != nil {
		return nil, err
	}

	sizes := make(map[string]int64, len(topics))
	seen := make(map[string]map[int32]bool, len(topics))
	for _, t := range topics {
		seen[t] = make(map[int32]bool)
	}
	all.Each(func(d kadm.DescribedLogDir) {
		d.Topics.Each(func(p kadm.DescribedLogDirPartition) {
			if !wanted[p.Topic] {
				return
			}
			if seen[p.Topic][p.Partition] {
				return
			}
			seen[p.Topic][p.Partition] = true
			sizes[p.Topic] += p.Size
		})
	})
	return sizes, nil
}
