// forced-retry демонстрирует, что делает идемпотентный продьюсер на самом
// деле. Сценарий: пишем N сообщений через сетевую обёртку, которая случайно
// рвёт TCP-соединение **после** того, как запрос уже улетел брокеру.
// Брокер успевает записать батч в лог, а ответ к клиенту не приходит —
// клиент думает, что запрос провалился, и ретраит. Без идемпотентности
// каждый успешный ретрай добавляет в лог ещё одну копию. С идемпотентностью
// брокер дедуплицирует на стороне партиции по (producer-id, sequence-number)
// — в логе ровно столько же записей, сколько мы намеревались отправить.
//
// Метрика — log delta (kadm.ListEndOffsets до и после прогона). Без
// идемпотентности дельта обычно заметно больше intended; с ней — равна
// числу успешных ProduceSync.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"sort"
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
	defaultTopic       = "lecture-02-03-idempotent"
	defaultPartitions  = 3
	defaultReplication = 3
	defaultMessages    = 100
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "имя топика")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании")
	rf := flag.Int("rf", defaultReplication, "replication factor при создании")
	messages := flag.Int("messages", defaultMessages, "сколько логических сообщений отправить")
	idempotent := flag.Bool("idempotent", true, "включена ли идемпотентность; false — kgo.DisableIdempotentWrite")
	dropRate := flag.Float64("drop-rate", 0.35, "вероятность оборвать TCP-чтение и тем самым форсировать ретрай (0..1)")
	deliveryTimeout := flag.Duration("delivery-timeout", 60*time.Second, "RecordDeliveryTimeout — общий потолок на доставку record'а")
	retries := flag.Int("retries", 30, "RecordRetries — сколько раз клиент ретраит один record")
	warmup := flag.Int("warmup-reads", 8, "сколько начальных Read'ов не обрывать (metadata/handshake)")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topic:           *topic,
		partitions:      int32(*partitions),
		rf:              int16(*rf),
		messages:        *messages,
		idempotent:      *idempotent,
		dropRate:        *dropRate,
		deliveryTimeout: *deliveryTimeout,
		retries:         *retries,
		warmupReads:     *warmup,
	}); err != nil {
		logger.Error("forced-retry failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic           string
	partitions      int32
	rf              int16
	messages        int
	idempotent      bool
	dropRate        float64
	deliveryTimeout time.Duration
	retries         int
	warmupReads     int
}

func run(ctx context.Context, o runOpts) error {
	admin, err := kafka.NewAdmin()
	if err != nil {
		return fmt.Errorf("kafka.NewAdmin: %w", err)
	}
	defer admin.Close()

	if err := ensureTopic(ctx, admin, o.topic, o.partitions, o.rf); err != nil {
		return fmt.Errorf("ensure topic %s: %w", o.topic, err)
	}

	// Дельту считаем относительно текущих end offsets — топик можно гонять
	// несколько прогонов подряд, накопленная история нам не мешает.
	before, err := totalEndOffsets(ctx, admin, o.topic)
	if err != nil {
		return fmt.Errorf("ListEndOffsets before: %w", err)
	}

	dropper := &lossyDialer{
		base:        net.Dialer{Timeout: 5 * time.Second},
		dropRate:    o.dropRate,
		warmupReads: int64(o.warmupReads),
	}

	opts := []kgo.Opt{
		kgo.Dialer(dropper.DialContext),
		kgo.RecordDeliveryTimeout(o.deliveryTimeout),
		kgo.RecordRetries(o.retries),
	}
	if !o.idempotent {
		// Без идемпотентности обязаны явно указать acks (иначе клиент
		// проверит несовместимые дефолты) и отключить ProducerID-обмен.
		opts = append(opts,
			kgo.DisableIdempotentWrite(),
			kgo.RequiredAcks(kgo.AllISRAcks()),
		)
	}

	cl, err := kafka.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("режим: idempotent=%v drop-rate=%.2f delivery-timeout=%s retries=%d\n",
		o.idempotent, o.dropRate, o.deliveryTimeout, o.retries)
	fmt.Printf("topic=%s end offsets до прогона: %d\n", o.topic, before)
	fmt.Println()

	res := produceLoop(ctx, cl, o)

	// Flush, чтобы все in-flight ретраи устаканились, и докинутый брокером
	// дубль (если был) уже сидел в логе к моменту замера.
	flushCtx, flushCancel := context.WithTimeout(ctx, 30*time.Second)
	_ = cl.Flush(flushCtx)
	flushCancel()

	// Гасим dropper и ждём пару секунд — даём брокеру дописать
	// возможно зависшие батчи перед тем, как смотреть на end offsets.
	dropper.disable()
	time.Sleep(500 * time.Millisecond)

	after, err := totalEndOffsets(ctx, admin, o.topic)
	if err != nil {
		return fmt.Errorf("ListEndOffsets after: %w", err)
	}

	delta := after - before
	dupes := delta - res.sent
	if dupes < 0 {
		dupes = 0
	}

	fmt.Println("результаты:")
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "METRIC\tVALUE")
	fmt.Fprintf(tw, "intended\t%d\n", o.messages)
	fmt.Fprintf(tw, "client SENT (FirstErr==nil)\t%d\n", res.sent)
	fmt.Fprintf(tw, "client FAILED\t%d\n", res.failed)
	fmt.Fprintf(tw, "log delta (after-before)\t%d\n", delta)
	fmt.Fprintf(tw, "duplicates (delta - SENT)\t%d\n", dupes)
	fmt.Fprintf(tw, "TCP reads dropped\t%d\n", dropper.dropped.Load())
	fmt.Fprintf(tw, "elapsed\t%s\n", res.elapsed.Round(time.Millisecond))
	_ = tw.Flush()

	if len(res.errs) > 0 {
		fmt.Println()
		fmt.Println("классы ошибок:")
		printErrTable(os.Stdout, res.errs)
	}
	return nil
}

type produceResult struct {
	sent    int64
	failed  int64
	elapsed time.Duration
	errs    map[string]int64
}

// produceLoop — последовательный ProduceSync на каждый record. Без батчинга
// внутри: каждый record должен пройти свой собственный путь retry/timeout,
// иначе несколько разных record'ов попадут в один батч и общая судьба
// сольётся.
func produceLoop(ctx context.Context, cl *kgo.Client, o runOpts) produceResult {
	res := produceResult{errs: make(map[string]int64)}
	start := time.Now()
	for i := 0; i < o.messages; i++ {
		if err := ctx.Err(); err != nil {
			break
		}
		rec := &kgo.Record{
			Topic: o.topic,
			Key:   []byte(fmt.Sprintf("k-%04d", i)),
			Value: []byte(fmt.Sprintf("msg-%04d", i)),
		}
		rpcCtx, rpcCancel := context.WithTimeout(ctx, o.deliveryTimeout+10*time.Second)
		out := cl.ProduceSync(rpcCtx, rec)
		rpcCancel()
		if err := out.FirstErr(); err != nil {
			res.failed++
			res.errs[classifyErr(err)]++
			continue
		}
		res.sent++
	}
	res.elapsed = time.Since(start)
	return res
}

// lossyDialer — обёртка над net.Dialer. Возвращает соединения, которые с
// заданной вероятностью обрывают Read после первой пары успешных чтений
// (warmup). Это имитирует «брокер ответил, но ответ потерялся в сети» —
// франц-go видит EOF, считает запрос неуспешным и ретраит. Сам брокер уже
// записал батч в лог.
type lossyDialer struct {
	base        net.Dialer
	dropRate    float64
	warmupReads int64
	disabled    atomic.Bool
	dropped     atomic.Int64
}

func (d *lossyDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	c, err := d.base.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &lossyConn{Conn: c, parent: d}, nil
}

func (d *lossyDialer) disable() { d.disabled.Store(true) }

type lossyConn struct {
	net.Conn
	parent *lossyDialer
	reads  atomic.Int64
}

func (c *lossyConn) Read(p []byte) (int, error) {
	n := c.reads.Add(1)
	if !c.parent.disabled.Load() && n > c.parent.warmupReads && rand.Float64() < c.parent.dropRate {
		c.parent.dropped.Add(1)
		_ = c.Conn.Close()
		return 0, io.EOF
	}
	return c.Conn.Read(p)
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

// totalEndOffsets суммирует latest по всем партициям топика. Это и есть
// «сколько физических записей лежит в логе» (с учётом retention — но здесь
// retention дефолтный, ничего не отрезается).
func totalEndOffsets(ctx context.Context, admin *kadm.Client, topic string) (int64, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ends, err := admin.ListEndOffsets(rpcCtx, topic)
	if err != nil {
		return 0, err
	}
	var total int64
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			return
		}
		total += o.Offset
	})
	return total, nil
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
