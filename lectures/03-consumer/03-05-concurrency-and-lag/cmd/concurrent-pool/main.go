// concurrent-pool — консьюмер с пулом per-key worker'ов и out-of-order
// offset tracker'ом.
//
// Задача — обогнать sequential по throughput, не нарушая порядок внутри
// одного ключа (это единственная гарантия, которую партиция Kafka даёт
// per-key). Решение — пул из N goroutine'ов; диспетчер кладёт каждый
// record в worker по hash(key) % N. Один и тот же ключ всегда у одного
// worker'а, разные ключи параллелятся между собой.
//
// Out-of-order proof: если worker[3] закончил offset=12 раньше, чем
// worker[1] закончил offset=10, коммитить 13 нельзя — иначе на рестарте
// мы потеряем offset=10. Tracker per-partition держит committed cursor
// `next` и set ещё-не-склеенных done offset'ов выше next'а. markDone
// двигает next по непрерывной полосе. CommitOffsets дёргается тикером
// раз в commit-interval и забирает текущий snapshot.
//
// Запуск: см. Makefile (run-pool).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic          = "lecture-03-05-events"
	defaultGroup          = "lecture-03-05-pool"
	defaultWorkers        = 8
	defaultWorkDelay      = 10 * time.Millisecond
	defaultBufferPerWorker = 1024
	defaultCommitInterval = 500 * time.Millisecond
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "входной топик")
	group := flag.String("group", defaultGroup, "group.id")
	workers := flag.Int("workers", defaultWorkers, "число worker-горутин в пуле")
	workDelay := flag.Duration("work-delay", defaultWorkDelay, "имитация работы — sleep на каждое сообщение")
	bufferSize := flag.Int("buffer", defaultBufferPerWorker, "размер канала per-worker")
	commitInterval := flag.Duration("commit-interval", defaultCommitInterval, "период async-коммита tracker snapshot")
	fromStart := flag.Bool("from-start", true, "при первом старте группы читать с earliest")
	flag.Parse()

	if *workers < 1 {
		fmt.Fprintln(os.Stderr, "workers должно быть >= 1")
		os.Exit(2)
	}
	if *commitInterval <= 0 {
		fmt.Fprintln(os.Stderr, "commit-interval должно быть > 0")
		os.Exit(2)
	}

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topic:          *topic,
		group:          *group,
		workers:        *workers,
		workDelay:      *workDelay,
		bufferSize:     *bufferSize,
		commitInterval: *commitInterval,
		fromStart:      *fromStart,
	}); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("concurrent-pool failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic          string
	group          string
	workers        int
	workDelay      time.Duration
	bufferSize     int
	commitInterval time.Duration
	fromStart      bool
}

// topicPartition — ключ tracker'а. Записываем topic явно: tracker
// общий по всем подписанным топикам, хоть в этой лекции он один.
type topicPartition struct {
	topic     string
	partition int32
}

// partTracker — per-partition состояние:
//   - next  — committed cursor; offset, который мы вернёмся читать
//     после рестарта (и пока что НЕ обработан);
//   - done  — set уже обработанных offset'ов выше next'а (out-of-order);
//   - epoch — leader epoch последнего виденного record'а в этой партиции;
//     нужен для CommitOffsets, иначе fence на смену лидера не отработает.
type partTracker struct {
	next  int64
	done  map[int64]struct{}
	epoch int32
}

type tracker struct {
	mu    sync.Mutex
	parts map[topicPartition]*partTracker
}

func newTracker() *tracker {
	return &tracker{parts: make(map[topicPartition]*partTracker)}
}

// observe — увидели offset с диска до обработки. Lazy-init tracker'а
// на случай первого record'а в партиции; обновляем эпоху.
func (t *tracker) observe(r *kgo.Record) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tp := topicPartition{r.Topic, r.Partition}
	pt, ok := t.parts[tp]
	if !ok {
		pt = &partTracker{
			next: r.Offset,
			done: make(map[int64]struct{}),
		}
		t.parts[tp] = pt
	}
	pt.epoch = r.LeaderEpoch
}

// markDone — worker отчитался об обработке. Двигаем next по непрерывной
// полосе done-offset'ов: 10,11,12 готовы → next прыгает с 10 до 13.
func (t *tracker) markDone(topic string, partition int32, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tp := topicPartition{topic, partition}
	pt, ok := t.parts[tp]
	if !ok {
		return
	}
	pt.done[offset] = struct{}{}
	for {
		if _, ok := pt.done[pt.next]; !ok {
			break
		}
		delete(pt.done, pt.next)
		pt.next++
	}
}

// snapshot — текущий committed cursor по каждой партиции.
// Формат — то, что ждёт kgo.Client.CommitOffsets.
func (t *tracker) snapshot() map[string]map[int32]kgo.EpochOffset {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]map[int32]kgo.EpochOffset, len(t.parts))
	for tp, pt := range t.parts {
		topic := out[tp.topic]
		if topic == nil {
			topic = make(map[int32]kgo.EpochOffset)
			out[tp.topic] = topic
		}
		topic[tp.partition] = kgo.EpochOffset{Epoch: pt.epoch, Offset: pt.next}
	}
	return out
}

// drop — партиция уехала на ребалансе; сбрасываем её состояние, чтобы
// при возврате не путаться со старым next'ом.
func (t *tracker) drop(topic string, partition int32) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.parts, topicPartition{topic, partition})
}

func run(ctx context.Context, o runOpts) error {
	tr := newTracker()
	var processed atomic.Int64

	opts := []kgo.Opt{
		kgo.ConsumerGroup(o.group),
		kgo.ConsumeTopics(o.topic),
		kgo.DisableAutoCommit(),
		kgo.ClientID("lecture-03-05-pool"),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			fmt.Fprintf(os.Stderr, "assigned: %v\n", m)
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			fmt.Fprintf(os.Stderr, "revoked: %v\n", m)
			for topic, ps := range m {
				for _, p := range ps {
					tr.drop(topic, p)
				}
			}
		}),
	}
	if o.fromStart {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	cl, err := kafka.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("concurrent-pool запущен: topic=%q group=%q workers=%d work-delay=%s commit-interval=%s\n",
		o.topic, o.group, o.workers, o.workDelay, o.commitInterval)
	fmt.Println("hash(key) % workers — каждый ключ всегда у одного worker'а; out-of-order tracker склеивает offset'ы.")
	fmt.Println("Ctrl+C — выход.")
	fmt.Println()

	// per-worker channels — диспетчер раскладывает по hash(key).
	channels := make([]chan *kgo.Record, o.workers)
	for i := range channels {
		channels[i] = make(chan *kgo.Record, o.bufferSize)
	}

	var wg sync.WaitGroup
	for i := 0; i < o.workers; i++ {
		wg.Add(1)
		go func(id int, ch <-chan *kgo.Record) {
			defer wg.Done()
			for r := range ch {
				time.Sleep(o.workDelay)
				tr.markDone(r.Topic, r.Partition, r.Offset)
				processed.Add(1)
			}
		}(i, channels[i])
	}

	// commit-loop: раз в commit-interval берёт snapshot tracker'а
	// и шлёт CommitOffsets. Async — то есть мы не ждём подтверждения
	// в цикле обработки; ошибка коммита просто залогируется.
	commitCtx, commitCancel := context.WithCancel(context.Background())
	var commitWg sync.WaitGroup
	commitWg.Add(1)
	go func() {
		defer commitWg.Done()
		ticker := time.NewTicker(o.commitInterval)
		defer ticker.Stop()
		for {
			select {
			case <-commitCtx.Done():
				return
			case <-ticker.C:
				commitSnapshot(commitCtx, cl, tr)
			}
		}
	}()

	// throughput-репортер раз в секунду.
	statsCtx, statsCancel := context.WithCancel(context.Background())
	var statsWg sync.WaitGroup
	statsWg.Add(1)
	go func() {
		defer statsWg.Done()
		reportThroughput(statsCtx, &processed)
	}()

	// poll-loop: получили fetches → observe + dispatch в worker.
	// Когда ctx отменяется, выходим из poll, закрываем worker-каналы,
	// ждём drain, делаем финальный sync-commit.
	start := time.Now()
	pollErr := pollLoop(ctx, cl, tr, channels)

	// shutdown sequence: нет новых записей в каналы → ждём workers
	// → останавливаем тикер commit'ов → финальный sync-commit.
	for _, ch := range channels {
		close(ch)
	}
	wg.Wait()

	commitCancel()
	commitWg.Wait()

	statsCancel()
	statsWg.Wait()

	finalCtx, finalCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer finalCancel()
	commitSnapshot(finalCtx, cl, tr)

	printSummary(processed.Load(), time.Since(start))
	if pollErr != nil && !errors.Is(pollErr, context.Canceled) {
		return pollErr
	}
	return nil
}

// pollLoop читает fetches и раскладывает по worker-каналам. Возврат —
// либо ошибка, либо nil после ctx.Done.
func pollLoop(
	ctx context.Context,
	cl *kgo.Client,
	tr *tracker,
	channels []chan *kgo.Record,
) error {
	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			tr.observe(r)
			idx := workerFor(r.Key, len(channels))
			select {
			case <-ctx.Done():
				return
			case channels[idx] <- r:
			}
		})
		if ctx.Err() != nil {
			return nil
		}
	}
}

// workerFor — стабильный fnv32a-хеш по ключу, mod N. Пустой ключ всегда
// уходит в worker 0; per-key ordering для безключевых записей не нужен.
func workerFor(key []byte, n int) int {
	if len(key) == 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(n))
}

func commitSnapshot(ctx context.Context, cl *kgo.Client, tr *tracker) {
	snap := tr.snapshot()
	if len(snap) == 0 {
		return
	}
	cl.CommitOffsetsSync(ctx, snap, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		if err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "commit error: %v\n", err)
		}
	})
}

func reportThroughput(ctx context.Context, processed *atomic.Int64) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	var prev int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cur := processed.Load()
			delta := cur - prev
			prev = cur
			fmt.Printf("[pool] processed=%d  rate=%d msg/sec\n", cur, delta)
		}
	}
}

func printSummary(processed int64, elapsed time.Duration) {
	rate := float64(0)
	if elapsed.Seconds() > 0 {
		rate = float64(processed) / elapsed.Seconds()
	}
	fmt.Printf("\nостановлен по сигналу. processed=%d elapsed=%s avg=%.0f msg/sec\n",
		processed, elapsed.Truncate(time.Millisecond), rate)
}
