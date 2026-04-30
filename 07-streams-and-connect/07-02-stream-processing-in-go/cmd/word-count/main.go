// word-count — stateful word-count к лекции 07-02.
//
// Базовая идея:
//   1. Читаем `text-events` (любая короткая строка-событие).
//   2. Бьём value на слова (lower-case, только латиница/кириллица/цифры).
//   3. Инкрементим per-слово счётчик в локальном Pebble store.
//   4. На каждое изменение пишем в `word-count-changelog` запись
//      (key=word, value=binary uint64 в текущем значении).
//   5. Каждые `flush` секунд эмитим текущий top-N в `word-counts`
//      (key=word, value=`{count,total_seen,...}`).
//
// Pebble — embedded LSM, аналог RocksDB. После kill -9 state остаётся
// на диске; чтобы продемонстрировать восстановление с нуля, удаляем
// директорию `state` и запускаем `cmd/changelog-restorer`. Restorer
// читает changelog с beginning, складывает в Pebble — после этого
// word-count стартует и продолжает с того же state.
//
// Зачем changelog отдельно от Pebble. Pebble живёт на одной машине;
// changelog — это compacted-топик в Kafka, репликация и долговечность
// у него такие же, как у любого другого топика. В Kafka Streams эта
// схема называется state store + changelog topic. Тут она руками.
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/cockroachdb/pebble"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultInputTopic     = "lecture-07-02-text-events"
	defaultChangelogTopic = "lecture-07-02-word-count-changelog"
	defaultOutputTopic    = "lecture-07-02-word-counts"
	defaultGroup          = "lecture-07-02-word-count"
	defaultStateDir       = "./state"
	defaultPartitions     = 3
	defaultReplication    = 3
	defaultFlushInterval  = 5 * time.Second
	defaultTopN           = 10
)

func main() {
	logger := log.New()

	inputTopic := flag.String("input", defaultInputTopic, "входной топик с событиями")
	changelogTopic := flag.String("changelog", defaultChangelogTopic, "compacted-топик с changelog state'а")
	outputTopic := flag.String("output", defaultOutputTopic, "топик с периодическим top-N снэпшотом")
	group := flag.String("group", defaultGroup, "consumer group")
	stateDir := flag.String("state", defaultStateDir, "директория Pebble state'а")
	flushInterval := flag.Duration("flush", defaultFlushInterval, "период публикации top-N в output")
	topN := flag.Int("top-n", defaultTopN, "сколько слов класть в snapshot")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании топиков")
	rf := flag.Int("rf", defaultReplication, "replication factor при создании топиков")
	resetState := flag.Bool("reset-state", false, "удалить директорию state перед стартом")

	flag.Parse()

	if *topN < 1 {
		fmt.Fprintln(os.Stderr, "top-n должно быть >= 1")
		os.Exit(2)
	}
	if *flushInterval <= 0 {
		fmt.Fprintln(os.Stderr, "flush должно быть > 0")
		os.Exit(2)
	}

	rootCtx, cancel := runctx.New()
	defer cancel()

	if *resetState {
		if err := os.RemoveAll(*stateDir); err != nil {
			logger.Error("reset state failed", "err", err)
			os.Exit(1)
		}
		fmt.Printf("state cleared: %s\n", *stateDir)
	}

	if err := ensureTopics(rootCtx, *inputTopic, *changelogTopic, *outputTopic, int32(*partitions), int16(*rf)); err != nil {
		logger.Error("ensure topics failed", "err", err)
		os.Exit(1)
	}

	store, err := pebble.Open(*stateDir, &pebble.Options{})
	if err != nil {
		logger.Error("pebble open failed", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	cl, err := kafka.NewClient(
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*inputTopic),
		// AtStart — чтобы при первом запуске на чистой группе подхватить
		// весь поток. Если консьюмер уже коммитил, franz-go продолжит
		// с committed offset; политика касается только новой группы.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		logger.Error("kafka client failed", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	wc := &wordCounter{
		store:          store,
		client:         cl,
		changelogTopic: *changelogTopic,
		outputTopic:    *outputTopic,
		topN:           *topN,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		wc.flushLoop(rootCtx, *flushInterval)
	}()

	fmt.Printf("word-count started: input=%q changelog=%q output=%q state=%q\n",
		*inputTopic, *changelogTopic, *outputTopic, *stateDir)

	if err := wc.run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("word-count failed", "err", err)
		cancel()
		wg.Wait()
		os.Exit(1)
	}
	cancel()
	wg.Wait()
}

func ensureTopics(ctx context.Context, input, changelog, output string, partitions int32, rf int16) error {
	admin, err := kafka.NewAdmin()
	if err != nil {
		return err
	}
	defer admin.Close()

	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	create := func(name string, configs map[string]*string) error {
		resp, err := admin.CreateTopic(rpcCtx, partitions, rf, configs, name)
		if err == nil && resp.Err == nil {
			fmt.Printf("topic %q создан: partitions=%d rf=%d\n", name, partitions, rf)
			return nil
		}
		cause := err
		if cause == nil {
			cause = resp.Err
		}
		if errors.Is(cause, kerr.TopicAlreadyExists) {
			return nil
		}
		return fmt.Errorf("create %q: %w", name, cause)
	}

	if err := create(input, nil); err != nil {
		return err
	}
	compact := "compact"
	if err := create(changelog, map[string]*string{"cleanup.policy": &compact}); err != nil {
		return err
	}
	if err := create(output, nil); err != nil {
		return err
	}
	return nil
}

type wordCounter struct {
	store          *pebble.DB
	client         *kgo.Client
	changelogTopic string
	outputTopic    string
	topN           int

	processed uint64
	mu        sync.Mutex
}

func (w *wordCounter) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		fetches := w.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return nil
				}
				fmt.Fprintf(os.Stderr, "fetch err on %s/%d: %v\n", e.Topic, e.Partition, e.Err)
			}
		}

		var produces []*kgo.Record
		// overlay — per-batch счётчики, накапливаются в памяти
		// БЕЗ записи в Pebble. Так слово, встретившееся в батче дважды,
		// получит cur+1, потом cur+2, как и положено.
		overlay := make(map[string]uint64)
		var procErr error
		var processedRecords uint64
		fetches.EachRecord(func(rec *kgo.Record) {
			if procErr != nil {
				return
			}
			words := tokenize(string(rec.Value))
			for _, word := range words {
				cur, ok := overlay[word]
				if !ok {
					stored, err := readUint64(w.store, []byte(word))
					if err != nil {
						procErr = fmt.Errorf("read %q: %w", word, err)
						return
					}
					cur = stored
				}
				cur++
				overlay[word] = cur
				produces = append(produces, &kgo.Record{
					Topic: w.changelogTopic,
					Key:   []byte(word),
					Value: encodeUint64(cur),
				})
			}
			processedRecords++
		})
		if procErr != nil {
			return procErr
		}

		if len(produces) == 0 {
			continue
		}

		// Порядок строгий: changelog → Pebble → commit offset.
		// Это нужно, чтобы changelog действительно был источником истины:
		// если упадём между changelog'ом и pebble, после рестарта
		// changelog-restorer перенесёт state и значения совпадут с тем,
		// что уже опубликовано. Если бы Pebble писали ДО changelog'а,
		// state ушёл бы вперёд — на рестарте changelog лагал бы.
		// Семантика всё равно at-least-once: между Pebble и commit
		// процесс может упасть, тогда те же записи приедут снова
		// и счётчики раздуются на батч. EOS дала бы лекция 04-02,
		// тут она увела бы в сторону от темы state stores.
		rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := w.client.ProduceSync(rpcCtx, produces...).FirstErr()
		cancel()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("changelog produce: %w", err)
		}

		batch := w.store.NewBatch()
		for word, count := range overlay {
			if err := batch.Set([]byte(word), encodeUint64(count), nil); err != nil {
				_ = batch.Close()
				return fmt.Errorf("pebble batch set: %w", err)
			}
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			_ = batch.Close()
			return fmt.Errorf("pebble batch commit: %w", err)
		}
		if err := batch.Close(); err != nil {
			return fmt.Errorf("pebble batch close: %w", err)
		}

		w.mu.Lock()
		w.processed += processedRecords
		w.mu.Unlock()

		commitCtx, commitCancel := context.WithTimeout(ctx, 10*time.Second)
		err = w.client.CommitUncommittedOffsets(commitCtx)
		commitCancel()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("commit offsets: %w", err)
		}
	}
}

func (w *wordCounter) flushLoop(ctx context.Context, interval time.Duration) {
	tk := time.NewTicker(interval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			if err := w.flushTopN(ctx); err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintf(os.Stderr, "flush top-N: %v\n", err)
			}
		}
	}
}

type wordCount struct {
	word  string
	count uint64
}

func (w *wordCounter) flushTopN(ctx context.Context) error {
	rows, err := w.collectAll()
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].count == rows[j].count {
			return rows[i].word < rows[j].word
		}
		return rows[i].count > rows[j].count
	})
	if len(rows) > w.topN {
		rows = rows[:w.topN]
	}

	w.mu.Lock()
	processed := w.processed
	w.mu.Unlock()

	now := time.Now().Format("15:04:05")
	fmt.Printf("\n[%s] processed=%d top-%d:\n", now, processed, len(rows))
	for i, r := range rows {
		fmt.Printf("  %2d. %-20s %d\n", i+1, r.word, r.count)
	}

	records := make([]*kgo.Record, 0, len(rows))
	for _, r := range rows {
		records = append(records, &kgo.Record{
			Topic: w.outputTopic,
			Key:   []byte(r.word),
			Value: encodeUint64(r.count),
		})
	}
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return w.client.ProduceSync(rpcCtx, records...).FirstErr()
}

func (w *wordCounter) collectAll() ([]wordCount, error) {
	iter, err := w.store.NewIter(nil)
	if err != nil {
		return nil, fmt.Errorf("pebble iter: %w", err)
	}
	defer iter.Close()
	var out []wordCount
	for iter.First(); iter.Valid(); iter.Next() {
		count, ok := decodeUint64(iter.Value())
		if !ok {
			continue
		}
		out = append(out, wordCount{
			word:  string(append([]byte(nil), iter.Key()...)),
			count: count,
		})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

func tokenize(s string) []string {
	s = strings.ToLower(s)
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	out := fields[:0]
	for _, f := range fields {
		if len(f) >= 2 {
			out = append(out, f)
		}
	}
	return out
}

func encodeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func decodeUint64(b []byte) (uint64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(b), true
}

func readUint64(store *pebble.DB, key []byte) (uint64, error) {
	val, closer, err := store.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("pebble get: %w", err)
	}
	defer closer.Close()
	v, ok := decodeUint64(val)
	if !ok {
		return 0, fmt.Errorf("pebble value for key %q has unexpected length %d", key, len(val))
	}
	return v, nil
}
