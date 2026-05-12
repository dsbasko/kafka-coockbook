package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultDLQ      = "payments-dlq"
	defaultGroup    = "lecture-04-04-dlq"
	defaultLogPath  = "/tmp/lecture-04-04-incidents.jsonl"
	defaultClientID = "lecture-04-04-dlq-processor"
)

type incident struct {
	DLQTopic         string `json:"dlq_topic"`
	DLQPartition     int32  `json:"dlq_partition"`
	DLQOffset        int64  `json:"dlq_offset"`
	Key              string `json:"key,omitempty"`
	OriginalTopic    string `json:"original_topic,omitempty"`
	OriginalPart     string `json:"original_partition,omitempty"`
	OriginalOffset   string `json:"original_offset,omitempty"`
	PreviousTopic    string `json:"previous_topic,omitempty"`
	RetryCount       string `json:"retry_count,omitempty"`
	ErrorClass       string `json:"error_class,omitempty"`
	ErrorMessage     string `json:"error_message,omitempty"`
	ErrorTimestamp   string `json:"error_timestamp,omitempty"`
	DLQRecordTime    string `json:"dlq_record_time"`
	PayloadByteCount int    `json:"payload_bytes"`
}

func main() {
	logger := log.New()

	dlq := flag.String("dlq", defaultDLQ, "DLQ топик")
	group := flag.String("group", defaultGroup, "group.id")
	logPath := flag.String("log-path", defaultLogPath, "файл incident-лога (append-only)")
	fromStart := flag.Bool("from-start", true, "при первом старте группы читать с earliest")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, *dlq, *group, *logPath, *fromStart); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("dlq-processor failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, dlq, group, logPath string, fromStart bool) error {
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open incident log %q: %w", logPath, err)
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	defer bw.Flush()

	opts := []kgo.Opt{
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(dlq),
		kgo.DisableAutoCommit(),
		kgo.ClientID(defaultClientID),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			fmt.Fprintf(os.Stderr, "assigned: %v\n", m)
		}),
	}
	if fromStart {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	cl, err := kafka.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()

	fmt.Printf("dlq-processor запущен: topic=%q group=%q log=%q\n", dlq, group, logPath)
	fmt.Println("читаем DLQ; на каждую запись — ALERT в stdout и строка в incident-лог.")
	fmt.Println("Ctrl+C — выход. Incident-лог пишется append-only.")
	fmt.Println()

	var seen atomic.Int64

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					fmt.Printf("\nостановлен по сигналу. seen=%d.\n", seen.Load())
					return nil
				}
				return fmt.Errorf("fetch %s/%d: %w", e.Topic, e.Partition, e.Err)
			}
		}

		batch := make([]*kgo.Record, 0)
		fetches.EachRecord(func(r *kgo.Record) { batch = append(batch, r) })
		if len(batch) == 0 {
			continue
		}

		for _, r := range batch {
			seen.Add(1)
			inc := buildIncident(r)
			printAlert(seen.Load(), r, inc)
			if err := writeIncident(bw, inc); err != nil {
				return fmt.Errorf("write incident log: %w", err)
			}
		}
		if err := bw.Flush(); err != nil {
			return fmt.Errorf("flush incident log: %w", err)
		}

		commitCtx, commitCancel := context.WithTimeout(ctx, 10*time.Second)
		err := cl.CommitRecords(commitCtx, batch...)
		commitCancel()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("CommitRecords: %w", err)
		}
	}
}

func buildIncident(r *kgo.Record) incident {
	idx := indexHeaders(r.Headers)
	return incident{
		DLQTopic:         r.Topic,
		DLQPartition:     r.Partition,
		DLQOffset:        r.Offset,
		Key:              string(r.Key),
		OriginalTopic:    idx["original.topic"],
		OriginalPart:     idx["original.partition"],
		OriginalOffset:   idx["original.offset"],
		PreviousTopic:    idx["previous.topic"],
		RetryCount:       idx["retry.count"],
		ErrorClass:       idx["error.class"],
		ErrorMessage:     idx["error.message"],
		ErrorTimestamp:   idx["error.timestamp"],
		DLQRecordTime:    r.Timestamp.UTC().Format(time.RFC3339Nano),
		PayloadByteCount: len(r.Value),
	}
}

func printAlert(n int64, r *kgo.Record, inc incident) {
	fmt.Printf("[ALERT] #%d  dlq=%s p=%d off=%d key=%s\n",
		n, r.Topic, r.Partition, r.Offset, string(r.Key))
	fmt.Printf("        original=%s/%s/%s previous=%s retries=%s\n",
		nz(inc.OriginalTopic), nz(inc.OriginalPart), nz(inc.OriginalOffset),
		nz(inc.PreviousTopic), nz(inc.RetryCount))
	fmt.Printf("        class=%s message=%q\n", nz(inc.ErrorClass), inc.ErrorMessage)
	fmt.Printf("        payload=%d bytes\n\n", inc.PayloadByteCount)
}

func writeIncident(w *bufio.Writer, inc incident) error {
	b, err := json.Marshal(inc)
	if err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	if err := w.WriteByte('\n'); err != nil {
		return err
	}
	return nil
}

func nz(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func indexHeaders(hs []kgo.RecordHeader) map[string]string {
	out := make(map[string]string, len(hs))
	for _, h := range hs {
		out[h.Key] = string(h.Value)
	}
	return out
}
