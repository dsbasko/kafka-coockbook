// inspect — создаёт топик `lecture-01-02-orders` с 3 партициями и
// печатает per-partition картину: leader, replicas, ISR.
//
// По умолчанию топик создаётся идемпотентно — если он уже есть с тем же
// числом партиций, считаем это успехом и просто описываем. Replication factor
// фиксируем в 3 (сколько нод в стенде, столько и реплик), чтобы лекция
// иллюстрировала RF=3 и соответствовала default'у docker-compose.yml.
//
// Через флаг -recreate=true можно сначала удалить топик и пересоздать заново —
// удобно, чтобы посмотреть, как меняется ассайнмент leader'ов.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultTopic       = "lecture-01-02-orders"
	defaultPartitions  = 3
	defaultReplication = 3
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик, который создаём и описываем")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании")
	rf := flag.Int("rf", defaultReplication, "replication factor при создании")
	recreate := flag.Bool("recreate", false, "удалить топик перед созданием")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, *topic, int32(*partitions), int16(*rf), *recreate); err != nil {
		logger.Error("inspect failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, topic string, partitions int32, rf int16, recreate bool) error {
	admin, err := kafka.NewAdmin()
	if err != nil {
		return fmt.Errorf("kafka.NewAdmin: %w", err)
	}
	defer admin.Close()

	rpcCtx, rpcCancel := context.WithTimeout(ctx, 15*time.Second)
	defer rpcCancel()

	if recreate {
		if err := deleteTopic(rpcCtx, admin, topic); err != nil {
			return fmt.Errorf("delete topic: %w", err)
		}
		fmt.Printf("topic %q удалён\n", topic)
	}

	created, err := ensureTopic(rpcCtx, admin, topic, partitions, rf)
	if err != nil {
		return fmt.Errorf("ensure topic: %w", err)
	}
	if created {
		fmt.Printf("topic %q создан: partitions=%d rf=%d\n", topic, partitions, rf)
	} else {
		fmt.Printf("topic %q уже существует — описываем\n", topic)
	}

	details, err := admin.ListTopics(rpcCtx, topic)
	if err != nil {
		return fmt.Errorf("ListTopics: %w", err)
	}
	td, ok := details[topic]
	if !ok {
		return fmt.Errorf("topic %q отсутствует в metadata-ответе", topic)
	}
	if td.Err != nil {
		return fmt.Errorf("topic %q load error: %w", topic, td.Err)
	}

	printTopic(td)
	return nil
}

// ensureTopic пытается создать топик. Если топик уже есть с любой
// конфигурацией — возвращает (false, nil); число партиций и rf проверяет
// describe ниже. Это нужно, чтобы make run был идемпотентным и не падал на
// втором запуске.
func ensureTopic(ctx context.Context, admin *kadm.Client, topic string, partitions int32, rf int16) (bool, error) {
	resp, err := admin.CreateTopic(ctx, partitions, rf, nil, topic)
	if err == nil && resp.Err == nil {
		return true, nil
	}

	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if errors.Is(cause, kerr.TopicAlreadyExists) {
		return false, nil
	}
	return false, cause
}

// deleteTopic тихо игнорирует ErrUnknownTopicOrPartition — для recreate-режима
// это не ошибка, а штатная ситуация (топика просто не было).
func deleteTopic(ctx context.Context, admin *kadm.Client, topic string) error {
	resp, err := admin.DeleteTopic(ctx, topic)
	if err == nil && resp.Err == nil {
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if errors.Is(cause, kerr.UnknownTopicOrPartition) {
		return nil
	}
	return cause
}

func printTopic(td kadm.TopicDetail) {
	fmt.Printf("\nTopic:       %s\n", td.Topic)
	fmt.Printf("TopicID:     %s\n", td.ID)
	fmt.Printf("Partitions:  %d\n", len(td.Partitions))
	fmt.Println()

	parts := td.Partitions.Sorted()

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PARTITION\tLEADER\tREPLICAS\tISR\tOFFLINE")
	for _, p := range parts {
		offline := fmt.Sprintf("%v", p.OfflineReplicas)
		if len(p.OfflineReplicas) == 0 {
			offline = "-"
		}
		fmt.Fprintf(tw, "%d\t%d\t%v\t%v\t%s\n", p.Partition, p.Leader, p.Replicas, p.ISR, offline)
	}
	_ = tw.Flush()
}
