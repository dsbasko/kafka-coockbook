// keyed-producer пишет 1000 сообщений в топик с ключом userID%10 и в конце
// показывает распределение записей по партициям и список ключей в каждой
// партиции. Видно две вещи сразу: записи разъезжаются по партициям не
// случайно, а через хэш ключа; и один и тот же ключ всегда летит в одну и
// ту же партицию.
//
// Топик создаётся идемпотентно с -partitions=3. Запись через ProduceSync,
// чтобы получить (partition, offset) сразу — это удобно для аналитики в
// конце. Дефолтный partitioner в franz-go — UniformBytesPartitioner, он же
// «sticky»: для записей с ключом он хеширует ключ через murmur2 и берёт
// результат по модулю числа партиций. Это та же формула, что в Java-клиенте
// Kafka по умолчанию.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
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
	defaultTopic       = "lecture-02-01-keyed"
	defaultPartitions  = 3
	defaultReplication = 3
	defaultMessages    = 1000
	defaultUsers       = 10
)

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик, в который пишем")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании")
	rf := flag.Int("rf", defaultReplication, "replication factor при создании")
	messages := flag.Int("messages", defaultMessages, "сколько сообщений записать")
	users := flag.Int("users", defaultUsers, "сколько уникальных userID — ключ берётся как user-{i % users}")
	flag.Parse()

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topic:      *topic,
		partitions: int32(*partitions),
		rf:         int16(*rf),
		messages:   *messages,
		users:      *users,
	}); err != nil {
		logger.Error("keyed-producer failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic      string
	partitions int32
	rf         int16
	messages   int
	users      int
}

func run(ctx context.Context, o runOpts) error {
	cl, err := kafka.NewClient()
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)

	if err := ensureTopic(ctx, admin, o); err != nil {
		return fmt.Errorf("ensure topic: %w", err)
	}

	fmt.Printf("пишем %d сообщений с ключами user-0..user-%d в топик %q (%d партиций)\n\n",
		o.messages, o.users-1, o.topic, o.partitions)

	// keyToPartitions: какой ключ в какие партиции попал. На корректно работающем
	// дефолтном partitioner'е у каждого ключа партиция должна быть одна.
	keyToPartitions := make(map[string]map[int32]int)
	partitionCount := make(map[int32]int)

	for i := 0; i < o.messages; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := fmt.Sprintf("user-%d", i%o.users)
		val := fmt.Sprintf("event-%d", i)
		rec := &kgo.Record{
			Topic: o.topic,
			Key:   []byte(key),
			Value: []byte(val),
		}

		rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
		res := cl.ProduceSync(rpcCtx, rec)
		rpcCancel()
		if err := res.FirstErr(); err != nil {
			return fmt.Errorf("produce %d: %w", i, err)
		}
		got := res[0].Record

		if _, ok := keyToPartitions[key]; !ok {
			keyToPartitions[key] = make(map[int32]int)
		}
		keyToPartitions[key][got.Partition]++
		partitionCount[got.Partition]++
	}

	fmt.Println("распределение по партициям:")
	printPartitionTable(partitionCount, o.partitions)

	fmt.Println()
	fmt.Println("в какую партицию ложился каждый ключ:")
	printKeyTable(keyToPartitions)

	fmt.Println()
	fmt.Println("сверка с end offsets из лога:")
	if err := printEndOffsets(ctx, admin, o.topic); err != nil {
		return fmt.Errorf("print end offsets: %w", err)
	}
	return nil
}

func ensureTopic(ctx context.Context, admin *kadm.Client, o runOpts) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	resp, err := admin.CreateTopic(rpcCtx, o.partitions, o.rf, nil, o.topic)
	if err == nil && resp.Err == nil {
		fmt.Printf("topic %q создан: partitions=%d rf=%d\n\n", o.topic, o.partitions, o.rf)
		return nil
	}
	cause := err
	if cause == nil {
		cause = resp.Err
	}
	if errors.Is(cause, kerr.TopicAlreadyExists) {
		fmt.Printf("topic %q уже существует — пишем как есть\n\n", o.topic)
		return nil
	}
	return cause
}

func printPartitionTable(counts map[int32]int, partitions int32) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PARTITION\tCOUNT")
	for p := int32(0); p < partitions; p++ {
		fmt.Fprintf(tw, "%d\t%d\n", p, counts[p])
	}
	_ = tw.Flush()
}

func printKeyTable(keyToPartitions map[string]map[int32]int) {
	keys := make([]string, 0, len(keyToPartitions))
	for k := range keyToPartitions {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "KEY\tPARTITION\tCOUNT\tNOTE")
	for _, k := range keys {
		parts := keyToPartitions[k]
		switch len(parts) {
		case 1:
			for p, c := range parts {
				fmt.Fprintf(tw, "%s\t%d\t%d\tone-key-one-partition ✓\n", k, p, c)
			}
		default:
			for p, c := range parts {
				fmt.Fprintf(tw, "%s\t%d\t%d\t!! ключ разъехался по партициям\n", k, p, c)
			}
		}
	}
	_ = tw.Flush()
}

func printEndOffsets(ctx context.Context, admin *kadm.Client, topic string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ends, err := admin.ListEndOffsets(rpcCtx, topic)
	if err != nil {
		return fmt.Errorf("ListEndOffsets: %w", err)
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PARTITION\tLATEST")
	var total int64
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			return
		}
		fmt.Fprintf(tw, "%d\t%d\n", o.Partition, o.Offset)
		total += o.Offset
	})
	fmt.Fprintf(tw, "TOTAL\t%d\n", total)
	return tw.Flush()
}
