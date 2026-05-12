package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
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
	defaultTopic       = "lecture-02-01-custom"
	defaultPartitions  = 3
	defaultReplication = 3
	defaultMessages    = 1000
	defaultPremiumPct  = 30

	premiumPrefix = "prem-"
	premiumPart   = 0
	rrFirst       = 1
	rrSecond      = 2
)

type premiumPartitioner struct{}

func (premiumPartitioner) ForTopic(string) kgo.TopicPartitioner {
	return &premiumTopicPartitioner{}
}

type premiumTopicPartitioner struct {
	rr int
}

func (p *premiumTopicPartitioner) RequiresConsistency(r *kgo.Record) bool {
	return bytes.HasPrefix(r.Key, []byte(premiumPrefix))
}

func (p *premiumTopicPartitioner) Partition(r *kgo.Record, n int) int {
	if n <= 0 {
		return 0
	}
	if bytes.HasPrefix(r.Key, []byte(premiumPrefix)) {
		if premiumPart < n {
			return premiumPart
		}
		return 0
	}
	first := rrFirst
	second := rrSecond
	if first >= n {
		first = n - 1
	}
	if second >= n {
		second = n - 1
	}
	if first == second {
		return first
	}
	choice := first
	if p.rr%2 == 1 {
		choice = second
	}
	p.rr++
	return choice
}

func main() {
	logger := log.New()

	topic := flag.String("topic", defaultTopic, "топик")
	partitions := flag.Int("partitions", defaultPartitions, "число партиций при создании")
	rf := flag.Int("rf", defaultReplication, "replication factor при создании")
	messages := flag.Int("messages", defaultMessages, "сколько сообщений записать")
	premiumPct := flag.Int("premium-pct", defaultPremiumPct, "процент премиум-ключей в потоке (0..100)")
	flag.Parse()

	if *premiumPct < 0 || *premiumPct > 100 {
		fmt.Fprintln(os.Stderr, "premium-pct должен быть в диапазоне 0..100")
		os.Exit(2)
	}

	rootCtx, cancel := runctx.New()
	defer cancel()

	if err := run(rootCtx, runOpts{
		topic:      *topic,
		partitions: int32(*partitions),
		rf:         int16(*rf),
		messages:   *messages,
		premiumPct: *premiumPct,
	}); err != nil {
		logger.Error("custom-partitioner failed", "err", err)
		os.Exit(1)
	}
}

type runOpts struct {
	topic      string
	partitions int32
	rf         int16
	messages   int
	premiumPct int
}

func run(ctx context.Context, o runOpts) error {
	cl, err := kafka.NewClient(
		kgo.RecordPartitioner(premiumPartitioner{}),
	)
	if err != nil {
		return fmt.Errorf("kafka.NewClient: %w", err)
	}
	defer cl.Close()
	admin := kadm.NewClient(cl)

	if err := ensureTopic(ctx, admin, o); err != nil {
		return fmt.Errorf("ensure topic: %w", err)
	}

	fmt.Printf("пишем %d сообщений в топик %q (%d партиций); премиум-доля ≈ %d%%\n\n",
		o.messages, o.topic, o.partitions, o.premiumPct)

	type stats struct {
		premium  int
		regular  int
		perPart  map[int32]int
		premOnly map[int32]int
		regOnly  map[int32]int
	}
	s := stats{
		perPart:  make(map[int32]int),
		premOnly: make(map[int32]int),
		regOnly:  make(map[int32]int),
	}

	rng := rand.New(rand.NewPCG(1, 1))

	for i := 0; i < o.messages; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var key string
		isPremium := rng.IntN(100) < o.premiumPct
		if isPremium {
			key = fmt.Sprintf("%s%d", premiumPrefix, rng.IntN(50))
		} else {
			key = fmt.Sprintf("reg-%d", rng.IntN(500))
		}
		val := fmt.Sprintf("order-%d", i)
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
		p := res[0].Record.Partition
		s.perPart[p]++
		if isPremium {
			s.premium++
			s.premOnly[p]++
		} else {
			s.regular++
			s.regOnly[p]++
		}
	}

	fmt.Printf("итого: premium=%d, regular=%d\n\n", s.premium, s.regular)

	fmt.Println("распределение по партициям:")
	printSplit(s.perPart, s.premOnly, s.regOnly, o.partitions)

	fmt.Println()
	fmt.Println("проверки:")
	checkInvariants(s.premOnly, s.regOnly, o.partitions)

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

func printSplit(total, prem, reg map[int32]int, partitions int32) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PARTITION\tTOTAL\tPREMIUM\tREGULAR\tNOTE")
	for p := int32(0); p < partitions; p++ {
		note := ""
		switch p {
		case premiumPart:
			note = "premium-only ожидаем"
		case rrFirst, rrSecond:
			note = "round-robin для regular"
		}
		fmt.Fprintf(tw, "%d\t%d\t%d\t%d\t%s\n", p, total[p], prem[p], reg[p], note)
	}
	_ = tw.Flush()
}

func checkInvariants(prem, reg map[int32]int, partitions int32) {
	premOK := true
	for p := int32(0); p < partitions; p++ {
		if p == premiumPart {
			continue
		}
		if prem[p] > 0 {
			premOK = false
			fmt.Printf("  ✗ премиум-запись попала в партицию %d (ожидали только в %d)\n", p, premiumPart)
		}
	}
	if premOK {
		fmt.Printf("  ✓ все премиум-записи лежат в партиции %d\n", premiumPart)
	}

	if reg[premiumPart] > 0 {
		fmt.Printf("  ✗ regular-запись попала в премиум-партицию %d\n", premiumPart)
	} else {
		fmt.Printf("  ✓ regular-записи не зашли в премиум-партицию %d\n", premiumPart)
	}

	if reg[rrFirst]+reg[rrSecond] == 0 {
		fmt.Printf("  · regular-записей в потоке не было — балансировку round-robin'а проверять нечем\n")
		return
	}
	skew := abs(reg[rrFirst] - reg[rrSecond])
	bound := (reg[rrFirst] + reg[rrSecond]) / 20
	if bound < 5 {
		bound = 5
	}
	if skew <= bound {
		fmt.Printf("  ✓ round-robin сбалансирован: P%d=%d, P%d=%d (skew=%d ≤ %d)\n",
			rrFirst, reg[rrFirst], rrSecond, reg[rrSecond], skew, bound)
	} else {
		fmt.Printf("  ! round-robin даёт перекос: P%d=%d, P%d=%d (skew=%d > %d)\n",
			rrFirst, reg[rrFirst], rrSecond, reg[rrSecond], skew, bound)
	}
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func printEndOffsets(ctx context.Context, admin *kadm.Client, topic string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	ends, err := admin.ListEndOffsets(rpcCtx, topic)
	if err != nil {
		return fmt.Errorf("ListEndOffsets: %w", err)
	}
	type row struct {
		part int32
		end  int64
	}
	var rows []row
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			return
		}
		rows = append(rows, row{part: o.Partition, end: o.Offset})
	})
	sort.Slice(rows, func(i, j int) bool { return rows[i].part < rows[j].part })

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PARTITION\tLATEST")
	var total int64
	for _, r := range rows {
		fmt.Fprintf(tw, "%d\t%d\n", r.part, r.end)
		total += r.end
	}
	fmt.Fprintf(tw, "TOTAL\t%d\n", total)
	return tw.Flush()
}
