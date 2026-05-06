// under-replicated-watch — дашборд кластера в одном цикле. Каждые N секунд
// дёргает ListBrokers + ListTopics, считает under-replicated partitions и
// печатает summary: кто живой, сколько UR-партиций, какие топики «горят».
//
// Сценарий лекции — запустить watcher, потом в соседнем терминале
// `make kill-broker`. На первом тике видно, что брокер из metadata пропал
// и часть партиций просела (ISR < Replicas). После `make restore-broker` —
// восстанавливается обратно.
//
// Под капотом — обычный admin.ListTopics(...). UnderReplicatedPartitions
// в kadm нет как отдельной метрики; считаем сами как `len(ISR) < len(Replicas)`.
// Это та же формула, что у JMX-метрики `UnderReplicatedPartitions` на брокере,
// просто наблюдаем со стороны клиента.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/kafka"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/log"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/runctx"
)

const (
	defaultInterval = 3 * time.Second
)

func main() {
	logger := log.New()

	interval := flag.Duration("interval", defaultInterval, "пауза между тиками")
	topicGlob := flag.String("topics", "", "если задан, фильтруем по списку через запятую (lecture-08-04-hot,...)")
	once := flag.Bool("once", false, "сделать один тик и выйти")
	flag.Parse()

	ctx, cancel := runctx.New()
	defer cancel()

	topics := splitNonEmpty(*topicGlob)

	if err := run(ctx, *interval, topics, *once); err != nil {
		logger.Error("under-replicated-watch failed", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, interval time.Duration, topics []string, once bool) error {
	admin, err := kafka.NewAdmin()
	if err != nil {
		return fmt.Errorf("kafka.NewAdmin: %w", err)
	}
	defer admin.Close()

	fmt.Printf("watching cluster every %s (Ctrl+C to stop)\n", interval)
	if len(topics) > 0 {
		fmt.Printf("filtering topics: %v\n", topics)
	}
	fmt.Println()

	if err := tick(ctx, admin, topics); err != nil {
		fmt.Fprintf(os.Stderr, "tick failed: %v\n", err)
	}
	if once {
		return nil
	}

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := tick(ctx, admin, topics); err != nil {
				fmt.Fprintf(os.Stderr, "tick failed: %v\n", err)
			}
		}
	}
}

// tick — один проход опроса metadata. Запросы ListBrokers и ListTopics
// делаются с коротким timeout'ом, чтобы остановленный брокер не подвешивал
// весь watcher.
func tick(ctx context.Context, admin *kadm.Client, topics []string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	brokers, err := admin.ListBrokers(rpcCtx)
	if err != nil {
		return fmt.Errorf("ListBrokers: %w", err)
	}

	td, err := admin.ListTopics(rpcCtx, topics...)
	if err != nil {
		return fmt.Errorf("ListTopics: %w", err)
	}

	now := time.Now().Format("15:04:05")
	fmt.Printf("[%s]\n", now)
	printBrokers(brokers)
	printSummary(td)
	printUnderReplicated(td)
	fmt.Println(strings.Repeat("-", 60))
	return nil
}

func printBrokers(brokers kadm.BrokerDetails) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "BROKERS")
	fmt.Fprintln(tw, "NODE\tHOST\tPORT\tRACK")
	sorted := append(kadm.BrokerDetails(nil), brokers...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].NodeID < sorted[j].NodeID })
	for _, b := range sorted {
		rack := "—"
		if b.Rack != nil {
			rack = *b.Rack
		}
		fmt.Fprintf(tw, "%d\t%s\t%d\t%s\n", b.NodeID, b.Host, b.Port, rack)
	}
	_ = tw.Flush()
}

// printSummary — total topics, total partitions, total under-replicated.
// Это аналог трёх главных циферок на любом kafka-дашборде: сколько всего
// и сколько проблемных.
func printSummary(td kadm.TopicDetails) {
	var totalParts, urParts int
	for _, t := range td {
		if t.Err != nil {
			continue
		}
		for _, p := range t.Partitions {
			totalParts++
			if len(p.ISR) < len(p.Replicas) {
				urParts++
			}
		}
	}
	fmt.Printf("\nTOPICS=%d  PARTITIONS=%d  UNDER-REPLICATED=%d\n", len(td), totalParts, urParts)
}

func printUnderReplicated(td kadm.TopicDetails) {
	type row struct {
		topic     string
		partition int32
		leader    int32
		replicas  []int32
		isr       []int32
	}
	var rows []row
	for _, t := range td {
		if t.Err != nil {
			continue
		}
		for _, p := range t.Partitions {
			if len(p.ISR) < len(p.Replicas) {
				rows = append(rows, row{
					topic:     t.Topic,
					partition: p.Partition,
					leader:    p.Leader,
					replicas:  append([]int32(nil), p.Replicas...),
					isr:       append([]int32(nil), p.ISR...),
				})
			}
		}
	}
	if len(rows) == 0 {
		fmt.Println("under-replicated partitions: none")
		return
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].topic != rows[j].topic {
			return rows[i].topic < rows[j].topic
		}
		return rows[i].partition < rows[j].partition
	})
	fmt.Println("\nUNDER-REPLICATED PARTITIONS:")
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TOPIC\tPARTITION\tLEADER\tREPLICAS\tISR\tMISSING")
	for _, r := range rows {
		sort.Slice(r.replicas, func(i, j int) bool { return r.replicas[i] < r.replicas[j] })
		sort.Slice(r.isr, func(i, j int) bool { return r.isr[i] < r.isr[j] })
		miss := missing(r.replicas, r.isr)
		fmt.Fprintf(tw, "%s\t%d\t%d\t%v\t%v\t%v\n", r.topic, r.partition, r.leader, r.replicas, r.isr, miss)
	}
	_ = tw.Flush()
}

func missing(replicas, isr []int32) []int32 {
	in := make(map[int32]struct{}, len(isr))
	for _, id := range isr {
		in[id] = struct{}{}
	}
	out := make([]int32, 0, len(replicas))
	for _, id := range replicas {
		if _, ok := in[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

func splitNonEmpty(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
