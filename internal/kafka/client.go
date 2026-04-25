// Package kafka собирает kgo.Client и kadm.Client с дефолтами курса.
//
// Точка входа для лекций: вместо того чтобы в каждом cmd/main.go писать
// один и тот же набор kgo.SeedBrokers + kgo.ClientID + таймауты — берём
// NewClient тут. Дефолты подобраны под локальный sandbox-стенд
// (kafka-1/2/3 на портах 19092/19093/19094) и переопределяются через
// переменную окружения KAFKA_BOOTSTRAP.
package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
)

// DefaultBootstrap — sandbox-стенд из корневого docker-compose.yml.
const DefaultBootstrap = "localhost:19092,localhost:19093,localhost:19094"

// DefaultClientID — общий ClientID для лекций. Каждая лекция при желании
// перетирает его через kgo.ClientID(...).
const DefaultClientID = "lectures"

// NewClient собирает kgo.Client с дефолтами курса. Дополнительные опции
// дописываются последними и могут перетирать дефолты — это нужно, например,
// для transactional producer'ов в модуле 04.
func NewClient(opts ...kgo.Opt) (*kgo.Client, error) {
	bootstrap := config.EnvOr("KAFKA_BOOTSTRAP", DefaultBootstrap)
	seeds := splitSeeds(bootstrap)
	if len(seeds) == 0 {
		return nil, fmt.Errorf("kafka.NewClient: KAFKA_BOOTSTRAP is empty")
	}

	base := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ClientID(DefaultClientID),
		kgo.DialTimeout(5 * time.Second),
		kgo.RequestTimeoutOverhead(10 * time.Second),
		kgo.RetryTimeout(30 * time.Second),
	}
	base = append(base, opts...)

	cl, err := kgo.NewClient(base...)
	if err != nil {
		return nil, fmt.Errorf("kafka.NewClient: %w", err)
	}
	return cl, nil
}

func splitSeeds(s string) []string {
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
