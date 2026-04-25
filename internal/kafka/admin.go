package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// NewAdmin собирает admin-клиент поверх kgo.Client. Используется в лекциях
// для CreateTopic, DescribeTopics, ListOffsets, DescribeCluster, Lag и т.п.
//
// Caller владеет возвращаемыми клиентами и должен закрыть admin (он закроет
// kgo.Client под собой).
func NewAdmin(opts ...kgo.Opt) (*kadm.Client, error) {
	cl, err := NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka.NewAdmin: %w", err)
	}
	return kadm.NewClient(cl), nil
}
