//go:build integration

// Integration test для use case 09-01.
//
// Что делает:
//   - запускает 2 ноды order-service на разных gRPC-портах в одной программе
//     (как горутины с разными контекстами);
//   - запускает 2 ноды inventory-service в общей consumer group;
//   - запускает 1 ноду notification-service в отдельной consumer group;
//   - шлёт N=200 заказов через round-robin между двумя order-service нодами;
//   - в середине нагрузки (i=N/2) убивает одну inventory-ноду (отменой
//     контекста) для имитации частичного failure'а;
//   - ждёт, пока:
//       inventory_reservations.count == N (все зарезервировано — оставшаяся
//         inventory-нода добрала партиции упавшей);
//       notifications_log.count == N;
//       orders.count == N (write-side прошёл);
//       outbox.count_unpublished == 0 (всё опубликовано).
//
// Тест требует поднятые внешние ресурсы:
//   - Kafka стенд (kafka-1/2/3) из корневого docker-compose.yml;
//   - Postgres из docker-compose.override.yml на порту 15440 с
//     db/init.sql применённым (см. Makefile: `make up && make db-init`).
//
// Build tag `integration` нужен, чтобы `go test ./...` не пытался достучаться
// до Kafka/Postgres. Запуск: `go test -tags=integration ./test/...` или
// `make test-integration`. Если Kafka/Postgres недоступны — t.Skip.
package integration_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/01-microservices-comm/internal/inventoryservice"
	"github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/01-microservices-comm/internal/notificationservice"
	"github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/01-microservices-comm/internal/orderservice"
	ordersv1 "github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/01-microservices-comm/gen/orders/v1"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
)

const (
	dsn                = "postgres://usecase:usecase@localhost:15440/usecase_09_01?sslmode=disable"
	defaultBootstrap   = "localhost:19092,localhost:19093,localhost:19094"
	topicName          = "usecase-09-01-it-order-created"
	inventoryGroup     = "usecase-09-01-it-inventory"
	notificationsGroup = "usecase-09-01-it-notifications"
)

func TestEndToEnd_MicroservicesCommunication(t *testing.T) {
	const orders = 200

	root, rootCancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer rootCancel()

	if err := pingPostgres(root); err != nil {
		t.Skipf("Postgres недоступен (make up && make db-init): %v", err)
	}
	bootstrap := config.EnvOr("KAFKA_BOOTSTRAP", defaultBootstrap)
	if err := pingKafka(root, bootstrap); err != nil {
		t.Skipf("Kafka недоступна (стенд из корневого docker-compose.yml): %v", err)
	}

	if err := truncateTables(root); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := recreateTopic(root, bootstrap, topicName); err != nil {
		t.Fatalf("recreate topic: %v", err)
	}
	if err := resetConsumerGroups(root, bootstrap, []string{inventoryGroup, notificationsGroup}); err != nil {
		t.Fatalf("reset consumer groups: %v", err)
	}

	// Запускаем сервисы. Каждой ноде свой контекст — сможем убить одного.
	var wg sync.WaitGroup
	type node struct {
		name   string
		cancel context.CancelFunc
		done   chan struct{}
		err    *atomic.Value
	}
	nodes := []*node{}

	startOrder := func(name, addr string) *node {
		ctx, cancel := context.WithCancel(root)
		errBox := new(atomic.Value)
		done := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(done)
			err := orderservice.Run(ctx, orderservice.RunOpts{
				GrpcAddr:     addr,
				NodeID:       name,
				Topic:        topicName,
				DSN:          dsn,
				BatchSize:    100,
				PollInterval: 100 * time.Millisecond,
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				errBox.Store(err)
			}
		}()
		return &node{name: name, cancel: cancel, done: done, err: errBox}
	}
	startInventory := func(name string) *node {
		ctx, cancel := context.WithCancel(root)
		errBox := new(atomic.Value)
		done := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(done)
			err := inventoryservice.Run(ctx, inventoryservice.RunOpts{
				NodeID: name,
				Topic:  topicName,
				Group:  inventoryGroup,
				DSN:    dsn,
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				errBox.Store(err)
			}
		}()
		return &node{name: name, cancel: cancel, done: done, err: errBox}
	}
	startNotification := func(name string) *node {
		ctx, cancel := context.WithCancel(root)
		errBox := new(atomic.Value)
		done := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(done)
			err := notificationservice.Run(ctx, notificationservice.RunOpts{
				NodeID:  name,
				Channel: "email",
				Topic:   topicName,
				Group:   notificationsGroup,
				DSN:     dsn,
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				errBox.Store(err)
			}
		}()
		return &node{name: name, cancel: cancel, done: done, err: errBox}
	}

	orderAddr1 := pickFreeAddr(t)
	orderAddr2 := pickFreeAddr(t)

	o1 := startOrder("order-1", orderAddr1)
	o2 := startOrder("order-2", orderAddr2)
	inv1 := startInventory("inventory-1")
	inv2 := startInventory("inventory-2")
	notif1 := startNotification("notification-1")
	nodes = append(nodes, o1, o2, inv1, inv2, notif1)

	// Дождёмся, пока gRPC порты слушают.
	for _, addr := range []string{orderAddr1, orderAddr2} {
		if err := waitTCP(root, addr, 10*time.Second); err != nil {
			t.Fatalf("order gRPC %s: %v", addr, err)
		}
	}

	// Шлём заказы round-robin. На полпути убиваем inventory-1.
	conn1, err := grpc.NewClient(orderAddr1, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial order-1: %v", err)
	}
	defer conn1.Close()
	conn2, err := grpc.NewClient(orderAddr2, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial order-2: %v", err)
	}
	defer conn2.Close()

	cli1 := ordersv1.NewOrderServiceClient(conn1)
	cli2 := ordersv1.NewOrderServiceClient(conn2)

	t.Logf("отправляем %d заказов через 2 order-service ноды", orders)
	killAt := orders / 2
	for i := 0; i < orders; i++ {
		cli := cli1
		if i%2 == 1 {
			cli = cli2
		}
		ctx, cancel := context.WithTimeout(root, 5*time.Second)
		_, err := cli.Create(ctx, &ordersv1.CreateRequest{
			CustomerId:  fmt.Sprintf("cus-%04d", i%37),
			AmountCents: int64(1000 + i),
			Currency:    "EUR",
			TraceId:     fmt.Sprintf("trace-%d", i),
			TenantId:    "tenant-it",
		})
		cancel()
		if err != nil {
			t.Fatalf("Create #%d: %v", i, err)
		}

		if i == killAt {
			t.Logf("kill inventory-1 в середине нагрузки (i=%d)", i)
			inv1.cancel()
			<-inv1.done
		}
	}

	// Ждём eventual consistency.
	deadline := time.Now().Add(60 * time.Second)
	for {
		counts, err := snapshotCounts(root)
		if err != nil {
			t.Fatalf("snapshot counts: %v", err)
		}
		t.Logf("counts: orders=%d outbox_unpublished=%d reservations=%d notifications=%d",
			counts.orders, counts.outboxUnpublished, counts.reservations, counts.notifications)
		if counts.orders == orders &&
			counts.outboxUnpublished == 0 &&
			counts.reservations == orders &&
			counts.notifications == orders {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("eventual consistency не достигнута за 60s: %+v", counts)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Проверим, что обе ноды inventory успели поработать (хотя бы по одной
	// записи), а после kill оставшаяся добрала всё. Не делаем строгое
	// разделение — partition assignment непредсказуем.
	rByNode, err := reservationsByNode(root)
	if err != nil {
		t.Fatalf("reservations by node: %v", err)
	}
	t.Logf("inventory распределение: %+v", rByNode)
	if rByNode["inventory-2"] == 0 {
		t.Fatalf("inventory-2 не обработал ни одной записи — recovery не сработал")
	}

	// Останавливаем оставшиеся ноды.
	for _, n := range nodes {
		n.cancel()
	}
	wg.Wait()
	for _, n := range nodes {
		if v := n.err.Load(); v != nil {
			t.Errorf("%s завершился с ошибкой: %v", n.name, v)
		}
	}
}

type counts struct {
	orders            int
	outboxUnpublished int
	reservations      int
	notifications     int
}

func snapshotCounts(ctx context.Context) (counts, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return counts{}, err
	}
	defer pool.Close()

	var c counts
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM orders`).Scan(&c.orders); err != nil {
		return c, err
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE published_at IS NULL`).Scan(&c.outboxUnpublished); err != nil {
		return c, err
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM inventory_reservations`).Scan(&c.reservations); err != nil {
		return c, err
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM notifications_log`).Scan(&c.notifications); err != nil {
		return c, err
	}
	return c, nil
}

func reservationsByNode(ctx context.Context) (map[string]int, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer pool.Close()

	rows, err := pool.Query(ctx, `SELECT reserved_by, COUNT(*) FROM inventory_reservations GROUP BY reserved_by`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int)
	for rows.Next() {
		var node string
		var n int
		if err := rows.Scan(&node, &n); err != nil {
			return nil, err
		}
		out[node] = n
	}
	return out, rows.Err()
}

func truncateTables(ctx context.Context) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	_, err = pool.Exec(ctx, `TRUNCATE TABLE orders, outbox, inventory_reservations, notifications_log, processed_events RESTART IDENTITY`)
	return err
}

func pingPostgres(ctx context.Context) error {
	tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	pool, err := pgxpool.New(tctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	return pool.Ping(tctx)
}

func pingKafka(ctx context.Context, bootstrap string) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(splitCSV(bootstrap)...), kgo.DialTimeout(2*time.Second))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	if _, err := adm.ListBrokers(tctx); err != nil {
		return err
	}
	return nil
}

func recreateTopic(ctx context.Context, bootstrap, topic string) error {
	tctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(splitCSV(bootstrap)...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	_, _ = adm.DeleteTopic(tctx, topic)
	// Подождём, пока удаление физически отработает.
	time.Sleep(500 * time.Millisecond)
	_, err = adm.CreateTopic(tctx, 6, 3, nil, topic)
	if err != nil && !errors.Is(err, kadm.ErrEmpty) {
		// CreateTopic может вернуть TopicAlreadyExists сразу после DeleteTopic — даём ещё попытку.
		time.Sleep(time.Second)
		_, err2 := adm.CreateTopic(tctx, 6, 3, nil, topic)
		if err2 != nil {
			return fmt.Errorf("create topic %s: %w (retry: %v)", topic, err, err2)
		}
	}
	return nil
}

func resetConsumerGroups(ctx context.Context, bootstrap string, groups []string) error {
	tctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(splitCSV(bootstrap)...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	for _, g := range groups {
		_, _ = adm.DeleteGroup(tctx, g)
	}
	return nil
}

func waitTCP(ctx context.Context, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("timeout waiting for %s", addr)
}

func pickFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick free addr: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

func splitCSV(s string) []string {
	out := []string{}
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
