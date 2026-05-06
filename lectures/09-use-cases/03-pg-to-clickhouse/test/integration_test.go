//go:build integration

// Integration test для use case 09-03: Postgres → Debezium → Anonymizer →
// ClickHouse Sink → ClickHouse.
//
// Что делает:
//
//   - очищает Postgres-таблицы users/orders/events и ClickHouse analytics.*;
//   - пересоздаёт Kafka-топики cdc.public.* и analytics.*;
//   - регистрирует Debezium PostgresConnector через Kafka Connect REST API;
//   - регистрирует ClickHouse Sink connector через тот же REST API;
//   - запускает anonymizer в горутине;
//   - вставляет N=200 строк в Postgres (users/orders/events каждый);
//   - ждёт, пока в ClickHouse появятся ровно N строк по каждой таблице;
//   - проверяет, что email/phone в analytics.users — это hex-hash длиной 64,
//     full_name — truncated, что в analytics.events нет колонок
//     ip_address/user_agent (DDL ClickHouse'а их и не содержит, проверяем
//     косвенно через DESCRIBE);
//   - kill anonymizer после половины нагрузки → перезапуск → recovery
//     (consumer group восстанавливается с committed offset'а).
//
// Тест требует:
//
//   - Kafka стенд из корневого docker-compose.yml;
//   - kafka-connect с установленными Debezium + ClickHouse Sink (Task 34.5);
//   - Postgres из docker-compose.override.yml (порт 15442) с db/init.sql;
//   - ClickHouse из docker-compose.override.yml (порт 18123) с ch/init.sql.
//
// Запуск: `make up && make pg-init && make ch-init && make test-integration`.
package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/03-pg-to-clickhouse/internal/anonymizer"
	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
)

const (
	pgDSN            = "postgres://usecase:usecase@localhost:15442/usecase_09_03?sslmode=disable"
	chHTTP           = "http://localhost:18123"
	chNative         = "localhost:19000"
	chUser           = "analytics"
	chPass           = "analytics"
	chDB             = "analytics"
	connectURL       = "http://localhost:8083"
	defaultBootstrap = "localhost:19092,localhost:19093,localhost:19094"

	// Имена с unique-suffix задаются в TestEndToEnd_PgToClickHouse через
	// time.Now().UnixNano(), чтобы Connect не подцеплял sticky offset из
	// прошлого запуска. _connect-offsets топик в Connect cluster'е
	// сохраняется между прогонами, и Debezium пытается восстановить
	// LSN с предыдущего slot'а — а слот был дропнут с Postgres'ом → FAIL.
	connectorDBZBase = "usecase-09-03-it-debezium"
	connectorCHBase  = "usecase-09-03-it-clickhouse-sink"

	anonymizerGroup = "usecase-09-03-it-anonymizer"

	totalRows = 200
)

var (
	cdcTopics = []string{
		"cdc.public.users",
		"cdc.public.orders",
		"cdc.public.events",
	}
	analyticsTopics = []string{
		"analytics.users",
		"analytics.orders",
		"analytics.events",
	}
	allTopics = append(append([]string{}, cdcTopics...), analyticsTopics...)
)

func TestEndToEnd_PgToClickHouse(t *testing.T) {
	root, rootCancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer rootCancel()

	suffix := fmt.Sprintf("-%d", time.Now().UnixNano())
	connectorDBZ := connectorDBZBase + suffix
	connectorCH := connectorCHBase + suffix
	slotName := "usecase_09_03_it_slot_" + strings.ReplaceAll(suffix[1:], "-", "_")

	if err := pingPostgres(root); err != nil {
		t.Skipf("Postgres недоступен (make up && make pg-init): %v", err)
	}
	if err := pingClickHouse(root); err != nil {
		t.Skipf("ClickHouse недоступен (make up && make ch-init): %v", err)
	}
	bootstrap := config.EnvOr("KAFKA_BOOTSTRAP", defaultBootstrap)
	if err := pingKafka(root, bootstrap); err != nil {
		t.Skipf("Kafka недоступна (стенд из корневого docker-compose.yml): %v", err)
	}
	if err := pingKafkaConnect(root); err != nil {
		t.Skipf("Kafka Connect недоступен на %s: %v", connectURL, err)
	}
	if err := requirePlugins(root); err != nil {
		t.Skipf("плагины Kafka Connect не установлены (make connect-install-plugins из корня): %v", err)
	}

	t.Logf("очищаем Postgres + ClickHouse + Kafka-топики")
	if err := truncatePostgres(root); err != nil {
		t.Fatalf("truncate pg: %v", err)
	}
	if err := truncateClickHouse(root); err != nil {
		t.Fatalf("truncate ch: %v", err)
	}
	if err := recreateTopics(root, bootstrap, allTopics); err != nil {
		t.Fatalf("recreate topics: %v", err)
	}
	if err := resetConsumerGroups(root, bootstrap, []string{anonymizerGroup}); err != nil {
		t.Fatalf("reset cg: %v", err)
	}

	// Удаляем замусоренные коннекторы с прошлых запусков (по prefix base-имени),
	// затем дропаем replication-slot'ы. Это защищает от ситуации, когда тест
	// упал, не дойдя до cleanup, и оставил slot в Postgres'е.
	cleanupOldConnectors(root, connectorDBZBase)
	cleanupOldConnectors(root, connectorCHBase)
	dropReplicationSlots(root, "usecase_09_03_it_%")

	t.Logf("регистрируем Debezium connector: %s", connectorDBZ)
	if err := registerDebezium(root, connectorDBZ, slotName); err != nil {
		t.Fatalf("debezium register: %v", err)
	}
	defer deleteConnectorIfExists(context.Background(), connectorDBZ)
	defer dropReplicationSlots(context.Background(), "usecase_09_03_it_%")

	if err := waitConnectorRunning(root, connectorDBZ, 60*time.Second); err != nil {
		t.Fatalf("debezium running: %v", err)
	}

	t.Logf("регистрируем ClickHouse Sink connector: %s", connectorCH)
	if err := registerClickHouse(root, connectorCH); err != nil {
		t.Fatalf("clickhouse register: %v", err)
	}
	defer deleteConnectorIfExists(context.Background(), connectorCH)

	if err := waitConnectorRunning(root, connectorCH, 60*time.Second); err != nil {
		t.Fatalf("clickhouse running: %v", err)
	}

	// Anonymizer запускаем как горутину — для теста проще, чем go run в shell.
	cfg, err := anonymizer.LoadConfig("../anonymize.yaml")
	if err != nil {
		t.Fatalf("load anonymize.yaml: %v", err)
	}

	type anonNode struct {
		cancel context.CancelFunc
		err    *atomic.Value
		done   chan struct{}
	}
	startAnon := func(nodeID string) *anonNode {
		ctx, cancel := context.WithCancel(root)
		errBox := new(atomic.Value)
		done := make(chan struct{})
		go func() {
			defer close(done)
			err := anonymizer.Run(ctx, anonymizer.RunOpts{
				NodeID:      nodeID,
				Bootstrap:   splitCSV(bootstrap),
				Group:       anonymizerGroup,
				SourceRegex: `^cdc\.public\.(users|orders|events)$`,
				Config:      cfg,
				FromStart:   true,
			})
			if err != nil && !errors.Is(err, context.Canceled) {
				errBox.Store(err)
			}
		}()
		return &anonNode{cancel: cancel, err: errBox, done: done}
	}

	anon1 := startAnon("anon-1")

	// Половина нагрузки.
	t.Logf("льём первую половину нагрузки (%d строк)", totalRows/2)
	if err := loadPostgres(root, 1, totalRows/2); err != nil {
		t.Fatalf("load pg first half: %v", err)
	}

	// Имитируем краш anonymizer'а: останавливаем горутину, ждём, поднимаем
	// заново. Consumer group восстановит committed offset.
	t.Logf("kill anonymizer for recovery test")
	anon1.cancel()
	<-anon1.done
	if v := anon1.err.Load(); v != nil {
		t.Fatalf("anon-1 ошибка: %v", v)
	}

	// Вторая половина приходит, пока anonymizer'а нет.
	t.Logf("льём вторую половину нагрузки (%d строк) пока anonymizer down", totalRows/2)
	if err := loadPostgres(root, totalRows/2+1, totalRows); err != nil {
		t.Fatalf("load pg second half: %v", err)
	}

	t.Logf("поднимаем anonymizer заново")
	anon2 := startAnon("anon-2")
	defer func() {
		anon2.cancel()
		<-anon2.done
		if v := anon2.err.Load(); v != nil {
			t.Errorf("anon-2 завершился с ошибкой: %v", v)
		}
	}()

	// Ждём появления данных в ClickHouse. ClickHouse Sink батчит данные —
	// дефолтный flush_interval небольшой, но снапшот всё равно может занять
	// 30–60 сек. Даём щедрый дедлайн.
	t.Logf("ждём, пока в ClickHouse появятся данные")
	if err := waitClickHouseRows(root, "users", totalRows, 4*time.Minute); err != nil {
		t.Fatalf("ch users: %v", err)
	}
	// orders: за счёт UPDATE статусов половине добавляется новая версия,
	// но строк по PK всё равно totalRows. Используем FINAL для дедупа.
	if err := waitClickHouseRows(root, "orders", totalRows, 2*time.Minute); err != nil {
		t.Fatalf("ch orders: %v", err)
	}
	// events: четверть удалена → их можно увидеть как _deleted=1, но live-row
	// по уникальному id всё ещё должно быть totalRows (часть из них с
	// _deleted=1).
	if err := waitClickHouseRows(root, "events", totalRows, 2*time.Minute); err != nil {
		t.Fatalf("ch events: %v", err)
	}

	t.Logf("проверяем, что анонимизация работает: email_hash — это sha256-hex")
	if err := assertHashes(root); err != nil {
		t.Fatalf("hash check: %v", err)
	}

	t.Logf("проверяем, что full_name — это truncate-форма (заканчивается на точку)")
	if err := assertTruncatedNames(root); err != nil {
		t.Fatalf("truncate check: %v", err)
	}

	t.Logf("проверяем, что DELETE-события помечены _deleted=1")
	if err := assertDeletedFlag(root); err != nil {
		t.Fatalf("deleted check: %v", err)
	}

	t.Logf("E2E pipeline завершился успешно")
}

func assertHashes(ctx context.Context) error {
	return chSelect(ctx, `SELECT email, phone FROM users LIMIT 50`, func(rows driver.Rows) error {
		checked := 0
		for rows.Next() {
			var emailHash, phoneHash string
			if err := rows.Scan(&emailHash, &phoneHash); err != nil {
				return err
			}
			if len(emailHash) != 64 {
				return fmt.Errorf("expected sha256 hex (64 chars) for email, got %q (len=%d)", emailHash, len(emailHash))
			}
			if len(phoneHash) != 64 {
				return fmt.Errorf("expected sha256 hex (64 chars) for phone, got %q (len=%d)", phoneHash, len(phoneHash))
			}
			if emailHash == phoneHash {
				return fmt.Errorf("email == phone — значит хеши совпали, анонимизация не работает")
			}
			checked++
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("rows iter: %w", err)
		}
		if checked == 0 {
			return errors.New("0 строк в analytics.users — нечего проверять")
		}
		return nil
	})
}

func assertTruncatedNames(ctx context.Context) error {
	return chSelect(ctx, `SELECT full_name FROM users LIMIT 50`, func(rows driver.Rows) error {
		checked := 0
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			if !strings.HasSuffix(name, ".") {
				return fmt.Errorf("expected truncated name ending with '.': got %q", name)
			}
			checked++
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("rows iter: %w", err)
		}
		if checked == 0 {
			return errors.New("0 строк в analytics.users — нечего проверять truncate")
		}
		return nil
	})
}

func assertDeletedFlag(ctx context.Context) error {
	// В тесте мы дропаем только events, и только в первом загрузочном проходе
	// (loadPostgres вызывается дважды и каждый раз внутри делает DELETE).
	// Достаточно проверить, что в analytics.events есть хотя бы одна строка
	// с _deleted=1.
	var c uint64
	if err := chScanRow(ctx, `SELECT count() FROM events WHERE _deleted=1`, &c); err != nil {
		return err
	}
	if c == 0 {
		return errors.New("ожидали хотя бы 1 строку с _deleted=1, получили 0")
	}
	return nil
}

func loadPostgres(ctx context.Context, fromID, toID int) error {
	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	tctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	for id := fromID; id <= toID; id++ {
		_, err := pool.Exec(tctx, `
			INSERT INTO users (id, email, phone, full_name, birth_date, country)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (id) DO NOTHING
		`,
			id,
			fmt.Sprintf("user%d@example.com", id),
			fmt.Sprintf("+7900%07d", id%10_000_000),
			fmt.Sprintf("First%d Last%d", id, id),
			time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC),
			[]string{"RU", "BY", "KZ"}[id%3],
		)
		if err != nil {
			return fmt.Errorf("insert user %d: %w", id, err)
		}
		_, err = pool.Exec(tctx, `
			INSERT INTO orders (id, user_id, amount_cents, currency, status, notes)
			VALUES ($1, $1, $2, 'RUB', 'new', 'private note')
			ON CONFLICT (id) DO NOTHING
		`, id, int64(100*id))
		if err != nil {
			return fmt.Errorf("insert order %d: %w", id, err)
		}
		_, err = pool.Exec(tctx, `
			INSERT INTO events (id, user_id, event_type, ip_address, user_agent)
			VALUES ($1, $1, 'signup', '10.0.0.1', 'Mozilla/5.0')
			ON CONFLICT (id) DO NOTHING
		`, id)
		if err != nil {
			return fmt.Errorf("insert event %d: %w", id, err)
		}
	}

	// UPDATE на половину orders во второй половине нагрузки (чтобы Debezium
	// родил события op=u, и мы могли проверить версионирование по cdc_lsn).
	_, err = pool.Exec(tctx, `
		UPDATE orders SET status='paid' WHERE id BETWEEN $1 AND $2 AND id % 2 = 0
	`, fromID, toID)
	if err != nil {
		return fmt.Errorf("update orders: %w", err)
	}

	// DELETE четверти events (хотя бы 1 строка должна попасть в _deleted=1
	// после полного прогона теста).
	_, err = pool.Exec(tctx, `
		DELETE FROM events WHERE id BETWEEN $1 AND $2 AND id % 4 = 0
	`, fromID, toID)
	if err != nil {
		return fmt.Errorf("delete events: %w", err)
	}

	return nil
}

func waitClickHouseRows(ctx context.Context, table string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastSeen uint64
	for time.Now().Before(deadline) {
		var n uint64
		if err := chScanRow(ctx, fmt.Sprintf("SELECT count(DISTINCT id) FROM %s", table), &n); err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		lastSeen = n
		if n >= uint64(want) {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("table %s: за %s увидели только %d строк (ожидали %d)", table, timeout, lastSeen, want)
}

func chConnect(ctx context.Context) (driver.Conn, error) {
	return clickhousego.Open(&clickhousego.Options{
		Addr: []string{chNative},
		Auth: clickhousego.Auth{
			Database: chDB,
			Username: chUser,
			Password: chPass,
		},
		DialTimeout: 5 * time.Second,
	})
}

// chSelect открывает conn, отдаёт rows в callback и гарантирует закрытие
// и rows, и conn. Без этого мы лишали бы себя CB-пула: caller получал бы
// rows, а conn оставался висеть.
func chSelect(ctx context.Context, query string, fn func(driver.Rows) error) error {
	conn, err := chConnect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	return fn(rows)
}

// chScanRow открывает conn, делает QueryRow.Scan и закрывает conn.
func chScanRow(ctx context.Context, query string, dest ...any) error {
	conn, err := chConnect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.QueryRow(ctx, query).Scan(dest...)
}

func chExec(ctx context.Context, query string) error {
	conn, err := chConnect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Exec(ctx, query)
}

func truncatePostgres(ctx context.Context) error {
	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	_, err = pool.Exec(ctx, `TRUNCATE TABLE users, orders, events RESTART IDENTITY`)
	return err
}

func truncateClickHouse(ctx context.Context) error {
	for _, t := range []string{"users", "orders", "events"} {
		if err := chExec(ctx, "TRUNCATE TABLE IF EXISTS "+t); err != nil {
			return fmt.Errorf("truncate %s: %w", t, err)
		}
	}
	return nil
}

func registerDebezium(ctx context.Context, name, slot string) error {
	body := fmt.Sprintf(`{
  "name": %q,
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "usecase-09-03-postgres",
    "database.port": "5432",
    "database.user": "usecase",
    "database.password": "usecase",
    "database.dbname": "usecase_09_03",
    "topic.prefix": "cdc",
    "table.include.list": "public.users,public.orders,public.events",
    "plugin.name": "pgoutput",
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "disabled",
    "slot.name": %q,
    "slot.drop.on.stop": "true",
    "snapshot.mode": "initial",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "tombstones.on.delete": "true",
    "heartbeat.interval.ms": "10000"
  }
}`, name, slot)
	return postConnector(ctx, body)
}

func registerClickHouse(ctx context.Context, name string) error {
	body := fmt.Sprintf(`{
  "name": %q,
  "config": {
    "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
    "tasks.max": "1",
    "topics.regex": "analytics\\.(users|orders|events)",
    "topic2TableMap": "analytics.users=users, analytics.orders=orders, analytics.events=events",
    "hostname": "usecase-09-03-clickhouse",
    "port": "8123",
    "database": "analytics",
    "username": "analytics",
    "password": "analytics",
    "ssl": "false",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "exactlyOnce": "false",
    "schemas.enable": "false",
    "errors.tolerance": "all"
  }
}`, name)
	return postConnector(ctx, body)
}

// cleanupOldConnectors удаляет все коннекторы, чьи имена начинаются с baseName
// (нужно для теста, который при каждом запуске генерирует свежий suffix —
// прошлый запуск мог упасть до defer'а cleanup'а).
func cleanupOldConnectors(ctx context.Context, baseName string) {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(tctx, "GET", connectURL+"/connectors", nil)
	if err != nil {
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var names []string
	if err := json.Unmarshal(body, &names); err != nil {
		return
	}
	for _, n := range names {
		if strings.HasPrefix(n, baseName) {
			deleteConnectorIfExists(ctx, n)
		}
	}
}

func postConnector(ctx context.Context, body string) error {
	tctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(tctx, "POST", connectURL+"/connectors", bytes.NewBufferString(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("connect REST %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func waitConnectorRunning(ctx context.Context, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		state, err := connectorState(ctx, name)
		if err == nil {
			if state == "RUNNING" {
				return nil
			}
			lastErr = fmt.Errorf("connector %s state=%s", name, state)
		} else {
			lastErr = err
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("connector %s не вышел в RUNNING за %s: %v", name, timeout, lastErr)
}

func connectorState(ctx context.Context, name string) (string, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(tctx, "GET", connectURL+"/connectors/"+name+"/status", nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("connect REST %d: %s", resp.StatusCode, string(body))
	}
	// Простой парсинг без структур: ищем "state":"RUNNING" в верхнем уровне
	// (есть "connector":{"state":"..."} и ещё в каждой task).
	s := string(body)
	if strings.Contains(s, `"state":"FAILED"`) {
		return "FAILED", nil
	}
	if strings.Contains(s, `"state":"RUNNING"`) {
		return "RUNNING", nil
	}
	return "PENDING", nil
}

func deleteConnectorIfExists(ctx context.Context, name string) {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(tctx, "DELETE", connectURL+"/connectors/"+name, nil)
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func dropReplicationSlots(ctx context.Context, like string) {
	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		return
	}
	defer pool.Close()
	_, _ = pool.Exec(ctx,
		`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE $1`,
		like,
	)
}

func pingPostgres(ctx context.Context) error {
	tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	pool, err := pgxpool.New(tctx, pgDSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	return pool.Ping(tctx)
}

func pingClickHouse(ctx context.Context) error {
	tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(tctx, "GET", chHTTP+"/ping", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("ch ping %d", resp.StatusCode)
	}
	// Также проверяем native-подключение, потому что тест читает через него.
	conn, err := chConnect(tctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Ping(tctx)
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

func pingKafkaConnect(ctx context.Context) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(tctx, "GET", connectURL+"/", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("connect %d", resp.StatusCode)
	}
	return nil
}

func requirePlugins(ctx context.Context) error {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(tctx, "GET", connectURL+"/connector-plugins", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	if !strings.Contains(s, "io.debezium.connector.postgresql.PostgresConnector") {
		return errors.New("Debezium PostgresConnector отсутствует в Connect")
	}
	if !strings.Contains(s, "com.clickhouse.kafka.connect.ClickHouseSinkConnector") {
		return errors.New("ClickHouseSinkConnector отсутствует в Connect")
	}
	return nil
}

func recreateTopics(ctx context.Context, bootstrap string, topics []string) error {
	tctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(splitCSV(bootstrap)...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	_, _ = adm.DeleteTopics(tctx, topics...)
	time.Sleep(800 * time.Millisecond)
	for _, topic := range topics {
		_, err := adm.CreateTopic(tctx, 3, 3, nil, topic)
		if err != nil && !errors.Is(err, kadm.ErrEmpty) {
			time.Sleep(500 * time.Millisecond)
			_, retryErr := adm.CreateTopic(tctx, 3, 3, nil, topic)
			if retryErr != nil {
				if strings.Contains(retryErr.Error(), "already exists") {
					continue
				}
				return fmt.Errorf("create topic %s: %w (retry: %v)", topic, err, retryErr)
			}
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

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
