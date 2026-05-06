//go:build integration

// Integration test для use case 09-04: Postgres → Debezium → Kafka →
// Elasticsearch Sink → Elasticsearch.
//
// Что делает:
//
//   - очищает Postgres-таблицы products/articles/users и удаляет ES-индексы
//     products_v*, articles_v*, users_v*;
//   - пересоздаёт Kafka-топики search.public.* и DLQ;
//   - применяет ES index template (settings + mappings);
//   - регистрирует Debezium PostgresConnector через Kafka Connect REST API;
//   - регистрирует ES Sink connector v1 через тот же REST API;
//   - заливает N=200 строк в каждую таблицу;
//   - ждёт, пока в products_v1 появятся N документов;
//   - проверяет full-text-search по полю name;
//   - UPDATE строки → видит обновление в ES за разумный таймаут;
//   - DELETE строки → видит исчезновение в ES;
//   - blue-green reindex: создаёт v2 sink, ждёт догона, переключает alias.
//
// Размер N=200 (а не плановые 50k) выбран как в use case'ах 09-01/02/03 —
// для скорости прогона на dev-машинах. Паттерн идентичен любому объёму;
// масштабирование меняет только дедлайны и пропускную способность.
//
// Запуск: `make up && make pg-init && make es-init && make test-integration`.
package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dsbasko/kafka-sandbox/lectures/internal/config"
)

const (
	pgDSN            = "postgres://usecase:usecase@localhost:15443/usecase_09_04?sslmode=disable"
	esURL            = "http://localhost:19200"
	connectURL       = "http://localhost:8083"
	defaultBootstrap = "localhost:19092,localhost:19093,localhost:19094"

	// Имена коннекторов с unique-suffix задаются в TestEndToEnd через
	// time.Now().UnixNano(), чтобы Connect не подцеплял sticky offset из
	// прошлого запуска (как в 09-03).
	connectorDBZBase  = "usecase-09-04-it-debezium"
	connectorESV1Base = "usecase-09-04-it-es-sink"
	connectorESV2Base = "usecase-09-04-it-es-sink-v2"

	totalRows = 200
)

var (
	cdcTopics = []string{
		"search.public.products",
		"search.public.articles",
		"search.public.users",
	}
	allTopics = append(append([]string{}, cdcTopics...), "search-dlq")

	indices = []string{
		"products_v1", "articles_v1", "users_v1",
		"products_v2", "articles_v2", "users_v2",
	}
)

func TestEndToEnd_PgToElasticsearch(t *testing.T) {
	root, rootCancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer rootCancel()

	suffix := fmt.Sprintf("-%d", time.Now().UnixNano())
	connectorDBZ := connectorDBZBase + suffix
	connectorESV1 := connectorESV1Base + suffix
	connectorESV2 := connectorESV2Base + suffix
	slotName := "usecase_09_04_it_slot_" + strings.ReplaceAll(suffix[1:], "-", "_")
	indexSuffix := strings.ReplaceAll(suffix[1:], "-", "_")
	v1Suffix := "_v1_" + indexSuffix
	v2Suffix := "_v2_" + indexSuffix

	if err := pingPostgres(root); err != nil {
		t.Skipf("Postgres недоступен (make up && make pg-init): %v", err)
	}
	if err := pingES(root); err != nil {
		t.Skipf("Elasticsearch недоступен (make up): %v", err)
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

	t.Logf("очищаем Postgres + ES + Kafka-топики")
	if err := truncatePostgres(root); err != nil {
		t.Fatalf("truncate pg: %v", err)
	}
	if err := deleteIndices(root, indices); err != nil {
		t.Fatalf("delete es indices: %v", err)
	}
	if err := recreateTopics(root, bootstrap, allTopics); err != nil {
		t.Fatalf("recreate topics: %v", err)
	}

	cleanupOldConnectors(root, connectorDBZBase)
	cleanupOldConnectors(root, connectorESV1Base)
	cleanupOldConnectors(root, connectorESV2Base)
	dropReplicationSlots(root, "usecase_09_04_it_%")

	t.Logf("заливаем index template")
	if err := putIndexTemplate(root); err != nil {
		t.Fatalf("put template: %v", err)
	}

	t.Logf("регистрируем Debezium connector: %s", connectorDBZ)
	if err := registerDebezium(root, connectorDBZ, slotName); err != nil {
		t.Fatalf("debezium register: %v", err)
	}
	defer deleteConnectorIfExists(context.Background(), connectorDBZ)
	defer dropReplicationSlots(context.Background(), "usecase_09_04_it_%")

	if err := waitConnectorRunning(root, connectorDBZ, 60*time.Second); err != nil {
		t.Fatalf("debezium running: %v", err)
	}

	t.Logf("регистрируем ES Sink v1 (route → *%s): %s", v1Suffix, connectorESV1)
	if err := registerESSink(root, connectorESV1, v1Suffix); err != nil {
		t.Fatalf("es sink register: %v", err)
	}
	defer deleteConnectorIfExists(context.Background(), connectorESV1)

	if err := waitConnectorRunning(root, connectorESV1, 60*time.Second); err != nil {
		t.Fatalf("es sink running: %v", err)
	}

	t.Logf("льём нагрузку (%d строк в каждой таблице)", totalRows)
	if err := loadPostgres(root, 1, totalRows); err != nil {
		t.Fatalf("load pg: %v", err)
	}

	productsIdx := "products" + v1Suffix
	articlesIdx := "articles" + v1Suffix
	usersIdx := "users" + v1Suffix

	t.Logf("ждём, пока %s наберёт >= %d документов", productsIdx, totalRows)
	if err := waitESCount(root, productsIdx, totalRows, 4*time.Minute); err != nil {
		t.Fatalf("es count products: %v", err)
	}
	if err := waitESCount(root, articlesIdx, totalRows, 2*time.Minute); err != nil {
		t.Fatalf("es count articles: %v", err)
	}
	if err := waitESCount(root, usersIdx, totalRows, 2*time.Minute); err != nil {
		t.Fatalf("es count users: %v", err)
	}

	t.Logf("проверяем full-text-search: match по полю 'name' = 'Product'")
	hits, err := matchSearch(root, productsIdx, "name", "Product")
	if err != nil {
		t.Fatalf("match search: %v", err)
	}
	if hits == 0 {
		t.Fatalf("ожидали хотя бы 1 хит по 'Product', получили 0")
	}
	t.Logf("found %d hits", hits)

	t.Logf("проверяем UPDATE: обновляем product id=1, ждём отражения в ES")
	if err := updateProduct(root, 1, "Updated Marker XYZ"); err != nil {
		t.Fatalf("update product: %v", err)
	}
	if err := waitESDocField(root, productsIdx, 1, "name", "Updated Marker XYZ", 90*time.Second); err != nil {
		t.Fatalf("wait update: %v", err)
	}

	t.Logf("проверяем DELETE: удаляем article id=2, ждём, пока пропадёт из ES")
	if err := deleteArticle(root, 2); err != nil {
		t.Fatalf("delete article: %v", err)
	}
	if err := waitESDocAbsent(root, articlesIdx, 2, 90*time.Second); err != nil {
		t.Fatalf("wait delete: %v", err)
	}

	t.Logf("blue-green reindex: создаём v2 sink (роут → *%s)", v2Suffix)
	if err := registerESSink(root, connectorESV2, v2Suffix); err != nil {
		t.Fatalf("es sink v2 register: %v", err)
	}
	defer deleteConnectorIfExists(context.Background(), connectorESV2)
	if err := waitConnectorRunning(root, connectorESV2, 60*time.Second); err != nil {
		t.Fatalf("es sink v2 running: %v", err)
	}

	productsIdxV2 := "products" + v2Suffix
	t.Logf("ждём, пока %s догонит %s", productsIdxV2, productsIdx)
	if err := waitESCount(root, productsIdxV2, totalRows, 4*time.Minute); err != nil {
		t.Fatalf("es count products v2: %v", err)
	}

	aliasName := "products_alias_" + indexSuffix
	t.Logf("переключаем alias %s: %s → %s", aliasName, productsIdx, productsIdxV2)
	if err := switchAlias(root, aliasName, productsIdx, productsIdxV2); err != nil {
		t.Fatalf("switch alias: %v", err)
	}
	defer removeAlias(context.Background(), aliasName)

	target, err := aliasTarget(root, aliasName)
	if err != nil {
		t.Fatalf("alias target: %v", err)
	}
	if target != productsIdxV2 {
		t.Fatalf("alias %s указывает на %s, ожидали %s", aliasName, target, productsIdxV2)
	}

	t.Logf("E2E pipeline 09-04 завершился успешно")
}

func loadPostgres(ctx context.Context, fromID, toID int) error {
	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	tctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	categories := []string{"books", "electronics", "kitchen", "garden", "toys"}
	authors := []string{"alice", "bob", "carol", "dave"}

	for id := fromID; id <= toID; id++ {
		_, err := pool.Exec(tctx, `
			INSERT INTO products (id, sku, name, description, category, price_cents, stock)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (id) DO NOTHING
		`,
			id,
			fmt.Sprintf("SKU-%07d", id),
			fmt.Sprintf("Product %d alpha", id),
			fmt.Sprintf("Описание товара %d", id),
			categories[id%len(categories)],
			int64(100+id*7),
			id%50,
		)
		if err != nil {
			return fmt.Errorf("insert product %d: %w", id, err)
		}
		_, err = pool.Exec(tctx, `
			INSERT INTO articles (id, title, body, author, tags, published_at)
			VALUES ($1, $2, $3, $4, $5, NOW())
			ON CONFLICT (id) DO NOTHING
		`,
			id,
			fmt.Sprintf("Article %d about kafka", id),
			fmt.Sprintf("Body %d. Lorem ipsum.", id),
			authors[id%len(authors)],
			"news",
		)
		if err != nil {
			return fmt.Errorf("insert article %d: %w", id, err)
		}
		_, err = pool.Exec(tctx, `
			INSERT INTO users (id, username, full_name, bio)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (id) DO NOTHING
		`,
			id,
			fmt.Sprintf("user_%d", id),
			fmt.Sprintf("First%d Last%d", id, id),
			fmt.Sprintf("Bio %d", id),
		)
		if err != nil {
			return fmt.Errorf("insert user %d: %w", id, err)
		}
	}
	return nil
}

func updateProduct(ctx context.Context, id int, newName string) error {
	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	_, err = pool.Exec(ctx, `UPDATE products SET name = $1, updated_at = NOW() WHERE id = $2`, newName, id)
	return err
}

func deleteArticle(ctx context.Context, id int) error {
	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	_, err = pool.Exec(ctx, `DELETE FROM articles WHERE id = $1`, id)
	return err
}

func waitESCount(ctx context.Context, index string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastSeen int64
	for time.Now().Before(deadline) {
		n, err := esCount(ctx, index)
		if err == nil {
			lastSeen = n
			if n >= int64(want) {
				return nil
			}
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("index %s: за %s увидели только %d документов (ожидали %d)", index, timeout, lastSeen, want)
}

func waitESDocField(ctx context.Context, index string, id int, field, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastVal string
	for time.Now().Before(deadline) {
		got, found, err := esGetField(ctx, index, id, field)
		if err == nil && found && got == want {
			return nil
		}
		if err == nil && found {
			lastVal = got
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("doc %s/%d: поле %s не дошло до %q (последнее видели %q) за %s", index, id, field, want, lastVal, timeout)
}

func waitESDocAbsent(ctx context.Context, index string, id int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		exists, err := esDocExists(ctx, index, id)
		if err == nil && !exists {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("doc %s/%d не пропал за %s", index, id, timeout)
}

func esCount(ctx context.Context, index string) (int64, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(tctx, "GET", fmt.Sprintf("%s/%s/_count", esURL, index), nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 404 {
		return 0, nil
	}
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("ES count %d: %s", resp.StatusCode, string(body))
	}
	var out struct {
		Count int64 `json:"count"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return 0, err
	}
	return out.Count, nil
}

func esGetField(ctx context.Context, index string, id int, field string) (string, bool, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	url := fmt.Sprintf("%s/%s/_doc/%d", esURL, index, id)
	req, err := http.NewRequestWithContext(tctx, "GET", url, nil)
	if err != nil {
		return "", false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 404 {
		return "", false, nil
	}
	if resp.StatusCode >= 300 {
		return "", false, fmt.Errorf("ES get %d: %s", resp.StatusCode, string(body))
	}
	var out struct {
		Found  bool                   `json:"found"`
		Source map[string]interface{} `json:"_source"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return "", false, err
	}
	if !out.Found {
		return "", false, nil
	}
	val, ok := out.Source[field]
	if !ok {
		return "", true, nil
	}
	return fmt.Sprintf("%v", val), true, nil
}

func esDocExists(ctx context.Context, index string, id int) (bool, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	url := fmt.Sprintf("%s/%s/_doc/%d", esURL, index, id)
	req, err := http.NewRequestWithContext(tctx, "HEAD", url, nil)
	if err != nil {
		return false, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return false, nil
	}
	if resp.StatusCode >= 300 {
		return false, fmt.Errorf("ES head %d", resp.StatusCode)
	}
	return true, nil
}

func matchSearch(ctx context.Context, index, field, query string) (int, error) {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	body := fmt.Sprintf(`{"size":5,"query":{"match":{%q:%q}}}`, field, query)
	req, err := http.NewRequestWithContext(tctx, "POST",
		fmt.Sprintf("%s/%s/_search", esURL, index),
		bytes.NewBufferString(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("ES search %d: %s", resp.StatusCode, string(respBody))
	}
	var out struct {
		Hits struct {
			Total struct {
				Value int `json:"value"`
			} `json:"total"`
		} `json:"hits"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		return 0, err
	}
	return out.Hits.Total.Value, nil
}

func switchAlias(ctx context.Context, alias, fromIdx, toIdx string) error {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	body := fmt.Sprintf(`{
		"actions": [
			{"add":    {"index": %q, "alias": %q}},
			{"remove": {"index": %q, "alias": %q}}
		]
	}`, toIdx, alias, fromIdx, alias)
	req, err := http.NewRequestWithContext(tctx, "POST", esURL+"/_aliases", bytes.NewBufferString(body))
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
		// Если remove не удался (alias не существовал), пробуем только add.
		if strings.Contains(string(respBody), "missing") || strings.Contains(string(respBody), "not exist") {
			body2 := fmt.Sprintf(`{"actions":[{"add":{"index":%q,"alias":%q}}]}`, toIdx, alias)
			req2, _ := http.NewRequestWithContext(tctx, "POST", esURL+"/_aliases", bytes.NewBufferString(body2))
			req2.Header.Set("Content-Type", "application/json")
			r2, e2 := http.DefaultClient.Do(req2)
			if e2 != nil {
				return e2
			}
			r2.Body.Close()
			if r2.StatusCode >= 300 {
				return fmt.Errorf("ES alias add %d", r2.StatusCode)
			}
			return nil
		}
		return fmt.Errorf("ES alias %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func aliasTarget(ctx context.Context, alias string) (string, error) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(tctx, "GET", esURL+"/_alias/"+alias, nil)
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
		return "", fmt.Errorf("ES alias get %d: %s", resp.StatusCode, string(body))
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(body, &out); err != nil {
		return "", err
	}
	for idx := range out {
		return idx, nil
	}
	return "", errors.New("alias не указывает ни на один индекс")
}

func removeAlias(ctx context.Context, alias string) {
	tctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	body := fmt.Sprintf(`{"actions":[{"remove":{"index":"*","alias":%q}}]}`, alias)
	req, _ := http.NewRequestWithContext(tctx, "POST", esURL+"/_aliases", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func putIndexTemplate(ctx context.Context) error {
	tctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	tplBytes, err := os.ReadFile("../es-template.json")
	if err != nil {
		return fmt.Errorf("read es-template: %w", err)
	}
	req, err := http.NewRequestWithContext(tctx, "PUT", esURL+"/_index_template/usecase-09-04-it", bytes.NewBuffer(tplBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("ES template PUT %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func deleteIndices(ctx context.Context, idxs []string) error {
	tctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Удаляем по wildcard'у — на случай прошлых suffix'ов из аварийных
	// прогонов (имена индексов содержат timestamp).
	for _, prefix := range []string{"products_v*", "articles_v*", "users_v*"} {
		req, err := http.NewRequestWithContext(tctx, "DELETE", esURL+"/"+prefix, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	_ = idxs
	return nil
}

func registerDebezium(ctx context.Context, name, slot string) error {
	body := fmt.Sprintf(`{
  "name": %q,
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "usecase-09-04-postgres",
    "database.port": "5432",
    "database.user": "usecase",
    "database.password": "usecase",
    "database.dbname": "usecase_09_04",
    "topic.prefix": "search",
    "table.include.list": "public.products,public.articles,public.users",
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

func registerESSink(ctx context.Context, name, indexSuffix string) error {
	// indexSuffix должен начинаться с подчёркивания, например "_v1_<ts>".
	// RegexRouter заменяет search\.public\.(.*) на $1<indexSuffix>.
	body := fmt.Sprintf(`{
  "name": %q,
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "1",
    "topics.regex": "search\\.public\\.(products|articles|users)",
    "connection.url": "http://usecase-09-04-elasticsearch:9200",
    "type.name": "_doc",
    "key.ignore": "false",
    "schema.ignore": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "behavior.on.null.values": "delete",
    "behavior.on.malformed.documents": "warn",
    "drop.invalid.message": "true",
    "write.method": "upsert",
    "max.retries": "10",
    "retry.backoff.ms": "1000",
    "batch.size": "200",
    "linger.ms": "200",
    "flush.timeout.ms": "30000",
    "transforms": "unwrap,extractKey,routeIndex",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "none",
    "transforms.extractKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.extractKey.field": "id",
    "transforms.routeIndex.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.routeIndex.regex": "search\\.public\\.(.*)",
    "transforms.routeIndex.replacement": "$1%s",
    "errors.tolerance": "all"
  }
}`, name, indexSuffix)
	return postConnector(ctx, body)
}

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

func pingES(ctx context.Context) error {
	tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(tctx, "GET", esURL+"/_cluster/health", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("es health %d", resp.StatusCode)
	}
	return nil
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
	if !strings.Contains(s, "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector") {
		return errors.New("ElasticsearchSinkConnector отсутствует в Connect")
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

func truncatePostgres(ctx context.Context) error {
	pool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	_, err = pool.Exec(ctx, `TRUNCATE TABLE products, articles, users RESTART IDENTITY`)
	return err
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
