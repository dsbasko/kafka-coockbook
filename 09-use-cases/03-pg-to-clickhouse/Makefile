.PHONY: help up down logs ps \
        pg-init ch-init db-shell ch-shell pg-truncate ch-truncate \
        topic-create-all topic-delete-all \
        proto-gen proto-lint \
        connect-plugin-check \
        connector-create-debezium connector-create-clickhouse-sink connector-create-all \
        connector-status connector-restart connector-delete-all connector-list \
        run-anonymizer db-load test-integration \
        build clean

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094

PG_CONTAINER      ?= usecase-09-03-postgres
PG_DB             ?= usecase_09_03
PG_USER           ?= usecase
PG_DSN            ?= postgres://usecase:usecase@localhost:15442/usecase_09_03?sslmode=disable

CH_CONTAINER      ?= usecase-09-03-clickhouse
CH_DB             ?= analytics
CH_USER           ?= analytics
CH_PASS           ?= analytics
CH_HTTP           ?= http://localhost:18123

CONNECT_URL       ?= http://localhost:8083
CONNECTOR_DBZ     ?= usecase-09-03-debezium-pg-source
CONNECTOR_CH      ?= usecase-09-03-clickhouse-sink
CONFIG_DBZ        ?= connectors/debezium-pg-source.json
CONFIG_CH         ?= connectors/clickhouse-sink.json

PARTITIONS        ?= 3
RF                ?= 3

# Все CDC- и analytics-топики, которые понадобятся pipeline'у. Создаём явно
# заранее, потому что Debezium при первом запуске часто не успевает создать
# их с нужным RF/partitions, а ClickHouse Sink начинает читать сразу.
ALL_TOPICS = \
	cdc.public.users cdc.public.orders cdc.public.events \
	analytics.users analytics.orders analytics.events \
	analytics-dlq

DLOAD_COUNT       ?= 200

help:
	@echo "make up                          - поднять Postgres (порт 15442) + ClickHouse (порт 18123)"
	@echo "make down                        - остановить и удалить volumes"
	@echo "make logs                        - tail -f логов всех сервисов"
	@echo "make ps                          - статус контейнеров"
	@echo ""
	@echo "make pg-init                     - применить db/init.sql (таблицы + publication)"
	@echo "make ch-init                     - применить ch/init.sql (analytics схема)"
	@echo "make db-shell                    - psql внутри контейнера"
	@echo "make ch-shell                    - clickhouse-client внутри контейнера"
	@echo "make pg-truncate                 - очистить users/orders/events"
	@echo "make ch-truncate                 - очистить analytics.users/orders/events"
	@echo ""
	@echo "make topic-create-all            - создать все CDC- и analytics-топики"
	@echo "make topic-delete-all            - удалить их"
	@echo ""
	@echo "make proto-gen                   - buf generate"
	@echo "make proto-lint                  - buf lint"
	@echo ""
	@echo "make connect-plugin-check        - проверить, что Debezium + ClickHouse Sink доступны"
	@echo "make connector-create-debezium   - создать source-connector ($(CONNECTOR_DBZ))"
	@echo "make connector-create-clickhouse-sink - создать ClickHouse Sink ($(CONNECTOR_CH))"
	@echo "make connector-create-all        - создать оба"
	@echo "make connector-status            - статус обоих коннекторов"
	@echo "make connector-restart           - reset failed task'и"
	@echo "make connector-delete-all        - удалить оба + drop replication slot"
	@echo "make connector-list              - GET /connectors"
	@echo ""
	@echo "make run-anonymizer              - запустить anonymizer (cdc.public.* → analytics.*)"
	@echo "make db-load DLOAD_COUNT=$(DLOAD_COUNT) - залить нагрузку в Postgres"
	@echo ""
	@echo "make test-integration            - go test -tags=integration ./test/... (E2E)"
	@echo ""
	@echo "make build                       - go build ./..."
	@echo "make clean                       - connector-delete-all + topic-delete-all + down"

up:
	docker compose -f docker-compose.override.yml up -d
	@echo "ждём Postgres..."
	@for i in $$(seq 1 30); do \
	  if docker exec $(PG_CONTAINER) pg_isready -U $(PG_USER) -d $(PG_DB) >/dev/null 2>&1; then \
	    echo "Postgres готов."; break; \
	  fi; \
	  sleep 1; \
	done
	@echo "ждём ClickHouse..."
	@for i in $$(seq 1 30); do \
	  if curl -fsS $(CH_HTTP)/ping >/dev/null 2>&1; then \
	    echo "ClickHouse готов."; exit 0; \
	  fi; \
	  sleep 1; \
	done; \
	echo "ClickHouse не поднялся за 30 сек"; exit 1

down: connector-delete-all
	docker compose -f docker-compose.override.yml down -v

logs:
	docker compose -f docker-compose.override.yml logs -f

ps:
	docker compose -f docker-compose.override.yml ps

pg-init:
	docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) < db/init.sql

ch-init:
	docker exec -i $(CH_CONTAINER) clickhouse-client --user=$(CH_USER) --password=$(CH_PASS) --multiquery < ch/init.sql

db-shell:
	docker exec -it $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB)

ch-shell:
	docker exec -it $(CH_CONTAINER) clickhouse-client --user=$(CH_USER) --password=$(CH_PASS) -d $(CH_DB)

pg-truncate:
	docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  'TRUNCATE TABLE users, orders, events RESTART IDENTITY;'

ch-truncate:
	docker exec -i $(CH_CONTAINER) clickhouse-client --user=$(CH_USER) --password=$(CH_PASS) --multiquery -q \
	  "TRUNCATE TABLE analytics.users; TRUNCATE TABLE analytics.orders; TRUNCATE TABLE analytics.events;"

topic-create-all:
	@for t in $(ALL_TOPICS); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --create --if-not-exists \
	    --topic $$t --partitions $(PARTITIONS) --replication-factor $(RF) || true; \
	done

topic-delete-all:
	@for t in $(ALL_TOPICS); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --delete --topic $$t || true; \
	done

proto-gen:
	buf generate

proto-lint:
	buf lint

connect-plugin-check:
	@echo "ищем io.debezium.connector.postgresql.PostgresConnector и com.clickhouse.kafka.connect.ClickHouseSinkConnector..."
	@curl -fsS $(CONNECT_URL)/connector-plugins | \
	  grep -q 'io.debezium.connector.postgresql.PostgresConnector' && \
	  echo "OK: Debezium доступен" || \
	  (echo "Debezium PostgresConnector НЕ найден. Запусти: make -C ../../ connect-install-plugins"; exit 1)
	@curl -fsS $(CONNECT_URL)/connector-plugins | \
	  grep -q 'com.clickhouse.kafka.connect.ClickHouseSinkConnector' && \
	  echo "OK: ClickHouse Sink доступен" || \
	  (echo "ClickHouseSinkConnector НЕ найден. Запусти: make -C ../../ connect-install-plugins"; exit 1)

connector-create-debezium:
	curl -fsS -X POST -H 'Content-Type: application/json' \
	  --data @$(CONFIG_DBZ) $(CONNECT_URL)/connectors | python3 -m json.tool

connector-create-clickhouse-sink:
	curl -fsS -X POST -H 'Content-Type: application/json' \
	  --data @$(CONFIG_CH) $(CONNECT_URL)/connectors | python3 -m json.tool

connector-create-all: connector-create-debezium connector-create-clickhouse-sink

connector-status:
	@echo "=== $(CONNECTOR_DBZ) ==="
	@curl -fsS $(CONNECT_URL)/connectors/$(CONNECTOR_DBZ)/status | python3 -m json.tool || true
	@echo
	@echo "=== $(CONNECTOR_CH) ==="
	@curl -fsS $(CONNECT_URL)/connectors/$(CONNECTOR_CH)/status | python3 -m json.tool || true

connector-restart:
	curl -fsS -X POST $(CONNECT_URL)/connectors/$(CONNECTOR_DBZ)/restart?includeTasks=true\&onlyFailed=true || true
	curl -fsS -X POST $(CONNECT_URL)/connectors/$(CONNECTOR_CH)/restart?includeTasks=true\&onlyFailed=true || true
	@$(MAKE) -s connector-status

connector-delete-all:
	curl -fsS -X DELETE $(CONNECT_URL)/connectors/$(CONNECTOR_DBZ) || true
	curl -fsS -X DELETE $(CONNECT_URL)/connectors/$(CONNECTOR_CH) || true
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'usecase_09_03_%';" 2>/dev/null || true

connector-list:
	curl -fsS $(CONNECT_URL)/connectors | python3 -m json.tool

run-anonymizer:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
	  go run ./cmd/anonymizer -from-start=true

db-load:
	PG_DSN='$(PG_DSN)' go run ./cmd/db-loader -count=$(DLOAD_COUNT)

test-integration:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
	  go test -tags=integration ./test/... -v -count=1 -timeout=600s

build:
	go build ./...

clean: connector-delete-all topic-delete-all
	docker compose -f docker-compose.override.yml down -v || true
