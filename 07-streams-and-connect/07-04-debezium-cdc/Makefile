.PHONY: help up down logs ps \
        db-init db-shell db-count slot-status \
        topic-list topic-delete-all \
        connect-plugin-check \
        connector-create-source connector-create-outbox connector-create-all \
        connector-status connector-restart connector-delete-source connector-delete-outbox connector-delete-all connector-list \
        run-loader run-cdc-consumer \
        clean build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094

PG_CONTAINER      ?= lecture-07-04-postgres
PG_DB             ?= lecture_07_04
PG_USER           ?= lecture

CONNECT_URL       ?= http://localhost:8083
CONNECTOR_SOURCE  ?= lecture-07-04-debezium-pg-source
CONNECTOR_OUTBOX  ?= lecture-07-04-debezium-outbox
CONFIG_SOURCE     ?= connectors/debezium-pg-source.json
CONFIG_OUTBOX     ?= connectors/outbox-router.json

COUNT             ?= 20

help:
	@echo "make up                          - поднять Postgres из docker-compose.override.yml (порт 15437, wal_level=logical)"
	@echo "make down                        - остановить Postgres + удалить volume + replication slots"
	@echo "make logs                        - tail -f логов Postgres"
	@echo "make ps                          - статус контейнера"
	@echo ""
	@echo "make db-init                     - создать users, outbox, publication"
	@echo "make db-shell                    - psql внутри контейнера"
	@echo "make db-count                    - SELECT COUNT(*) FROM users / outbox"
	@echo "make slot-status                 - показать pg_replication_slots (Debezium держит slot активным)"
	@echo ""
	@echo "make topic-list                  - kafka-topics --list (видны cdc.public.users, events.user)"
	@echo "make topic-delete-all            - снести cdc-* и events.* (для полного reset'а)"
	@echo ""
	@echo "make connect-plugin-check        - проверить, что Debezium PostgresConnector доступен"
	@echo "make connector-create-source     - создать $(CONNECTOR_SOURCE) (raw CDC users)"
	@echo "make connector-create-outbox     - создать $(CONNECTOR_OUTBOX) (outbox + EventRouter SMT)"
	@echo "make connector-create-all        - создать оба"
	@echo "make connector-status            - статус обоих коннекторов и task'ов"
	@echo "make connector-restart           - reset failed task'и"
	@echo "make connector-delete-source     - удалить source connector"
	@echo "make connector-delete-outbox     - удалить outbox connector"
	@echo "make connector-delete-all        - удалить оба + drop replication slots"
	@echo "make connector-list              - GET /connectors"
	@echo ""
	@echo "make run-loader COUNT=$(COUNT)   - залить INSERT/UPDATE/DELETE в users + outbox"
	@echo "make run-cdc-consumer            - читать cdc.public.users и events.* и печатать"
	@echo ""
	@echo "make clean                       - connector-delete-all + topic-delete-all"
	@echo "make build                       - go build ./..."

up:
	docker compose -f docker-compose.override.yml up -d
	@echo "ждём Postgres..."
	@for i in $$(seq 1 30); do \
	  if docker exec $(PG_CONTAINER) pg_isready -U $(PG_USER) -d $(PG_DB) >/dev/null 2>&1; then \
	    echo "Postgres готов."; exit 0; \
	  fi; \
	  sleep 1; \
	done; \
	echo "Postgres не поднялся за 30 сек"; exit 1

down: connector-delete-all
	docker compose -f docker-compose.override.yml down -v

logs:
	docker compose -f docker-compose.override.yml logs -f postgres

ps:
	docker compose -f docker-compose.override.yml ps

db-init:
	docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) < db/init.sql

db-shell:
	docker exec -it $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB)

db-count:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  "SELECT 'users' AS tbl, COUNT(*) FROM users UNION ALL SELECT 'outbox', COUNT(*) FROM outbox;"

slot-status:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  "SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots;"

topic-list:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --list | grep -E '^(cdc\.|events\.)' || true

topic-delete-all:
	@for t in $$(docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list | grep -E '^(cdc\.|events\.)'); do \
	  echo "удаляю $$t"; \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --delete --topic $$t || true; \
	done

connect-plugin-check:
	@echo "ищем io.debezium.connector.postgresql.PostgresConnector..."
	@curl -fsS $(CONNECT_URL)/connector-plugins | \
	  grep -q 'io.debezium.connector.postgresql.PostgresConnector' && \
	  echo "OK: Debezium доступен" || \
	  (echo "Debezium PostgresConnector НЕ найден."; \
	   echo "Запусти из корня репо: make connect-install-plugins"; \
	   exit 1)

connector-create-source:
	curl -fsS -X POST -H 'Content-Type: application/json' \
	  --data @$(CONFIG_SOURCE) \
	  $(CONNECT_URL)/connectors | python3 -m json.tool

connector-create-outbox:
	curl -fsS -X POST -H 'Content-Type: application/json' \
	  --data @$(CONFIG_OUTBOX) \
	  $(CONNECT_URL)/connectors | python3 -m json.tool

connector-create-all: connector-create-source connector-create-outbox

connector-status:
	@echo "=== $(CONNECTOR_SOURCE) ==="
	@curl -fsS $(CONNECT_URL)/connectors/$(CONNECTOR_SOURCE)/status | python3 -m json.tool || true
	@echo
	@echo "=== $(CONNECTOR_OUTBOX) ==="
	@curl -fsS $(CONNECT_URL)/connectors/$(CONNECTOR_OUTBOX)/status | python3 -m json.tool || true

connector-restart:
	curl -fsS -X POST $(CONNECT_URL)/connectors/$(CONNECTOR_SOURCE)/restart?includeTasks=true\&onlyFailed=true || true
	curl -fsS -X POST $(CONNECT_URL)/connectors/$(CONNECTOR_OUTBOX)/restart?includeTasks=true\&onlyFailed=true || true
	@echo
	@$(MAKE) -s connector-status

connector-delete-source:
	curl -fsS -X DELETE $(CONNECT_URL)/connectors/$(CONNECTOR_SOURCE) || true

connector-delete-outbox:
	curl -fsS -X DELETE $(CONNECT_URL)/connectors/$(CONNECTOR_OUTBOX) || true

# После удаления коннекторов replication slot'ы остаются висеть в Postgres'е
# и продолжают накапливать WAL — это типичная operational ловушка. Сбрасываем
# их явно, иначе диск будет расти.
connector-delete-all: connector-delete-source connector-delete-outbox
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'lecture_07_04_%';" 2>/dev/null || true

connector-list:
	curl -fsS $(CONNECT_URL)/connectors | python3 -m json.tool

run-loader:
	go run ./cmd/db-loader -count=$(COUNT)

run-cdc-consumer:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/cdc-consumer

clean: connector-delete-all topic-delete-all

build:
	go build ./...
