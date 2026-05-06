.PHONY: help up down logs ps \
        db-init db-shell db-count db-truncate \
        topic-create topic-delete topic-describe end-offsets \
        connector-create connector-status connector-delete connector-restart connector-list \
        connect-plugin-check \
        run-producer \
        clean build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
TOPIC             ?= lecture-07-03-orders
PARTITIONS        ?= 3
RF                ?= 3

PG_CONTAINER      ?= lecture-07-03-postgres
PG_DB             ?= lecture_07_03
PG_USER           ?= lecture

CONNECT_URL       ?= http://localhost:8083
CONNECTOR_NAME    ?= lecture-07-03-jdbc-sink-orders
CONNECTOR_CONFIG  ?= connectors/jdbc-sink-orders.json

COUNT             ?= 100
START_ID          ?= 1

help:
	@echo "make up                 - поднять Postgres из docker-compose.override.yml (порт 15436)"
	@echo "make down               - остановить Postgres + удалить volume"
	@echo "make logs               - tail -f логов Postgres"
	@echo "make ps                 - статус контейнера"
	@echo ""
	@echo "make db-init            - создать таблицу orders"
	@echo "make db-shell           - psql внутри контейнера"
	@echo "make db-count           - SELECT COUNT(*) FROM orders"
	@echo "make db-truncate        - TRUNCATE orders"
	@echo ""
	@echo "make topic-create       - создать $(TOPIC) (P=$(PARTITIONS) RF=$(RF))"
	@echo "make topic-delete       - удалить $(TOPIC)"
	@echo "make topic-describe     - kafka-topics --describe"
	@echo "make end-offsets        - latest offset per partition"
	@echo ""
	@echo "make connect-plugin-check - проверить, что JdbcSinkConnector доступен в kafka-connect"
	@echo "make connector-create   - POST $(CONNECTOR_CONFIG) в kafka-connect"
	@echo "make connector-status   - GET статуса коннектора и его task'ов"
	@echo "make connector-restart  - POST /restart (reset failed task'и)"
	@echo "make connector-delete   - DELETE коннектор"
	@echo "make connector-list     - GET /connectors"
	@echo ""
	@echo "make run-producer COUNT=$(COUNT)  - залить N заказов в $(TOPIC)"
	@echo ""
	@echo "make clean              - db-truncate + connector-delete + topic-delete"
	@echo "make build              - go build ./..."
	@echo ""
	@echo "Переопределяемые переменные:"
	@echo "  COUNT=500                  - больше заказов в run-producer"
	@echo "  START_ID=1000              - стартовый id (повторный запуск с тем же start-id => UPSERT)"

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

down:
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
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -At -c \
	  'SELECT COUNT(*) FROM orders;'

db-truncate:
	docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  'TRUNCATE TABLE orders RESTART IDENTITY;'

topic-create:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions $(PARTITIONS) --replication-factor $(RF)

topic-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC) || true

topic-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC)

end-offsets:
	docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
	  --bootstrap-server kafka-1:9092 --topic $(TOPIC) --time -1

connect-plugin-check:
	@echo "ищем io.confluent.connect.jdbc.JdbcSinkConnector в /connector-plugins..."
	@curl -fsS $(CONNECT_URL)/connector-plugins | \
	  grep -q 'io.confluent.connect.jdbc.JdbcSinkConnector' && \
	  echo "OK: JdbcSinkConnector доступен" || \
	  (echo "JdbcSinkConnector НЕ найден. Поставь его и перезапусти kafka-connect:"; \
	   echo "  docker exec -it kafka-connect confluent-hub install confluentinc/kafka-connect-jdbc:latest --no-prompt --component-dir /usr/share/confluent-hub-components --worker-configs /etc/kafka-connect/kafka-connect.properties"; \
	   echo "  docker compose restart kafka-connect"; \
	   exit 1)

connector-create:
	curl -fsS -X POST -H 'Content-Type: application/json' \
	  --data @$(CONNECTOR_CONFIG) \
	  $(CONNECT_URL)/connectors | python3 -m json.tool

connector-status:
	curl -fsS $(CONNECT_URL)/connectors/$(CONNECTOR_NAME)/status | python3 -m json.tool

connector-restart:
	curl -fsS -X POST $(CONNECT_URL)/connectors/$(CONNECTOR_NAME)/restart?includeTasks=true\&onlyFailed=true || true
	@echo
	@$(MAKE) -s connector-status

connector-delete:
	curl -fsS -X DELETE $(CONNECT_URL)/connectors/$(CONNECTOR_NAME) || true

connector-list:
	curl -fsS $(CONNECT_URL)/connectors | python3 -m json.tool

run-producer:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
	  go run ./cmd/orders-producer \
	  -topic=$(TOPIC) -partitions=$(PARTITIONS) -rf=$(RF) \
	  -count=$(COUNT) -start-id=$(START_ID)

clean: db-truncate connector-delete topic-delete

build:
	go build ./...
