.PHONY: help up down logs ps \
        db-init db-shell db-count db-truncate orders-count view-count reservations-count outbox-pending \
        topic-create topic-delete topic-describe \
        proto-gen proto-lint \
        run-order run-inventory run-query \
        grpcurl-create grpcurl-get grpcurl-list \
        build clean

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
TOPIC             ?= lecture-06-04-order-created
PARTITIONS        ?= 3
RF                ?= 3

DATABASE_URL      ?= postgres://lecture:lecture@localhost:15434/lecture_06_04?sslmode=disable
PG_CONTAINER      ?= lecture-06-04-postgres
PG_DB             ?= lecture_06_04
PG_USER           ?= lecture

ORDER_GRPC        ?= localhost:50061
QUERY_GRPC        ?= localhost:50062

help:
	@echo "make up                      - поднять Postgres из docker-compose.override.yml (порт 15434)"
	@echo "make down                    - остановить Postgres + удалить volume"
	@echo "make logs                    - tail -f логов Postgres"
	@echo "make ps                      - статус контейнера"
	@echo ""
	@echo "make db-init                 - создать таблицы orders, outbox, orders_view, inventory_reservations, processed_events"
	@echo "make db-shell                - psql внутри контейнера"
	@echo "make db-truncate             - очистить все таблицы"
	@echo "make orders-count            - SELECT COUNT(*) FROM orders"
	@echo "make view-count              - SELECT COUNT(*) FROM orders_view"
	@echo "make reservations-count      - SELECT COUNT(*) FROM inventory_reservations"
	@echo "make outbox-pending          - SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"
	@echo ""
	@echo "make topic-create            - создать $(TOPIC) (P=$(PARTITIONS) RF=$(RF))"
	@echo "make topic-delete            - удалить $(TOPIC)"
	@echo "make topic-describe          - kafka-topics --describe"
	@echo ""
	@echo "make proto-gen               - сгенерировать gen/orders/v1/{orders.pb.go,orders_grpc.pb.go}"
	@echo "make proto-lint              - buf lint"
	@echo ""
	@echo "make run-order               - order-service: gRPC CommandService + outbox publisher на $(ORDER_GRPC)"
	@echo "make run-inventory           - inventory-service: consumer order.created → inventory_reservations"
	@echo "make run-query               - order-query-service: gRPC QueryService + projector на $(QUERY_GRPC)"
	@echo ""
	@echo "make grpcurl-list            - перечислить сервисы через reflection (на order-service)"
	@echo "make grpcurl-create          - дёрнуть Create через order-service"
	@echo "make grpcurl-get ID=<uuid>   - дёрнуть Get через order-query-service"
	@echo ""
	@echo "make build                   - go build ./..."
	@echo "make clean                   - удалить gen/"

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

db-truncate:
	docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  'TRUNCATE TABLE orders, outbox, orders_view, inventory_reservations, processed_events RESTART IDENTITY;'

orders-count:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -At -c \
	  'SELECT COUNT(*) FROM orders;'

view-count:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -At -c \
	  'SELECT COUNT(*) FROM orders_view;'

reservations-count:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -At -c \
	  'SELECT COUNT(*) FROM inventory_reservations;'

outbox-pending:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -At -c \
	  'SELECT COUNT(*) FROM outbox WHERE published_at IS NULL;'

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

proto-gen:
	buf generate

proto-lint:
	buf lint

run-order:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/order-service \
	  -addr=$(ORDER_GRPC) -topic=$(TOPIC)

run-inventory:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/inventory-service -topic=$(TOPIC)

run-query:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/order-query-service \
	  -addr=$(QUERY_GRPC) -topic=$(TOPIC)

# grpcurl нужен отдельно — `brew install grpcurl`. Reflection включён.
grpcurl-list:
	grpcurl -plaintext $(ORDER_GRPC) list

grpcurl-create:
	grpcurl -plaintext -d '{"customer_id":"cus-007","amount_cents":2599,"currency":"EUR","trace_id":"trace-demo","tenant_id":"tenant-a"}' \
	  $(ORDER_GRPC) orders.v1.CommandService/Create

# Передавай id явно: `make grpcurl-get ID=<uuid из ответа Create>`.
grpcurl-get:
	@if [ -z "$(ID)" ]; then \
	  echo "usage: make grpcurl-get ID=<uuid>"; exit 2; \
	fi
	grpcurl -plaintext -d '{"id":"$(ID)"}' \
	  $(QUERY_GRPC) orders.v1.QueryService/Get

build:
	go build ./...

clean:
	rm -rf gen
