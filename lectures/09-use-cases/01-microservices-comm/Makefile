.PHONY: help up down logs ps \
        db-init db-shell db-truncate \
        orders-count outbox-pending reservations-count notifications-count \
        topic-create topic-delete topic-describe \
        proto-gen proto-lint \
        run-order-1 run-order-2 run-inventory-1 run-inventory-2 run-notification \
        grpcurl-create grpcurl-list \
        test-integration \
        build clean

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
TOPIC             ?= usecase-09-01-order-created
PARTITIONS        ?= 6
RF                ?= 3

DATABASE_URL      ?= postgres://usecase:usecase@localhost:15440/usecase_09_01?sslmode=disable
PG_CONTAINER      ?= usecase-09-01-postgres
PG_DB             ?= usecase_09_01
PG_USER           ?= usecase

ORDER_1_ADDR      ?= localhost:50081
ORDER_2_ADDR      ?= localhost:50082

help:
	@echo "make up                      - поднять Postgres из docker-compose.override.yml (порт 15440)"
	@echo "make down                    - остановить Postgres + удалить volume"
	@echo "make logs                    - tail -f логов Postgres"
	@echo "make ps                      - статус контейнера"
	@echo ""
	@echo "make db-init                 - применить db/init.sql (orders, outbox, reservations, notifications, processed_events)"
	@echo "make db-shell                - psql внутри контейнера"
	@echo "make db-truncate             - очистить все таблицы"
	@echo "make orders-count            - SELECT COUNT(*) FROM orders"
	@echo "make outbox-pending          - SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"
	@echo "make reservations-count      - SELECT COUNT(*) FROM inventory_reservations"
	@echo "make notifications-count     - SELECT COUNT(*) FROM notifications_log"
	@echo ""
	@echo "make topic-create            - создать $(TOPIC) (P=$(PARTITIONS) RF=$(RF))"
	@echo "make topic-delete            - удалить $(TOPIC)"
	@echo "make topic-describe          - kafka-topics --describe"
	@echo ""
	@echo "make proto-gen               - сгенерировать gen/orders/v1/* и gen/inventory/v1/*"
	@echo "make proto-lint              - buf lint"
	@echo ""
	@echo "make run-order-1             - order-service на $(ORDER_1_ADDR), node-id=order-1"
	@echo "make run-order-2             - order-service на $(ORDER_2_ADDR), node-id=order-2"
	@echo "make run-inventory-1         - inventory-service node-id=inventory-1"
	@echo "make run-inventory-2         - inventory-service node-id=inventory-2"
	@echo "make run-notification        - notification-service node-id=notification-1"
	@echo ""
	@echo "make grpcurl-list            - reflection: список сервисов на order-1"
	@echo "make grpcurl-create          - дёрнуть Create через order-1"
	@echo ""
	@echo "make test-integration        - go test ./test/... -v -count=1 -timeout=180s"
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
	  'TRUNCATE TABLE orders, outbox, inventory_reservations, notifications_log, processed_events RESTART IDENTITY;'

orders-count:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -At -c \
	  'SELECT COUNT(*) FROM orders;'

outbox-pending:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -At -c \
	  'SELECT COUNT(*) FROM outbox WHERE published_at IS NULL;'

reservations-count:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -At -c \
	  'SELECT COUNT(*) FROM inventory_reservations;'

notifications-count:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -At -c \
	  'SELECT COUNT(*) FROM notifications_log;'

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

run-order-1:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/order-service \
	  -addr=$(ORDER_1_ADDR) -node-id=order-1 -topic=$(TOPIC)

run-order-2:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/order-service \
	  -addr=$(ORDER_2_ADDR) -node-id=order-2 -topic=$(TOPIC)

run-inventory-1:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/inventory-service -node-id=inventory-1 -topic=$(TOPIC)

run-inventory-2:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/inventory-service -node-id=inventory-2 -topic=$(TOPIC)

run-notification:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/notification-service -node-id=notification-1 -topic=$(TOPIC)

# grpcurl нужен отдельно — `brew install grpcurl`. Reflection включён.
grpcurl-list:
	grpcurl -plaintext $(ORDER_1_ADDR) list

grpcurl-create:
	grpcurl -plaintext -d '{"customer_id":"cus-007","amount_cents":2599,"currency":"EUR","trace_id":"trace-demo","tenant_id":"tenant-a"}' \
	  $(ORDER_1_ADDR) orders.v1.OrderService/Create

test-integration:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go test -tags=integration ./test/... -v -count=1 -timeout=180s

build:
	go build ./...

clean:
	rm -rf gen
