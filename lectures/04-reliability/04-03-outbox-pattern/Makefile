.PHONY: help up down logs ps \
        db-init db-shell db-count db-truncate outbox-count outbox-pending dup-count \
        topic-create topic-delete topic-describe end-offsets \
        run-service run-publisher run-consumer chaos-kill-publisher \
        group-describe group-delete \
        clean build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
TOPIC             ?= lecture-04-03-orders
PARTITIONS        ?= 3
RF                ?= 3
GROUP             ?= lecture-04-03-orders-consumer

DATABASE_URL      ?= postgres://lecture:lecture@localhost:15433/lecture_04_03?sslmode=disable
PG_CONTAINER      ?= lecture-04-03-postgres
PG_DB             ?= lecture_04_03
PG_USER           ?= lecture

COUNT             ?= 100
BATCH_SIZE        ?= 50
POLL              ?= 500ms
IDLE              ?= 8s

help:
	@echo "make up                      - поднять Postgres из docker-compose.override.yml (порт 15433)"
	@echo "make down                    - остановить Postgres + удалить volume"
	@echo "make logs                    - tail -f логов Postgres"
	@echo "make ps                      - статус контейнера"
	@echo ""
	@echo "make db-init                 - создать таблицы orders, outbox, processed_outbox_ids"
	@echo "make db-shell                - psql внутри контейнера"
	@echo "make db-count                - SELECT COUNT(*) FROM orders"
	@echo "make outbox-count            - SELECT COUNT(*) FROM outbox (всего записей)"
	@echo "make outbox-pending          - SELECT COUNT(*) FROM outbox WHERE published_at IS NULL"
	@echo "make dup-count               - SELECT COUNT(*) FROM processed_outbox_ids"
	@echo "make db-truncate             - TRUNCATE всех трёх таблиц"
	@echo ""
	@echo "make topic-create            - создать $(TOPIC) (P=$(PARTITIONS) RF=$(RF))"
	@echo "make topic-delete            - удалить $(TOPIC)"
	@echo "make topic-describe          - kafka-topics --describe"
	@echo "make end-offsets             - latest offset per partition"
	@echo ""
	@echo "make run-service COUNT=$(COUNT)        - создать N заказов (orders + outbox в одной TX)"
	@echo "make run-publisher           - publisher до idle (BATCH_SIZE=$(BATCH_SIZE) POLL=$(POLL))"
	@echo "make run-consumer            - consumer до idle ($(IDLE)), с dedup по outbox.id"
	@echo "make chaos-kill-publisher    - publisher с -crash-after-produce: упадёт после Produce, ДО UPDATE"
	@echo ""
	@echo "make group-describe          - committed offsets для $(GROUP)"
	@echo "make group-delete            - сбросить committed offset'ы группы"
	@echo ""
	@echo "make clean                   - db-truncate + group-delete + topic-delete"
	@echo "make build                   - go build ./..."
	@echo ""
	@echo "Переопределяемые переменные:"
	@echo "  COUNT=500            - больше заказов в run-service"
	@echo "  BATCH_SIZE=200       - размер outbox-батча у publisher"
	@echo "  IDLE=2s              - короче idle-таймаут consumer'а"

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

outbox-count:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -At -c \
	  'SELECT COUNT(*) FROM outbox;'

outbox-pending:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -At -c \
	  'SELECT COUNT(*) FROM outbox WHERE published_at IS NULL;'

dup-count:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -At -c \
	  'SELECT COUNT(*) FROM processed_outbox_ids;'

db-truncate:
	docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  'TRUNCATE TABLE orders, outbox, processed_outbox_ids RESTART IDENTITY;'

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

run-service:
	DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/orders-service \
	  -count=$(COUNT) -topic=$(TOPIC)

run-publisher:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/outbox-publisher \
	  -batch-size=$(BATCH_SIZE) -poll-interval=$(POLL) -idle=$(IDLE)

run-consumer:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/orders-consumer \
	  -topic=$(TOPIC) -group=$(GROUP) -idle=$(IDLE)

chaos-kill-publisher:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/outbox-publisher \
	  -batch-size=$(BATCH_SIZE) -poll-interval=$(POLL) -idle=$(IDLE) \
	  -crash-after-produce=true

group-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --describe --group $(GROUP) || true

group-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(GROUP) || true

clean: db-truncate group-delete topic-delete

build:
	go build ./...
