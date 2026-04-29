.PHONY: help up down logs ps \
        db-init db-shell saga-list saga-truncate \
        topic-create-all topic-delete-all topic-describe \
        proto-gen proto-lint \
        run-place-order \
        run-order-choreo run-payment-choreo run-inventory-choreo run-shipment-choreo \
        run-orchestrator run-payment-orch run-inventory-orch run-shipment-orch \
        run-choreo-all run-orch-all chaos-fail-shipment \
        build clean

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
PARTITIONS        ?= 3
RF                ?= 3

DATABASE_URL      ?= postgres://lecture:lecture@localhost:15435/lecture_06_05?sslmode=disable
PG_CONTAINER      ?= lecture-06-05-postgres
PG_DB             ?= lecture_06_05
PG_USER           ?= lecture

MODE              ?= choreo
COUNT             ?= 1
FAIL_RATE         ?= 0

# Список всех 16 топиков саги. Используется в topic-create-all/delete-all.
CHOREO_TOPICS = \
  lecture-06-05-choreo.order-requested \
  lecture-06-05-choreo.payment-completed \
  lecture-06-05-choreo.payment-failed \
  lecture-06-05-choreo.payment-refunded \
  lecture-06-05-choreo.inventory-reserved \
  lecture-06-05-choreo.inventory-failed \
  lecture-06-05-choreo.inventory-released \
  lecture-06-05-choreo.shipment-scheduled \
  lecture-06-05-choreo.shipment-failed

ORCH_TOPICS = \
  lecture-06-05-orch.place-order \
  lecture-06-05-orch.payment-cmd \
  lecture-06-05-orch.payment-reply \
  lecture-06-05-orch.inventory-cmd \
  lecture-06-05-orch.inventory-reply \
  lecture-06-05-orch.shipment-cmd \
  lecture-06-05-orch.shipment-reply

ALL_TOPICS = $(CHOREO_TOPICS) $(ORCH_TOPICS)

help:
	@echo "make up                       - поднять Postgres из docker-compose.override.yml (порт 15435)"
	@echo "make down                     - остановить Postgres + удалить volume"
	@echo "make logs                     - tail -f логов Postgres"
	@echo "make ps                       - статус контейнера"
	@echo ""
	@echo "make db-init                  - создать таблицу saga_state"
	@echo "make db-shell                 - psql внутри контейнера"
	@echo "make saga-list                - SELECT * FROM saga_state"
	@echo "make saga-truncate            - TRUNCATE saga_state"
	@echo ""
	@echo "make topic-create-all         - создать все 16 топиков (P=$(PARTITIONS) RF=$(RF))"
	@echo "make topic-delete-all         - удалить все топики саги"
	@echo "make topic-describe TOPIC=X   - kafka-topics --describe для конкретного топика"
	@echo ""
	@echo "make proto-gen                - сгенерировать gen/saga/v1/saga.pb.go"
	@echo "make proto-lint               - buf lint"
	@echo ""
	@echo "make run-place-order MODE=choreo|orch COUNT=N - триггер N саг (по умолчанию choreo, 1)"
	@echo ""
	@echo "Choreography (4 терминала, каждый — отдельный сервис):"
	@echo "  make run-payment-choreo     - payment-service[choreo]"
	@echo "  make run-inventory-choreo   - inventory-service[choreo]"
	@echo "  make run-shipment-choreo    - shipment-service[choreo]"
	@echo "  make run-order-choreo       - наблюдатель таймлайнов саг"
	@echo "  make run-choreo-all         - подсказка как запустить всё разом"
	@echo ""
	@echo "Orchestration:"
	@echo "  make run-orchestrator       - центральный orchestrator (нужен Postgres)"
	@echo "  make run-payment-orch       - payment-service[orch]"
	@echo "  make run-inventory-orch     - inventory-service[orch]"
	@echo "  make run-shipment-orch      - shipment-service[orch]"
	@echo "  make run-orch-all           - подсказка как запустить всё разом"
	@echo ""
	@echo "make chaos-fail-shipment      - запустить shipment-service[choreo] с FAIL_RATE=1.0"
	@echo "                                (для orch варианта: SHIPMENT_FAIL_RATE=1 make run-shipment-orch)"
	@echo ""
	@echo "make build                    - go build ./..."
	@echo "make clean                    - удалить gen/"

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

saga-list:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  "SELECT substr(saga_id,1,8) AS saga, current_step, status, last_event, payment_id, reservation_id, shipment_id, COALESCE(failure_reason,'') AS reason FROM saga_state ORDER BY created_at DESC LIMIT 20;"

saga-truncate:
	docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  'TRUNCATE TABLE saga_state;'

topic-create-all:
	@for t in $(ALL_TOPICS); do \
	  echo "creating $$t"; \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --create --if-not-exists \
	    --topic $$t --partitions $(PARTITIONS) --replication-factor $(RF) || true; \
	done

topic-delete-all:
	@for t in $(ALL_TOPICS); do \
	  echo "deleting $$t"; \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --delete --topic $$t || true; \
	done

topic-describe:
	@if [ -z "$(TOPIC)" ]; then \
	  echo "usage: make topic-describe TOPIC=lecture-06-05-..."; exit 2; \
	fi
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC)

proto-gen:
	buf generate

proto-lint:
	buf lint

run-place-order:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
	  go run ./cmd/place-order -mode=$(MODE) -count=$(COUNT)

# === Choreography ===

run-payment-choreo:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) FAIL_RATE=$(FAIL_RATE) \
	  go run ./cmd/payment-service -mode=choreo

run-inventory-choreo:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) FAIL_RATE=$(FAIL_RATE) \
	  go run ./cmd/inventory-service -mode=choreo

run-shipment-choreo:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) FAIL_RATE=$(FAIL_RATE) \
	  go run ./cmd/shipment-service -mode=choreo

run-order-choreo:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
	  go run ./cmd/order-service-choreo

run-choreo-all:
	@echo "Открой 4 терминала и запусти параллельно:"
	@echo "  make run-payment-choreo"
	@echo "  make run-inventory-choreo"
	@echo "  make run-shipment-choreo"
	@echo "  make run-order-choreo"
	@echo "Затем триггер: make run-place-order MODE=choreo COUNT=3"

# === Orchestration ===

run-orchestrator:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/orchestrator

run-payment-orch:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) FAIL_RATE=$(FAIL_RATE) \
	  go run ./cmd/payment-service -mode=orch

run-inventory-orch:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) FAIL_RATE=$(FAIL_RATE) \
	  go run ./cmd/inventory-service -mode=orch

run-shipment-orch:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) FAIL_RATE=$(SHIPMENT_FAIL_RATE) \
	  go run ./cmd/shipment-service -mode=orch

run-orch-all:
	@echo "Открой 4 терминала, поднят ли Postgres (make up)? Запусти:"
	@echo "  make run-orchestrator"
	@echo "  make run-payment-orch"
	@echo "  make run-inventory-orch"
	@echo "  make run-shipment-orch"
	@echo "Затем триггер: make run-place-order MODE=orch COUNT=3"
	@echo "Состояние саг — make saga-list"

# === Chaos ===

chaos-fail-shipment:
	@echo "shipment-service[choreo] с FAIL_RATE=1.0 — все доставки будут срываться"
	@echo "(для orch: SHIPMENT_FAIL_RATE=1 make run-shipment-orch)"
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) FAIL_RATE=1.0 \
	  go run ./cmd/shipment-service -mode=choreo

build:
	go build ./...

clean:
	rm -rf gen
