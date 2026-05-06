.PHONY: help up down logs ps \
        db-init db-count db-truncate db-shell \
        topic-create topic-delete topic-load \
        run kill-mid \
        group-describe group-delete \
        clean build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
TOPIC             ?= lecture-03-03-events
PARTITIONS        ?= 3
GROUP             ?= lecture-03-03-idempotent
SEED_MESSAGES     ?= 30

DATABASE_URL      ?= postgres://lecture:lecture@localhost:15432/lecture_03_03?sslmode=disable
PG_CONTAINER      ?= lecture-03-03-postgres
PG_DB             ?= lecture_03_03
PG_USER           ?= lecture

# CRASH=N — упасть после N обработанных записей (после insert'а, до commit'а offset'а).
CRASH             ?= 0
WORK_DELAY        ?= 200ms

help:
	@echo "make up                    - поднять Postgres из docker-compose.override.yml"
	@echo "make down                  - остановить и удалить Postgres + volume"
	@echo "make logs                  - tail -f логов Postgres"
	@echo "make ps                    - статус контейнера"
	@echo ""
	@echo "make db-init               - создать таблицу messages (PRIMARY KEY topic,partition,offset)"
	@echo "make db-count              - сколько строк в messages"
	@echo "make db-truncate           - очистить messages"
	@echo "make db-shell              - psql внутри контейнера"
	@echo ""
	@echo "make topic-create          - создать $(TOPIC) с $(PARTITIONS) партициями (RF=3)"
	@echo "make topic-delete          - удалить $(TOPIC)"
	@echo "make topic-load            - залить $(SEED_MESSAGES) сообщений в $(TOPIC) (ключи k-0..k-N)"
	@echo ""
	@echo "make run                   - idempotent-pg consumer (CRASH=N — упасть после N записей)"
	@echo "make kill-mid              - run с CRASH=10, work-delay=100ms — гарантированно упасть в середине"
	@echo ""
	@echo "make group-describe        - committed offsets для $(GROUP)"
	@echo "make group-delete          - сбросить committed offset'ы $(GROUP) (читать заново с earliest)"
	@echo ""
	@echo "make clean                 - db-truncate + group-delete + topic-delete"
	@echo "make build                 - go build ./..."
	@echo ""
	@echo "Переопределяемые переменные:"
	@echo "  CRASH=10           — упасть после 10 обработанных записей"
	@echo "  SEED_MESSAGES=50   — сколько сообщений лить в topic-load"
	@echo "  WORK_DELAY=500ms   — sleep на каждое сообщение"

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

db-count:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -At -c \
	  'SELECT COUNT(*) AS rows FROM messages;'

db-truncate:
	docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  'TRUNCATE TABLE messages;'

db-shell:
	docker exec -it $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB)

topic-create:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions $(PARTITIONS) --replication-factor 3

topic-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC) || true

topic-load:
	@echo "льём $(SEED_MESSAGES) сообщений в $(TOPIC)..."
	@for i in $$(seq 1 $(SEED_MESSAGES)); do \
	  echo "k-$$i:event-$$i"; \
	done | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
	  --bootstrap-server kafka-1:9092 --topic $(TOPIC) \
	  --property parse.key=true --property key.separator=:

run:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/idempotent-pg \
	  -topic=$(TOPIC) -group=$(GROUP) \
	  -work-delay=$(WORK_DELAY) -crash-after=$(CRASH)

kill-mid:
	$(MAKE) run CRASH=10 WORK_DELAY=100ms

group-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --describe --group $(GROUP) || true

group-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(GROUP) || true

clean: db-truncate group-delete topic-delete

build:
	go build ./...
