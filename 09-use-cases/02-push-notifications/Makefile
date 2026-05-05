.PHONY: help up down logs ps \
        db-init db-shell db-truncate db-history \
        topic-create-all topic-delete-all \
        proto-gen proto-lint \
        run-router \
        run-firebase-sender run-apns-sender run-webhook-sender \
        run-firebase-dlq run-apns-dlq run-webhook-dlq \
        run-mocks chaos-fail-50 chaos-clear mock-stats \
        seed test-integration \
        build clean

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094

DATABASE_URL      ?= postgres://usecase:usecase@localhost:15441/usecase_09_02?sslmode=disable
PG_CONTAINER      ?= usecase-09-02-postgres
PG_DB             ?= usecase_09_02
PG_USER           ?= usecase

SOURCE_TOPIC      ?= usecase-09-02-notification-events
ROUTER_DLQ_TOPIC  ?= usecase-09-02-notification-events-dlq
PARTITIONS        ?= 6
RF                ?= 3

# Каналы и их топики (main + 2 ступени retry + dlq).
FIREBASE_TOPIC    ?= usecase-09-02-notification-firebase
FIREBASE_RETRY30  ?= usecase-09-02-notification-firebase-retry-30s
FIREBASE_RETRY5M  ?= usecase-09-02-notification-firebase-retry-5m
FIREBASE_DLQ      ?= usecase-09-02-notification-firebase-dlq

APNS_TOPIC        ?= usecase-09-02-notification-apns
APNS_RETRY30      ?= usecase-09-02-notification-apns-retry-30s
APNS_RETRY5M      ?= usecase-09-02-notification-apns-retry-5m
APNS_DLQ          ?= usecase-09-02-notification-apns-dlq

WEBHOOK_TOPIC     ?= usecase-09-02-notification-webhook
WEBHOOK_RETRY30   ?= usecase-09-02-notification-webhook-retry-30s
WEBHOOK_RETRY5M   ?= usecase-09-02-notification-webhook-retry-5m
WEBHOOK_DLQ       ?= usecase-09-02-notification-webhook-dlq

ALL_TOPICS = $(SOURCE_TOPIC) $(ROUTER_DLQ_TOPIC) \
             $(FIREBASE_TOPIC) $(FIREBASE_RETRY30) $(FIREBASE_RETRY5M) $(FIREBASE_DLQ) \
             $(APNS_TOPIC) $(APNS_RETRY30) $(APNS_RETRY5M) $(APNS_DLQ) \
             $(WEBHOOK_TOPIC) $(WEBHOOK_RETRY30) $(WEBHOOK_RETRY5M) $(WEBHOOK_DLQ)

# Failure rates для chaos-сценариев.
FAIL_RATE_503     ?= 0
FAIL_RATE_TIMEOUT ?= 0
LATENCY_MS        ?= 5

SEED_MESSAGES     ?= 100

help:
	@echo "make up                     - поднять Postgres + 3 mock'а из docker-compose.override.yml"
	@echo "make down                   - остановить и удалить volume"
	@echo "make logs                   - tail -f логов всех контейнеров"
	@echo "make ps                     - статус"
	@echo ""
	@echo "make db-init                - применить db/init.sql"
	@echo "make db-shell               - psql внутри контейнера"
	@echo "make db-truncate            - очистить notifications_log + processed_events"
	@echo "make db-history             - SELECT с разбивкой channel/status"
	@echo ""
	@echo "make topic-create-all       - создать все 14 топиков (source + router-dlq + 4 на канал)"
	@echo "make topic-delete-all       - удалить все 14 топиков"
	@echo ""
	@echo "make proto-gen              - buf generate"
	@echo "make proto-lint             - buf lint"
	@echo ""
	@echo "make run-router             - запустить notification-router"
	@echo "make run-firebase-sender    - запустить firebase-sender (deliver mode)"
	@echo "make run-apns-sender        - запустить apns-sender"
	@echo "make run-webhook-sender     - запустить webhook-sender"
	@echo "make run-firebase-dlq       - DLQ-consumer для firebase: пишет в notifications_log status='dlq'"
	@echo "make run-apns-dlq           - DLQ-consumer для apns"
	@echo "make run-webhook-dlq        - DLQ-consumer для webhook"
	@echo ""
	@echo "make run-mocks              - поднять 3 mock'а в docker (subset of up)"
	@echo "make chaos-fail-50          - перезапустить mock'и с FAIL_RATE_503=0.5"
	@echo "make chaos-clear            - перезапустить mock'и без отказов"
	@echo "make mock-stats             - curl /stats у трёх mock'ов"
	@echo ""
	@echo "make seed                   - залить $(SEED_MESSAGES) уведомлений в $(SOURCE_TOPIC)"
	@echo ""
	@echo "make test-integration       - go test -tags=integration ./test/..."
	@echo ""
	@echo "make build                  - go build ./..."
	@echo "make clean                  - удалить gen/, остановить контейнеры"

up:
	docker compose -f docker-compose.override.yml up -d --build
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
	docker compose -f docker-compose.override.yml logs -f

ps:
	docker compose -f docker-compose.override.yml ps

db-init:
	docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) < db/init.sql

db-shell:
	docker exec -it $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB)

db-truncate:
	docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  'TRUNCATE TABLE notifications_log, processed_events RESTART IDENTITY;'

db-history:
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  "SELECT channel, status, COUNT(*) FROM notifications_log GROUP BY channel, status ORDER BY channel, status;"

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

run-router:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/notification-router \
	  -source=$(SOURCE_TOPIC) -firebase=$(FIREBASE_TOPIC) -apns=$(APNS_TOPIC) -webhook=$(WEBHOOK_TOPIC) \
	  -router-dlq=$(ROUTER_DLQ_TOPIC)

# Sender-команды передают -from-start, чтобы при первом запуске группы прочитать
# уже накопленные сообщения. На последующих запусках ничего не меняется —
# committed offsets важнее ConsumeResetOffset.
run-firebase-sender:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/firebase-sender -from-start=true

run-apns-sender:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/apns-sender -from-start=true

run-webhook-sender:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/webhook-sender -from-start=true

run-firebase-dlq:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/firebase-sender -mode=dlq -from-start=true

run-apns-dlq:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/apns-sender -mode=dlq -from-start=true

run-webhook-dlq:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) DATABASE_URL='$(DATABASE_URL)' \
	  go run ./cmd/webhook-sender -mode=dlq -from-start=true

run-mocks:
	FAIL_RATE_503=$(FAIL_RATE_503) FAIL_RATE_TIMEOUT=$(FAIL_RATE_TIMEOUT) LATENCY_MS=$(LATENCY_MS) \
	  docker compose -f docker-compose.override.yml up -d --build mock-firebase mock-apns mock-webhook

chaos-fail-50:
	@$(MAKE) run-mocks FAIL_RATE_503=0.5 FAIL_RATE_TIMEOUT=0
	@$(MAKE) restart-mocks

chaos-clear:
	@$(MAKE) run-mocks FAIL_RATE_503=0 FAIL_RATE_TIMEOUT=0
	@$(MAKE) restart-mocks

restart-mocks:
	docker restart usecase-09-02-mock-firebase usecase-09-02-mock-apns usecase-09-02-mock-webhook

mock-stats:
	@echo "firebase:"
	@curl -s http://localhost:8091/stats || echo "  не отвечает"
	@echo "apns:"
	@curl -s http://localhost:8092/stats || echo "  не отвечает"
	@echo "webhook:"
	@curl -s http://localhost:8093/stats || echo "  не отвечает"

# seed льёт SEED_MESSAGES уведомлений с round-robin'ом по каналам через
# минимальный go-скрипт (inline). Канал кодируется enum-целым числом
# (1=firebase 2=apns 3=webhook). Используем go run на cmd/seed-tool/main.go.
seed:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
	  go run ./cmd/seed-tool -count=$(SEED_MESSAGES) -topic=$(SOURCE_TOPIC)

test-integration:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go test -tags=integration ./test/... -v -count=1 -timeout=300s

build:
	go build ./...

clean:
	rm -rf gen
	docker compose -f docker-compose.override.yml down -v || true
