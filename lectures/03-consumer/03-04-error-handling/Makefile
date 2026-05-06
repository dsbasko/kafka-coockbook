.PHONY: help \
        topic-create-all topic-delete-all \
        seed run-processor run-dlq-reader \
        group-describe group-delete-all dlq-count \
        clean build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
TOPIC             ?= payments
DLQ_TOPIC         ?= payments-dlq
PARTITIONS        ?= 3
GROUP             ?= lecture-03-04-processor
DLQ_GROUP         ?= lecture-03-04-dlq-reader

# Сколько сообщений и в какой пропорции лить через `make seed`.
# Сумма OK + TRANSIENT + PERMANENT = 100 (если меньше — недостача идёт в OK,
# если больше — выкидывает с ошибкой). PERMANENT отправляется как невалидный JSON
# (poison-pill) или как валидный JSON с mode=permanent — пополам.
SEED_MESSAGES     ?= 30
TRANSIENT_PCT     ?= 30
PERMANENT_PCT     ?= 20

# In-place retry — три попытки по 200ms, 400ms, 800ms.
MAX_RETRIES       ?= 3
BASE_BACKOFF      ?= 200ms
WORK_DELAY        ?= 50ms

help:
	@echo "make topic-create-all      - создать $(TOPIC) и $(DLQ_TOPIC) с $(PARTITIONS) партициями"
	@echo "make topic-delete-all      - удалить $(TOPIC) и $(DLQ_TOPIC)"
	@echo ""
	@echo "make seed                  - залить $(SEED_MESSAGES) mock-сообщений в $(TOPIC)"
	@echo "                             (TRANSIENT_PCT=$(TRANSIENT_PCT)%, PERMANENT_PCT=$(PERMANENT_PCT)%)"
	@echo ""
	@echo "make run-processor         - multi-strategy consumer на $(TOPIC) → DLQ при провале"
	@echo "make run-dlq-reader        - dlq-reader на $(DLQ_TOPIC), печатает headers + payload"
	@echo ""
	@echo "make group-describe        - committed offsets для processor-группы"
	@echo "make group-delete-all      - удалить committed offset'ы processor + dlq-reader"
	@echo "make dlq-count             - сколько сообщений в $(DLQ_TOPIC) (через kafka-run-class)"
	@echo ""
	@echo "make clean                 - group-delete-all + topic-delete-all"
	@echo "make build                 - go build ./..."
	@echo ""
	@echo "Переопределяемые переменные:"
	@echo "  SEED_MESSAGES=100  — сколько сообщений лить в seed"
	@echo "  TRANSIENT_PCT=30   — процент 'transient' сообщений (исцелятся после 2 retry)"
	@echo "  PERMANENT_PCT=20   — процент 'permanent' сообщений (часть — битый JSON)"
	@echo "  MAX_RETRIES=3      — попытки in-place retry перед DLQ"
	@echo "  BASE_BACKOFF=200ms — стартовый backoff (растёт x2 на попытку)"

topic-create-all:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions $(PARTITIONS) --replication-factor 3
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(DLQ_TOPIC) --partitions $(PARTITIONS) --replication-factor 3

topic-delete-all:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC) || true
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(DLQ_TOPIC) || true

# seed — генерит SEED_MESSAGES записей с заданными долями transient/permanent.
# Внутри permanent-доли половина — невалидный JSON (poison-pill),
# половина — валидный JSON с mode=permanent.
# Остальное — mode=ok.
seed:
	@echo "льём $(SEED_MESSAGES) mock-сообщений в $(TOPIC) (transient=$(TRANSIENT_PCT)% permanent=$(PERMANENT_PCT)%)..."
	@total=$(SEED_MESSAGES); tp=$(TRANSIENT_PCT); pp=$(PERMANENT_PCT); \
	  if [ $$((tp + pp)) -gt 100 ]; then echo "TRANSIENT_PCT + PERMANENT_PCT > 100"; exit 1; fi; \
	  for i in $$(seq 1 $$total); do \
	    r=$$((RANDOM % 100)); \
	    if [ $$r -lt $$pp ]; then \
	      if [ $$((RANDOM % 2)) -eq 0 ]; then \
	        echo "k-$$i:not-a-json-$$i"; \
	      else \
	        echo "k-$$i:{\"id\":\"k-$$i\",\"mode\":\"permanent\"}"; \
	      fi; \
	    elif [ $$r -lt $$((pp + tp)) ]; then \
	      echo "k-$$i:{\"id\":\"k-$$i\",\"mode\":\"transient\",\"amount\":$$i}"; \
	    else \
	      echo "k-$$i:{\"id\":\"k-$$i\",\"mode\":\"ok\",\"amount\":$$i}"; \
	    fi; \
	  done | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
	    --bootstrap-server kafka-1:9092 --topic $(TOPIC) \
	    --property parse.key=true --property key.separator=:

run-processor:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/multi-strategy \
	  -topic=$(TOPIC) -dlq=$(DLQ_TOPIC) -group=$(GROUP) \
	  -max-retries=$(MAX_RETRIES) -base-backoff=$(BASE_BACKOFF) \
	  -work-delay=$(WORK_DELAY)

run-dlq-reader:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/dlq-reader \
	  -dlq=$(DLQ_TOPIC) -group=$(DLQ_GROUP)

group-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --describe --group $(GROUP) || true

group-delete-all:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(GROUP) || true
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(DLQ_GROUP) || true

# dlq-count — суммарное число сообщений в DLQ топике (по всем партициям).
dlq-count:
	@docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
	  --bootstrap-server kafka-1:9092 --topic $(DLQ_TOPIC) --time -1 \
	  | awk -F: '{ s += $$3 } END { print "DLQ messages:", s }'

clean: group-delete-all topic-delete-all

build:
	go build ./...
