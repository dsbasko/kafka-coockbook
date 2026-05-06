.PHONY: help \
        topic-create-pipeline topic-delete-pipeline topic-describe-all \
        seed-with-failures \
        run-processor run-processor-fast run-dlq replay replay-dry \
        group-describe group-delete-all dlq-count main-count \
        clean build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094

MAIN_TOPIC        ?= payments
RETRY_30S         ?= payments-retry-30s
RETRY_5M          ?= payments-retry-5m
RETRY_1H          ?= payments-retry-1h
DLQ_TOPIC         ?= payments-dlq

PARTITIONS        ?= 3
RF                ?= 3

GROUP             ?= lecture-04-04-processor
DLQ_GROUP         ?= lecture-04-04-dlq

# Сколько mock-сообщений и пропорции для seed-with-failures.
SEED_MESSAGES     ?= 30
TRANSIENT_PCT     ?= 50
PERMANENT_PCT     ?= 20

# По умолчанию сокращаем реальные задержки 30s/5m/1h до 5s/15s/30s,
# чтобы лекция не превращалась в часовой просмотр sleep'а. На демо
# можно поставить production-значения через переменные.
DELAY_30S         ?= 5s
DELAY_5M          ?= 15s
DELAY_1H          ?= 30s

# Параметры replay по умолчанию.
REPLAY_FROM       ?= $(DLQ_TOPIC)
REPLAY_TO         ?= $(MAIN_TOPIC)
REPLAY_SINCE      ?= 1h
REPLAY_CLASS      ?=
REPLAY_IDLE       ?= 5s

INCIDENT_LOG      ?= /tmp/lecture-04-04-incidents.jsonl

help:
	@echo "make topic-create-pipeline   - создать $(MAIN_TOPIC), $(RETRY_30S), $(RETRY_5M), $(RETRY_1H), $(DLQ_TOPIC)"
	@echo "make topic-delete-pipeline   - удалить все 5 топиков"
	@echo "make topic-describe-all      - kafka-topics --describe для всех 5 топиков"
	@echo ""
	@echo "make seed-with-failures      - залить $(SEED_MESSAGES) mock-сообщений в $(MAIN_TOPIC)"
	@echo "                               (TRANSIENT_PCT=$(TRANSIENT_PCT)% PERMANENT_PCT=$(PERMANENT_PCT)%)"
	@echo ""
	@echo "make run-processor           - processor: main + retry-30s + retry-5m + retry-1h"
	@echo "                               задержки переопределены: 30s=$(DELAY_30S) 5m=$(DELAY_5M) 1h=$(DELAY_1H)"
	@echo "make run-processor-fast      - processor с задержками 1s/2s/3s (быстрая прогонка пайплайна)"
	@echo "make run-dlq                 - dlq-processor: ALERT в stdout + incident-лог в $(INCIDENT_LOG)"
	@echo ""
	@echo "make replay                  - replay-cli: $(REPLAY_FROM) → $(REPLAY_TO), since=$(REPLAY_SINCE)"
	@echo "make replay-dry              - replay в режиме --dry-run (ничего не публикует)"
	@echo ""
	@echo "make group-describe          - committed offsets для processor-группы"
	@echo "make group-delete-all        - снести committed offsets всех групп лекции"
	@echo "make main-count              - end-offset $(MAIN_TOPIC) (сумма по партициям)"
	@echo "make dlq-count               - end-offset $(DLQ_TOPIC) (сумма по партициям)"
	@echo ""
	@echo "make clean                   - group-delete-all + topic-delete-pipeline"
	@echo "make build                   - go build ./..."
	@echo ""
	@echo "Переопределяемые переменные:"
	@echo "  SEED_MESSAGES=200      - больше тест-сообщений"
	@echo "  TRANSIENT_PCT=30       - доля transient (всё уйдёт в DLQ как 'exhausted')"
	@echo "  PERMANENT_PCT=20       - доля permanent (мимо retry, сразу в DLQ)"
	@echo "  DELAY_30S=2s           - сократить задержку retry-30s для тестов"
	@echo "  REPLAY_SINCE=24h       - replay только последний день"
	@echo "  REPLAY_CLASS=transient - replay только сообщения с error.class=transient"

topic-create-pipeline:
	@for t in $(MAIN_TOPIC) $(RETRY_30S) $(RETRY_5M) $(RETRY_1H) $(DLQ_TOPIC); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --create --if-not-exists \
	    --topic $$t --partitions $(PARTITIONS) --replication-factor $(RF); \
	done

topic-delete-pipeline:
	@for t in $(MAIN_TOPIC) $(RETRY_30S) $(RETRY_5M) $(RETRY_1H) $(DLQ_TOPIC); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --delete --topic $$t || true; \
	done

topic-describe-all:
	@for t in $(MAIN_TOPIC) $(RETRY_30S) $(RETRY_5M) $(RETRY_1H) $(DLQ_TOPIC); do \
	  echo "=== $$t ==="; \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --describe --topic $$t; \
	  echo ""; \
	done

# seed-with-failures: льёт SEED_MESSAGES записей в основной топик с заданными
# долями transient/permanent. transient повторяемые (после исчерпания retry
# попадут в DLQ как 'exhausted'), permanent сразу летят в DLQ. ok-сообщения
# проходят с первого раза.
seed-with-failures:
	@echo "льём $(SEED_MESSAGES) mock-сообщений в $(MAIN_TOPIC) (transient=$(TRANSIENT_PCT)% permanent=$(PERMANENT_PCT)%)..."
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
	    --bootstrap-server kafka-1:9092 --topic $(MAIN_TOPIC) \
	    --property parse.key=true --property key.separator=:

run-processor:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/processor \
	  -main=$(MAIN_TOPIC) -retry-30s=$(RETRY_30S) -retry-5m=$(RETRY_5M) -retry-1h=$(RETRY_1H) \
	  -dlq=$(DLQ_TOPIC) -group=$(GROUP) \
	  -delay-30s=$(DELAY_30S) -delay-5m=$(DELAY_5M) -delay-1h=$(DELAY_1H)

run-processor-fast:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/processor \
	  -main=$(MAIN_TOPIC) -retry-30s=$(RETRY_30S) -retry-5m=$(RETRY_5M) -retry-1h=$(RETRY_1H) \
	  -dlq=$(DLQ_TOPIC) -group=$(GROUP) \
	  -delay-30s=1s -delay-5m=2s -delay-1h=3s

run-dlq:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/dlq-processor \
	  -dlq=$(DLQ_TOPIC) -group=$(DLQ_GROUP) -log-path=$(INCIDENT_LOG)

replay:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/replay-cli \
	  -from-topic=$(REPLAY_FROM) -to-topic=$(REPLAY_TO) \
	  -since=$(REPLAY_SINCE) -error-class='$(REPLAY_CLASS)' -idle=$(REPLAY_IDLE)

replay-dry:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/replay-cli \
	  -from-topic=$(REPLAY_FROM) -to-topic=$(REPLAY_TO) \
	  -since=$(REPLAY_SINCE) -error-class='$(REPLAY_CLASS)' -idle=$(REPLAY_IDLE) \
	  -dry-run=true

group-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --describe --group $(GROUP) || true

group-delete-all:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(GROUP) || true
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(DLQ_GROUP) || true

main-count:
	@docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
	  --bootstrap-server kafka-1:9092 --topic $(MAIN_TOPIC) --time -1 \
	  | awk -F: '{ s += $$3 } END { print "$(MAIN_TOPIC) end-offsets sum:", s }'

dlq-count:
	@docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
	  --bootstrap-server kafka-1:9092 --topic $(DLQ_TOPIC) --time -1 \
	  | awk -F: '{ s += $$3 } END { print "$(DLQ_TOPIC) end-offsets sum:", s }'

clean: group-delete-all topic-delete-pipeline
	@rm -f $(INCIDENT_LOG)

build:
	go build ./...
