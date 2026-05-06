.PHONY: help \
        up-mock down-mock logs-mock \
        topic-create topic-delete topic-describe \
        seed seed-fast \
        run-courier run-mock \
        chaos-fail-50 chaos-timeout-30 chaos-clear \
        mock-stats group-describe group-delete \
        clean build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094

TOPIC             ?= notifications
PARTITIONS        ?= 3
RF                ?= 3

GROUP             ?= lecture-04-05-courier

# webhook target — на 127.0.0.1:8090. up-mock запускает контейнер на этом
# порту; run-mock запускает то же бинарём как локальный go-процесс.
TARGET            ?= http://localhost:8090/deliver
HMAC_SECRET       ?= lecture-04-05

# Сколько mock-сообщений и каким темпом льёт seed.
SEED_MESSAGES     ?= 100
SEED_DELAY        ?= 0.05

# Параметры courier'а — переопределимы через env.
HTTP_TIMEOUT      ?= 3s
MAX_ATTEMPTS      ?= 4
INITIAL_BACKOFF   ?= 200ms
MAX_BACKOFF       ?= 5s
CB_TRIP_AFTER     ?= 5
CB_OPEN_TIMEOUT   ?= 15s
PAUSE_AFTER       ?= 10s

# Параметры mock-webhook'а: проценты отказов. Меняй через make chaos-* или
# напрямую через переменные окружения перед docker compose up.
FAIL_RATE_503     ?= 0
FAIL_RATE_TIMEOUT ?= 0
LATENCY_MS        ?= 50

help:
	@echo "make up-mock          - поднять mock-webhook в docker (порт 8090)"
	@echo "make down-mock        - остановить mock-webhook (с volume)"
	@echo "make logs-mock        - tail логов контейнера"
	@echo ""
	@echo "make topic-create     - создать $(TOPIC) ($(PARTITIONS) партиции, RF=$(RF))"
	@echo "make topic-delete     - удалить $(TOPIC)"
	@echo "make topic-describe   - kafka-topics --describe"
	@echo ""
	@echo "make seed             - залить $(SEED_MESSAGES) mock-уведомлений в $(TOPIC) (delay=$(SEED_DELAY)s между сообщениями)"
	@echo "make seed-fast        - то же без delay (для нагрузки)"
	@echo ""
	@echo "make run-courier      - courier: топик→HTTP с CB, backoff+jitter, HMAC, PauseFetchPartitions"
	@echo "make run-mock         - mock-webhook как локальный go-процесс (альтернатива up-mock)"
	@echo ""
	@echo "make chaos-fail-50    - перезапустить mock с FAIL_RATE_503=0.5"
	@echo "make chaos-timeout-30 - перезапустить mock с FAIL_RATE_TIMEOUT=0.3"
	@echo "make chaos-clear      - перезапустить mock без отказов"
	@echo "make mock-stats       - curl /stats у mock-webhook'а"
	@echo ""
	@echo "make group-describe   - committed offsets у $(GROUP)"
	@echo "make group-delete     - удалить committed offsets группы $(GROUP)"
	@echo ""
	@echo "make clean            - group-delete + topic-delete + down-mock"
	@echo "make build            - go build ./..."
	@echo ""
	@echo "Переопределяемые переменные:"
	@echo "  FAIL_RATE_503=0.7        - 70% запросов падают с 503 (для chaos-* через env)"
	@echo "  FAIL_RATE_TIMEOUT=0.2    - 20% запросов виснут (HTTP timeout)"
	@echo "  PAUSE_AFTER=5s           - быстрее паузим партиции при затяжном Open"
	@echo "  CB_TRIP_AFTER=3          - CB чувствительнее (3 подряд → Open)"

up-mock:
	FAIL_RATE_503=$(FAIL_RATE_503) \
	FAIL_RATE_TIMEOUT=$(FAIL_RATE_TIMEOUT) \
	LATENCY_MS=$(LATENCY_MS) \
	docker compose -f docker-compose.override.yml up -d --build

down-mock:
	docker compose -f docker-compose.override.yml down -v

logs-mock:
	docker compose -f docker-compose.override.yml logs -f mock-webhook

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

# seed — пушит SEED_MESSAGES JSON-уведомлений с user-id ключом, чтобы по
# партиции один user попадал в одну и ту же. Значение — обычный JSON,
# courier его не парсит, просто шлёт как body.
seed:
	@echo "льём $(SEED_MESSAGES) уведомлений в $(TOPIC) с задержкой $(SEED_DELAY)s..."
	@for i in $$(seq 1 $(SEED_MESSAGES)); do \
	  echo "user-$$((i % 20)):{\"id\":\"n-$$i\",\"to\":\"user-$$((i % 20))\",\"text\":\"hello-$$i\"}"; \
	  sleep $(SEED_DELAY); \
	done | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
	  --bootstrap-server kafka-1:9092 --topic $(TOPIC) \
	  --property parse.key=true --property key.separator=:

seed-fast:
	@$(MAKE) seed SEED_DELAY=0

run-courier:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/courier \
	  -topic=$(TOPIC) -group=$(GROUP) \
	  -target=$(TARGET) -hmac-secret=$(HMAC_SECRET) \
	  -http-timeout=$(HTTP_TIMEOUT) \
	  -max-attempts=$(MAX_ATTEMPTS) \
	  -initial-backoff=$(INITIAL_BACKOFF) -max-backoff=$(MAX_BACKOFF) \
	  -cb-trip-after=$(CB_TRIP_AFTER) -cb-open-timeout=$(CB_OPEN_TIMEOUT) \
	  -pause-after=$(PAUSE_AFTER)

run-mock:
	LISTEN_ADDR=:8090 \
	FAIL_RATE_503=$(FAIL_RATE_503) \
	FAIL_RATE_TIMEOUT=$(FAIL_RATE_TIMEOUT) \
	LATENCY_MS=$(LATENCY_MS) \
	go run ./cmd/mock-webhook

chaos-fail-50:
	@$(MAKE) up-mock FAIL_RATE_503=0.5 FAIL_RATE_TIMEOUT=0

chaos-timeout-30:
	@$(MAKE) up-mock FAIL_RATE_503=0 FAIL_RATE_TIMEOUT=0.3

chaos-clear:
	@$(MAKE) up-mock FAIL_RATE_503=0 FAIL_RATE_TIMEOUT=0

mock-stats:
	@curl -s http://localhost:8090/stats || echo "mock-webhook не отвечает на 127.0.0.1:8090"

group-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --describe --group $(GROUP) || true

group-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(GROUP) || true

clean: group-delete topic-delete down-mock

build:
	go build ./...
