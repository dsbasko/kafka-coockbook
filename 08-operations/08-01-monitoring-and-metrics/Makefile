.PHONY: help up down logs ps \
        topic-create topic-delete topic-describe \
        run-load \
        kminion-metrics prometheus-targets \
        open-grafana open-prometheus open-kminion \
        clean build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
TOPIC             ?= lecture-08-01-events
PARTITIONS        ?= 6
GROUP             ?= lecture-08-01-slow

# нагрузка по умолчанию: 200 msg/sec producer, 20ms consume-delay
RATE              ?= 200
CONSUME_DELAY     ?= 20ms
PAYLOAD_KB        ?= 1

help:
	@echo "make up                    - поднять Prometheus + Grafana + kminion"
	@echo "make down                  - остановить и удалить volumes"
	@echo "make logs                  - tail -f всех сервисов override'а"
	@echo "make ps                    - статус контейнеров"
	@echo ""
	@echo "make topic-create          - создать $(TOPIC) с $(PARTITIONS) партициями (RF=3)"
	@echo "make topic-delete          - удалить $(TOPIC)"
	@echo "make topic-describe        - leader/replicas/ISR per-partition"
	@echo ""
	@echo "make run-load              - producer ($(RATE) msg/sec) + slow consumer ($(CONSUME_DELAY)/msg)"
	@echo ""
	@echo "make kminion-metrics       - curl /metrics из kminion (head -50)"
	@echo "make prometheus-targets    - проверить, что таргеты в Prometheus UP"
	@echo ""
	@echo "make open-grafana          - открыть http://localhost:3000 (anonymous viewer + admin/admin)"
	@echo "make open-prometheus       - открыть http://localhost:9090"
	@echo "make open-kminion          - открыть http://localhost:8088/metrics"
	@echo ""
	@echo "make clean                 - down + topic-delete"
	@echo "make build                 - go build ./..."
	@echo ""
	@echo "Параметры нагрузки:"
	@echo "  RATE=1000 CONSUME_DELAY=50ms PAYLOAD_KB=4 make run-load"

up:
	docker compose -f docker-compose.override.yml up -d
	@echo "ждём kminion..."
	@for i in $$(seq 1 30); do \
	  if curl -fsS http://localhost:8088/metrics >/dev/null 2>&1; then \
	    echo "kminion готов: http://localhost:8088/metrics"; \
	    echo "Grafana:    http://localhost:3000  (admin/admin или anonymous)"; \
	    echo "Prometheus: http://localhost:9090"; \
	    exit 0; \
	  fi; \
	  sleep 1; \
	done; \
	echo "kminion не поднялся за 30 сек"; exit 1

down:
	docker compose -f docker-compose.override.yml down -v

logs:
	docker compose -f docker-compose.override.yml logs -f

ps:
	docker compose -f docker-compose.override.yml ps

topic-create:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions $(PARTITIONS) --replication-factor 3

topic-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC) || true

topic-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC)

run-load:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/load-generator \
	  -topic=$(TOPIC) -group=$(GROUP) \
	  -rate=$(RATE) -consume-delay=$(CONSUME_DELAY) -payload-kb=$(PAYLOAD_KB)

kminion-metrics:
	@curl -fsS http://localhost:8088/metrics | grep -E '^kminion_kafka_' | head -50

prometheus-targets:
	@curl -fsS http://localhost:9090/api/v1/targets | python3 -m json.tool | \
	  grep -E '"(job|health)"'

open-grafana:
	@command -v open >/dev/null 2>&1 && open http://localhost:3000 || \
	  echo "открой http://localhost:3000 вручную"

open-prometheus:
	@command -v open >/dev/null 2>&1 && open http://localhost:9090 || \
	  echo "открой http://localhost:9090 вручную"

open-kminion:
	@command -v open >/dev/null 2>&1 && open http://localhost:8088/metrics || \
	  echo "открой http://localhost:8088/metrics вручную"

clean: down topic-delete

build:
	go build ./...
