.PHONY: help build \
        topic-create-compact topic-create-delete topic-describe topic-delete-all \
        run-compaction run-retention \
        du-volume

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
TOPIC_COMPACT     ?= lecture-08-02-users-state
TOPIC_DELETE      ?= lecture-08-02-events

# compaction-demo defaults
KEYS              ?= 1000
UPDATES           ?= 100000
TOMBSTONE_KEYS    ?= 100
WAIT              ?= 30s

# retention-demo defaults
RETENTION         ?= 60s
SEGMENT           ?= 10s
RATE              ?= 5
POLL              ?= 5s

help:
	@echo "make run-compaction          - cmd/compaction-demo: 100k обновлений на 1k ключей, tombstone'ы, размер до/после"
	@echo "make run-retention           - cmd/retention-demo: фоновый продьюсер + опрос earliest/latest, видим как лог тает"
	@echo ""
	@echo "make topic-create-compact    - создать $(TOPIC_COMPACT) с cleanup.policy=compact"
	@echo "make topic-create-delete     - создать $(TOPIC_DELETE) с cleanup.policy=delete"
	@echo "make topic-describe          - описание обоих топиков (kafka-configs.sh)"
	@echo "make topic-delete-all        - удалить оба топика"
	@echo ""
	@echo "make du-volume               - размер директории топика на kafka-1 (du внутри контейнера)"
	@echo "make build                   - go build ./..."
	@echo ""
	@echo "Параметры:"
	@echo "  KEYS=$(KEYS) UPDATES=$(UPDATES) TOMBSTONE_KEYS=$(TOMBSTONE_KEYS) WAIT=$(WAIT)"
	@echo "  RETENTION=$(RETENTION) SEGMENT=$(SEGMENT) RATE=$(RATE) POLL=$(POLL)"

build:
	go build ./...

run-compaction:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/compaction-demo \
	  -topic=$(TOPIC_COMPACT) \
	  -keys=$(KEYS) \
	  -updates=$(UPDATES) \
	  -tombstone-keys=$(TOMBSTONE_KEYS) \
	  -wait=$(WAIT)

run-retention:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/retention-demo \
	  -topic=$(TOPIC_DELETE) \
	  -retention=$(RETENTION) \
	  -segment=$(SEGMENT) \
	  -rate=$(RATE) \
	  -poll=$(POLL)

topic-create-compact:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC_COMPACT) --partitions 1 --replication-factor 3 \
	  --config cleanup.policy=compact \
	  --config segment.ms=5000 \
	  --config min.cleanable.dirty.ratio=0.001 \
	  --config min.compaction.lag.ms=0 \
	  --config max.compaction.lag.ms=10000 \
	  --config delete.retention.ms=5000 \
	  --config min.insync.replicas=2

topic-create-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC_DELETE) --partitions 1 --replication-factor 3 \
	  --config cleanup.policy=delete \
	  --config retention.ms=60000 \
	  --config segment.ms=10000 \
	  --config min.insync.replicas=2

topic-describe:
	@echo "--- $(TOPIC_COMPACT) ---"
	@docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh \
	  --bootstrap-server kafka-1:9092 --entity-type topics --entity-name $(TOPIC_COMPACT) --describe || true
	@echo ""
	@echo "--- $(TOPIC_DELETE) ---"
	@docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh \
	  --bootstrap-server kafka-1:9092 --entity-type topics --entity-name $(TOPIC_DELETE) --describe || true

topic-delete-all:
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC_COMPACT)
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC_DELETE)

du-volume:
	@echo "--- $(TOPIC_COMPACT) на kafka-1 ---"
	@docker exec kafka-1 sh -c 'du -sb /var/lib/kafka/data/$(TOPIC_COMPACT)-* 2>/dev/null || echo "директорий нет"'
	@echo ""
	@echo "--- $(TOPIC_DELETE) на kafka-1 ---"
	@docker exec kafka-1 sh -c 'du -sb /var/lib/kafka/data/$(TOPIC_DELETE)-* 2>/dev/null || echo "директорий нет"'
