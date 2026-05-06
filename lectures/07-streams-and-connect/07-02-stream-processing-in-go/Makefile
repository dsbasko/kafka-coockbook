.PHONY: help topic-create-all topic-delete-all seed-text run kill-and-restart restore build

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
INPUT_TOPIC ?= lecture-07-02-text-events
CHANGELOG_TOPIC ?= lecture-07-02-word-count-changelog
OUTPUT_TOPIC ?= lecture-07-02-word-counts
GROUP ?= lecture-07-02-word-count
PARTITIONS ?= 3
RF ?= 3
STATE_DIR ?= ./state
FLUSH ?= 5s
TOP_N ?= 10

help:
	@echo "make topic-create-all   - создать input/changelog/output топики"
	@echo "make topic-delete-all   - удалить все три топика после лекции"
	@echo "make seed-text          - залить набор тестовых строк в input"
	@echo "make run                - запустить word-count (Pebble в $(STATE_DIR))"
	@echo "make kill-and-restart   - оборвать word-count, рестарт без потери state"
	@echo "make restore            - снести Pebble и восстановить из changelog"
	@echo "make build              - go build ./..."

topic-create-all:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(INPUT_TOPIC) --partitions $(PARTITIONS) --replication-factor $(RF)
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(CHANGELOG_TOPIC) --partitions $(PARTITIONS) --replication-factor $(RF) \
	  --config cleanup.policy=compact \
	  --config segment.ms=60000 \
	  --config min.cleanable.dirty.ratio=0.01
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(OUTPUT_TOPIC) --partitions $(PARTITIONS) --replication-factor $(RF)

topic-delete-all:
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(INPUT_TOPIC)
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(CHANGELOG_TOPIC)
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(OUTPUT_TOPIC)

seed-text:
	@echo "сею набор строк в $(INPUT_TOPIC) (бесконечно, Ctrl+C чтобы остановить)..."
	@while :; do \
	  for line in \
	    "kafka streams in go are nice" \
	    "go go go word counting forever" \
	    "pebble store keeps the count safe" \
	    "changelog topic restores the state" \
	    "kafka and pebble work well together" \
	    "stateful processing without kafka streams" \
	    "tumbling windows are the boring default" \
	    "the count of go events is rising" \
	    "stream processing is just events plus state"; do \
	    echo "$$line" | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
	      --bootstrap-server kafka-1:9092 --topic $(INPUT_TOPIC) >/dev/null; \
	  done; \
	  sleep 1; \
	done

run:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/word-count \
	  -input=$(INPUT_TOPIC) -changelog=$(CHANGELOG_TOPIC) -output=$(OUTPUT_TOPIC) \
	  -group=$(GROUP) -state=$(STATE_DIR) -flush=$(FLUSH) -top-n=$(TOP_N) \
	  -partitions=$(PARTITIONS) -rf=$(RF)

kill-and-restart:
	@echo "1) убей текущий word-count (Ctrl+C)"
	@echo "2) запусти снова: make run"
	@echo "Pebble в $(STATE_DIR) переживёт перезапуск, счётчики продолжат расти с того же значения."

restore:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/changelog-restorer \
	  -changelog=$(CHANGELOG_TOPIC) -state=$(STATE_DIR) -clean=true

build:
	go build ./...
