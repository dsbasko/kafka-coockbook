.PHONY: help build \
        run run-recreate describe topic-delete-all

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
PREFIX          ?= lecture-08-03
CDC_TOPIC       := $(PREFIX)-cdc
METRICS_TOPIC   := $(PREFIX)-metrics
EVENTS_TOPIC    := $(PREFIX)-events

help:
	@echo "make run                  - cmd/topic-profiles: создать 3 топика и распечатать DescribeTopicConfigs"
	@echo "make run-recreate         - то же, но сначала удалить старые топики (детерминированный вывод)"
	@echo "make describe             - kafka-configs.sh для всех трёх топиков"
	@echo "make topic-delete-all     - удалить все три топика"
	@echo ""
	@echo "Параметры:"
	@echo "  PREFIX=$(PREFIX)"
	@echo "  KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP)"

build:
	go build ./...

run:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/topic-profiles -prefix=$(PREFIX)

run-recreate:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/topic-profiles -prefix=$(PREFIX) -recreate

describe:
	@echo "--- $(CDC_TOPIC) ---"
	@docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh \
	  --bootstrap-server kafka-1:9092 --entity-type topics --entity-name $(CDC_TOPIC) --describe || true
	@echo ""
	@echo "--- $(METRICS_TOPIC) ---"
	@docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh \
	  --bootstrap-server kafka-1:9092 --entity-type topics --entity-name $(METRICS_TOPIC) --describe || true
	@echo ""
	@echo "--- $(EVENTS_TOPIC) ---"
	@docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh \
	  --bootstrap-server kafka-1:9092 --entity-type topics --entity-name $(EVENTS_TOPIC) --describe || true

topic-delete-all:
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(CDC_TOPIC)
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(METRICS_TOPIC)
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(EVENTS_TOPIC)
