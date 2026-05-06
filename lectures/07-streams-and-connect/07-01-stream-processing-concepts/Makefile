.PHONY: help topic-create topic-describe topic-delete run-events run-aggregator build

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
TOPIC ?= lecture-07-01-events
PARTITIONS ?= 3
RF ?= 3
GROUP ?= lecture-07-01-aggregator
RATE ?= 50ms
LATE_PROB ?= 0.10
WINDOW ?= 1m
PRINT ?= 5s
PRINT_WINDOWS ?= 8

help:
	@echo "make topic-create     - создать топик $(TOPIC) (partitions=$(PARTITIONS), rf=$(RF))"
	@echo "make topic-describe   - kafka-topics.sh --describe для $(TOPIC)"
	@echo "make topic-delete     - удалить топик после лекции"
	@echo "make run-events       - producer: пишет события с event-time лагом 0..60s + поздние"
	@echo "make run-aggregator   - consumer: считает count по 1-min окнам в обеих системах координат"
	@echo "make build            - go build ./..."

topic-create:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions $(PARTITIONS) --replication-factor $(RF)

topic-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC)

topic-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC)

run-events:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/event-time-vs-processing-time \
	  -role=events -topic=$(TOPIC) \
	  -partitions=$(PARTITIONS) -rf=$(RF) \
	  -rate=$(RATE) -late-prob=$(LATE_PROB)

run-aggregator:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/event-time-vs-processing-time \
	  -role=aggregator -topic=$(TOPIC) \
	  -partitions=$(PARTITIONS) -rf=$(RF) \
	  -group=$(GROUP) -window=$(WINDOW) \
	  -print=$(PRINT) -print-windows=$(PRINT_WINDOWS)

build:
	go build ./...
