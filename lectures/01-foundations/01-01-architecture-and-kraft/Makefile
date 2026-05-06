.PHONY: help run quorum-cli build

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094

help:
	@echo "make run         - запустить Go-вариант quorum-status (через kadm.BrokerMetadata)"
	@echo "make quorum-cli  - тот же запрос через kafka-metadata-quorum.sh внутри kafka-1"
	@echo "make build       - go build ./..."

run:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/quorum-status

quorum-cli:
	docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
	  --bootstrap-server kafka-1:9092 describe --status

build:
	go build ./...
