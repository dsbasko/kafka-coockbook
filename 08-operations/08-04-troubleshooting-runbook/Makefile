.PHONY: help build \
        topic-create topic-delete \
        run-hot run-fixed run-watch run-watch-once \
        kill-broker restore-broker

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
HOT_TOPIC       ?= lecture-08-04-hot
FIXED_TOPIC     ?= lecture-08-04-fixed
PARTITIONS      ?= 4
RF              ?= 3
DURATION        ?= 10s
HOT_RATE        ?= 1000
NORMAL_RATE     ?= 10
BUCKETS         ?= 4
WATCH_INTERVAL  ?= 3s
BROKER          ?= kafka-2

help:
	@echo "make run-hot          - hot-partition-demo: один 'hot' ключ + 10 user-ключей, видно перекос"
	@echo "make run-fixed        - composite-key-fix: тот же объём, но раскинут по bucket'ам"
	@echo "make run-watch        - дашборд кластера, каждые $(WATCH_INTERVAL): brokers + UR partitions"
	@echo "make run-watch-once   - один тик watcher'а и выход (для тестов)"
	@echo "make kill-broker      - docker stop $(BROKER) — спровоцировать under-replicated"
	@echo "make restore-broker   - docker start $(BROKER) — вернуть в строй"
	@echo "make topic-create     - создать оба топика (hot и fixed) идемпотентно"
	@echo "make topic-delete     - удалить оба топика"
	@echo ""
	@echo "Параметры:"
	@echo "  HOT_TOPIC=$(HOT_TOPIC)"
	@echo "  FIXED_TOPIC=$(FIXED_TOPIC)"
	@echo "  PARTITIONS=$(PARTITIONS)  RF=$(RF)"
	@echo "  DURATION=$(DURATION)  HOT_RATE=$(HOT_RATE)  NORMAL_RATE=$(NORMAL_RATE)"
	@echo "  BUCKETS=$(BUCKETS)  WATCH_INTERVAL=$(WATCH_INTERVAL)  BROKER=$(BROKER)"

build:
	go build ./...

topic-create:
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create \
	  --topic $(HOT_TOPIC) --partitions $(PARTITIONS) --replication-factor $(RF)
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create \
	  --topic $(FIXED_TOPIC) --partitions $(PARTITIONS) --replication-factor $(RF)

topic-delete:
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(HOT_TOPIC)
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(FIXED_TOPIC)

run-hot:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/hot-partition-demo \
	  -topic=$(HOT_TOPIC) -partitions=$(PARTITIONS) -rf=$(RF) \
	  -duration=$(DURATION) -hot-rate=$(HOT_RATE) -normal-rate=$(NORMAL_RATE)

run-fixed:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/composite-key-fix \
	  -topic=$(FIXED_TOPIC) -partitions=$(PARTITIONS) -rf=$(RF) \
	  -duration=$(DURATION) -hot-rate=$(HOT_RATE) -normal-rate=$(NORMAL_RATE) \
	  -buckets=$(BUCKETS)

run-watch:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/under-replicated-watch \
	  -interval=$(WATCH_INTERVAL)

run-watch-once:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/under-replicated-watch -once

kill-broker:
	docker stop $(BROKER)

restore-broker:
	docker start $(BROKER)
