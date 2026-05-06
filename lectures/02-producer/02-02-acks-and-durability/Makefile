.PHONY: help run topic-create topic-describe topic-delete kill-broker restore-broker build

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
TOPIC_PREFIX  ?= lecture-02-02-acks
PARTITIONS    ?= 3
RF            ?= 3
MIN_ISR       ?= 3
MESSAGES      ?= 1000
PAYLOAD       ?= 1024
KILL_BROKER   ?= kafka-2

help:
	@echo "make run               - параллельно прогоняет acks=0/1/all по $(MESSAGES) сообщений в каждый из"
	@echo "                         $(TOPIC_PREFIX)-{0,1,all} (partitions=$(PARTITIONS), rf=$(RF), min.ISR=$(MIN_ISR))"
	@echo "make topic-create      - создать все три топика руками (run сам создаёт идемпотентно)"
	@echo "make topic-describe    - kafka-topics.sh --describe для всех трёх топиков"
	@echo "make topic-delete      - удалить все три топика"
	@echo "make kill-broker       - docker compose stop $(KILL_BROKER); ISR падает до 2, для acks=all"
	@echo "                         включаются NotEnoughReplicas (RF=3, min.ISR=$(MIN_ISR))"
	@echo "make restore-broker    - docker compose start $(KILL_BROKER); ждёт пока ISR восстановится"
	@echo "make build             - go build ./..."

run:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/bench-acks \
	  -topic-prefix=$(TOPIC_PREFIX) \
	  -partitions=$(PARTITIONS) -rf=$(RF) \
	  -messages=$(MESSAGES) -payload=$(PAYLOAD)

topic-create:
	@for tail in 0 1 all; do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --create --if-not-exists \
	    --topic $(TOPIC_PREFIX)-$$tail --partitions $(PARTITIONS) --replication-factor $(RF) \
	    --config min.insync.replicas=$(MIN_ISR); \
	done

topic-describe:
	@for tail in 0 1 all; do \
	  echo "--- $(TOPIC_PREFIX)-$$tail ---"; \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC_PREFIX)-$$tail; \
	done

topic-delete:
	@for tail in 0 1 all; do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC_PREFIX)-$$tail || true; \
	done

kill-broker:
	docker stop $(KILL_BROKER)

restore-broker:
	docker start $(KILL_BROKER)
	@echo "ждём, пока ISR восстановится до 3 (порядка 30 секунд):"
	@sleep 5
	@docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC_PREFIX)-all

build:
	go build ./...
