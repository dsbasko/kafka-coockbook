.PHONY: help run topic-create topic-describe topic-delete build

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
TOPIC_PREFIX  ?= lecture-02-04-batching
PARTITIONS    ?= 3
RF            ?= 3
MESSAGES      ?= 100000
PAYLOAD       ?= 1024

# Полные имена топиков матрицы. Совпадает с тем, как cmd/bench их
# собирает (см. main.go: <prefix>-l<lingerMs>-<comp>).
LINGERS    := 0ms 5ms 50ms
CODECS     := none lz4 zstd
TOPIC_TAILS := $(foreach l,$(LINGERS),$(foreach c,$(CODECS),l$(l)-$(c)))

help:
	@echo "make run             - матрица 3 linger × 3 compression × $(MESSAGES) сообщений по $(PAYLOAD) B"
	@echo "                       пишет в $(words $(TOPIC_TAILS)) топиков $(TOPIC_PREFIX)-l<linger>-<comp>"
	@echo "make topic-create    - создать все 9 топиков руками (run сам создаёт идемпотентно)"
	@echo "make topic-describe  - kafka-topics.sh --describe для всех 9 топиков"
	@echo "make topic-delete    - удалить все 9 топиков; полезно перед повторным прогоном,"
	@echo "                       чтобы дельта на диске считалась с нуля"
	@echo "make build           - go build ./..."

run:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/bench \
	  -topic-prefix=$(TOPIC_PREFIX) \
	  -partitions=$(PARTITIONS) -rf=$(RF) \
	  -messages=$(MESSAGES) -payload=$(PAYLOAD)

topic-create:
	@for tail in $(TOPIC_TAILS); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --create --if-not-exists \
	    --topic $(TOPIC_PREFIX)-$$tail --partitions $(PARTITIONS) --replication-factor $(RF); \
	done

topic-describe:
	@for tail in $(TOPIC_TAILS); do \
	  echo "--- $(TOPIC_PREFIX)-$$tail ---"; \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC_PREFIX)-$$tail; \
	done

topic-delete:
	@for tail in $(TOPIC_TAILS); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC_PREFIX)-$$tail || true; \
	done

build:
	go build ./...
