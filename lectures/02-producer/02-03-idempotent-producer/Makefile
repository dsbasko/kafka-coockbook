.PHONY: help run-with-idempotence run-without-idempotence topic-create topic-count topic-describe topic-delete build

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
TOPIC           ?= lecture-02-03-idempotent
PARTITIONS      ?= 3
RF              ?= 3
MESSAGES        ?= 100
DROP_RATE       ?= 0.35

help:
	@echo "make run-with-idempotence    - продьюсер по дефолту (idempotent=true): "
	@echo "                               drop_rate $(DROP_RATE), $(MESSAGES) сообщений; в логе ровно $(MESSAGES)"
	@echo "make run-without-idempotence - тот же drop_rate, но idempotent=false: "
	@echo "                               в логе будет > $(MESSAGES) (дубли от ретраев)"
	@echo "make topic-create            - создать $(TOPIC) (run сам создаёт идемпотентно)"
	@echo "make topic-count             - сумма end-offsets по партициям топика"
	@echo "make topic-describe          - kafka-topics.sh --describe для $(TOPIC)"
	@echo "make topic-delete            - удалить $(TOPIC); используй между прогонами,"
	@echo "                               чтобы дельта шла с нуля"
	@echo "make build                   - go build ./..."

run-with-idempotence:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/forced-retry \
	  -topic=$(TOPIC) -partitions=$(PARTITIONS) -rf=$(RF) \
	  -messages=$(MESSAGES) -drop-rate=$(DROP_RATE) -idempotent=true

run-without-idempotence:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/forced-retry \
	  -topic=$(TOPIC) -partitions=$(PARTITIONS) -rf=$(RF) \
	  -messages=$(MESSAGES) -drop-rate=$(DROP_RATE) -idempotent=false

topic-create:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions $(PARTITIONS) --replication-factor $(RF)

topic-count:
	@docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
	  --bootstrap-server kafka-1:9092 --topic $(TOPIC) --time -1 \
	  | awk -F: '{ s += $$3 } END { print "TOTAL", s }'

topic-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC)

topic-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC) || true

build:
	go build ./...
