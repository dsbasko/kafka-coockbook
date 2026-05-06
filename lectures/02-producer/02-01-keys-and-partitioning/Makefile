.PHONY: help run-keyed run-custom topic-create topic-create-keyed topic-create-custom topic-describe topic-delete consume-cli build

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
TOPIC_KEYED  ?= lecture-02-01-keyed
TOPIC_CUSTOM ?= lecture-02-01-custom
PARTITIONS ?= 3
RF ?= 3
MESSAGES ?= 1000
USERS ?= 10
PREMIUM_PCT ?= 30

help:
	@echo "make run-keyed         - $(MESSAGES) сообщений с ключом user-{i mod $(USERS)} в $(TOPIC_KEYED)"
	@echo "make run-custom        - $(MESSAGES) сообщений с premium/regular ключами через свой partitioner в $(TOPIC_CUSTOM)"
	@echo "make topic-create      - создать оба топика (alias: запуски сами создают идемпотентно)"
	@echo "make topic-describe    - kafka-topics.sh --describe для обоих топиков"
	@echo "make consume-cli T=... - kafka-console-consumer (T=имя топика, по умолчанию $(TOPIC_KEYED))"
	@echo "make topic-delete      - удалить оба топика после лекции"
	@echo "make build             - go build ./..."

run-keyed:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/keyed-producer \
	  -topic=$(TOPIC_KEYED) -partitions=$(PARTITIONS) -rf=$(RF) \
	  -messages=$(MESSAGES) -users=$(USERS)

run-custom:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/custom-partitioner \
	  -topic=$(TOPIC_CUSTOM) -partitions=$(PARTITIONS) -rf=$(RF) \
	  -messages=$(MESSAGES) -premium-pct=$(PREMIUM_PCT)

topic-create: topic-create-keyed topic-create-custom

topic-create-keyed:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC_KEYED) --partitions $(PARTITIONS) --replication-factor $(RF)

topic-create-custom:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC_CUSTOM) --partitions $(PARTITIONS) --replication-factor $(RF)

T ?= $(TOPIC_KEYED)
topic-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC_KEYED)
	@echo
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC_CUSTOM)

consume-cli:
	docker exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server kafka-1:9092 --topic $(T) \
	  --from-beginning --property print.key=true --property print.partition=true \
	  --property print.offset=true --property key.separator=" | "

topic-delete:
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC_KEYED)
	-docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC_CUSTOM)

build:
	go build ./...
