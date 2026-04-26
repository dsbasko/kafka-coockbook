.PHONY: help run-1 run-2 run-3 run-eager run-coop topic-create-6p topic-describe topic-delete group-describe group-delete seed build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
TOPIC             ?= lecture-03-01-groups
GROUP             ?= lecture-03-01-loud
PARTITIONS        ?= 6
STRATEGY          ?= cooperative-sticky
SEED_MESSAGES     ?= 60

help:
	@echo "make topic-create-6p   - создать $(TOPIC) с $(PARTITIONS) партициями (RF=3)"
	@echo "make topic-describe    - kafka-topics.sh --describe для $(TOPIC)"
	@echo "make topic-delete      - удалить $(TOPIC)"
	@echo "make seed              - залить $(SEED_MESSAGES) тестовых записей в $(TOPIC), чтобы было что читать"
	@echo "make run-1             - запустить loud-member с MEMBER_ID=1 (терминал №1)"
	@echo "make run-2             - то же с MEMBER_ID=2 (терминал №2; видно ребаланс)"
	@echo "make run-3             - то же с MEMBER_ID=3 (терминал №3; партиции 2/2/2)"
	@echo "make run-eager         - run-1, но со sticky (eager) — отзывается всё, потом раздаётся заново"
	@echo "make run-coop          - run-1, но с cooperative-sticky (по умолчанию)"
	@echo "make group-describe    - kafka-consumer-groups.sh --describe для $(GROUP)"
	@echo "make group-delete      - удалить committed offset'ы группы (после этого run-1 опять читает с earliest)"
	@echo "make build             - go build ./..."

topic-create-6p:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions $(PARTITIONS) --replication-factor 3

topic-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC)

topic-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC) || true

seed:
	@echo "льём $(SEED_MESSAGES) сообщений в $(TOPIC) (ключи k-0..k-N для распределения по 6 партициям)..."
	@for i in $$(seq 1 $(SEED_MESSAGES)); do \
	  echo "k-$$i:hello-$$i"; \
	done | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
	  --bootstrap-server kafka-1:9092 --topic $(TOPIC) \
	  --property parse.key=true --property key.separator=:

run-1:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) MEMBER_ID=1 go run ./cmd/loud-member \
	  -topic=$(TOPIC) -group=$(GROUP) -strategy=$(STRATEGY)

run-2:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) MEMBER_ID=2 go run ./cmd/loud-member \
	  -topic=$(TOPIC) -group=$(GROUP) -strategy=$(STRATEGY)

run-3:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) MEMBER_ID=3 go run ./cmd/loud-member \
	  -topic=$(TOPIC) -group=$(GROUP) -strategy=$(STRATEGY)

run-eager:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) MEMBER_ID=1 go run ./cmd/loud-member \
	  -topic=$(TOPIC) -group=$(GROUP) -strategy=sticky

run-coop:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) MEMBER_ID=1 go run ./cmd/loud-member \
	  -topic=$(TOPIC) -group=$(GROUP) -strategy=cooperative-sticky

group-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --describe --group $(GROUP)

group-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(GROUP)

build:
	go build ./...
