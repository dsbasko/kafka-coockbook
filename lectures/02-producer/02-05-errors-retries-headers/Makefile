.PHONY: help run-errors run-errors-retriable run-headers run-headers-producer run-headers-consumer topic-create topic-delete kill-broker restore-broker build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
TOPIC_NR          ?= lecture-02-05-non-retriable
TOPIC_R           ?= lecture-02-05-retriable
TOPIC_HEADERS     ?= lecture-02-05-headers
KILL_BROKER       ?= kafka-2
DELIVERY_TIMEOUT  ?= 20s
PAYLOAD           ?= 4096
MESSAGES          ?= 5

help:
	@echo "make run-errors            - non-retriable: пишем 4КБ в топик с max.message.bytes=1024,"
	@echo "                             ждём MESSAGE_TOO_LARGE сразу, без ретраев"
	@echo "make run-errors-retriable  - retriable: acks=all в топик с min.insync.replicas=3;"
	@echo "                             перед запуском нужен make kill-broker, тогда видны ретраи"
	@echo "                             через debug-лог franz-go"
	@echo "make run-headers           - producer + consumer в одном процессе; пишет $(MESSAGES) record'ов"
	@echo "                             с traceparent/correlation-id/message-type/source-service,"
	@echo "                             тут же читает и печатает headers в табличке"
	@echo "make run-headers-producer  - только producer (для запуска в двух терминалах)"
	@echo "make run-headers-consumer  - только consumer (Ctrl+C для выхода)"
	@echo "make topic-create          - создать все три топика руками с нужными конфигами"
	@echo "make topic-delete          - удалить все три топика курса 02-05"
	@echo "make kill-broker           - docker stop $(KILL_BROKER); ISR падает до 2 для retriable-демки"
	@echo "make restore-broker        - docker start $(KILL_BROKER); ждёт восстановления ISR"
	@echo "make build                 - go build ./..."

run-errors:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/error-classes \
	  -mode=non-retriable \
	  -topic-non-retriable=$(TOPIC_NR) \
	  -payload=$(PAYLOAD)

run-errors-retriable:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/error-classes \
	  -mode=retriable \
	  -topic-retriable=$(TOPIC_R) \
	  -delivery-timeout=$(DELIVERY_TIMEOUT)

run-headers:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/headers-demo \
	  -mode=roundtrip \
	  -topic=$(TOPIC_HEADERS) \
	  -messages=$(MESSAGES)

run-headers-producer:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/headers-demo \
	  -mode=producer \
	  -topic=$(TOPIC_HEADERS) \
	  -messages=$(MESSAGES)

run-headers-consumer:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/headers-demo \
	  -mode=consumer \
	  -topic=$(TOPIC_HEADERS)

topic-create:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC_NR) --partitions 3 --replication-factor 3 \
	  --config max.message.bytes=1024
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC_R) --partitions 3 --replication-factor 3 \
	  --config min.insync.replicas=3
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC_HEADERS) --partitions 3 --replication-factor 3

topic-delete:
	@for t in $(TOPIC_NR) $(TOPIC_R) $(TOPIC_HEADERS); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --delete --topic $$t || true; \
	done

kill-broker:
	docker stop $(KILL_BROKER)
	@echo "$(KILL_BROKER) остановлен; ISR упал до 2, для acks=all включаются NOT_ENOUGH_REPLICAS"

restore-broker:
	docker start $(KILL_BROKER)
	@echo "$(KILL_BROKER) запущен; ждём, пока ISR вернётся к 3 (порядка 30 секунд):"
	@sleep 5
	@docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC_R)

build:
	go build ./...
