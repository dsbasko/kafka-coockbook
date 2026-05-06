.PHONY: help proto-gen proto-lint proto-breaking topic-create topic-describe topic-delete run-producer run-consumer build clean

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094
TOPIC           ?= lecture-05-02-orders-proto
GROUP           ?= lecture-05-02-consumer
COUNT           ?= 10

help:
	@echo "make proto-gen        - сгенерировать gen/orders/v1/order.pb.go из proto/"
	@echo "make proto-lint       - buf lint (STANDARD set)"
	@echo "make proto-breaking   - buf breaking против HEAD~1 (для git-репо)"
	@echo ""
	@echo "make topic-create     - создать топик $(TOPIC) (3 партиции, RF=3)"
	@echo "make topic-describe   - kafka-topics --describe"
	@echo "make topic-delete     - удалить топик"
	@echo ""
	@echo "make run-producer     - произвести COUNT=$(COUNT) Order'ов в $(TOPIC)"
	@echo "make run-consumer     - читать $(TOPIC) в группе $(GROUP), печатать структуры"
	@echo ""
	@echo "make build            - go build ./..."
	@echo "make clean            - topic-delete + удалить gen/"
	@echo ""
	@echo "Переопределяемые переменные:"
	@echo "  TOPIC=other            - другой топик"
	@echo "  COUNT=1000             - больше сообщений за один прогон"

# proto-gen — основной workflow лекции. Требует buf и protoc-gen-go в PATH:
#   go install github.com/bufbuild/buf/cmd/buf@latest
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
proto-gen:
	buf generate

proto-lint:
	buf lint

# proto-breaking — практическая защита от ломающих изменений. На свежем
# репозитории сравнивает текущее состояние proto/ с HEAD~1; если HEAD~1
# не существует (первый коммит), вернётся ошибка — это нормально.
proto-breaking:
	buf breaking --against ".git#ref=HEAD~1"

topic-create:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions 3 --replication-factor 3

topic-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC)

topic-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC) || true

run-producer:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/producer \
	  -topic=$(TOPIC) -count=$(COUNT)

run-consumer:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/consumer \
	  -topic=$(TOPIC) -group=$(GROUP)

build:
	go build ./...

clean: topic-delete
	rm -rf gen
