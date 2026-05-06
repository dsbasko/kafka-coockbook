.PHONY: help proto-gen proto-lint topic-create topic-describe topic-delete \
        run-producer run-consumer build clean \
        sr-list-subjects sr-describe sr-delete-subject kcat-decoded

KAFKA_BOOTSTRAP     ?= localhost:19092,localhost:19093,localhost:19094
SCHEMA_REGISTRY_URL ?= http://localhost:8081
TOPIC               ?= lecture-05-03-orders-sr
SUBJECT             ?= $(TOPIC)-value
GROUP               ?= lecture-05-03-consumer
COUNT               ?= 10

help:
	@echo "make proto-gen           - сгенерировать gen/orders/v1/order.pb.go"
	@echo "make proto-lint          - buf lint"
	@echo ""
	@echo "make topic-create        - создать $(TOPIC) (3 партиции, RF=3)"
	@echo "make topic-describe      - kafka-topics --describe"
	@echo "make topic-delete        - удалить топик"
	@echo ""
	@echo "make run-producer        - произвести COUNT=$(COUNT) Order'ов с magic byte+schema_id"
	@echo "make run-consumer        - читать $(TOPIC), вытаскивать schema_id из каждого record'а"
	@echo ""
	@echo "make sr-list-subjects    - GET /subjects"
	@echo "make sr-describe         - GET /subjects/$(SUBJECT)/versions/latest"
	@echo "make sr-delete-subject   - DELETE /subjects/$(SUBJECT) (hard delete)"
	@echo "make kcat-decoded        - kcat -s value=s -r ... (если установлен kcat)"
	@echo ""
	@echo "make build               - go build ./..."
	@echo "make clean               - topic-delete + sr-delete-subject + удалить gen/"
	@echo ""
	@echo "Переопределяемые переменные:"
	@echo "  TOPIC=other            - другой топик"
	@echo "  COUNT=1000             - больше сообщений за один прогон"

# proto-gen — требует buf и protoc-gen-go в PATH:
#   go install github.com/bufbuild/buf/cmd/buf@latest
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
proto-gen:
	buf generate

proto-lint:
	buf lint

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
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
	  go run ./cmd/producer -topic=$(TOPIC) -count=$(COUNT)

run-consumer:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
	  go run ./cmd/consumer -topic=$(TOPIC) -group=$(GROUP)

sr-list-subjects:
	@curl -sS $(SCHEMA_REGISTRY_URL)/subjects | jq .

sr-describe:
	@curl -sS $(SCHEMA_REGISTRY_URL)/subjects/$(SUBJECT)/versions/latest | jq .

# hard-delete: сначала soft (удаляет subject), потом ?permanent=true (стирает id из SR).
sr-delete-subject:
	@curl -sS -X DELETE $(SCHEMA_REGISTRY_URL)/subjects/$(SUBJECT) || true
	@curl -sS -X DELETE $(SCHEMA_REGISTRY_URL)/subjects/$(SUBJECT)?permanent=true || true

# kcat при наличии в системе печатает декодированные через SR значения. Полезно
# как cross-check, что наши байты — это валидный Confluent wire format.
kcat-decoded:
	kcat -b $(KAFKA_BOOTSTRAP) -t $(TOPIC) -C -e -o beginning \
	  -s value=s -r $(SCHEMA_REGISTRY_URL)

build:
	go build ./...

clean: topic-delete sr-delete-subject
	rm -rf gen
