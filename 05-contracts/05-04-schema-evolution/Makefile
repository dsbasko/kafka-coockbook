.PHONY: help proto-gen proto-lint proto-breaking-check \
        topic-create-v1 topic-create-v3 topic-describe topic-delete \
        register-v1 register-v3 try-register-v4 \
        sr-list-versions-v3 sr-get-config-v3 sr-set-compat-backward-v3 sr-set-compat-none-v3 \
        sr-delete-subject-v1 sr-delete-subject-v3 \
        run-producer-v1 run-producer-v3 run-consumer-v1 \
        build clean

KAFKA_BOOTSTRAP     ?= localhost:19092,localhost:19093,localhost:19094
SCHEMA_REGISTRY_URL ?= http://localhost:8081

# У нас два логически разных subject'а — для v1 и v3. У каждого свой
# топик; имя subject'а = `<topic>-value` по TopicNameStrategy. v3 ещё
# участвует в compat-демо: туда пытаемся встать v4_breaking и получаем
# 409.
TOPIC_V1            ?= lecture-05-04-orders-v1
TOPIC_V3            ?= lecture-05-04-orders-v3
SUBJECT_V1          ?= $(TOPIC_V1)-value
SUBJECT_V3          ?= $(TOPIC_V3)-value
GROUP               ?= lecture-05-04-consumer-v1
COUNT               ?= 5

help:
	@echo "make proto-gen                  - сгенерировать gen/orders/{v1,v2,v3}/order.pb.go"
	@echo "make proto-lint                 - buf lint"
	@echo "make proto-breaking-check       - buf breaking сравнит v3 и v4_breaking"
	@echo ""
	@echo "make topic-create-v1            - создать $(TOPIC_V1)"
	@echo "make topic-create-v3            - создать $(TOPIC_V3)"
	@echo "make topic-describe             - kafka-topics --describe для обоих топиков"
	@echo "make topic-delete               - удалить оба топика"
	@echo ""
	@echo "make register-v1                - зарегистрировать v1 в SR (subject $(SUBJECT_V1))"
	@echo "make register-v3                - зарегистрировать v3 в SR (subject $(SUBJECT_V3))"
	@echo "make try-register-v4            - встать v4_breaking версией 2 в subject $(SUBJECT_V3) (ждём 409)"
	@echo "make sr-list-versions-v3        - GET /subjects/$(SUBJECT_V3)/versions"
	@echo "make sr-get-config-v3           - текущий compatibility mode subject'а v3"
	@echo "make sr-set-compat-backward-v3  - установить BACKWARD для v3-subject (дефолт)"
	@echo "make sr-set-compat-none-v3      - снять проверку, чтобы зарегистрировать что угодно"
	@echo "make sr-delete-subject-v1       - hard-delete subject'а v1"
	@echo "make sr-delete-subject-v3       - hard-delete subject'а v3"
	@echo ""
	@echo "make run-producer-v1            - произвести COUNT=$(COUNT) Order'ов по схеме v1"
	@echo "make run-producer-v3            - произвести COUNT=$(COUNT) Order'ов по схеме v3"
	@echo "make run-consumer-v1            - читать $(TOPIC_V3) и разбирать в *.v1.Order"
	@echo ""
	@echo "make build                      - go build ./..."
	@echo "make clean                      - удалить топики, subject'ы и gen/"

proto-gen:
	buf generate

proto-lint:
	buf lint

# proto-breaking-check копирует v3 и v4_breaking в tmp-каталог, собирает
# их в buf-image'ы и сравнивает. Они оба объявляют `package orders.v3`
# — без этого buf не нашёл бы общего предка. Buf breaking возвращает
# 100 при найденных нарушениях, а в нашем демо это и есть желаемый
# исход; код инвертируем.
proto-breaking-check:
	@tmpdir=$$(mktemp -d); \
	  trap 'rm -rf $$tmpdir' EXIT; \
	  mkdir -p $$tmpdir/v3 $$tmpdir/v4; \
	  cp proto/orders/v3/order.proto $$tmpdir/v3/; \
	  cp proto/orders/v4_breaking/order.proto $$tmpdir/v4/; \
	  ( cd $$tmpdir && \
	      buf build v3 -o v3.bin && \
	      buf build v4 -o v4.bin && \
	      buf breaking v4.bin --against v3.bin ); \
	  rc=$$?; \
	  if [ $$rc -eq 0 ]; then \
	    echo "FAIL: ожидали breaking changes между v3 и v4_breaking, buf не нашёл"; \
	    exit 1; \
	  else \
	    echo ""; \
	    echo "OK: buf корректно зарепортил несовместимость v3 → v4_breaking"; \
	    exit 0; \
	  fi

topic-create-v1:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC_V1) --partitions 3 --replication-factor 3

topic-create-v3:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC_V3) --partitions 3 --replication-factor 3

topic-describe:
	@for t in $(TOPIC_V1) $(TOPIC_V3); do \
	  echo "=== $$t ==="; \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --describe --topic $$t; \
	done

topic-delete:
	@for t in $(TOPIC_V1) $(TOPIC_V3); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --delete --topic $$t || true; \
	done

# register-v1 / register-v3 — POST схемы в SR из соответствующего файла.
# proto-схемы в curl-payload экранируются через jq -Rs, чтобы не
# страдать с переводами строк и кавычками.
register-v1:
	@payload=$$(jq -Rs '{schemaType: "PROTOBUF", schema: .}' < proto/orders/v1/order.proto); \
	  curl -sS -X POST \
	    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
	    --data "$$payload" \
	    $(SCHEMA_REGISTRY_URL)/subjects/$(SUBJECT_V1)/versions | jq .

register-v3:
	@payload=$$(jq -Rs '{schemaType: "PROTOBUF", schema: .}' < proto/orders/v3/order.proto); \
	  curl -sS -X POST \
	    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
	    --data "$$payload" \
	    $(SCHEMA_REGISTRY_URL)/subjects/$(SUBJECT_V3)/versions | jq .

# try-register-v4: ждём HTTP 409 Conflict + body вида
# {"error_code":409, "message":"Schema being registered is incompatible..."}.
# Печатаем код, чтобы сразу было видно — отбилось.
try-register-v4:
	@payload=$$(jq -Rs '{schemaType: "PROTOBUF", schema: .}' < proto/orders/v4_breaking/order.proto); \
	  http_code=$$(curl -sS -o /tmp/sr-resp.json -w "%{http_code}" -X POST \
	    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
	    --data "$$payload" \
	    $(SCHEMA_REGISTRY_URL)/subjects/$(SUBJECT_V3)/versions); \
	  echo "HTTP $$http_code"; \
	  jq . < /tmp/sr-resp.json; \
	  if [ "$$http_code" = "409" ]; then \
	    echo "OK: SR корректно отверг v4_breaking как несовместимый"; \
	  else \
	    echo "WARN: ожидали 409 Conflict, получили $$http_code"; \
	  fi

sr-set-compat-backward-v3:
	@curl -sS -X PUT \
	  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
	  --data '{"compatibility":"BACKWARD"}' \
	  $(SCHEMA_REGISTRY_URL)/config/$(SUBJECT_V3) | jq .

sr-set-compat-none-v3:
	@curl -sS -X PUT \
	  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
	  --data '{"compatibility":"NONE"}' \
	  $(SCHEMA_REGISTRY_URL)/config/$(SUBJECT_V3) | jq .

sr-get-config-v3:
	@curl -sS $(SCHEMA_REGISTRY_URL)/config/$(SUBJECT_V3) | jq .

sr-list-versions-v3:
	@curl -sS $(SCHEMA_REGISTRY_URL)/subjects/$(SUBJECT_V3)/versions | jq .

sr-delete-subject-v1:
	@curl -sS -X DELETE $(SCHEMA_REGISTRY_URL)/subjects/$(SUBJECT_V1) || true
	@curl -sS -X DELETE $(SCHEMA_REGISTRY_URL)/subjects/$(SUBJECT_V1)?permanent=true || true

sr-delete-subject-v3:
	@curl -sS -X DELETE $(SCHEMA_REGISTRY_URL)/subjects/$(SUBJECT_V3) || true
	@curl -sS -X DELETE $(SCHEMA_REGISTRY_URL)/subjects/$(SUBJECT_V3)?permanent=true || true

run-producer-v1:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
	  go run ./cmd/producer-v1 -topic=$(TOPIC_V1) -count=$(COUNT)

run-producer-v3:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) SCHEMA_REGISTRY_URL=$(SCHEMA_REGISTRY_URL) \
	  go run ./cmd/producer-v3 -topic=$(TOPIC_V3) -count=$(COUNT)

run-consumer-v1:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
	  go run ./cmd/consumer-v1 -topic=$(TOPIC_V3) -group=$(GROUP)

build:
	go build ./...

clean: topic-delete sr-delete-subject-v1 sr-delete-subject-v3
	rm -rf gen
