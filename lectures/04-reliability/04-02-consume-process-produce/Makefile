.PHONY: help \
        topic-create-all topic-delete-all topic-describe \
        seed seed-vip end-offsets \
        run-pipeline run-pipeline-crash \
        run-downstream run-downstream-ru \
        verify group-describe group-delete \
        clean build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
PARTITIONS        ?= 3
RF                ?= 3

INPUT             ?= cpp-orders
OUTPUT            ?= cpp-orders-enriched

TXN_ID            ?= lecture-04-02-pipeline
GROUP             ?= lecture-04-02-pipeline
DOWNSTREAM_GROUP  ?= lecture-04-02-downstream

SEED_COUNT        ?= 100
CRASH_PROB        ?= 0.3
IDLE              ?= 8s

TOPICS            := $(INPUT) $(OUTPUT)

help:
	@echo "make topic-create-all          - создать $(INPUT) и $(OUTPUT) (P=$(PARTITIONS) RF=$(RF))"
	@echo "make topic-delete-all          - удалить оба топика"
	@echo "make topic-describe            - kafka-topics --describe для обоих"
	@echo ""
	@echo "make seed                      - залить $(SEED_COUNT) сообщений в $(INPUT) (key=k-i)"
	@echo "make seed-vip                  - то же, но половина с префиксом vip-* (флаг vip:true в обогащении)"
	@echo "make end-offsets               - latest offset per partition по обоим топикам"
	@echo ""
	@echo "make run-pipeline              - cpp-pipeline без крашей (crash-prob=0)"
	@echo "make run-pipeline-crash        - cpp-pipeline с crash-prob=$(CRASH_PROB), запускай повторно до 'idle X — больше нет'"
	@echo ""
	@echo "make run-downstream            - read_committed-консьюмер на $(OUTPUT)"
	@echo "make run-downstream-ru         - read_uncommitted — для сравнения, видит и аборнутые записи"
	@echo ""
	@echo "make verify                    - сравнить count в $(INPUT) и в read_committed-консьюмере по $(OUTPUT)"
	@echo "make group-describe            - committed offsets по $(GROUP) и $(DOWNSTREAM_GROUP)"
	@echo "make group-delete              - сбросить committed offset'ы обеих групп"
	@echo ""
	@echo "make clean                     - удалить топики и группы"
	@echo "make build                     - go build ./..."
	@echo ""
	@echo "Переопределяемые переменные:"
	@echo "  SEED_COUNT=500 CRASH_PROB=0.5  — больше нагрузки и крашей"
	@echo "  IDLE=2s                        — короче таймаут на холостой polling"

topic-create-all:
	@for t in $(TOPICS); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --create --if-not-exists \
	    --topic $$t --partitions $(PARTITIONS) --replication-factor $(RF) ; \
	done

topic-delete-all:
	@for t in $(TOPICS); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --delete --topic $$t || true ; \
	done

topic-describe:
	@for t in $(TOPICS); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --describe --topic $$t ; \
	  echo "" ; \
	done

end-offsets:
	@for t in $(TOPICS); do \
	  echo "=== $$t ===" ; \
	  docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
	    --bootstrap-server kafka-1:9092 --topic $$t --time -1 ; \
	done

seed:
	@echo "льём $(SEED_COUNT) сообщений в $(INPUT)..."
	@for i in $$(seq 1 $(SEED_COUNT)); do \
	  echo "k-$$i:order-$$i"; \
	done | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
	  --bootstrap-server kafka-1:9092 --topic $(INPUT) \
	  --property parse.key=true --property key.separator=:

seed-vip:
	@echo "льём $(SEED_COUNT) сообщений в $(INPUT) (50% vip-*)..."
	@for i in $$(seq 1 $(SEED_COUNT)); do \
	  if [ $$((i % 2)) -eq 0 ]; then \
	    echo "vip-$$i:order-$$i"; \
	  else \
	    echo "k-$$i:order-$$i"; \
	  fi; \
	done | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
	  --bootstrap-server kafka-1:9092 --topic $(INPUT) \
	  --property parse.key=true --property key.separator=:

run-pipeline:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/cpp-pipeline \
	  -transactional-id=$(TXN_ID) -group=$(GROUP) \
	  -input=$(INPUT) -output=$(OUTPUT) \
	  -crash-prob=0 -idle=$(IDLE)

run-pipeline-crash:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/cpp-pipeline \
	  -transactional-id=$(TXN_ID) -group=$(GROUP) \
	  -input=$(INPUT) -output=$(OUTPUT) \
	  -crash-prob=$(CRASH_PROB) -idle=$(IDLE)

run-downstream:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/downstream-rc \
	  -topic=$(OUTPUT) -group=$(DOWNSTREAM_GROUP) -isolation=committed -idle=$(IDLE)

run-downstream-ru:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/downstream-rc \
	  -topic=$(OUTPUT) -group=$(DOWNSTREAM_GROUP)-ru -isolation=uncommitted -idle=$(IDLE)

verify:
	@echo "=== input $(INPUT) end-offset ==="
	@docker exec kafka-1 /opt/kafka/bin/kafka-get-offsets.sh \
	  --bootstrap-server kafka-1:9092 --topic $(INPUT) --time -1
	@echo ""
	@echo "=== output $(OUTPUT) (read_committed через downstream-rc) ==="
	@KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/downstream-rc \
	  -topic=$(OUTPUT) -group=$(DOWNSTREAM_GROUP)-verify-$$$$ \
	  -isolation=committed -idle=$(IDLE) | tail -8

group-describe:
	@for g in $(GROUP) $(DOWNSTREAM_GROUP) ; do \
	  echo "=== $$g ===" ; \
	  docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	    --bootstrap-server kafka-1:9092 --describe --group $$g || true ; \
	done

group-delete:
	@for g in $(GROUP) $(DOWNSTREAM_GROUP) $(DOWNSTREAM_GROUP)-ru ; do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	    --bootstrap-server kafka-1:9092 --delete --group $$g || true ; \
	done

clean: group-delete topic-delete-all

build:
	go build ./...
