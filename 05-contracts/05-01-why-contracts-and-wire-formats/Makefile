.PHONY: help proto-gen run topic-create-all topic-delete-all topic-describe du-topics build clean

KAFKA_BOOTSTRAP ?= localhost:19092,localhost:19093,localhost:19094

JSON_TOPIC      ?= lecture-05-01-orders-json
AVRO_TOPIC      ?= lecture-05-01-orders-avro
PROTO_TOPIC     ?= lecture-05-01-orders-proto

# Объём прогона. По умолчанию 100k Order'ов на каждый топик — достаточно,
# чтобы разница в размере была наглядной за 5–15 секунд.
COUNT           ?= 100000
ITEMS           ?= 3

help:
	@echo "make proto-gen          - в этой лекции no-op: 05-01 показывает wire-формат"
	@echo "                          вручную через protowire. Полноценный buf-pipeline в 05-02."
	@echo ""
	@echo "make topic-create-all   - пересоздать три топика: $(JSON_TOPIC) / $(AVRO_TOPIC) / $(PROTO_TOPIC)"
	@echo "make topic-delete-all   - удалить все три"
	@echo "make topic-describe     - kafka-topics --describe для всех трёх"
	@echo ""
	@echo "make run                - format-bench: пишет COUNT=$(COUNT) Order'ов в каждый формат и"
	@echo "                          печатает таблицу payload bytes + on-disk bytes"
	@echo "make du-topics          - du -sh по партициям трёх топиков на kafka-1/2/3"
	@echo ""
	@echo "make build              - go build ./..."
	@echo "make clean              - topic-delete-all"
	@echo ""
	@echo "Переопределяемые переменные:"
	@echo "  COUNT=1000000           - больше Order'ов; разница форматов масштабируется линейно"
	@echo "  ITEMS=10                - больше OrderItem'ов в каждом — растёт repeated-секция в proto"

# 05-01 не использует кодген. Файл proto/order.proto — это эталонный
# контракт, который читают люди и по которому свёрстан encodeProto в
# cmd/format-bench/main.go. Полная генерация .pb.go и Go-структур через
# buf — в 05-02. Здесь — no-op с явным сообщением.
proto-gen:
	@echo "proto-gen в 05-01 — no-op."
	@echo "wire-формат собирается вручную в encodeProto (см. cmd/format-bench/main.go)"
	@echo "и сверяется с тегами из proto/order.proto."
	@echo "Полный buf-pipeline и кодген — в лекции 05-02."

topic-create-all:
	@for t in $(JSON_TOPIC) $(AVRO_TOPIC) $(PROTO_TOPIC); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --create --if-not-exists \
	    --topic $$t --partitions 1 --replication-factor 3; \
	done

topic-delete-all:
	@for t in $(JSON_TOPIC) $(AVRO_TOPIC) $(PROTO_TOPIC); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --delete --topic $$t || true; \
	done

topic-describe:
	@for t in $(JSON_TOPIC) $(AVRO_TOPIC) $(PROTO_TOPIC); do \
	  echo "--- $$t ---"; \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --describe --topic $$t || true; \
	done

# run пересоздаёт топики внутри format-bench (через kadm), так что отдельный
# topic-create-all не обязателен — но он удобен, если хочется только
# создать пустые топики и потыкать в kafka-ui.
run:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/format-bench \
	  -count=$(COUNT) -items=$(ITEMS) \
	  -json-topic=$(JSON_TOPIC) \
	  -avro-topic=$(AVRO_TOPIC) \
	  -proto-topic=$(PROTO_TOPIC)

# du-topics — независимое подтверждение чисел из format-bench. Лезем
# в /var/lib/kafka/data на каждом брокере и считаем размеры партиций
# нужных топиков. На стенде RF=3, так что суммарный размер по всем
# брокерам ~ в 3 раза больше payload'а из run-вывода.
du-topics:
	@for t in $(JSON_TOPIC) $(AVRO_TOPIC) $(PROTO_TOPIC); do \
	  echo "=== $$t ==="; \
	  for n in 1 2 3; do \
	    sz=$$(docker exec kafka-$$n sh -c "du -sb /var/lib/kafka/data/$$t-0 2>/dev/null | cut -f1"); \
	    [ -z "$$sz" ] && sz="0 (нет реплики на этой ноде)"; \
	    echo "  kafka-$$n: $$sz bytes"; \
	  done; \
	done

build:
	go build ./...

clean: topic-delete-all
