.PHONY: help topic-create topic-delete topic-load \
        run-auto run-sync run-async \
        group-describe-auto group-describe-sync group-describe-async \
        group-delete-auto group-delete-sync group-delete-async group-delete-all \
        count-auto count-sync count-async count-all \
        clean-logs clean build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
TOPIC             ?= lecture-03-02-commits
PARTITIONS        ?= 3
GROUP_AUTO        ?= lecture-03-02-auto
GROUP_SYNC        ?= lecture-03-02-sync
GROUP_ASYNC       ?= lecture-03-02-async
SEED_MESSAGES     ?= 30

# Crash после N обработанных записей. 0 — не падать (читать до Ctrl+C).
CRASH             ?= 0
WORK_DELAY        ?= 200ms
COMMIT_INTERVAL   ?= 2s
COMMIT_EVERY      ?= 500ms

help:
	@echo "make topic-create          - создать $(TOPIC) с $(PARTITIONS) партициями (RF=3)"
	@echo "make topic-delete          - удалить $(TOPIC)"
	@echo "make topic-load            - залить $(SEED_MESSAGES) сообщений в $(TOPIC) (ключи k-0..k-N)"
	@echo ""
	@echo "make run-auto              - auto-commit consumer (CRASH=N — упасть после N обработанных)"
	@echo "make run-sync              - manual-sync consumer  (CRASH=N — упасть до commit'а батча)"
	@echo "make run-async             - manual-async consumer (CRASH=N — упасть до flush mark'ов)"
	@echo ""
	@echo "make group-describe-auto   - committed offsets для GROUP_AUTO ($(GROUP_AUTO))"
	@echo "make group-describe-sync   - committed offsets для GROUP_SYNC ($(GROUP_SYNC))"
	@echo "make group-describe-async  - committed offsets для GROUP_ASYNC ($(GROUP_ASYNC))"
	@echo ""
	@echo "make group-delete-auto     - удалить committed offset'ы GROUP_AUTO (читать заново с начала)"
	@echo "make group-delete-sync     - то же для GROUP_SYNC"
	@echo "make group-delete-async    - то же для GROUP_ASYNC"
	@echo "make group-delete-all      - сбросить все три группы"
	@echo ""
	@echo "make count-auto            - сколько в processed-auto.log всего и сколько уникальных offset'ов"
	@echo "make count-sync            - то же для processed-sync.log"
	@echo "make count-async           - то же для processed-async.log"
	@echo "make count-all             - всё подряд"
	@echo ""
	@echo "make clean-logs            - удалить processed-*.log"
	@echo "make clean                 - clean-logs + group-delete-all + topic-delete"
	@echo "make build                 - go build ./..."
	@echo ""
	@echo "Переопределяемые переменные:"
	@echo "  CRASH=10           — упасть после 10 обработанных записей"
	@echo "  SEED_MESSAGES=50   — сколько сообщений лить в topic-load"
	@echo "  WORK_DELAY=500ms   — sleep на каждое сообщение"

topic-create:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions $(PARTITIONS) --replication-factor 3

topic-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC) || true

topic-load:
	@echo "льём $(SEED_MESSAGES) сообщений в $(TOPIC)..."
	@for i in $$(seq 1 $(SEED_MESSAGES)); do \
	  echo "k-$$i:msg-$$i"; \
	done | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
	  --bootstrap-server kafka-1:9092 --topic $(TOPIC) \
	  --property parse.key=true --property key.separator=:

run-auto:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/auto-commit \
	  -topic=$(TOPIC) -group=$(GROUP_AUTO) \
	  -work-delay=$(WORK_DELAY) -commit-interval=$(COMMIT_INTERVAL) \
	  -crash-after=$(CRASH)

run-sync:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/manual-sync \
	  -topic=$(TOPIC) -group=$(GROUP_SYNC) \
	  -work-delay=$(WORK_DELAY) \
	  -crash-after=$(CRASH)

run-async:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/manual-async \
	  -topic=$(TOPIC) -group=$(GROUP_ASYNC) \
	  -work-delay=$(WORK_DELAY) -commit-every=$(COMMIT_EVERY) \
	  -crash-after=$(CRASH)

group-describe-auto:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --describe --group $(GROUP_AUTO) || true

group-describe-sync:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --describe --group $(GROUP_SYNC) || true

group-describe-async:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --describe --group $(GROUP_ASYNC) || true

group-delete-auto:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(GROUP_AUTO) || true

group-delete-sync:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(GROUP_SYNC) || true

group-delete-async:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(GROUP_ASYNC) || true

group-delete-all: group-delete-auto group-delete-sync group-delete-async

count-auto:
	@$(MAKE) -s _count F=processed-auto.log

count-sync:
	@$(MAKE) -s _count F=processed-sync.log

count-async:
	@$(MAKE) -s _count F=processed-async.log

count-all: count-auto count-sync count-async

# _count — внутренняя цель; печатает по файлу total/unique/duplicates.
_count:
	@if [ ! -f $(F) ]; then echo "$(F): файл не найден"; exit 0; fi; \
	  total=$$(wc -l < $(F) | tr -d ' '); \
	  uniq=$$(awk -F, '{print $$1","$$2}' $(F) | sort -u | wc -l | tr -d ' '); \
	  dup=$$((total - uniq)); \
	  echo "$(F): total=$$total unique-offsets=$$uniq duplicates=$$dup"

clean-logs:
	rm -f processed-auto.log processed-sync.log processed-async.log

clean: clean-logs group-delete-all topic-delete

build:
	go build ./...
