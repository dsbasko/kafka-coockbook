.PHONY: help \
        topic-create-all topic-delete-all topic-describe \
        run-tx-producer run-tx-producer-mostly-abort \
        run-zombie-1 run-zombie-2 \
        run-rc-consumer run-ru-consumer \
        end-offsets clean build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
PARTITIONS        ?= 3
RF                ?= 3

TXN_ID            ?= lecture-04-01-tx-producer
ZOMBIE_TXN_ID     ?= lecture-04-01-zombie

ATTEMPTS          ?= 10
COMMIT_PROB       ?= 0.7
PAUSE             ?= 200ms

GROUP_RC          ?= lecture-04-01-rc
GROUP_RU          ?= lecture-04-01-ru
COUNT             ?= 0
IDLE              ?= 5s

TOPICS            := tx-orders tx-payments tx-shipments

help:
	@echo "make topic-create-all          - создать tx-orders, tx-payments, tx-shipments (P=$(PARTITIONS) RF=$(RF))"
	@echo "make topic-delete-all          - удалить все три топика"
	@echo "make topic-describe            - kafka-topics --describe для всех трёх"
	@echo "make end-offsets               - latest offset (включая markers) per partition"
	@echo ""
	@echo "make run-tx-producer           - $(ATTEMPTS) транзакций, commit с вероятностью $(COMMIT_PROB)"
	@echo "make run-tx-producer-mostly-abort - то же, но почти все aborted (commit-prob=0.2)"
	@echo ""
	@echo "make run-zombie-1              - первый процесс (txn-id=$(ZOMBIE_TXN_ID), role=first)"
	@echo "make run-zombie-2              - второй процесс с тем же txn-id (role=second) — выгоняет первого"
	@echo ""
	@echo "make run-rc-consumer           - read_committed (group=$(GROUP_RC), idle=$(IDLE))"
	@echo "make run-ru-consumer           - read_uncommitted (group=$(GROUP_RU))"
	@echo ""
	@echo "make clean                     - удалить consumer группы и топики"
	@echo "make build                     - go build ./..."
	@echo ""
	@echo "Переопределяемые переменные:"
	@echo "  ATTEMPTS=50 COMMIT_PROB=0.9   — больше транзакций, реже abort'ы"
	@echo "  COUNT=20                       — выйти из rc/ru после 20 прочитанных"
	@echo "  IDLE=2s                        — выйти, если 2 секунды ничего не приходит"

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

run-tx-producer:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/transactional-producer \
	  -transactional-id=$(TXN_ID) -attempts=$(ATTEMPTS) -commit-prob=$(COMMIT_PROB) -pause=$(PAUSE)

run-tx-producer-mostly-abort:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/transactional-producer \
	  -transactional-id=$(TXN_ID) -attempts=$(ATTEMPTS) -commit-prob=0.2 -pause=$(PAUSE)

run-zombie-1:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/zombie-fence \
	  -transactional-id=$(ZOMBIE_TXN_ID) -role=first

run-zombie-2:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/zombie-fence \
	  -transactional-id=$(ZOMBIE_TXN_ID) -role=second

run-rc-consumer:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/read-committed \
	  -group=$(GROUP_RC) -isolation=committed -count=$(COUNT) -idle=$(IDLE)

run-ru-consumer:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/read-committed \
	  -group=$(GROUP_RU) -isolation=uncommitted -count=$(COUNT) -idle=$(IDLE)

clean:
	@for g in $(GROUP_RC) $(GROUP_RU) ; do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	    --bootstrap-server kafka-1:9092 --delete --group $$g || true ; \
	done
	$(MAKE) topic-delete-all

build:
	go build ./...
