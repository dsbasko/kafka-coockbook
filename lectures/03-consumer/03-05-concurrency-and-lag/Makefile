.PHONY: help \
        topic-create topic-delete topic-describe \
        run-seq run-pool run-lag seed-fast \
        group-describe-seq group-describe-pool group-delete-all \
        clean build

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094
TOPIC             ?= lecture-03-05-events
PARTITIONS        ?= 6
GROUP_SEQ         ?= lecture-03-05-sequential
GROUP_POOL        ?= lecture-03-05-pool

# параметры обработки
WORK_DELAY        ?= 10ms
WORKERS           ?= 8
COMMIT_INTERVAL   ?= 500ms

# параметры seed
SEED_MESSAGES     ?= 100000
SEED_KEYS         ?= 32

# параметры lag-watcher
LAG_GROUP         ?= $(GROUP_SEQ)
LAG_INTERVAL      ?= 2s

help:
	@echo "make topic-create          - создать $(TOPIC) с $(PARTITIONS) партициями (RF=3)"
	@echo "make topic-delete          - удалить $(TOPIC)"
	@echo "make topic-describe        - leader/replicas/ISR per-partition"
	@echo ""
	@echo "make seed-fast             - залить $(SEED_MESSAGES) сообщений с $(SEED_KEYS) ключами (Go-producer)"
	@echo ""
	@echo "make run-seq               - sequential консьюмер (group=$(GROUP_SEQ), work-delay=$(WORK_DELAY))"
	@echo "make run-pool              - concurrent-pool (group=$(GROUP_POOL), workers=$(WORKERS))"
	@echo "make run-lag               - lag-watcher на group=$(LAG_GROUP), интервал $(LAG_INTERVAL)"
	@echo ""
	@echo "make group-describe-seq    - committed offsets sequential-группы"
	@echo "make group-describe-pool   - committed offsets pool-группы"
	@echo "make group-delete-all      - сбросить committed offset'ы обеих групп"
	@echo ""
	@echo "make clean                 - group-delete-all + topic-delete"
	@echo "make build                 - go build ./..."
	@echo ""
	@echo "Переопределяемые переменные:"
	@echo "  PARTITIONS=12      — больше партиций, больше потолок параллелизма"
	@echo "  WORK_DELAY=50ms    — тяжелее обработка одного сообщения"
	@echo "  WORKERS=16         — больше worker'ов в пуле"
	@echo "  SEED_MESSAGES=500000 SEED_KEYS=128 — нагрузка побольше"
	@echo "  LAG_GROUP=$(GROUP_POOL) — следить за пулом вместо sequential"

topic-create:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --create --if-not-exists \
	  --topic $(TOPIC) --partitions $(PARTITIONS) --replication-factor 3

topic-delete:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --delete --topic $(TOPIC) || true

topic-describe:
	docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 --describe --topic $(TOPIC)

seed-fast:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/seed \
	  -topic=$(TOPIC) -messages=$(SEED_MESSAGES) -keys=$(SEED_KEYS)

run-seq:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/sequential \
	  -topic=$(TOPIC) -group=$(GROUP_SEQ) -work-delay=$(WORK_DELAY)

run-pool:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/concurrent-pool \
	  -topic=$(TOPIC) -group=$(GROUP_POOL) -workers=$(WORKERS) \
	  -work-delay=$(WORK_DELAY) -commit-interval=$(COMMIT_INTERVAL)

run-lag:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) go run ./cmd/lag-watcher \
	  -group=$(LAG_GROUP) -interval=$(LAG_INTERVAL)

group-describe-seq:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --describe --group $(GROUP_SEQ) || true

group-describe-pool:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --describe --group $(GROUP_POOL) || true

group-delete-all:
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(GROUP_SEQ) || true
	docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
	  --bootstrap-server kafka-1:9092 --delete --group $(GROUP_POOL) || true

clean: group-delete-all topic-delete

build:
	go build ./...
