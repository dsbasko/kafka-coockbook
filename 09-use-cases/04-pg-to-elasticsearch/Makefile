.PHONY: help up down logs ps \
        pg-init es-init db-shell es-shell es-indices pg-truncate es-delete \
        topic-create-all topic-delete-all \
        connect-plugin-check \
        connector-create-debezium connector-create-es-sink connector-create-es-sink-v2 connector-create-all \
        connector-status connector-restart connector-delete-all connector-list \
        run-load run-search alias-init reindex-blue-green alias-show \
        test-integration build clean

KAFKA_BOOTSTRAP   ?= localhost:19092,localhost:19093,localhost:19094

PG_CONTAINER      ?= usecase-09-04-postgres
PG_DB             ?= usecase_09_04
PG_USER           ?= usecase
PG_DSN            ?= postgres://usecase:usecase@localhost:15443/usecase_09_04?sslmode=disable

ES_CONTAINER      ?= usecase-09-04-elasticsearch
ES_HTTP           ?= http://localhost:19200

CONNECT_URL       ?= http://localhost:8083
CONNECTOR_DBZ     ?= usecase-09-04-debezium-pg-source
CONNECTOR_ES_V1   ?= usecase-09-04-es-sink
CONNECTOR_ES_V2   ?= usecase-09-04-es-sink-v2
CONFIG_DBZ        ?= connectors/debezium-pg-source.json
CONFIG_ES_V1      ?= connectors/es-sink.json
CONFIG_ES_V2      ?= connectors/es-sink-v2.json
ES_TEMPLATE       ?= es-template.json

PARTITIONS        ?= 3
RF                ?= 3

# CDC-топики Debezium плюс DLQ Sink'а. Создаём заранее, потому что
# Debezium при первом запуске может не успеть с RF/partitions, а Sink
# начинает читать сразу.
ALL_TOPICS = \
	search.public.products search.public.articles search.public.users \
	search-dlq

DLOAD_COUNT       ?= 50

help:
	@echo "make up                          - поднять Postgres (15443) + Elasticsearch (19200)"
	@echo "make down                        - остановить и удалить volumes"
	@echo "make logs                        - tail -f логов сервисов"
	@echo "make ps                          - статус контейнеров"
	@echo ""
	@echo "make pg-init                     - применить db/init.sql (таблицы + publication)"
	@echo "make es-init                     - залить index template (es-template.json)"
	@echo "make db-shell                    - psql внутри контейнера"
	@echo "make es-shell                    - alias для curl-ов с правильным URL"
	@echo "make es-indices                  - GET /_cat/indices?v"
	@echo "make pg-truncate                 - очистить products/articles/users"
	@echo "make es-delete                   - удалить индексы products_v*, articles_v*, users_v*"
	@echo ""
	@echo "make topic-create-all            - создать CDC-топики и DLQ"
	@echo "make topic-delete-all            - удалить их"
	@echo ""
	@echo "make connect-plugin-check        - проверить, что Debezium + ES Sink доступны"
	@echo "make connector-create-debezium   - создать source-connector ($(CONNECTOR_DBZ))"
	@echo "make connector-create-es-sink    - создать sink в индексы *_v1"
	@echo "make connector-create-es-sink-v2 - создать sink в индексы *_v2"
	@echo "make connector-create-all        - debezium + es-sink-v1"
	@echo "make connector-status            - статус всех созданных коннекторов"
	@echo "make connector-restart           - reset failed task'и"
	@echo "make connector-delete-all        - удалить коннекторы + drop replication slot"
	@echo "make connector-list              - GET /connectors"
	@echo ""
	@echo "make run-load DLOAD_COUNT=$(DLOAD_COUNT)  - залить нагрузку в Postgres"
	@echo "make run-search                  - search-tester против индекса products_v1"
	@echo ""
	@echo "make alias-init                  - создать alias products → products_v1 (идемпотентно)"
	@echo "make reindex-blue-green          - запустить v2 sink, дождаться, переключить alias"
	@echo "make alias-show                  - GET /_alias/products"
	@echo ""
	@echo "make test-integration            - go test -tags=integration ./test/... (E2E)"
	@echo ""
	@echo "make build                       - go build ./..."
	@echo "make clean                       - connector-delete-all + topic-delete-all + down"

up:
	docker compose -f docker-compose.override.yml up -d
	@echo "ждём Postgres..."
	@for i in $$(seq 1 30); do \
	  if docker exec $(PG_CONTAINER) pg_isready -U $(PG_USER) -d $(PG_DB) >/dev/null 2>&1; then \
	    echo "Postgres готов."; break; \
	  fi; \
	  sleep 1; \
	done
	@echo "ждём Elasticsearch..."
	@for i in $$(seq 1 60); do \
	  if curl -fsS $(ES_HTTP)/_cluster/health >/dev/null 2>&1; then \
	    echo "Elasticsearch готов."; exit 0; \
	  fi; \
	  sleep 1; \
	done; \
	echo "Elasticsearch не поднялся за 60 сек"; exit 1

down: connector-delete-all
	docker compose -f docker-compose.override.yml down -v

logs:
	docker compose -f docker-compose.override.yml logs -f

ps:
	docker compose -f docker-compose.override.yml ps

pg-init:
	docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) < db/init.sql

es-init:
	curl -fsS -X PUT $(ES_HTTP)/_index_template/usecase-09-04 \
	  -H 'Content-Type: application/json' \
	  --data @$(ES_TEMPLATE) | python3 -m json.tool

db-shell:
	docker exec -it $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB)

es-shell:
	@echo "ES URL: $(ES_HTTP)"
	@echo "Примеры:"
	@echo "  curl -s '$(ES_HTTP)/_cat/indices?v'"
	@echo "  curl -s '$(ES_HTTP)/products_v1/_count'"
	@echo "  curl -s -X POST '$(ES_HTTP)/products_v1/_search' -H 'Content-Type: application/json' -d '{\"query\":{\"match\":{\"name\":\"alpha\"}}}'"

es-indices:
	curl -fsS "$(ES_HTTP)/_cat/indices?v"

pg-truncate:
	docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  'TRUNCATE TABLE products, articles, users RESTART IDENTITY;'

es-delete:
	@for idx in products_v1 products_v2 articles_v1 articles_v2 users_v1 users_v2; do \
	  curl -fsS -X DELETE "$(ES_HTTP)/$$idx" >/dev/null 2>&1 || true; \
	done; echo "удалены products_v*, articles_v*, users_v*"

topic-create-all:
	@for t in $(ALL_TOPICS); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --create --if-not-exists \
	    --topic $$t --partitions $(PARTITIONS) --replication-factor $(RF) || true; \
	done

topic-delete-all:
	@for t in $(ALL_TOPICS); do \
	  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
	    --bootstrap-server kafka-1:9092 --delete --topic $$t || true; \
	done

connect-plugin-check:
	@echo "ищем io.debezium.connector.postgresql.PostgresConnector и io.confluent.connect.elasticsearch.ElasticsearchSinkConnector..."
	@curl -fsS $(CONNECT_URL)/connector-plugins | \
	  grep -q 'io.debezium.connector.postgresql.PostgresConnector' && \
	  echo "OK: Debezium доступен" || \
	  (echo "Debezium PostgresConnector НЕ найден. Запусти: make -C ../../ connect-install-plugins"; exit 1)
	@curl -fsS $(CONNECT_URL)/connector-plugins | \
	  grep -q 'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector' && \
	  echo "OK: ES Sink доступен" || \
	  (echo "ElasticsearchSinkConnector НЕ найден. Запусти: make -C ../../ connect-install-plugins"; exit 1)

connector-create-debezium:
	curl -fsS -X POST -H 'Content-Type: application/json' \
	  --data @$(CONFIG_DBZ) $(CONNECT_URL)/connectors | python3 -m json.tool

connector-create-es-sink:
	curl -fsS -X POST -H 'Content-Type: application/json' \
	  --data @$(CONFIG_ES_V1) $(CONNECT_URL)/connectors | python3 -m json.tool

connector-create-es-sink-v2:
	curl -fsS -X POST -H 'Content-Type: application/json' \
	  --data @$(CONFIG_ES_V2) $(CONNECT_URL)/connectors | python3 -m json.tool

connector-create-all: connector-create-debezium connector-create-es-sink

connector-status:
	@echo "=== $(CONNECTOR_DBZ) ==="
	@curl -fsS $(CONNECT_URL)/connectors/$(CONNECTOR_DBZ)/status | python3 -m json.tool || true
	@echo
	@echo "=== $(CONNECTOR_ES_V1) ==="
	@curl -fsS $(CONNECT_URL)/connectors/$(CONNECTOR_ES_V1)/status | python3 -m json.tool || true
	@echo
	@echo "=== $(CONNECTOR_ES_V2) ==="
	@curl -fsS $(CONNECT_URL)/connectors/$(CONNECTOR_ES_V2)/status 2>/dev/null | python3 -m json.tool || echo "(не создан)"

connector-restart:
	curl -fsS -X POST $(CONNECT_URL)/connectors/$(CONNECTOR_DBZ)/restart?includeTasks=true\&onlyFailed=true || true
	curl -fsS -X POST $(CONNECT_URL)/connectors/$(CONNECTOR_ES_V1)/restart?includeTasks=true\&onlyFailed=true || true
	curl -fsS -X POST $(CONNECT_URL)/connectors/$(CONNECTOR_ES_V2)/restart?includeTasks=true\&onlyFailed=true || true
	@$(MAKE) -s connector-status

connector-delete-all:
	curl -fsS -X DELETE $(CONNECT_URL)/connectors/$(CONNECTOR_DBZ) 2>/dev/null || true
	curl -fsS -X DELETE $(CONNECT_URL)/connectors/$(CONNECTOR_ES_V1) 2>/dev/null || true
	curl -fsS -X DELETE $(CONNECT_URL)/connectors/$(CONNECTOR_ES_V2) 2>/dev/null || true
	@docker exec -i $(PG_CONTAINER) psql -U $(PG_USER) -d $(PG_DB) -c \
	  "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'usecase_09_04_%';" 2>/dev/null || true

connector-list:
	curl -fsS $(CONNECT_URL)/connectors | python3 -m json.tool

run-load:
	PG_DSN='$(PG_DSN)' go run ./cmd/db-loader -count=$(DLOAD_COUNT)

run-search:
	PG_DSN='$(PG_DSN)' go run ./cmd/search-tester -es-url=$(ES_HTTP) -alias=products_v1

# alias-init: создаёт alias products → products_v1. Идемпотентно: ES
# при добавлении уже-существующей пары index+alias возвращает 200.
# Нужно как prerequisite для reindex-blue-green — атомарный _aliases с
# remove на несуществующем alias'е падает целиком и rolling back.
alias-init:
	@curl -fsS -X POST "$(ES_HTTP)/_aliases" -H 'Content-Type: application/json' -d '{ \
	  "actions": [ \
	    {"add": {"index": "products_v1", "alias": "products"}} \
	  ] \
	}' | python3 -m json.tool

# reindex-blue-green: создаём v2 sink (он начнёт писать в *_v2, читая
# с beginning через свежую consumer-group). Ждём, пока количество
# документов в v2 догонит v1, потом переключаем alias.
reindex-blue-green: alias-init
	@echo "=== создаём v2 sink (пишет в *_v2) ==="
	@$(MAKE) -s connector-create-es-sink-v2
	@echo
	@echo "=== ждём, пока products_v2 догонит products_v1 ==="
	@for i in $$(seq 1 60); do \
	  v1=$$(curl -fsS "$(ES_HTTP)/products_v1/_count" 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin).get("count",0))' 2>/dev/null || echo 0); \
	  v2=$$(curl -fsS "$(ES_HTTP)/products_v2/_count" 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin).get("count",0))' 2>/dev/null || echo 0); \
	  echo "  v1=$$v1 v2=$$v2"; \
	  if [ "$$v1" = "$$v2" ] && [ "$$v1" != "0" ]; then \
	    break; \
	  fi; \
	  sleep 2; \
	done
	@echo
	@echo "=== переключаем alias products → products_v2 ==="
	@curl -fsS -X POST "$(ES_HTTP)/_aliases" -H 'Content-Type: application/json' -d '{ \
	  "actions": [ \
	    {"remove": {"index": "products_v1", "alias": "products"}}, \
	    {"add":    {"index": "products_v2", "alias": "products"}} \
	  ] \
	}' | python3 -m json.tool
	@echo
	@echo "=== удаляем v1 sink ==="
	@curl -fsS -X DELETE $(CONNECT_URL)/connectors/$(CONNECTOR_ES_V1) 2>/dev/null || true
	@echo
	@echo "=== готово. alias products теперь указывает на products_v2 ==="

alias-show:
	curl -fsS "$(ES_HTTP)/_alias/products" 2>/dev/null | python3 -m json.tool || echo "alias 'products' не существует"
	curl -fsS "$(ES_HTTP)/_alias/articles" 2>/dev/null | python3 -m json.tool || echo "alias 'articles' не существует"
	curl -fsS "$(ES_HTTP)/_alias/users" 2>/dev/null | python3 -m json.tool || echo "alias 'users' не существует"

test-integration:
	KAFKA_BOOTSTRAP=$(KAFKA_BOOTSTRAP) \
	  go test -tags=integration ./test/... -v -count=1 -timeout=600s

build:
	go build ./...

clean: connector-delete-all topic-delete-all
	docker compose -f docker-compose.override.yml down -v || true
