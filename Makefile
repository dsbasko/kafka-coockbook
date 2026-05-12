.PHONY: help list lecture sync build connect-install-plugins connect-verify-plugins web-dev web-build web-lint web-typecheck web-test web-clean web-install web-check-coverage web-generate-readme-toc

# Корневой Makefile тонкий: владеет только connect-* целями
# (потому что connect-plugins/ остаётся в корне).
# Всё остальное делегируется в lectures/Makefile.

# Версии Kafka Connect plugins (нужны для лекции 07-04 и use cases 09-03/09-04).
# Обновляются вручную при апгрейде; смотри connect-plugins/README.md.
DEBEZIUM_VERSION   ?= 3.5.0.Final
CLICKHOUSE_VERSION ?= v1.3.7
ES_VERSION         ?= 15.1.1

CONNECT_PLUGINS_DIR := $(CURDIR)/connect-plugins

help:
	@echo "make list                                   - вывести дерево лекций"
	@echo "make lecture L=<path>                       - запустить make в директории лекции"
	@echo "  пример: make lecture L=01-foundations/01-01-architecture-and-kraft"
	@echo "make sync                                   - go work sync (внутри lectures/)"
	@echo "make build                                  - собрать все workspace-модули"
	@echo "make connect-install-plugins                - скачать и установить Connect plugins (Debezium, ClickHouse, ES)"
	@echo "make connect-verify-plugins                 - убедиться, что Connect видит установленные plugins"
	@echo "make web-install                            - pnpm install в web/"
	@echo "make web-dev                                - запустить Next.js dev-сервер (http://localhost:3000)"
	@echo "make web-build                              - собрать статический сайт в web/out/"
	@echo "make web-lint                               - eslint проверка web/"
	@echo "make web-typecheck                          - tsc --noEmit в web/"
	@echo "make web-test                               - vitest run в web/"
	@echo "make web-check-coverage                     - сверить course.yaml с lectures/ + RU/EN translation coverage"
	@echo "make web-generate-readme-toc [TOC_LANG=en|ru] - напечатать markdown TOC для root README (по умолчанию EN)"
	@echo "make web-clean                              - удалить web/.next и web/out"

list:
	@$(MAKE) --no-print-directory -C lectures list

lecture:
ifndef L
	$(error L is required: make lecture L=01-foundations/01-01-architecture-and-kraft)
endif
	@$(MAKE) --no-print-directory -C lectures lecture L=$(L)

sync:
	@$(MAKE) --no-print-directory -C lectures sync

build:
	@$(MAKE) --no-print-directory -C lectures build

# --- web targets ----------------------------------------------------------
WEB_DIR := $(CURDIR)/web

web-install:
	@cd "$(WEB_DIR)" && pnpm install

web-dev:
	@cd "$(WEB_DIR)" && pnpm dev

web-build:
	@cd "$(WEB_DIR)" && pnpm build

web-lint:
	@cd "$(WEB_DIR)" && pnpm lint

web-typecheck:
	@cd "$(WEB_DIR)" && pnpm typecheck

web-test:
	@cd "$(WEB_DIR)" && pnpm test

web-check-coverage:
	@cd "$(WEB_DIR)" && pnpm exec tsx scripts/check-course-coverage.ts

# Печатает markdown TOC для корневого README. Дефолт TOC_LANG=en.
# TOC_LANG=ru — печатает TOC с русскими заголовками и линками на /i18n/ru/README.md.
# Имя переменной не "LANG", чтобы не конфликтовать с системным locale env.
TOC_LANG ?= en
web-generate-readme-toc:
	@cd "$(WEB_DIR)" && pnpm exec tsx scripts/generate-readme-toc.ts --lang=$(TOC_LANG)

web-clean:
	@rm -rf "$(WEB_DIR)/.next" "$(WEB_DIR)/out"

# Скачать и распаковать три connector plugin'а в connect-plugins/.
# Перезапустить kafka-connect и проверить, что классы видны через REST.
connect-install-plugins:
	@mkdir -p "$(CONNECT_PLUGINS_DIR)"
	@echo "==> Debezium PostgresConnector $(DEBEZIUM_VERSION)"
	@if [ ! -d "$(CONNECT_PLUGINS_DIR)/debezium-connector-postgres" ]; then \
	  curl -fsSL "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/$(DEBEZIUM_VERSION)/debezium-connector-postgres-$(DEBEZIUM_VERSION)-plugin.tar.gz" \
	    | tar -xz -C "$(CONNECT_PLUGINS_DIR)"; \
	else echo "    уже установлен — пропускаю"; fi
	@echo "==> ClickHouse Sink $(CLICKHOUSE_VERSION)"
	@if [ ! -d "$(CONNECT_PLUGINS_DIR)/clickhouse-kafka-connect-$(CLICKHOUSE_VERSION)" ]; then \
	  curl -fsSL "https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/$(CLICKHOUSE_VERSION)/clickhouse-kafka-connect-$(CLICKHOUSE_VERSION).zip" \
	    -o /tmp/ch-sink.zip && \
	  unzip -q -o /tmp/ch-sink.zip -d "$(CONNECT_PLUGINS_DIR)" && rm /tmp/ch-sink.zip; \
	else echo "    уже установлен — пропускаю"; fi
	@echo "==> Confluent Elasticsearch Sink $(ES_VERSION)"
	@if [ ! -d "$(CONNECT_PLUGINS_DIR)/confluentinc-kafka-connect-elasticsearch-$(ES_VERSION)" ]; then \
	  curl -fsSL "https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-elasticsearch/versions/$(ES_VERSION)/confluentinc-kafka-connect-elasticsearch-$(ES_VERSION).zip" \
	    -o /tmp/es-sink.zip && \
	  unzip -q -o /tmp/es-sink.zip -d "$(CONNECT_PLUGINS_DIR)" && rm /tmp/es-sink.zip; \
	else echo "    уже установлен — пропускаю"; fi
	@echo "==> рестарт kafka-connect"
	docker restart kafka-connect
	@echo "==> жду REST API"
	@until curl -fs http://localhost:8083/ >/dev/null 2>&1; do sleep 3; done
	@$(MAKE) connect-verify-plugins

connect-verify-plugins:
	@echo "==> classes из http://localhost:8083/connector-plugins:"
	@curl -fs http://localhost:8083/connector-plugins \
	  | python3 -c "import json,sys; [print('  ',p['class']) for p in json.load(sys.stdin)]" \
	  | sort
	@echo "==> проверка обязательных классов"
	@curl -fs http://localhost:8083/connector-plugins | python3 -c "$$VERIFY_SCRIPT"

define VERIFY_SCRIPT
import json, sys
need = {
  'io.debezium.connector.postgresql.PostgresConnector',
  'com.clickhouse.kafka.connect.ClickHouseSinkConnector',
  'io.confluent.connect.elasticsearch.ElasticsearchSinkConnector',
}
have = {p['class'] for p in json.load(sys.stdin)}
missing = need - have
if missing:
    print('   ОТСУТСТВУЮТ:', ', '.join(sorted(missing)))
    sys.exit(1)
print('   все три connector class зарегистрированы')
endef
export VERIFY_SCRIPT
