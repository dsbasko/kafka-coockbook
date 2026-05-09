# Lectures

Здесь живёт весь учебный материал курса — 9 модулей, 42 единицы (37 лекций + 4 use case'а в `09-use-cases/`). Полный обзор курса, оглавление и инструкции — в [корневом README](../README.md). Этот файл описывает только содержимое директории.

## Структура

```
lectures/
├── go.work                # workspace, объединяет все модули лекций
├── go.work.sum
├── Makefile               # цели list / lecture L=… / sync / build
├── internal/              # shared helpers (kafka, config, runctx, log)
├── 01-foundations/        # модуль 01 — основы Kafka
├── 02-producer/           # модуль 02 — продьюсер
├── 03-consumer/           # модуль 03 — консьюмер
├── 04-reliability/        # модуль 04 — надёжность (EOS, outbox, retry/DLQ)
├── 05-contracts/          # модуль 05 — Protobuf, Schema Registry
├── 06-communication-patterns/  # модуль 06 — gRPC, sync vs async, saga
├── 07-streams-and-connect/     # модуль 07 — stream-processing, Connect, Debezium
├── 08-operations/         # модуль 08 — мониторинг, retention, sizing, troubleshooting
└── 09-use-cases/          # модуль 09 — сквозные use case'ы
```

Каждая лекция — отдельный Go-модуль со своим `go.mod`, `README.md` (на русском) и `Makefile` (как минимум цель `run`, плюс что-то специфичное теме). Use case'ы крупнее — несколько сервисов, proto-схемы, иногда `docker-compose.override.yml`.

## Запуск

Из корня репозитория:

```sh
make list                                                    # дерево всех 42 лекций
make lecture L=01-foundations/01-01-architecture-and-kraft   # запустить конкретную
make sync                                                    # go work sync
make build                                                   # собрать все модули workspace'а
```

Целями владеет `lectures/Makefile`; корневой `Makefile` их проксирует.

## Конвенции

- Module path: `github.com/dsbasko/kafka-sandbox/lectures/<NN-module>/<MM-short>`.
- В `go.mod` каждой лекции — `replace github.com/dsbasko/kafka-sandbox/internal => ../../internal` (после реструктуризации в `lectures/` относительный путь сохранил ту же глубину `../../`).
- Внутри лекции: `cmd/<binary>/main.go` для одно-двух бинарей, `pkg/` или плоские файлы — для чего-то крупнее.
- README — обычный markdown (без MDX). Картинки в `images/` рядом с README, относительные ссылки `./images/foo.png`. Внутрикурсовые ссылки на соседние лекции — `../<module>/<slug>/README.md`. Сайт переписывает их на site-URL автоматически.

## Как добавить лекцию

1. Создать `lectures/<NN-module>/<MM-short-name>/` с `go.mod`, `README.md`, `Makefile`, `cmd/`.
2. Добавить `use ./<NN-module>/<MM-short-name>` в `lectures/go.work`.
3. Описать лекцию в корневом `course.yaml` (slug, title, duration, опциональные tags) — иначе сайт-билд упадёт в `generateStaticParams`.
4. Обновить раздел «Оглавление» в корневом `README.md`.
5. Прогнать `make web-check-coverage` — должен вывести `mismatches: 0`.
