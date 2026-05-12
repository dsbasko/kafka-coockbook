# 06-04 — Hybrid: gRPC + Kafka

В прошлой лекции мы сравнили синхронный gRPC и асинхронный Kafka на одном и том же сценарии «user signed up». Победителя нет. У одного — низкая latency и предсказуемые ошибки. У другого — decoupling и replay. В реальном сервисе ты редко выбираешь одно. Чаще берёшь оба и собираешь гибрид.

Эта лекция — про самый частый рисунок такого гибрида: write-side с gRPC API, шина событий через Kafka, отдельный read-side. Лекция концептуальная, на single-node setup; production-вариант с multi-node, integration-тестом и failure recovery — это use case [Коммуникация микросервисов](../../../../09-use-cases/01-microservices-comm/i18n/ru/README.md), лежит в модуле 09. Тут — паттерн в чистом виде.

## Зачем вообще «и то, и то»

Возьми типичный order-flow. Клиент создаёт заказ. Что дальше:

1. Клиент хочет ответ «принято/нет» сразу же. Это синхронная история, gRPC.
2. Inventory должен зарезервировать товар. Аналитика хочет лог события. Notifications — отправить письмо. Каждому из этих потребителей плевать на остальных.

Если делать всё через gRPC — order-service знает все downstream URL'ы, синхронно дёргает каждого, ждёт всех, каскадно падает. Если делать всё через Kafka — клиент ждёт, пока асинхронный pipeline всё подтвердит, и за этим стоят костыли вроде долгого long-polling.

Гибрид режет поровну. Клиенту — короткий синхронный API, ответ сразу после COMMIT'а в БД. Всем downstream'ам — событие в Kafka, своя consumer group, свой темп. Никто никого не блокирует.

## Чем рисуется типовой гибрид

Три сервиса. Я их называю по тому, что они делают, а не по тому, на каком протоколе говорят.

```
┌──────────────┐  CreateRequest      ┌────────────────────┐
│ gRPC client  ├────────────────────►│   order-service    │
└──────────────┘                     │  (CommandService)  │
                                     │                    │
                                     │  Postgres TX:      │
                                     │  orders + outbox   │
                                     │  ↓                 │
                                     │  outbox publisher  │
                                     └────────┬───────────┘
                                              │
                                              ▼
                                     ┌────────────────────┐
                                     │   Kafka topic      │
                                     │ order.created      │
                                     └────────┬───────────┘
                                              │
                              ┌───────────────┼───────────────┐
                              ▼               ▼               ▼
                    ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
                    │  inventory   │ │ order-query  │ │ analytics?   │
                    │   service    │ │   service    │ │ notifications│
                    │              │ │ (projector + │ │  (whatever)  │
                    │ reservations │ │  QueryService│ │              │
                    └──────────────┘ └──────────────┘ └──────────────┘
                                              ▲
                                              │ Get(id)
                                     ┌──────────────┐
                                     │  gRPC client │
                                     └──────────────┘
```

Левая половина — write-path. Один gRPC handler, одна транзакция, две таблицы в одной БД. Outbox publisher живёт в той же программе горутиной.

Правая половина — read-path и downstream-сервисы. Они слушают шину и ничего не знают друг про друга. Кому надо — добавился в новый consumer group, прочитал всё с beginning, начал отвечать.

В лекции три процесса, по бинарнику на каждый:

1. `cmd/order-service` — gRPC `CommandService.Create` + outbox publisher горутиной
2. `cmd/inventory-service` — consumer на `order.created`, пишет в `inventory_reservations`
3. `cmd/order-query-service` — gRPC `QueryService.Get` + projector в `orders_view`

Один Postgres под все три ради компактности — в проде у каждого сервиса своя БД. Один Kafka-топик `lecture-06-04-order-created`.

## Write-path: orders + outbox в одной транзакции

Главное правило write-path: никакого Produce внутри RPC handler'а. Если упадём после Produce и до COMMIT'а — у нас событие в Kafka про заказ, которого в БД нет. Никакая идемпотентность это не лечит. Лекция [Outbox-паттерн](../../../../04-reliability/04-03-outbox-pattern/i18n/ru/README.md) разбирала это в деталях — тут переиспользуем тот же паттерн.

В транзакции пишем одновременно сам заказ и «надо позже опубликовать» — строку в outbox. И всё. Сама публикация — отдельный шаг, и отказ публикации никак не ломает консистентность БД.

Вот ядро handler'а Create — проверки опущены, тело транзакции:

```go
err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
    if _, err := tx.Exec(ctx, insertOrderSQL,
        id, req.GetCustomerId(), req.GetAmountCents(), req.GetCurrency(),
        ordersv1.OrderStatus_ORDER_STATUS_NEW.String(),
    ); err != nil {
        return fmt.Errorf("INSERT orders: %w", err)
    }
    aggregateID := "order-" + id
    if err := tx.QueryRow(ctx, insertOutboxSQL, aggregateID, s.topic, string(payload)).Scan(&outboxID); err != nil {
        return fmt.Errorf("INSERT outbox: %w", err)
    }
    return nil
})
```

Главное смотреть сюда: `tx` — общая, оба INSERT'а под одним COMMIT, никакого `cl.ProduceSync(...)` в этом блоке нет. Это и есть граница «синхронной части» гибрида: COMMIT прошёл — клиенту отвечаем `OK`.

Outbox publisher живёт горутиной в том же процессе. Каждые 500мс читает неопубликованные, шлёт в Kafka, помечает published_at:

```go
records := make([]*kgo.Record, len(batch))
for i, r := range batch {
    var evt orderEvent
    _ = json.Unmarshal([]byte(r.payload), &evt)
    records[i] = &kgo.Record{
        Topic: r.topic,
        Key:   []byte(r.aggregateID),
        Value: []byte(r.payload),
        Headers: []kgo.RecordHeader{
            {Key: "outbox-id", Value: []byte(strconv.FormatInt(r.id, 10))},
            {Key: "aggregate-id", Value: []byte(r.aggregateID)},
            {Key: "trace-id", Value: []byte(evt.TraceID)},
            {Key: "tenant-id", Value: []byte(evt.TenantID)},
            {Key: "event-type", Value: []byte("order.created")},
        },
    }
}
results := cl.ProduceSync(ctx, records...)
```

Ключ записи — `aggregate-id` (`order-<uuid>`). Все события одного заказа летят в одну партицию, порядок per-key сохраняется. В headers — outbox-id (для dedup'а на consumer'ах) и propagation-поля.

Гарантия — at-least-once. Между ProduceSync и UPDATE published_at есть окно. Падение в этом окне → запись в Kafka осталась, в outbox `published_at` всё ещё NULL, на рестарте мы пошлём её повторно. Защита от дубля — на consumer'ах. Тут она простая: PRIMARY KEY (consumer, outbox_id) в `processed_events` и INSERT ON CONFLICT DO NOTHING перед каждой обработкой. RowsAffected = 0 → видели, пропускаем.

## CQRS: write-side и read-side как разные сервисы

Стандартный приём, который рисуют в любом учебнике по микросервисам, и в гибриде он сам собой получается.

`CommandService.Create` живёт в `order-service` и пишет в `orders`. Никакого Get у него нет — намеренно. Если бы Get был на этом же сервисе, он читал бы ту же таблицу `orders`, и read бы конкурировал за место с write. Read и write масштабируются по-разному: запись часто лимитирована БД, чтение — кешем и репликами.

`QueryService.Get` живёт в `order-query-service` и читает `orders_view`. Это отдельная таблица, обновляется отдельным процессом-проектором, который и сам — consumer на тот же `order.created`. Get никогда не идёт в `orders`. Его API проще, его БД проще, его кэш-инвалидация (если появится) — отдельная задача.

Смешно, что между этими двумя API нет общего кода вообще. Только proto. Один proto, два сервиса, два процесса, две таблицы. Всё.

Get выглядит так:

```go
err := s.pool.QueryRow(ctx, selectViewSQL, req.GetId()).Scan(
    &id, &customerID, &amountCents, &currency, &statusStr, &createdAt,
)
if err != nil {
    if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, sql.ErrNoRows) {
        return nil, status.Errorf(codes.NotFound,
            "order %q not found in read-store (eventual consistency lag)", req.GetId())
    }
    return nil, status.Errorf(codes.Internal, "select view: %v", err)
}
```

Заметь сообщение NotFound — оно намеренное. Если сразу после Create дёрнуть Get с тем же id, можно получить эту ошибку. Это контракт: между COMMIT'ом write-side и UPSERT'ом проектора есть лаг. Чем хуже сеть и нагрузка — тем больше лаг. Ребаланс на consumer-группе projector'а тоже временно тормозит догон.

Проектор — обычный consumer с manual commit и dedup'ом. Сердцевина:

```go
tag, err := pool.Exec(ctx, dedupSQL, consumerName, outboxID)
if err != nil {
    return fmt.Errorf("dedup outbox-id=%d: %w", outboxID, err)
}
if tag.RowsAffected() == 0 {
    skipped.Add(1)
    continue
}
// ... unmarshal evt ...
if _, err := pool.Exec(ctx, upsertViewSQL,
    evt.ID, evt.CustomerID, evt.AmountCents, evt.Currency, evt.Status, createdAt,
); err != nil {
    return fmt.Errorf("upsert view order=%s: %w", evt.ID, err)
}
```

UPSERT, а не INSERT. Если потом прилетит событие смены статуса (в этой лекции мы такие не публикуем, но в проде это правило), `ON CONFLICT DO UPDATE` обновит view.

## Eventual consistency. Где это болит

Кратко — везде, где UI ожидает «прочитал то, что только что написал».

Стандартные обходы:

1. После Create клиент держит state локально и показывает «в обработке», пока projector не догонит. Get используется только для долговечного отображения.
2. Read-your-writes через прилипание: `Get` маршрутизируется в специальный shard, в который этот клиент пишет, и там стоит read replica с low lag (или вообще — пишем в кеш сразу из write-side, отдельный поток).
3. Sticky session к одному nodes-набору, если он стейтфул.

Что-то из этого делать «по умолчанию» — overkill. Сначала решаешь, нужен ли read-your-writes этому конкретному UI. Часто оказывается, что лаг 200мс никого не беспокоит.

Inventory вообще не страдает от лага. Это другой сервис, у него своя «правда» (резерв). Eventual consistency между ним и order-side — это фича: они loosely coupled, и точка.

```go
if _, err := pool.Exec(ctx, reserveSQL, evt.ID, evt.CustomerID, evt.AmountCents); err != nil {
    return fmt.Errorf("reserve order=%s: %w", evt.ID, err)
}
```

Один UPSERT в `inventory_reservations`, никаких ссылок на orders. В реальной системе тут была бы своя БД, своя проверка остатков, и в случае «нельзя зарезервировать» — публикация события `order.rejected`, которое вернётся в order-side и переведёт заказ в CANCELLED. Это уже choreography saga, отдельная лекция ([Saga: choreography vs orchestration](../../../06-05-saga-choreography/i18n/ru/README.md)).

## Tracing context propagation

Маленькая, но критичная деталь. Любая цепочка через Kafka рвёт обычный gRPC-tracing: span'ы из одного процесса не пробрасываются в другой автоматически. Решение тривиальное — кладём `trace_id` (и заодно `tenant_id`) в payload и в Kafka headers. Consumer'ы первым делом достают их и стартуют свой span как child от того, что пришёл в headers.

В коде Create — поля просто сохраняются:

```go
evt := orderEvent{
    ID:          id,
    CustomerID:  req.GetCustomerId(),
    AmountCents: req.GetAmountCents(),
    Currency:    req.GetCurrency(),
    Status:      ordersv1.OrderStatus_ORDER_STATUS_NEW.String(),
    CreatedAt:   createdAt.Format(time.RFC3339Nano),
    TraceID:     req.GetTraceId(),
    TenantID:    req.GetTenantId(),
}
```

В записи Kafka они дублируются и в payload, и в headers — это копеечно по байтам, и обоим сторонам удобно: глазами через kcat/kafka-ui без парсинга, в коде consumer'а без unmarshal.

Auth-context (user-id, scopes) в outbox-flow обычно идёт тем же путём. Лекция показывает только trace-id для краткости — добавление новых полей это копипаст.

## Запуск

Стенд из корня репозитория должен быть поднят (`docker compose up -d`).

Дальше из директории лекции:

```sh
make up && make db-init    # Postgres на :15434, схема создана
make topic-create          # lecture-06-04-order-created (3 партиции, RF=3)
make run-order             # терминал 1: gRPC :50061 + outbox publisher
make run-inventory         # терминал 2: consumer → inventory_reservations
make run-query             # терминал 3: gRPC :50062 + projector → orders_view
```

Триггер сценария — grpcurl:

```sh
make grpcurl-create        # → ответ с id
make grpcurl-get ID=<uuid> # сразу после Create — может вернуть NotFound (lag)
                           # повторить через ~100мс — отдаст Order из orders_view
```

Полезные счётчики во время игры:

```sh
make orders-count          # сколько заказов в write-side
make view-count            # сколько спроецировано в read-side
make reservations-count    # сколько зарезервировано inventory
make outbox-pending        # сколько ещё не опубликовано publisher'ом
```

В нормальном течении после паузы все три count'а сравниваются. Если view-count или reservations-count отстаёт — посмотри, не убит ли соответствующий consumer.

Чистка между прогонами:

```sh
make db-truncate           # очистить все таблицы (RESTART IDENTITY)
```

## Что отдельно стоит покрутить руками

- Запусти ТОЛЬКО `make run-order`, без inventory и query, насоздавай 50 заказов через grpcurl или просто `for i in $(seq 1 50); do make grpcurl-create; done`. Дальше подними inventory и query — они догонят с beginning, потому что у нас `ConsumeResetOffset(AtStart())`.
- Убей `run-query` посреди потока, пересоздай заказы, подними query обратно. orders_view догонится. Если выключить dedup (стереть `processed_events` через `make db-truncate` перед поднятием) — увидишь, что повторная обработка идемпотентна благодаря UPSERT'у.
- Запусти query и inventory одновременно с разными group-id (они так и сделаны): два разных consumer group читают одни и те же сообщения параллельно, не мешая друг другу — это и есть pub/sub.

## Что эта лекция намеренно НЕ делает

- Нет multi-node. У всех сервисов один экземпляр. В use case [Коммуникация микросервисов](../../../../09-use-cases/01-microservices-comm/i18n/ru/README.md) будут 2-3 ноды на сервис, рекомендованная картинка для прода.
- Нет integration-теста. Лекции не тестируются, тесты — у use case'ов.
- Нет failure recovery beyond at-least-once + dedup. Никаких saga, компенсаций, reject-flow. Тоже use case или [Saga: choreography vs orchestration](../../../06-05-saga-choreography/i18n/ru/README.md).
- Нет Schema Registry. Payload — сырой JSON. Это уровень концепции; production-вариант — Protobuf через SR (лекция [Schema Registry](../../../../05-contracts/05-03-schema-registry/i18n/ru/README.md)), но в этой лекции мы фокусируемся на самом гибриде, чтобы не тащить SR через все файлы.
- Outbox publisher тут в том же процессе, что gRPC server. Это нормально для лекции и для маленьких сервисов; в крупных системах его выносят в отдельный бинарник (или CDC через Debezium — лекция [Debezium CDC](../../../../07-streams-and-connect/07-04-debezium-cdc/i18n/ru/README.md)).

## Что забрать с собой

Гибрид gRPC + Kafka — это разделение работы по двум осям. Синхронный API отвечает клиенту прямо сейчас. Асинхронные эффекты происходят потом, без оглядки на клиента. Outbox замыкает зазор между БД и Kafka. CQRS отделяет write от read — каждая сторона эволюционирует своим темпом. Eventual consistency тут — это контракт. Срабатывает он стабильно, и считать его багом — значит проектировать систему с неправильным ожиданием.

Эту картинку имеет смысл держать в голове любому, кто проектирует back-end любой сложности больше «один сервис → одна БД». Дальше уже идут саги (когда нужно скоординировать несколько сервисов в одном бизнес-процессе) и stream processing (когда event log — основной носитель бизнес-логики). Лекции [Saga: choreography vs orchestration](../../../06-05-saga-choreography/i18n/ru/README.md) и 07-* про это.
