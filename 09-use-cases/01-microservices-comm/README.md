# Use case 09-01 — Microservices Communication

Лекция 06-04 показала гибрид gRPC + Kafka на одном узле каждой роли. Это был концепт. Тут — продакшн-вариант того же гибрида, но в реалистичной форме: каждый сервис работает в нескольких копиях, между ними распределяется нагрузка, в середине нагрузки одна копия падает — система продолжает доводить заказы до конца. Всё это закрыто integration-тестом, который запускается одной командой.

## Что внутри

Три сервиса:

1. `order-service` — gRPC API `OrderService.Create` плюс встроенный outbox publisher. Запускается в нескольких копиях с разными gRPC-портами. Все копии бьют в один Postgres, в одну таблицу `outbox`, и синхронно конкурируют за неопубликованные строки через `FOR UPDATE SKIP LOCKED`. Дубли невозможны: одна строка достаётся ровно одной ноде.
2. `inventory-service` — consumer на `order.created` в группе `inventory`. Тоже несколько копий. Партиции делятся между нодами через consumer-group механизм, каждое сообщение обрабатывает ровно одна нода. При падении ноды — ребаланс, оставшиеся подбирают её партиции.
3. `notification-service` — второй consumer на тот же топик, но в собственной группе `notifications`. Это как раз тот трюк, ради которого в гибрид добавляли Kafka: новый downstream добавляется без правок в `order-service` и без согласований. Включил группу — прочитал лог с нуля — поехал.

Один Postgres под всех ради компактности, как в лекции 06-04. В проде у каждого сервиса своя БД. Один топик `usecase-09-01-order-created` с 6 партициями и RF=3.

Всё, что выходит из `order-service` в Kafka — это `OrderCreated`-payload в Protobuf, плюс набор headers (`outbox-id`, `aggregate-id`, `publisher-node`, `trace-id`, `tenant-id`, `event-type`, `content-type`). Headers — для propagation сквозного контекста и для dedup'а. Payload — proto-байты, без Schema Registry. Лекция 05-03 показывает как наслаивать SR поверх; тут SR опущен, чтобы integration test был самодостаточен и не зависел от регистрации схем по сети при каждом прогоне.

## Чем этот use case отличается от лекции 06-04

| Что | 06-04 (концепт) | 09-01 (use case) |
|---|---|---|
| Узлов на сервис | 1 | 2+ (в тесте — две на order и две на inventory) |
| Outbox publisher | в том же процессе, что и gRPC | то же — но проверено на конкуренции через `FOR UPDATE SKIP LOCKED` |
| Failure recovery | не показано | в тесте: kill -9 одной inventory-ноды на полпути нагрузки |
| Verification | глазами в логах | integration test с ассертами по 4 счётчикам в БД |
| Сериализация события | JSON в outbox payload | proto.Marshal, headers с `content-type` |
| Tracing | trace_id в headers | то же, но с `publisher-node` для отладки multi-node |

## Архитектура

```
            ┌────────────────┐         ┌────────────────┐
            │   gRPC client  │────────►│  order-service │
            └────────────────┘  Create │     :50081     │  ───┐
            ┌────────────────┐         │  (node order-1)│     │
            │   gRPC client  │────────►│                │     │
            └────────────────┘  Create └────────────────┘     │
                                       ┌────────────────┐     │  Postgres TX:
                                       │  order-service │     │  orders + outbox
                                       │     :50082     │     │  ↑
                                       │  (node order-2)│ ◄───┘
                                       └────────────────┘
                                                │
                                  outbox publisher (горутина в каждой копии)
                                                │
                                                ▼
                                  ┌────────────────────────┐
                                  │   Kafka topic           │
                                  │   order.created (P=6)   │
                                  └────────────────────────┘
                                                │
                          ┌─────────────────────┴─────────────────────┐
                          │                                           │
                  consumer group                                consumer group
                  "inventory"                                   "notifications"
                          │                                           │
                ┌─────────┴─────────┐                          ┌──────┴──────┐
                ▼                   ▼                          ▼
        ┌─────────────┐    ┌─────────────┐            ┌─────────────────┐
        │ inventory-1 │    │ inventory-2 │            │ notification-1  │
        └─────────────┘    └─────────────┘            └─────────────────┘
                │                   │                          │
                └─────────┬─────────┘                          │
                          ▼                                    ▼
                  inventory_reservations                notifications_log
```

Левая половина — write-path. Никакого Produce внутри RPC handler'а: только `INSERT INTO orders` плюс `INSERT INTO outbox` под одним COMMIT. Это контракт outbox-паттерна (см. 04-03 и 06-04). Outbox publisher — отдельная горутина в том же процессе. На каждом тике опрашивает `SELECT ... WHERE published_at IS NULL ... FOR UPDATE SKIP LOCKED`, шлёт батч в Kafka, помечает `published_at = NOW()`. Если две копии `order-service` опрашивают параллельно — `SKIP LOCKED` не даст им зацепиться за одну строку, не нужно ни лидер-выбор, ни шардинг по нодам.

Правая половина — два независимых консьюмера. У `inventory` несколько нод в одной группе, партиции делятся. У `notifications` — одна нода в своей группе, читает все партиции. Принципиально не отличается: один и тот же топик читают две независимые проекции, ничего не знают друг про друга.

## Как падает inventory и что происходит дальше

Сценарий, который проверяет integration test: на середине заливки (после ~100 заказов из 200) останавливается `inventory-1`. Что было до:

```
inventory-1 владеет партициями [0, 2, 4]
inventory-2 владеет партициями [1, 3, 5]
```

После остановки одной из нод:

```
inventory-2 владеет партициями [0, 1, 2, 3, 4, 5]   ← после ребаланса
```

Партиции `[0, 2, 4]` не остались без обработчика — Kafka заметила выход через session timeout (15 секунд), запустила ребаланс, передала их единственной живой ноде. Те сообщения, которые `inventory-1` уже успел получить, но не закоммитить — будут переотправлены на `inventory-2`. Дубли отлавливаются на уровне dedup-таблицы `processed_events (consumer, outbox_id)` и `ON CONFLICT DO NOTHING` в `inventory_reservations`.

В выводе теста это выглядит так:

```
[inventory-1] inventory-service остановлен. processed=N reserved=N skipped=0
counts: orders=200 outbox_unpublished=0 reservations=87  notifications=200
counts: orders=200 outbox_unpublished=0 reservations=163 notifications=200
counts: orders=200 outbox_unpublished=0 reservations=200 notifications=200
inventory распределение: map[inventory-2:200]
```

Сначала reservations=87 (то, что успело обработаться до kill'а), потом плато (ребаланс — несколько секунд паузы), потом цифра доползает до 200. Notifications не моргнули — у них своя группа, ребаланса не было.

## Ключевой код

`order-service` и `inventory-service` реализованы как тонкие `cmd/<name>/main.go` плюс пакеты `internal/<name>service` с экспортированной функцией `Run(ctx, opts) error`. Это нужно integration-тесту: он импортирует `internal/orderservice`, `internal/inventoryservice`, `internal/notificationservice` напрямую и запускает их в горутинах одного процесса. Без этого пришлось бы поднимать реальные процессы через `os/exec` и общаться с ними через PID-файлы — лишний шум для тестового сценария.

Сама транзакция order-service:

```go
err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
    if _, err := tx.Exec(ctx, insertOrderSQL,
        id, req.GetCustomerId(), req.GetAmountCents(), req.GetCurrency(),
        ordersv1.OrderStatus_ORDER_STATUS_NEW.String(),
    ); err != nil {
        return fmt.Errorf("INSERT orders: %w", err)
    }
    aggregateID := "order-" + id
    if err := tx.QueryRow(ctx, insertOutboxSQL, aggregateID, s.topic, payload).Scan(&outboxID); err != nil {
        return fmt.Errorf("INSERT outbox: %w", err)
    }
    return nil
})
```

В `tx` оба INSERT'а под одним COMMIT. Никакого `cl.ProduceSync(...)` тут нет. Если COMMIT упал — клиент видит Internal, ретраит, попадает в новую транзакцию. Если COMMIT прошёл, а процесс умер до того, как outbox publisher успел отправить запись — она лежит в `outbox` с `published_at IS NULL` и публикуется при рестарте любой ноды.

Сам publisher — голый `FOR UPDATE SKIP LOCKED`:

```go
const fetchBatchSQL = `
SELECT id, aggregate_id, topic, payload
  FROM outbox
 WHERE published_at IS NULL
 ORDER BY id
 LIMIT $1
 FOR UPDATE SKIP LOCKED
`
```

Тут весь секрет конкуренции между несколькими `order-service`. Postgres гарантирует, что строки, выбранные одной транзакцией, не покажутся другой, пока эта первая не закоммитится или не откатится. `SKIP LOCKED` означает «если строка занята — пропусти, не жди». Две ноды одновременно заходят в `SELECT`, каждая получает свой набор строк, никто никого не блокирует.

Записи в Kafka собираются с headers'ами:

```go
records[i] = &kgo.Record{
    Topic: r.topic,
    Key:   []byte(r.aggregateID),
    Value: r.payload,
    Headers: []kgo.RecordHeader{
        {Key: "outbox-id", Value: []byte(strconv.FormatInt(r.id, 10))},
        {Key: "aggregate-id", Value: []byte(r.aggregateID)},
        {Key: "publisher-node", Value: []byte(o.NodeID)},
        {Key: "trace-id", Value: []byte(evt.GetTraceId())},
        {Key: "tenant-id", Value: []byte(evt.GetTenantId())},
        {Key: "event-type", Value: []byte("order.created")},
        {Key: "content-type", Value: []byte("application/x-protobuf")},
    },
}
```

`aggregate-id` идёт в `Key` — все события одного заказа летят в одну партицию, порядок сохраняется per-key. `outbox-id` — это та самая зацепка для dedup'а на consumer'ах: у каждого `order-service` свой autoincrement в Postgres (один и тот же `BIGSERIAL` в общей таблице, что ещё проще), и эта пара (consumer, outbox_id) уникальна. `publisher-node` — для отладки: видно, какая нода `order-service` опубликовала запись. Полезно когда читаешь kafka-logs на проде и пытаешься понять, какой инстанс был автором.

Сам dedup на consumer-стороне:

```go
tag, err := pool.Exec(ctx, dedupSQL, consumerName, outboxID)
if err != nil { return err }
if tag.RowsAffected() == 0 {
    skipped.Add(1)
    continue
}
```

`dedupSQL` — `INSERT INTO processed_events (consumer, outbox_id) VALUES ($1, $2) ON CONFLICT (consumer, outbox_id) DO NOTHING`. Если уже есть строка — `RowsAffected()` вернёт 0, обработка пропускается. Это эквивалент `SELECT 1 FROM processed_events ... + INSERT`, но в одно SQL-выражение и без race condition между SELECT и INSERT.

## Integration test

Тест в `test/integration_test.go`. Что он делает:

1. Пингует Postgres и Kafka. Если что-то не отвечает — `t.Skip()`. Тест зависит от внешней инфраструктуры по дизайну: лекция о том, как этот код работает на реальном стенде.
2. Очищает все таблицы (`TRUNCATE`) и пересоздаёт топик с нуля. Это важно — иначе старые offsets из предыдущих прогонов перетянут consumer'ов в нелогичную точку.
3. Удаляет существующие consumer-группы (`adm.DeleteGroup`). Тоже про идемпотентность прогона.
4. Запускает 5 нод как горутины: 2 × `order-service`, 2 × `inventory-service`, 1 × `notification-service`. У каждой свой `context.WithCancel(root)` — чтобы можно было убить одну, не трогая остальных.
5. Через gRPC шлёт 200 `Create`-запросов round-robin между двумя `order-service`. На полпути (i=100) дёргает `inv1.cancel()` и ждёт `<-inv1.done`. Это синхронная остановка.
6. После этого продолжает слать оставшиеся 100 заказов. К ним в это время идёт ребаланс — пара секунд `inventory-2` подбирает партиции.
7. Дальше polling: каждые 500мс читает `COUNT(*)` из четырёх таблиц. Ждёт пока все четыре сравняются с 200. Дедлайн — 60 секунд.
8. Финальная проверка — `inventory-2` должен был обработать хотя бы какие-то записи (`map[inventory-2:N], N > 0`). Если он не обработал ничего — значит recovery не сработало, тест падает.

Запуск:

```sh
make up                 # postgres
make db-init            # создать таблицы
make test-integration   # go test ./test/... -v -count=1 -timeout=180s
```

Полный цикл — около 30 секунд на моём стенде. Большая часть — ожидание ребаланса (15 секунд session timeout) и догон reservations.

## Запуск вручную

Для отладки или демонстрации руками — всё разнесено на цели Makefile. В разных терминалах:

```sh
# терминал 1
make up && make db-init
make topic-create

# терминал 2
make run-order-1

# терминал 3
make run-order-2

# терминал 4
make run-inventory-1

# терминал 5
make run-inventory-2

# терминал 6
make run-notification

# терминал 7
make grpcurl-create     # отправить тестовый Create
make orders-count       # увеличилось на 1
make reservations-count # должно тоже увеличиться (с лагом)
make notifications-count
```

Дальше можно `Ctrl+C` любую `inventory-N` и посмотреть, как оставшаяся подбирает партиции. Через 15 секунд session timeout (логи покажут rebalance) и обработка продолжится.

## Что в этом use case намеренно упрощено

- **Один Postgres на всех.** В проде у каждого сервиса своя БД. Тут — общая ради компактности кода и теста. Логика от этого не меняется: dedup-таблица всё равно per-consumer, write-path всё равно атомарный per-сервис.
- **Нет Schema Registry.** События в Kafka — голые protobuf-байты. SR показан в лекции 05-03 и наслаивается поверх через `sr.Serde`. Здесь без него: тест должен быть быстрым и самодостаточным.
- **Нет real-world load patterns.** 200 заказов через round-robin на двух нодах — это unit-stress, не нагрузочный тест. Бенчмарки по throughput не цель этой лекции.
- **Mock notification.** `notification-service` пишет в `notifications_log` вместо реальной доставки. Реальные каналы (Firebase / APNs / webhook с CB и retry) — следующий use case 09-02.

Что не упрощено — outbox publisher с `FOR UPDATE SKIP LOCKED` на нескольких нодах, multi-node consumer-group с recovery, dedup на consumer'ах через `processed_events`. Это работает один-в-один как на проде.

## Связи с другими лекциями

- **04-03 — Outbox Pattern** — сама идея outbox'а. Тут она просто масштабируется на N нод.
- **06-04 — Hybrid: gRPC + Kafka** — концепция гибрида. Этот use case — её production-edition.
- **03-01 — Consumer Groups & Rebalance** — что такое ребаланс, что такое session timeout, почему `cooperative-sticky` лучше `range`.
- **03-03 — Processing Guarantees** — почему dedup по `(consumer, outbox_id)` превращает at-least-once в effectively-once.
- **05-02 — Protobuf in Go** — генерация Go-кода через `buf`, который тут используется.
