# 08-02 — Retention & Compaction

Kafka хранит сообщения. Не вечно. На каждом топике крутится политика очистки — она решает, что держим, а что выкидываем. Политик две: `delete` (по времени или по объёму) и `compact` (по ключу). На словах звучит просто. На практике начинается интересное — про сегменты, dirty ratio, tombstone'ы и про то, почему «retention.ms=7d» не означает, что сообщение лежит ровно семь дней.

Эта лекция — про то, как лог реально меняется на диске под обеими политиками. Запустим два демо и посмотрим глазами оператора.

## Что внутри

- `cmd/compaction-demo/main.go` — пишет 100 000 обновлений на 1 000 ключей в топик `lecture-08-02-users-state` с `cleanup.policy=compact`. После записи ждёт компактор и снимает размеры через `DescribeAllLogDirs`. Затем пишет tombstone'ы (`Value=nil`) для ста ключей и в финале считает уникальные ключи в логе.
- `cmd/retention-demo/main.go` — фоновый продьюсер льёт в топик `lecture-08-02-events` с `cleanup.policy=delete` (плюс `retention.ms=60s` и `segment.ms=10s`). Раз в несколько секунд печатает `earliest`/`latest` и размер на диске. Видно, как старые сегменты уходят и earliest скачет вверх.
- `Makefile` — точки входа на каждое демо плюс `topic-describe` через `kafka-configs.sh` и `du-volume` (du внутри `kafka-1` для проверки реального размера директории топика).

## Сегмент — единица всего

Прежде чем говорить о retention и compaction, надо понять одну вещь. Kafka не оперирует «сообщениями» при удалении и сжатии. Она оперирует **сегментами** — файлами, в которые партиция нарезана.

Партиция на диске — это директория `lecture-08-02-events-0/` (имя топика плюс номер партиции). Внутри — пары файлов вроде `00000000000000000000.log` и `00000000000000000000.index`. Каждая такая пара — сегмент. Один сегмент — активный (туда пишут прямо сейчас), остальные закрыты. Активный закрывается и становится «закрытым» по двум условиям:

- набрался размер `segment.bytes` (по умолчанию 1 GiB);
- прошло `segment.ms` с момента создания сегмента (по умолчанию неделя).

И вот ключевой момент. **Retention и compaction трогают только закрытые сегменты.** Активный неприкосновенен. Поэтому если у тебя `retention.ms=1h`, но `segment.ms=7d` и трафик небольшой — активный сегмент может жить неделю, и часовое retention не сработает. Сообщения от часовой давности будут лежать в активном сегменте и формально нарушать «свою» retention. Так оно устроено.

В нашем демо `retention-demo` поэтому стоит `segment.ms=10s` — хочется быстрых видимых эффектов.

## Cleanup.policy=delete

Самая частая политика. Удаляем по возрасту или по объёму.

- `retention.ms` — удалить сегмент, если он закрыт и его последняя запись старше N миллисекунд.
- `retention.bytes` — оставить не больше N байт на партицию, остальное — в утиль.

Можно комбинировать. Срабатывает любое условие первым.

Брокер отдельным потоком (`log-retention-thread`) ходит по всем партициям и режет сегменты, которые попали под условие. Интервал — `log.retention.check.interval.ms`, дефолт — 5 минут. Это и есть тот зазор, из-за которого «семь дней» — ориентир, а не точная цифра. На стенде интервал дефолтный, а в `retention-demo` мы ставим `retention.ms=60s` и наблюдаем минуты, не секунды.

Что показывает `retention-demo`. Запускается фоновый продьюсер на `rate` сообщений в секунду, в каждом — payload по 256 байт. Раз в `poll` секунд клиент опрашивает offset'ы и размер. Вот фоновый цикл записи:

```go
t := time.NewTicker(interval)
defer t.Stop()
payload := make([]byte, 256)
for i := range payload {
    payload[i] = byte('a' + (i % 26))
}
var seq int64
for {
    select {
    case <-ctx.Done():
        cl.Flush(context.Background())
        return
    case <-t.C:
        seq++
        rec := &kgo.Record{
            Topic: topic,
            Key:   []byte(fmt.Sprintf("k-%d", seq%16)),
            Value: payload,
        }
        cl.Produce(ctx, rec, nil)
    }
}
```

Тут ничего особенного — это просто чтобы лог рос. Главное — следующий блок: чтение earliest/latest и размера на диске:

```go
starts, err := admin.ListStartOffsets(rpcCtx, topic)
ends,   err := admin.ListEndOffsets(rpcCtx, topic)
size,   err := topicSize(rpcCtx, admin, topic)
// ...
fmt.Fprintf(tw, "%d\t%d\t%d\t%d\n", s.Partition, s.Offset, latest, latest-s.Offset)
fmt.Printf("size on disk (одна реплика): %d bytes\n", size)
```

`ListStartOffsets` возвращает offset первой ещё живой записи в каждой партиции. До retention — это 0. Когда брокер срежет первый закрытый сегмент, earliest скакнёт сразу на offset начала следующего сегмента. Это всегда прыжок «по сегменту», запись по записи earliest не двигается.

`topicSize` — чуть хитрее. У партиции `rf=3`, на диске лежат три копии в трёх log dirs. Считать сумму всех трёх — значит говорить «размер кластера», что путает. Поэтому фильтруем по первой увиденной реплике партиции:

```go
all, err := admin.DescribeAllLogDirs(ctx, nil)
seen := make(map[int32]bool)
var size int64
all.Each(func(d kadm.DescribedLogDir) {
    d.Topics.Each(func(p kadm.DescribedLogDirPartition) {
        if p.Topic != topic {
            return
        }
        if seen[p.Partition] {
            return
        }
        seen[p.Partition] = true
        size += p.Size
    })
})
```

Что наблюдать в выводе. Минут через одну-две (зависит от того, как часто бегает `log-retention-thread`) earliest начинает прыгать, а total size — снижаться. Если запустить и оставить надолго — общий размер стабилизируется около `retention.ms × rate × payload_size`. Это и есть лимит.

## Cleanup.policy=compact

Другая модель. Тут retention по времени уходит на второй план — лог хранит «по одной актуальной записи на ключ». Снимок состояния, а не журнал событий. Полезно для топиков с длинноживущим состоянием по ключу: `users`, `accounts`, `prices`, `inventory`. Каждая новая запись с тем же ключом перезаписывает предыдущую, старые версии не нужны.

Compactor работает иначе, чем retention. Он:

1. Делит закрытые сегменты на «грязные» (есть устаревшие версии ключей) и «чистые» (после прошлой компакции).
2. Считает соотношение размеров: `dirty_size / total_size` — это и есть **dirty ratio**.
3. Если dirty ratio выше `min.cleanable.dirty.ratio` (дефолт 0.5) — забирает грязные сегменты в работу. Остальные не трогает.
4. Сжимает: для каждого ключа оставляет только самое свежее значение.
5. Перезаписывает сегменты на диске (новые имена, старые удаляются).

Контролирующих ручек больше, чем у delete:

- `min.cleanable.dirty.ratio` — порог запуска (0.0–1.0).
- `min.compaction.lag.ms` — нижняя граница: запись младше N мс компактор не трогает (полезно, чтобы консьюмер успел догнать live-trail).
- `max.compaction.lag.ms` — верхняя граница: даже если dirty ratio низкий, через N мс запись будет рассмотрена.
- `delete.retention.ms` — сколько живут tombstone-ы после того, как compactor их «увидел».

В демо мы крутим эти параметры на минимум, чтобы эффект был виден за 30 секунд:

```
cleanup.policy             = compact
segment.ms                 = 5000
min.cleanable.dirty.ratio  = 0.001
min.compaction.lag.ms      = 0
max.compaction.lag.ms      = 10000
delete.retention.ms        = 5000
```

В проде так делать нельзя — компактор будет крутиться без остановки и есть IO. Но для лекции — самое то.

Что показывает `compaction-demo`. Программа проходит пять стадий, и в каждой печатает earliest/latest/size. Сама стадия записи — обычный fire-and-forget продьюсер, ничего хитрого:

```go
for i := 0; i < updates; i++ {
    k := i % keys
    rec := &kgo.Record{
        Topic: topic,
        Key:   []byte(fmt.Sprintf("user-%05d", k)),
        Value: []byte(fmt.Sprintf(`{"v":%d,"ts":%d}`, i, time.Now().UnixMilli())),
    }
    cl.Produce(rpcCtx, rec, nil)
}
if err := cl.Flush(rpcCtx); err != nil {
    return fmt.Errorf("flush: %w", err)
}
```

100 000 записей на 1 000 ключей — ровно по 100 версий на каждый ключ. После записи и недолгого ожидания компактор должен оставить ровно по одной — то есть лог уменьшается примерно в 100 раз.

Дальше — пауза. Но не пустая: чтобы активный сегмент закрывался по `segment.ms=5s`, в активный сегмент нужно изредка писать. Иначе по каким-то таймерам новый сегмент может не родиться, и компактор будет видеть один и тот же закрытый сегмент. На это есть отдельный helper:

```go
func waitWithHeartbeats(ctx context.Context, cl *kgo.Client, topic string, wait time.Duration) error {
    deadline := time.Now().Add(wait)
    tick := time.NewTicker(2 * time.Second)
    defer tick.Stop()
    hb := 0
    for {
        select {
        case <-ctx.Done():
            return nil
        case <-tick.C:
            if time.Now().After(deadline) {
                return nil
            }
            hb++
            rec := &kgo.Record{
                Topic: topic,
                Key:   []byte(fmt.Sprintf("__heartbeat-%d", hb)),
                Value: []byte("hb"),
            }
            // ...
            cl.ProduceSync(rpcCtx, rec).FirstErr()
        }
    }
}
```

Эти heartbeat-записи попадают в счёт `latest`, но не в счёт пользовательских ключей. На этапе `STEP 5` они отфильтровываются по префиксу `__`.

Что увидим в логе. На примере прогона с `keys=50, updates=2000, tombstone-keys=10`:

```
[после записи]                     latest=2000  size=23.3 KB
[после первой компакции]           latest=2009  size=1.4 KB    ← компактор отработал
[после tombstone'ов]               latest=2028  size=2.7 KB    ← tombstone'ы добавились
уникальных user-ключей в логе: 40                              ← 50 минус 10 удалённых
```

Размер упал в шестнадцать раз. С 1 000 ключей и 100 версиями на ключ ratio будет ещё выразительнее.

## Tombstone

Чтобы удалить ключ из compact-топика, пишут запись с `Value=nil` и тем же ключом. Это и есть tombstone — «надгробие». Compactor его «увидит» при следующей компакции и для всех старых версий этого ключа сделает то же, что и обычно — оставит только самое свежее. Но самое свежее — это nil. И вот тут вступает `delete.retention.ms`: tombstone должен полежать в логе ещё это время, чтобы все консьюмеры успели его прочитать и обработать «удаление». Только потом сам tombstone тоже выкидывается.

В `compaction-demo` мы пишем 100 tombstone'ов:

```go
for i := 0; i < n; i++ {
    rec := &kgo.Record{
        Topic: topic,
        Key:   []byte(fmt.Sprintf("user-%05d", i)),
        Value: nil,
    }
    if err := cl.ProduceSync(rpcCtx, rec).FirstErr(); err != nil {
        return fmt.Errorf("tombstone %d: %w", i, err)
    }
}
```

И в финале читаем топик с earliest и считаем:

```go
fetches.EachRecord(func(r *kgo.Record) {
    read++
    k := string(r.Key)
    if len(k) > 2 && k[:2] == "__" {
        heartbeats++
        return
    }
    if r.Value == nil {
        tombstones++
        delete(keys, k)
        return
    }
    keys[k] = struct{}{}
})
```

Видим: tombstone'ы ещё в логе (мы успели их прочитать до истечения `delete.retention.ms=5s`), но при подсчёте уникальных ключей мы их применяем — `delete(keys, k)` — и итог совпадает с ожиданием.

## Combined cleanup: compact + delete

Бонусная политика: `cleanup.policy=compact,delete`. Можно. И иногда нужно.

Сценарий: нужен compact-топик с TTL. Например, профили пользователей, которые удалены давно — пусть пропадают, даже если tombstone никто не написал. Тогда: `cleanup.policy=compact,delete` и `retention.ms=90d` (плюс какой-нибудь умеренный `min.cleanable.dirty.ratio`). Компактор сжимает по ключу, retention доедает то, что старше трёх месяцев — даже актуальные версии. Это рабочая комбинация для long-tail сценариев, но включай её осознанно: можно случайно потерять записи, на которые рассчитывает downstream.

## Operations cheat-sheet

Что куда смотреть, когда что-то не так.

- **Лог не уменьшается на compact-топике.** Проверь `min.cleanable.dirty.ratio`: если 0.5 (дефолт), компактор подождёт, пока половина лога устареет. На медленно меняющихся ключах это месяцы. Опусти ratio или подожди.
- **Активный сегмент бесконечно растёт.** Проверь `segment.ms` и `segment.bytes`. Если оба слишком большие — сегмент не закрывается, retention/compact на него не действуют.
- **Earliest стоит на месте, хотя retention.ms давно истёк.** Жди `log.retention.check.interval.ms` — это интервал retention-thread'а. Дефолт 5 минут. На стенде это иногда выглядит как «сообщение лежит лишних три минуты сверх retention».
- **Tombstone не удаляет данные.** Удаление происходит только при компакции. Если dirty ratio низкий — компакции нет, tombstone лежит как обычная запись.
- **На диске в десятки раз больше, чем ожидаешь.** Не забудь, что `rf=3`. `DescribeAllLogDirs` показывает все реплики, не одну.

## Запуск

```sh
make help
make run-compaction          # ~30 сек после записи + ожидание компактора, по умолчанию 30s
make run-retention           # фоновый прогон, Ctrl+C — выход; через 1–2 минуты earliest начинает прыгать
make du-volume               # размер директории топика на диске kafka-1
make topic-describe          # kafka-configs.sh для обоих топиков
make topic-delete-all        # удалить оба топика
```

Параметры через переменные окружения:

```sh
KEYS=2000 UPDATES=200000 WAIT=60s make run-compaction
RETENTION=120s SEGMENT=20s RATE=20 make run-retention
```
