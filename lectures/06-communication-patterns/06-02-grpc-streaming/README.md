# 06-02 — gRPC Streaming

В прошлой лекции ([gRPC: основы](../../06-communication-patterns/06-01-grpc-basics/README.md)) мы сделали unary RPC: один запрос, один ответ. Это покрывает 80% сценариев. Но иногда модель «спросил-получил» жмёт. Серверу есть что слать долго — а клиент должен открывать новое соединение каждую секунду. Клиент льёт батчем тысячу записей — а каждая должна катиться отдельным HTTP-вызовом. Двум сторонам надо переписываться — и каждый раз поднимать новый запрос.

HTTP/2 умеет потоки, gRPC их прокидывает наверх как первоклассный примитив. Это лекция про то, как ими пользоваться, и про то, чего ими делать не надо.

## Три типа стримов

Технически все три — это один и тот же HTTP/2 stream под капотом. На уровне gRPC они отличаются тем, кто из участников может слать больше одного сообщения:

1. **Server-stream** — клиент посылает один request, сервер отвечает потоком. Подписки, дашборды, прогресс долгой операции, server-sent events.
2. **Client-stream** — клиент льёт поток request'ов, сервер в конце возвращает один response. Загрузка батчей, бэкапы, импорт данных, телеметрия с устройства.
3. **Bidi** — оба направления независимы, оба шлют по сколько хотят. Чаты, двусторонняя синхронизация, интерактивные сессии.

Unary тут условно — это четвёртый случай, когда обе стороны шлют ровно одно сообщение. По сути, специальный случай bidi с лимитом 1+1.

Главное, что часто упускают: bidi-стримы независимы по send/recv. Клиент может отправить десять message'ей подряд, ничего не читая. Сервер — то же самое. Это не request-response с буферизацией; это два почти независимых потока данных, которые делят один stream-id внутри HTTP/2 фрейма. Я к этому ещё вернусь.

## .proto

Контракт — один файл с сервисом из трёх RPC. Ключевое слово в proto, которое включает стрим — `stream`:

```proto
service StreamingService {
  rpc Subscribe(SubscribeRequest) returns (stream OrderEvent);
  rpc UploadOrders(stream OrderInput) returns (UploadSummary);
  rpc Chat(stream ChatMessage) returns (stream ChatMessage);
}
```

Где стоит `stream` — там много message'ей. Где не стоит — одно. Никакой разницы в синтаксисе сообщений (`OrderEvent`, `OrderInput`, `ChatMessage` — обычные protobuf-структуры). Вся механика прячется в сгенерированных интерфейсах под `*_grpc.pb.go`.

После `buf generate` для server-stream'а на сервере появляется такая сигнатура:

```go
func (s *streamingServer) Subscribe(req *ordersv1.SubscribeRequest, stream grpc.ServerStreamingServer[ordersv1.OrderEvent]) error
```

Один вход — request. Внутри — `stream.Send()` сколько надо, `stream.Context()` для cancel'а. Никакого `stream.Recv()` тут нет — клиент уже всё сказал в request. Возврат `error` — gRPC сам зашлёт его клиенту со status-кодом.

Для client-stream'а наоборот:

```go
func (s *streamingServer) UploadOrders(stream grpc.ClientStreamingServer[ordersv1.OrderInput, ordersv1.UploadSummary]) error
```

Внутри — `stream.Recv()` в цикле, в конце `stream.SendAndClose(summary)`. Метод `SendAndClose` отвечает ровно один раз и закрывает стрим.

Bidi — самый «голый»:

```go
func (s *streamingServer) Chat(stream grpc.BidiStreamingServer[ordersv1.ChatMessage, ordersv1.ChatMessage]) error
```

И `Recv`, и `Send` — независимо, без лимита.

Реальный proto со всеми message'ами — `proto/orders/v1/streaming.proto`.

## Сервер

Стартовая часть та же, что в [gRPC: основы](../../06-communication-patterns/06-01-grpc-basics/README.md): `net.Listen`, `grpc.NewServer`, `RegisterStreamingServiceServer`, `Serve`. Интересное — в реализациях методов.

### Subscribe (server-stream)

Тикер раз в секунду, цикл с двумя case'ами в select: тик — шлём event, контекст закрылся — выходим. Никакой goroutine'ной возни, всё линейно.

```go
ticker := time.NewTicker(s.tickInterval)
defer ticker.Stop()

var sent int32
for {
    select {
    case <-stream.Context().Done():
        s.logger.Info("subscribe ended: client gone", "sent", sent, "err", stream.Context().Err())
        return nil

    case t := <-ticker.C:
        ev := mockOrderEvent(req.GetCustomerId(), t)
        if err := stream.Send(ev); err != nil {
            s.logger.Warn("subscribe send failed", "sent", sent, "err", err)
            return err
        }
        sent++
        if req.GetLimit() > 0 && sent >= req.GetLimit() {
            return nil
        }
    }
}
```

Тут важно отслеживать `stream.Context()`. Когда клиент закрыл соединение или поймал deadline, контекст отменяется — без этой проверки сервер будет тикать в пустоту, копить goroutine'ы и забивать TCP-буфер ошибками `Send`. Внутри Send это и так вылезет (вернёт ошибку), но через context.Done() выйти быстрее и чище.

### UploadOrders (client-stream)

Recv в цикле. EOF от gRPC — это сигнал «клиент закрыл send-сторону». Любая другая ошибка — сетевой обрыв или отвал клиента, gRPC поднимет её до клиента.

```go
for {
    in, err := stream.Recv()
    if errors.Is(err, io.EOF) {
        return stream.SendAndClose(&summary)
    }
    if err != nil {
        return err
    }

    summary.Received++
    if in.GetAmountCents() <= 0 || in.GetCustomerId() == "" {
        summary.Rejected++
        continue
    }
    summary.Accepted++
    summary.TotalCents += in.GetAmountCents()
}
```

Невалидные записи мы не отвергаем выкриком в виде ошибки — просто инкрементим `rejected` и едем дальше. Если бы вернули `error`, gRPC закрыл бы весь стрим, и клиент потерял бы возможность дослать остальное. Это типичный выбор для client-stream'а: либо строгий режим (одна плохая запись валит всё), либо мягкий (плохие учтены в summary, остальные приняты). Контракт лучше зафиксировать в .proto-комментарии или в API-документации, чтобы клиент знал, чего ждать.

### Chat (bidi)

Тут впервые понадобился goroutine. Recv блокирующий, Send блокирующий — без отдельного потока read'а нельзя одновременно крутить read и write на одном стриме.

```go
type incoming struct {
    msg *ordersv1.ChatMessage
    err error
}
in := make(chan incoming, 8)

go func() {
    for {
        m, err := stream.Recv()
        in <- incoming{msg: m, err: err}
        if err != nil {
            close(in)
            return
        }
    }
}()
```

Отдельная горутина читает Recv и кладёт результат в канал. Главный цикл сидит в select по этому каналу и по контексту, и шлёт echo через `stream.Send` — с задержкой `echoDelay`. Эта задержка специально оставлена настраиваемой: меняешь её и видишь, как клиент успевает зашить пять сообщений раньше, чем сервер ответит на первое. Это и есть демонстрация независимости направлений в bidi.

Координация на закрытие — через io.EOF в Recv (клиент закрыл send-сторону) или через `stream.Context().Done()` (отвал/cancel). Возврат из метода — gRPC закрывает свою сторону.

## Клиенты

Три клиента — по одному на каждый тип стрима. Все живут в `cmd/client-*`.

### client-server-stream

Минимально: позвал Subscribe, дальше крутишь Recv до EOF.

```go
stream, err := client.Subscribe(ctx, &ordersv1.SubscribeRequest{
    CustomerId: *customer,
    Limit:      int32(*limit),
})
if err != nil { ... }

for {
    ev, err := stream.Recv()
    if errors.Is(err, io.EOF) {
        fmt.Printf("stream closed by server, got %d events\n", received)
        return
    }
    if err != nil { ... }
    received++
    fmt.Printf("[%d] order_id=%s ...\n", received, ev.GetOrderId(), ...)
}
```

Если сервер по своему усмотрению закроет стрим (как у нас при достижении `limit`) — Recv вернёт EOF, без ошибки. Если клиент сам отменил контекст по SIGINT — Recv вернёт ошибку с кодом Canceled. Эти два случая надо различать в логе, иначе непонятно, кто кого закрыл.

### client-upload

Send в цикле, в конце один CloseAndRecv:

```go
for i := 0; i < *count; i++ {
    input := &ordersv1.OrderInput{ ... }
    if i < *bad {
        input.AmountCents = 0
    }
    if err := stream.Send(input); err != nil { ... }
    time.Sleep(*pause)
}

summary, err := stream.CloseAndRecv()
```

`CloseAndRecv` делает два дела одним вызовом: говорит серверу «я закончил» (это то самое EOF, которое сервер ловит в `Recv`) и читает финальный response. Никакого второго read'а не нужно — после CloseAndRecv стрим закрыт.

### client-bidi

Send и Recv одновременно — значит снова две горутины. Но клиент проще сервера: read'ом занимается одна горутина, write'ом — main, координация через WaitGroup и канал отмены.

```go
var wg sync.WaitGroup
wg.Add(1)
go func() {
    defer wg.Done()
    readEchoes(ctx, stream)
}()

for i := 0; i < *count; i++ {
    msg := &ordersv1.ChatMessage{
        From:   *from,
        Text:   fmt.Sprintf("ping %d", i+1),
        SentAt: timestamppb.Now(),
    }
    if err := stream.Send(msg); err != nil { break }
    time.Sleep(*interval)
}

if err := stream.CloseSend(); err != nil { ... }
wg.Wait()
```

`CloseSend` — клиентский аналог сигнала «я больше ничего не пришлю». Сервер увидит EOF в Recv, закроет свою сторону, наша read-горутина увидит EOF и тоже выйдет. Если CloseSend забыть — сервер будет ждать клиента бесконечно (ну, до deadline'а), наша горутина тоже зависнет. Частая ошибка.

При запуске видно, как порядок «отправили — получили» расходится:

```
> sent: ping 1
> sent: ping 2
> sent: ping 3
> sent: ping 4
< echo from server: echo: ping 1
< echo from server: echo: ping 2
...
```

Все четыре Send улетели за 80мс (interval=20ms), echo идут с throttling'ом на стороне сервера в 200мс. Если бы это был request-response, такого расхождения бы не было — Send бы блокировался до прихода response. Тут не блокируется. Вот и доказательство независимости.

## Backpressure: кто кого тормозит

В стрим-API легко забыть, что между Send на одной стороне и Recv на другой стоит TCP плюс буферы gRPC. Если получатель медленнее отправителя, буфер заполняется и Send начнёт блокироваться. Это и есть backpressure на уровне транспорта — оно работает само, но это transport-level, не application-level.

Что значит «работает само»: TCP не даст переполнить память, бесконтрольного роста очереди не будет. Это хорошо. Минус — отправитель узнает о проблеме только когда упрётся в TCP-окно. До этого он будет считать, что всё ок. Если получатель медленный по бизнес-причине (handler делает дорогой запрос на каждое сообщение), отправитель не узнает об этом, пока не накопится десятки килобайт в буфере.

Application-level backpressure делается отдельно. Самый простой паттерн в bidi — ACK'и: получатель шлёт обратно `processed_id`, отправитель не шлёт следующую партию пока не пришли все ACK'и за предыдущую. Это руками, gRPC такого не даёт из коробки.

Для server-stream'а паттерн другой — клиент в любой момент может оторвать соединение через cancel контекста, и сервер увидит это в `stream.Context().Done()`. Грубо, но работает.

Для client-stream'а сервер контролирует — если он медленно читает Recv, клиентский Send начнёт ждать. Это и есть backpressure без явного протокола.

## Когда стрим vs когда unary

Перед тем как тянуть стрим — спроси себя: правда ли надо.

Server-stream имеет смысл, когда:

- ответы могут идти долго, и клиент готов получать их по мере появления (live updates, прогресс импорта);
- объём ответа неизвестен заранее, и собирать всё в один response — это блокировать клиента и жрать память;
- хочется отрезать работу сервера через cancel контекста с клиента.

Server-stream не нужен, если:

- ответ всегда влезает в одно сообщение, даже если оно большое (10MB protobuf — норма);
- клиенту нужны все данные разом (типа `getAll`), и стримить нет смысла, всё равно собирать на клиенте.

Client-stream имеет смысл при больших batch upload'ах: тысяча записей одной транзакцией — лучше стримом, чем одним 50MB request'ом, который будет лежать в памяти и сервера, и клиента целиком. Но если записей десять — обычный unary с массивом внутри проще.

Bidi — это про сценарии, где обе стороны равноправны: чаты, REPL-сессии, многостадийные диалоги, peer-to-peer синхронизация. Если у тебя один поток управления (клиент диктует, сервер выполняет), bidi — overkill, сделай server-stream или ping-pong unary.

## Чего стримами делать НЕ надо

Эту часть прочитай внимательно — gRPC streaming любят пихать туда, где он становится плохой заменой Kafka. Граница примерно такая:

**gRPC stream подходит для:**

- short-lived подключений с одного клиента к одному серверу (минуты, максимум часы);
- сценариев без durability — данные нужны прямо сейчас, потерял — ну и ладно;
- 1:1 коммуникации без необходимости replay'а.

**gRPC stream НЕ подходит для:**

- **durability** — стрим — это поток в RAM. Сервер упал — поток порвался, нет ни буфера, ни WAL. Если события надо точно не потерять, нужен Kafka (или любая другая система с persistent log'ом).
- **replay** — посмотреть, что было час назад? В gRPC stream'е ничего нет, поток уже закрыт. В Kafka достаточно сместить consumer offset.
- **fan-out на много получателей** — gRPC stream — это 1:1. Хочешь шлёт одно событие десяти подписчикам — либо клиенты сами поднимают по одному стриму к каждому источнику (N×M соединений), либо ставь брокер. Брокер — это и есть Kafka.
- **долгоживущие подписки на дни/недели** — gRPC stream держит TCP-соединение открытым. Это значит зависимость от стабильности сети, retry-логику на клиенте, переподключения с потерей позиции. Kafka consumer переподключается прозрачно и помнит, докуда дочитал.

Грубое правило: если хоть один из вопросов «надо ли это сохранять?», «надо ли потом перечитать?», «будет ли больше одного получателя?» отвечается «да» — это Kafka, не gRPC stream.

Это не значит, что gRPC streaming бесполезен. Live UI dashboard, прогресс долгого импорта, file upload, интерактивный CLI — нормальные случаи. Только не используй его как очередь.

## Что запустить

Сначала сервер в одном терминале:

```sh
make run-server
```

Дальше в трёх отдельных терминалах:

```sh
make run-subscribe   # server-stream: 5 событий с тиком в 1 секунду
make run-upload      # client-stream: 20 OrderInput'ов, summary в конце
make run-chat        # bidi: 5 ping'ов, echo с throttling'ом
```

В логах сервера видно, как стартует и завершается каждый стрим:

```
streaming-server started addr=:50052 tick=1s echo_delay=200ms
subscribe started customer_id="" limit=5
subscribe ended: limit reached sent=5
upload ended received=20 accepted=17 rejected=3
chat ended: client closed send
```

Сценарии для пощупать:

- запусти `run-subscribe -limit=0` и убей через Ctrl+C — увидишь в логе «subscribe ended: client gone»;
- запусти `run-chat -count=20 -interval=10ms` — клиент успеет зашить все 20 message'ей раньше, чем сервер начнёт отвечать (echo тикают каждые 200мс);
- запусти `run-upload -bad=5` — summary вернётся с `rejected=5`, остальные приняты.

## Что дальше

В следующей лекции ([Sync vs async: gRPC и Kafka](../../06-communication-patterns/06-03-sync-vs-async/README.md)) разберём decision matrix «sync vs async»: семь измерений, по которым выбираешь между gRPC и Kafka на уровне дизайна сервиса. Спойлер — стримы там не появятся, они слишком частный случай, чтобы попасть в матрицу. А вот в [Гибрид gRPC + Kafka](../../06-communication-patterns/06-04-hybrid-grpc-and-kafka/README.md) (gRPC + Kafka гибрид) и [Saga: choreography vs orchestration](../../06-communication-patterns/06-05-saga-choreography/README.md) (saga) мы увидим, как unary gRPC и Kafka топики сочетаются в одном сервисе.
