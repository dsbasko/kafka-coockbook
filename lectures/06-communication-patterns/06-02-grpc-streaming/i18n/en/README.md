# 06-02 — gRPC Streaming

In the previous lesson ([gRPC: basics](../../../06-01-grpc-basics/i18n/en/README.md)) we built a unary RPC: one request, one response. That covers 80% of scenarios. But sometimes the ask-and-receive model is too tight. The server has data to stream for a long time — yet the client must open a new connection every second. The client pushes a batch of a thousand records — yet each one has to travel in a separate HTTP call. Two parties need to exchange messages — and each exchange requires spinning up a new request.

HTTP/2 has streams; gRPC surfaces them as a first-class primitive. This lesson is about how to use them — and what not to do with them.

## Three stream types

Technically all three use the same HTTP/2 stream under the hood. At the gRPC level they differ only in which participant can send more than one message:

1. **Server-stream** — the client sends one request, the server replies with a stream. Subscriptions, dashboards, long-operation progress, server-sent events.
2. **Client-stream** — the client pushes a stream of requests, the server returns one response at the end. Batch upload, backups, data import, device telemetry.
3. **Bidi** — both directions are independent, both sides send as many messages as they want. Chats, bidirectional sync, interactive sessions.

Unary is the implicit fourth case — both sides send exactly one message. Effectively a special case of bidi with a 1+1 limit.

The key thing people often miss: bidi streams are independent in send/recv. The client can send ten messages back-to-back without reading anything. The server can do the same. This is not request-response with buffering; it's two nearly independent data flows sharing a single stream-id inside an HTTP/2 frame. I'll come back to this.

## .proto

The contract is a single file with a service of three RPCs. The keyword in proto that enables streaming is `stream`:

```proto
service StreamingService {
  rpc Subscribe(SubscribeRequest) returns (stream OrderEvent);
  rpc UploadOrders(stream OrderInput) returns (UploadSummary);
  rpc Chat(stream ChatMessage) returns (stream ChatMessage);
}
```

Where `stream` appears — multiple messages flow. Where it doesn't — one message. No difference in message syntax (`OrderEvent`, `OrderInput`, `ChatMessage` are ordinary protobuf structs). All the mechanics hide in the generated interfaces under `*_grpc.pb.go`.

After `buf generate`, the server-stream handler gets this signature:

```go
func (s *streamingServer) Subscribe(req *ordersv1.SubscribeRequest, stream grpc.ServerStreamingServer[ordersv1.OrderEvent]) error
```

One input — the request. Inside — `stream.Send()` as many times as needed, `stream.Context()` for cancellation. No `stream.Recv()` here — the client already said everything in the request. The returned `error` is forwarded to the client by gRPC with the appropriate status code.

For client-stream it's the reverse:

```go
func (s *streamingServer) UploadOrders(stream grpc.ClientStreamingServer[ordersv1.OrderInput, ordersv1.UploadSummary]) error
```

Inside — `stream.Recv()` in a loop, then `stream.SendAndClose(summary)` at the end. `SendAndClose` responds exactly once and closes the stream.

Bidi is the most "bare":

```go
func (s *streamingServer) Chat(stream grpc.BidiStreamingServer[ordersv1.ChatMessage, ordersv1.ChatMessage]) error
```

Both `Recv` and `Send` — independently, without limits.

The real proto with all messages is in `proto/orders/v1/streaming.proto`.

## Server

The startup is identical to [gRPC: basics](../../../06-01-grpc-basics/i18n/en/README.md): `net.Listen`, `grpc.NewServer`, `RegisterStreamingServiceServer`, `Serve`. The interesting part is in the method implementations.

### Subscribe (server-stream)

A ticker fires every second; a loop with two `case`s in `select`: on tick — send event; on context close — exit. No goroutine juggling, everything is linear.

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

Tracking `stream.Context()` matters here. When the client closes the connection or hits a deadline, the context is cancelled — without this check the server keeps ticking into the void, accumulating goroutines and flooding the TCP buffer with `Send` errors. `Send` itself will surface the error eventually, but exiting via `context.Done()` is faster and cleaner.

### UploadOrders (client-stream)

`Recv` in a loop. EOF from gRPC is the signal "client closed the send side". Any other error is a network drop or client disconnect; gRPC will propagate it to the client.

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

Invalid records are not rejected with an error — just increment `rejected` and continue. Returning an `error` would make gRPC close the entire stream, and the client would lose the ability to send the rest. This is the classic client-stream choice: strict mode (one bad record kills everything) vs. lenient mode (bad records counted in summary, rest accepted). Document the contract in a `.proto` comment or API documentation so the client knows what to expect.

### Chat (bidi)

This is the first case that requires a goroutine. `Recv` is blocking, `Send` is blocking — without a separate read goroutine you can't run read and write on the same stream simultaneously.

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

A separate goroutine reads `Recv` and pushes the result into a channel. The main loop sits in a `select` on that channel and the context, and sends echoes via `stream.Send` — with an `echoDelay`. That delay is intentionally configurable: change it and watch the client push five messages before the server replies to the first. That's the demonstration of bidi's independent directions.

Shutdown coordination happens via `io.EOF` in `Recv` (client closed the send side) or `stream.Context().Done()` (disconnect/cancel). Returning from the method — gRPC closes its side.

## Clients

Three clients — one per stream type. All live in `cmd/client-*`.

### client-server-stream

Minimal: call `Subscribe`, then loop `Recv` until EOF.

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

When the server closes the stream on its own terms (as we do when reaching `limit`) — `Recv` returns EOF, no error. When the client cancelled the context via SIGINT — `Recv` returns an error with code `Canceled`. Distinguish these two cases in the log, otherwise it's unclear who closed whom.

### client-upload

`Send` in a loop, then one `CloseAndRecv` at the end:

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

`CloseAndRecv` does two things in one call: tells the server "I'm done" (the EOF the server catches in `Recv`) and reads the final response. No second read needed — after `CloseAndRecv` the stream is closed.

### client-bidi

`Send` and `Recv` simultaneously — so two goroutines again. But the client is simpler than the server: one goroutine handles reading, main handles writing, coordination via `WaitGroup` and a cancel channel.

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

`CloseSend` is the client-side signal "I have nothing more to send". The server sees EOF in `Recv`, closes its side, our read goroutine sees EOF and exits too. Forget `CloseSend` — the server waits forever (well, until the deadline), and our goroutine hangs with it. Common mistake.

At runtime you can see the send/receive order diverge:

```
> sent: ping 1
> sent: ping 2
> sent: ping 3
> sent: ping 4
< echo from server: echo: ping 1
< echo from server: echo: ping 2
...
```

All four `Send`s went out in 80ms (interval=20ms), echoes arrive with throttling on the server side at 200ms. If this were request-response, no divergence — `Send` would block until the response arrived. Here it doesn't block. That's the proof of independence.

## Backpressure: who slows whom

In the stream API it's easy to forget that between `Send` on one side and `Recv` on the other sits TCP plus gRPC buffers. If the receiver is slower than the sender, the buffer fills and `Send` starts blocking. That's transport-level backpressure — it works automatically, but it's transport-level, not application-level.

What "works automatically" means: TCP won't let memory overflow, there's no unbounded queue growth. Good. The downside — the sender finds out about the problem only when it hits the TCP window. Until then it assumes everything is fine. If the receiver is slow for business reasons (handler makes an expensive request per message), the sender won't know until tens of kilobytes pile up in the buffer.

Application-level backpressure is built separately. The simplest pattern in bidi is ACKs: the receiver sends back a `processed_id`, the sender doesn't push the next batch until all ACKs for the previous one arrive. Manual, not provided by gRPC out of the box.

For server-stream the pattern is different — the client can tear the connection at any time via context cancel, and the server sees it in `stream.Context().Done()`. Blunt, but it works.

For client-stream the server controls — if it reads `Recv` slowly, the client's `Send` starts waiting. That is backpressure without an explicit protocol.

## When to stream vs. when to use unary

Before reaching for a stream — ask yourself whether you actually need one.

Server-stream makes sense when:

- responses may take a long time, and the client is ready to receive them as they arrive (live updates, import progress);
- the response size is unknown upfront, and collecting everything into one response would block the client and eat memory;
- you want to cut the server's work via a context cancel from the client.

Server-stream is not needed when:

- the response always fits in one message, even a large one (10 MB protobuf is fine);
- the client needs all data at once (like `getAll`), and streaming makes no sense — it'll be assembled on the client anyway.

Client-stream makes sense for large batch uploads: a thousand records as one transaction — better as a stream than a single 50 MB request that sits fully in memory on both server and client. But if there are ten records — a plain unary with an array inside is simpler.

Bidi is for scenarios where both sides are equal: chats, REPL sessions, multi-step dialogues, peer-to-peer sync. If you have a single control flow (client dictates, server executes), bidi is overkill — use server-stream or ping-pong unary.

## What NOT to do with streams

Read this part carefully — gRPC streaming often gets pushed into places where it becomes a poor replacement for Kafka. The boundary looks roughly like this:

**gRPC stream is appropriate for:**

- short-lived connections from one client to one server (minutes, hours at most);
- scenarios without durability — data is needed right now, lost data is acceptable;
- 1:1 communication with no need for replay.

**gRPC stream is NOT appropriate for:**

- **durability** — a stream is a flow in RAM. Server crashed — stream severed, no buffer, no WAL. If events must not be lost, use Kafka (or any other system with a persistent log).
- **replay** — want to see what happened an hour ago? Nothing in the gRPC stream, it's already closed. In Kafka, shift the consumer offset.
- **fan-out to many recipients** — a gRPC stream is 1:1. Want to send one event to ten subscribers — either clients each open a separate stream to every source (N×M connections), or add a broker. The broker is Kafka.
- **long-lived subscriptions spanning days/weeks** — a gRPC stream keeps a TCP connection open. That means dependency on network stability, retry logic on the client, reconnects with position loss. A Kafka consumer reconnects transparently and remembers where it left off.

Rough rule: if any one of "does this need to be persisted?", "does this need to be re-read later?", "will there be more than one recipient?" is answered "yes" — that's Kafka, not a gRPC stream.

This doesn't mean gRPC streaming is useless. A live UI dashboard, long import progress, file upload, interactive CLI — all valid. Just don't use it as a queue.

## What to run

Start the server in one terminal:

```sh
make run-server
```

Then in three separate terminals:

```sh
make run-subscribe   # server-stream: 5 events with a 1-second tick
make run-upload      # client-stream: 20 OrderInputs, summary at the end
make run-chat        # bidi: 5 pings, echo with throttling
```

The server logs show each stream starting and finishing:

```
streaming-server started addr=:50052 tick=1s echo_delay=200ms
subscribe started customer_id="" limit=5
subscribe ended: limit reached sent=5
upload ended received=20 accepted=17 rejected=3
chat ended: client closed send
```

Scenarios to try:

- run `run-subscribe -limit=0` and kill it with Ctrl+C — the log shows "subscribe ended: client gone";
- run `run-chat -count=20 -interval=10ms` — the client will push all 20 messages before the server starts replying (echoes tick every 200ms);
- run `run-upload -bad=5` — the summary comes back with `rejected=5`, the rest accepted.

## What's next

The next lesson ([Sync vs async: gRPC and Kafka](../../../06-03-sync-vs-async/i18n/en/README.md)) breaks down the "sync vs async" decision matrix: seven dimensions for choosing between gRPC and Kafka at the service design level. Spoiler — streams won't appear there, they're too narrow a case to make it into the matrix. In [Hybrid gRPC + Kafka](../../../06-04-hybrid-grpc-and-kafka/i18n/en/README.md) and [Saga: choreography vs orchestration](../../../06-05-saga-choreography/i18n/en/README.md) we'll see how unary gRPC and Kafka topics combine in a single service.
