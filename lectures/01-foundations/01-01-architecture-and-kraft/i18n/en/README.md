# 01-01 — Architecture and KRaft

This is where the conversation starts. What Kafka actually is, what it consists of, and why the course sandbox runs on three nodes in KRaft mode with no separate ZooKeeper.

## What Kafka is

Kafka is a distributed log. The word "log" trips up newcomers, so let's nail down what it means here.

Log here means a sequence of messages, ordered by write time. Nothing in common with an error file. You append to the end, read from any position. You can't delete from the middle — you wait until retention drops old records (retention gets its own lecture). The result is an append-only tape that you can replicate across nodes and read independently from many places.

Why is it needed? When two services want to talk, they usually have one of two options: a synchronous request (HTTP, gRPC) or a queue. Kafka is about the queue. But an unusual one:

- messages don't vanish after being read (you can re-read them),
- multiple consumers see them independently (one doesn't "take" a message from another like in RabbitMQ),
- order is guaranteed within a partition,
- throughput is hundreds of thousands and millions of messages per second on normal hardware,
- retention is whatever you configure (even forever).

These properties drive the use cases: microservice integration (event-driven), CDC, analytics, task queues, audit log. Anywhere you need to "pass a stream of data and not lose it".

## The actors

Kafka has several types of participants. Memorise them right away — they'll show up in every later lecture.

1. **Broker** — a node that stores data. Topics and partitions live on brokers. The sandbox has three — `kafka-1`/`kafka-2`/`kafka-3`.
2. **Controller** — the brain of the cluster. Assigns leaders to partitions, maintains the ISR list, reassigns partitions when nodes fall, validates topic schema changes. In KRaft the controller is a role carried by one of the broker nodes; previously (before KRaft) ZooKeeper held the metadata.
3. **Producer** — the client that writes messages. This is your Go code with `kgo.Client.Produce(...)`.
4. **Consumer** — the client that reads. Also Go code, more often through a consumer group.

Brokers and the controller are the server. Producer and consumer are the client. On the sandbox the server lives inside docker compose, the clients live inside your lectures.

## What KRaft is

Kafka could not run without ZooKeeper before. ZK held all metadata — the list of topics, ACLs, the broker→partition map, who is the leader, who is in the ISR. That scheme had its share of pain: a separate ZK cluster with its own failure mode, and a limit on the number of partitions — metadata over znodes scaled poorly past a couple hundred thousand.

KRaft — Kafka Raft. Metadata moved inside Kafka, into a special topic `__cluster_metadata`. This topic is a regular log, replicated across nodes through Raft consensus. The nodes that participate in Raft and vote for the leader-controller are called **voters**. The active leader among voters is the current cluster controller.

What this gives you in practice:

- one system instead of two (Kafka + ZK → just Kafka),
- one metadata format (a topic-log, not a ZK znode tree),
- faster recovery after a controller fall,
- easier to keep millions of partitions in memory.

One downside: the ecosystem is still catching up. Part of the tutorials and Stack Overflow answers are still about ZK. KRaft is stable since Kafka 3.5 and became the default starting with Kafka 4.0 — the sandbox runs 4.2.0, ZK isn't even mentioned.

There are two node layouts. Combined mode — every node is both a broker and a potential controller. Dedicated mode — separate controller nodes (voters only, no partition storage). For a small cluster combined mode is simpler; for prod with dozens of brokers people usually go dedicated.

## Sandbox topology

The course sandbox is combined mode. Three nodes, each one a broker and a voter at the same time.

```
                      host (your Mac/Linux)
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
   localhost:19092     localhost:19093     localhost:19094
          │                   │                   │
   ┌──────┴───────┐    ┌──────┴───────┐    ┌──────┴───────┐
   │   kafka-1    │    │   kafka-2    │    │   kafka-3    │
   │ broker + ctl │    │ broker + ctl │    │ broker + ctl │
   │  node-id 1   │    │  node-id 2   │    │  node-id 3   │
   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
          │                   │                   │
          └───────────────────┼───────────────────┘
                              │
                       Raft over :9093
                       (controller listener)
                              │
                    elect the active controller
                    replicate __cluster_metadata
```

Now layer by layer:

- **EXTERNAL listener (:9094 inside the container, mapped to 19092/19093/19094 on the host)** — for clients from your machine. This is where `kgo.Client` knocks.
- **INTERNAL listener (:9092)** — for broker-to-broker traffic inside the docker network. Partition replication runs here.
- **CONTROLLER listener (:9093)** — Raft. Voters vote for the leader, replicate metadata. A client has no business going there.

ClusterID is fixed (`5nnS6DRtQnKwoMjkkVxxug`) — set in `docker-compose.yml` so the sandbox survives `docker compose down` without losing identity.

Min ISR = 2, default replication factor = 3. That means: data lives on three nodes, and a write needs an acknowledgement from two. If one falls — you won't notice. If two fall — a producer with `acks=all` will start getting `NotEnoughReplicas`. More on this in [Acks and durability](../../../../02-producer/02-02-acks-and-durability/i18n/ru/README.md) and [Transactions and EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/ru/README.md).

## What's inside `__cluster_metadata`

It helps to see it once so it stops being scary. The topic is hidden (a system topic), but it's there.

Inside — records about topics, partitions, configs, ACLs, voter membership changes. Every broker on start pulls this log from the beginning, rebuilds a local metadata snapshot, then watches the tail and applies updates as they arrive. The controller writes any changes there through its Raft layer.

You can peek like this:

```sh
docker exec kafka-1 /opt/kafka/bin/kafka-dump-log.sh \
  --cluster-metadata-decoder \
  --files /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log | head -50
```

You'll see records like `RegisterBrokerRecord`, `TopicRecord`, `PartitionRecord`, `ConfigRecord` and so on. Don't dig into the format right now — just remember that it's a regular log with typed records.

## What our program shows

`cmd/quorum-status/main.go` is the Go equivalent of `kafka-metadata-quorum.sh ... describe --status`. It makes two requests:

1. **`kadm.Client.BrokerMetadata`** — returns ClusterID, the broker count, host/port/rack of each one, and a `Controller` field. A small trap: in KRaft this field does not show the Raft-leader directly. The broker returns the id of the broker through which controller-requests can be proxied. The name in the output — `MetadataControllerProxy`.
2. **`kmsg.DescribeQuorumRequest`** on the `__cluster_metadata` topic, partition 0 — returns the real Raft-leader and the list of voters. This is the same thing that `kafka-metadata-quorum.sh ... describe --status` prints. The name in the output — `RaftLeader`.

Why two requests at once. The first — to see that the general cluster metadata is reachable without a shell. The second — to see that for KRaft-specific questions ("who is the current metadata leader") you have to drop down to `kmsg` (the low-level Kafka API), because kadm doesn't wrap DescribeQuorum in a convenient method yet. This is normal franz-go practice: high-level kadm for the common case, kmsg for the rare and specific one.

The first request — one line through kadm:

```go
admin := kadm.NewClient(cl)

md, err := admin.BrokerMetadata(rpcCtx)
// md.Cluster      — cluster ClusterID (the same UUID as in docker-compose.yml)
// md.Controller   — id of the proxy broker for controller-requests (NOT the Raft-leader)
// md.Brokers      — []BrokerDetail with NodeID/Host/Port/Rack
```

The second — by hand through kmsg, because there's no ready wrapper:

```go
req := kmsg.NewPtrDescribeQuorumRequest()
topic := kmsg.NewDescribeQuorumRequestTopic()
topic.Topic = "__cluster_metadata"
part := kmsg.NewDescribeQuorumRequestTopicPartition()
part.Partition = 0
topic.Partitions = []kmsg.DescribeQuorumRequestTopicPartition{part}
req.Topics = []kmsg.DescribeQuorumRequestTopic{topic}

resp, err := req.RequestWith(ctx, cl)
p := resp.Topics[0].Partitions[0]
// p.LeaderID       — the real Raft-leader (the active controller)
// p.CurrentVoters  — list of voters: [{ReplicaID:1}, {ReplicaID:2}, {ReplicaID:3}]
```

Then the code just glues the two answers together: the broker with the id from `LeaderID` gets the `broker + active controller` role in the table, the other voters — `broker + voter`.

Further in the course we almost never call the CLI — everything goes through franz-go and kadm. We'll come back here in [Groups and rebalance](../../../../03-consumer/03-01-groups-and-rebalance/i18n/ru/README.md) and [Transactions and EOS](../../../../04-reliability/04-01-transactions-and-eos/i18n/ru/README.md), when you need to know who the current controller is to understand the consequences of its re-election.

## Run

The sandbox must be up (`docker compose up -d` from the repo root). Then:

```sh
make run
```

Expected output (ids will differ, RaftLeader — any of 1/2/3):

```
ClusterID:               5nnS6DRtQnKwoMjkkVxxug
Brokers:                 3
MetadataControllerProxy: 1  (BrokerMetadata.Controller; in KRaft — proxy, not Raft-leader)
RaftLeader:              3  (DescribeQuorum on __cluster_metadata; this is the active controller)
CurrentVoters:           [1 2 3]

NODE  HOST       PORT   RACK  ROLE
1     localhost  19092  -     broker + voter
2     localhost  19093  -     broker + voter
3     localhost  19094  -     broker + active controller
```

If you want to compare against the CLI version:

```sh
make quorum-cli
```

This target pokes `kafka-metadata-quorum.sh describe --status` inside the kafka-1 container — the official shell script from the Kafka distribution. The fields differ, but `LeaderId` from the CLI matches `RaftLeader` from the Go version (and `CurrentVoters` — our list). If it matches — great, you can now talk to Kafka from Go without a shell.

## What you learned

- Kafka is an append-only log that scales through partitions and is replicated across brokers.
- The broker stores data, the controller hands out roles, the producer writes, the consumer reads.
- KRaft is Kafka without ZooKeeper, metadata lives in `__cluster_metadata` through Raft.
- The sandbox is three nodes in combined mode, the voters double as brokers.
- Any CLI operation on metadata can be repeated from Go through `kadm.Client`.

In the next lecture ([Topics and partitions](../../../01-02-topics-and-partitions/i18n/ru/README.md)) we'll dig into topics and partitions — what "to partition" means, how the key drives distribution, why the partition count can't be reduced, and why the model is so strict in the first place.
