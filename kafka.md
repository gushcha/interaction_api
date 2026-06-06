# Kafka integration — interaction_api

`interaction_api` is the **client-facing gateway**. It bridges WebSocket connections
to the Kafka message bus: forwarding client requests in, and delivering results back
to the right client.

---

## Message flow

```
browser
  │  {action:"get_weather"}
  ▼
WebSocket handler ──► weather.requests  (Kafka)
                                │
                         core_api processes
                                │
result consumer   ◄── weather.results   (Kafka)
  │  broadcast
  ▼
WebSocket handler ──► browser (if client_id matches)
```

## Topics

| Topic              | Role     | Action                                           |
|--------------------|----------|--------------------------------------------------|
| `weather.requests` | Producer | Publishes `{action:"get_weather", client_id}`    |
| `weather.results`  | Consumer | Receives `{action:"weather_result", client_id, payload}` |

## Client tracking

Each WebSocket connection is assigned a UUID (`client_id`) at connect time. This ID
travels inside the Kafka envelope so the correct connection receives the result.

The result consumer publishes received envelopes to a `tokio::sync::broadcast` channel.
Every active WebSocket handler has a receiver and checks `envelope.client_id == my_id`
before forwarding to the browser.

This is a simple fan-out approach suited for a study project. In production you would
use a per-client channel map to avoid broadcasting to every connection.

Reference: [tokio broadcast channel](https://docs.rs/tokio/latest/tokio/sync/broadcast/index.html)

## Consumer group

Group id `interaction-api`. With 2 pods, Kafka assigns partitions across both — each
result message is consumed by exactly one pod. If the pod that receives the result
doesn't have the target WebSocket connection, the broadcast simply finds no match and
the message is dropped. In practice this means ~50% of results are silently dropped
with 2 pods and a single-partition topic.

**For a study project this is acceptable.** For production: use a shared session store
(Redis pub/sub, or sticky routing at the ingress level) so results always reach the
pod holding the connection.

Reference: [Kafka partitions and consumers](https://developer.confluent.io/courses/apache-kafka/partitions/)

## Configuration

`KAFKA_BROKERS` environment variable — set in k8s Deployment, defaults to `localhost:9092`.

## Library: rdkafka

[rdkafka](https://docs.rs/rdkafka) wraps librdkafka (Confluent's C library). The
`cmake-build` cargo feature compiles it from source — no system dependency needed.

Reference: [rdkafka docs](https://docs.rs/rdkafka/latest/rdkafka/)
