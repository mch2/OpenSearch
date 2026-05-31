# Producer Back-Pressure

## Background: why the eventloop queue exists

Multiple producer threads may call `channel.sendResponseBatch(...)` concurrently
on the same stream (concurrent segment search, parallel batch generation). Arrow
Flight's `ServerStreamListener.putNext` is **not** safe for concurrent calls, and
the contract requires `start → putNext × N → completed` in strict order on a
single thread; out-of-order or interleaved calls corrupt the stream.

Each `FlightServerChannel` therefore funnels submissions through a per-channel
single-threaded executor (the "eventloop"). Producer threads enqueue
`BatchTask`s; the eventloop dequeues them and performs the actual zero-copy
transfer plus `putNext`. This serialises ordering without making producer threads
contend on a per-channel mutex — important because direct mutex contention at
`putNext` time was measurably blocking concurrent slice threads under load.



The Flight transport ships two server-side producer implementations. Selection is
node-scope and decided at startup.

| Producer | Default | When `putNext` is called |
|---|---|---|
| `BackpressureArrowFlightProducer` | enabled | only after gRPC reports `isReady() == true` for the stream |
| `ArrowFlightProducer` | — | unconditionally (no flow-control gating) |

## Why back-pressure exists

gRPC's `CallStreamObserver.onNext` is non-blocking. If the application keeps calling
it past the per-stream readiness threshold, gRPC will buffer the bytes (in its own
write queue plus Netty's outbound buffer) until the wire drains. Under a slow
consumer those buffers retain `ArrowBuf` references and eventually exhaust the
flight pool allocator, causing `OutOfMemoryException` on the next batch's
`VectorSchemaRoot.create`.

`BackpressureArrowFlightProducer` honours gRPC's `isReady()` contract: before the
producer thread submits a batch, it parks on `BackpressureStrategy.waitForListener`
until gRPC drains its outbound buffer below the readiness threshold (default 32 MiB,
defined in `OSFlightServer.DEFAULT_BACKPRESSURE_THRESHOLD`).

## Settings

| Setting | Default | Property |
|---|---|---|
| `arrow.flight.producer.backpressure.enabled` | `true` | node-scope |
| `arrow.flight.channel.ready_timeout` | `30s` | node-scope |

`ready_timeout` caps how long the producer thread parks before failing the batch
with `StreamErrorCode.TIMED_OUT`. `100ms` minimum.

## Operator-facing behaviour

### Fast consumer (steady state)
The readiness check is essentially free: the producer's call to `awaitReadyOrThrow`
returns immediately because gRPC's outbound buffer stays below threshold.
Throughput matches the original (unguarded) producer.

### Slow consumer
- `isReady()` flips false once gRPC's per-stream outbound buffered-bytes crosses
  the threshold. The producer's next `sendResponseBatch` parks.
- gRPC fires `OnReadyHandler` after the consumer drains some bytes from the wire
  (HTTP/2 `WINDOW_UPDATE` arriving). The producer wakes and resumes.
- Sustained slowness for longer than `ready_timeout` causes the batch to fail
  with `TIMED_OUT`; the producer thread is freed and the stream terminates.

### Cancellation
Client-cancel propagates via gRPC's `OnCancelHandler` into the strategy's cancel
callback. Any thread parked on `waitForListener` wakes promptly with
`StreamErrorCode.CANCELLED`.

## Sizing the flight pool

The per-channel pinned memory at steady state is approximately:

```
gRPC threshold (32 MiB)
  + a few in-flight batches in the eventloop's queue
  + small allocator overhead (schema, dictionary, metadata)
```

Rule of thumb for `native.allocator.pool.flight.max`:

```
flight pool max  >=  N concurrent streams × (32 MiB + ~16 MiB headroom)
```

If the pool max is smaller than the gRPC threshold, the back-pressure mechanism
cannot engage — the allocator OOMs before `isReady()` flips. Size accordingly.

## Known limitation

The per-channel eventloop executor's queue is currently unbounded. A producer that
allocates batches significantly faster than gRPC can drain (i.e. pathologically
tight loop with negligible per-batch compute, paired with a slow consumer) can fill
the queue with retained batches before `isReady()` flips false on the gRPC side.
In that case the producer can still OOM the pool.

Realistic streaming workloads (aggregation, transform) have non-trivial per-batch
compute that paces the producer enough for back-pressure to engage. A byte-aware
bounded queue would tighten the bound; it is not part of this iteration.

## Disabling

To exercise the original (unguarded) producer — for example, to compare allocator
behaviour:

```yaml
arrow.flight.producer.backpressure.enabled: false
```

This selects `ArrowFlightProducer`, which calls `putNext` unconditionally.
