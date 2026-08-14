# Replication Log Performance Analysis and Metrics Guide

This document captures the analysis of the Phoenix HA replication log performance,
the queueing-theory framework used to reason about it, and a metrics plan for
production observability.

## Background

The replication log path uses a single LMAX Disruptor consumer thread per HA group.
Producers (RPC handlers in `IndexRegionObserver`) call `ReplicationLogGroup.append()`
and later `ReplicationLogGroup.sync()`. The disruptor consumer drains events,
delegates to `LogFileWriter`, and triggers HDFS fsync at sync-event boundaries.

A perf comparison was made against HBase WAL on the same HDFS, with the same
durability setting (`hbase.wal.hsync`). The replication path appeared to have
lower per-sync time, which raised the question of whether that was a real win or a
measurement artifact.

A separate optimization (PHOENIX-7862, fix size-based rotation loop) reduced
rotation events from 17,473 to 1,879 in a perf run. `appendTime` max dropped
from ~67ms to ~36ms occasionally, but mean append throughput barely moved. This
document explains why.

## Where time is spent in the replication path

### Producer side: `ReplicationLogGroup.append()`

Located at `phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLogGroup.java:550`.

The producer-side work is small:

- two volatile reads (`isClosed`, `fatalException`)
- `ringBuffer.next()` — claims a sequence; **only blocks if the ring buffer is full**
- `event.setValues()` + `ringBuffer.publish()`
- a metric update

Producer-observed latency (`appendTime`) is dominated by `ringBuffer.next()`. When
the buffer has space, this is sub-microsecond. When the buffer is full, the call
blocks until the consumer drains a slot.

### Producer side: `ReplicationLogGroup.sync()`

Located at `ReplicationLogGroup.java:594` calling `syncInternal()` at line 617.

`sync()` publishes a SYNC event and blocks on `syncFuture.get(syncTimeoutMs)`.
The future completes only after the consumer has drained everything ahead of the
SYNC event and run `currentModeImpl.sync()` (HDFS fsync).

So `syncTime` includes:

- ring-buffer wait for the SYNC event
- consumer drain time for everything queued ahead
- the actual fsync to HDFS (`modeSyncTime` measures this portion)

### Consumer side: `LogEventHandler.onEvent`

Located at `ReplicationLogGroup.java:1153`.

Per event the consumer does:

- `LogFileWriter.append()` — serialization + write to in-memory buffer
- occasional inline fsync if `append()` fills a block (block-full sync at
  `ReplicationLog.java:343`)
- at end-of-batch: `processPendingSyncs()` which runs `currentModeImpl.sync()`
  (HDFS fsync) for any pending SYNC futures

The consumer is **single-threaded by design**. It is the one cashier in the queue.

## Queueing theory primer

### Little's Law

For any stable queue, regardless of distribution:

```
L = λ × W
```

- **L** = average number of items in the system (queue + being served)
- **λ** = arrival rate (items per unit time)
- **W** = average time an item spends in the system (waiting + being served)

This is a conservation law, not a model. Service time is bundled inside `W`.

For the ring buffer:

```
average queue depth = arrival rate × ringBufferTime
```

### Service rate and utilization

- **μ (service rate)** = `1 / mean_service_time_per_item`. A property of the
  consumer.
- **λ (arrival rate)** = how often producers call `append()`. A property of the
  workload.
- **ρ (utilization)** = `λ / μ`. The fraction of time the consumer is busy.

Three regimes:

- ρ < 1: stable, queue finite on average
- ρ = 1: queue grows without bound under variability
- ρ > 1: consumer can't keep up, queue grows forever

### The hockey stick

For an M/M/1 queue (Poisson arrivals, exponential service times, one server),
mean wait time is:

```
W = 1 / (μ - λ)
```

Wait time grows nonlinearly as ρ approaches 1:

| λ      | ρ = λ/μ | W (assuming μ = 5,000/sec) |
|--------|---------|----------------------------|
| 1,000  | 0.20    | 0.25 ms                    |
| 4,000  | 0.80    | 1.00 ms                    |
| 4,500  | 0.90    | 2.00 ms                    |
| 4,900  | 0.98    | 10.00 ms                   |
| 4,990  | 0.998   | 100.00 ms                  |
| 5,000  | 1.00    | ∞                          |

From 80% to 98% utilization, wait time grew 10×. This is why "the system was fine
yesterday and is on fire today" — small bumps at high ρ cause large latency
jumps.

### M/M/1 notation

`M/M/1` = Poisson arrivals (M) / exponential service (M) / one server (1).

The replication system is closer to **M/G/1** (general service distribution) with
high service-time variance, because consumer time is bimodal: most events are
fast, but block-full syncs and rotations occasionally take milliseconds. M/M/1
underestimates wait time when service variance is high — real waits will be
worse than M/M/1 predicts.

M/M/1 is still useful for back-of-envelope reasoning because the qualitative
shape (hockey stick at high ρ) is the same across all queueing models.

## Why per-record vs per-batch matters

HBase WAL emits **one WALEdit per batch** of mutations. The replication path
emits **one record per mutation**.

Effect on consumer load: for the same logical batch of N mutations,

- WAL: 1 ring buffer event, 1 `writer.append()`, 1 fsync at sync point
- Replication: N ring buffer events, N `LogFileWriter.append()` calls, 1 fsync

So `λ_replication = N × λ_WAL`. With the same μ on the consumer side,
`ρ_replication = N × ρ_WAL`.

If WAL operates at ρ=0.1 (safe), replication at the same workload operates at
ρ=1.0 (saturated). Coalescing to one event per batch would cut λ by N, moving
the system far down the hockey stick.

This is a structural change, not a tuning knob, but it is likely the single
biggest lever for closing the throughput gap with WAL. The decision should be
made after `modeSyncTime` data is available in production to confirm where the
time actually goes.

## Diagnosing the perf run

### Observation 1: `appendTime` max reduced (67ms → 36ms) after rotation fix

The fix reduced rotation events 9× (17,473 → 1,879). Because rotation runs on
the consumer thread (replay + writer swap), each rotation is a consumer stall.
Long stalls fill the 32K ring buffer, at which point producers block in
`ringBuffer.next()` and `appendTime` spikes.

Cutting rotations 9× reduces the *frequency* of stall episodes, so the worst
producer stall in any window is shorter. Mean append throughput is unchanged
because steady-state appends never hit a full ring buffer; the buffer carries
significant queue depth without ever filling.

Conclusion: tail latency win, not a throughput win.

### Observation 2: `ringBufferTime` p99 in ms, but `appendTime` mostly fast

This is consistent, not contradictory. Apply Little's Law:

```
queue depth = arrival rate × ringBufferTime
```

At λ = 5,000/sec and ringBufferTime = 5ms, queue depth = 25 events. The 32K
buffer is essentially empty. Producers don't block.

For the buffer to actually fill at this arrival rate, ringBufferTime would have
to grow to 32,768 / 5,000 ≈ 6.5 seconds. That doesn't happen in steady state.

What "high ringBufferTime" really tells you: the consumer's per-event cost
times the small queue depth produces ms-scale wait, but the queue depth itself
is small.

### Observation 3: median ringBufferTime = 600µs, p99 = 20ms

Median 600µs means the consumer is generally busy, not idle. If it were idle, an
arriving event would be picked up within `YieldingWaitStrategy` wakeup latency
(single-digit µs).

p99/median ≈ 33×. In steady-state M/M/1, p99/median ≈ 6–7×. The 33× ratio
indicates **bursty stalls, not steady saturation**. Likely sources:

- block-full syncs in `LogFileWriter.append()` (occasional inline fsync)
- rotation/replay episodes
- mode transitions or HDFS pipeline hiccups

### Observation 4: `syncTime > ringBufferTime`

Expected and structural:

```
syncTime ≈ ringBufferTime(of the SYNC event) + modeSyncTime + small overhead
```

`syncTime` includes everything `sync()` waits for; `ringBufferTime` is just the
queue wait portion.

### Observation 5: HBase WAL `syncTime` is pure HDFS fsync wall time

This is critical for any comparison: HBase WAL `syncTime` measures pure
HDFS hsync wall time, not handler-end-to-end wait. Tracing the metric chain:

- `MetricsWAL.postSync(timeInNanos, handlerSyncs)` is the listener callback
  (`hbase-server/.../MetricsWAL.java:50`). It does
  `source.incrementSyncTime(timeInNanos / 1_000_000)`.
- `incrementSyncTime` adds to a histogram registered as `SYNC_TIME = "syncTime"`
  (`MetricsWALSourceImpl.java:123`, `MetricsWALSource.java:58`).
- `postSync` is dispatched from `AbstractFSWAL.postSync` which iterates
  listeners.

Two WAL implementations call `postSync`, both with similar semantics:

**AsyncFSWAL** (`AsyncFSWAL.java:382`):
```java
postSync(System.nanoTime() - startTimeNs, finishSync());
```
where `startTimeNs` is set immediately before the async sync call at line 420:
```java
final long startTimeNs = System.nanoTime();
addListener(writer.sync(shouldUseHsync), (result, error) -> { ... });
```
So `syncTime` is `writer.sync()` wall time only. Pure HDFS hsync round-trip.

**FSHLog** (`FSHLog.java:688`):
```java
postSync(System.nanoTime() - start, syncCount);
```
where `start` is set after a SyncRunner has dequeued the SyncFuture, just
before calling `writer.sync()` (line 662). It does NOT include the time the
handler waited for a SyncRunner to pick up its SyncFuture.

In both cases, **HBase WAL `syncTime` ≈ pure HDFS hsync wall time**. It does
not include handler wait, queueing, or append work.

**Implication for the Phoenix-vs-HBase comparison:**

The right comparison is:

| Phoenix metric | HBase metric | Compares |
|----------------|--------------|----------|
| `modeSyncTime` (when shipped) | HBase WAL `syncTime` | apples-to-apples HDFS hsync |
| `syncTime` | (nothing direct) | end-to-end caller wait, includes queueing |

So statements like "Phoenix replication is N ms slower than HBase WAL" must
specify which Phoenix metric:

- "Phoenix `syncTime` p50 is ~1.5ms slower than HBase WAL `syncTime` p50" is
  comparing caller-observed end-to-end wait against pure HDFS fsync. The
  difference includes queueing and drain in addition to any HDFS-layer
  difference. **This is what we have today, and it is apples-to-oranges.**
- "Phoenix `modeSyncTime` p50 is N ms slower than HBase WAL `syncTime` p50"
  would be the apples-to-apples HDFS-layer comparison. **This requires
  shipping `modeSyncTime`.**

If `modeSyncTime` matches HBase WAL `syncTime`, the entire Phoenix-vs-HBase
gap is on the Phoenix consumer path (queueing, drain, per-event overhead),
not at the HDFS layer. If `modeSyncTime` is itself slower than HBase WAL
`syncTime`, the HDFS layer contributes — likely candidates: different output
stream class (standard `FSDataOutputStream` vs HBase's optimized
`AsyncFSOutput`), block placement locality, pipeline length, or
bytes-per-fsync.

## Reducing `ringBufferTime`

In rough order of impact:

### 1. Coalesce mutations into per-batch records (highest leverage)

Group all mutations in a batch into a single record. Cuts λ by the average batch
size, moving down the hockey stick exponentially. Matches WAL's coalescing
model.

### 2. Move serialization off the consumer

If serialization happens on the producer side (before publishing), the consumer
just memcpys bytes into the in-memory buffer. Per-event consumer cost drops
significantly.

### 3. Decouple block-full syncs from `append()`

Currently when `LogFileWriter.append()` fills a block, fsync happens inline on
the consumer thread. Pipeline the block-full sync — buffer the next block's
appends while the previous block syncs in another thread.

### 4. Drop per-event work on the consumer

- `checkAndReplaceWriter(true)` inside `apply()` does a CAS on every append.
  Could be a cheaper `pendingWriter.get() != null` short-circuit.
- The `apply()` retry-loop scaffolding even on the success path.

### 5. Tune the wait strategy

`YieldingWaitStrategy` yields when consumer outruns producers.
`BusySpinWaitStrategy` cuts wakeup latency at the cost of pinning a core. Useful
if CPU is plentiful and latency matters more than CPU.

### 6. Single-producer ring buffer (if applicable)

`ProducerType.SINGLE` removes the CAS in `ringBuffer.next()`. Reduces
producer-side cost. Doesn't directly reduce `ringBufferTime`.

### 7. Larger ring buffer

This **increases** tolerance for consumer stalls (fewer producer-blocking
episodes) but does **not** reduce `ringBufferTime` — actually makes it worse,
because more queue depth means more wait time for events behind. Don't increase
ring size if the goal is `ringBufferTime`.

## Production metrics plan

### Aggregation scope and percentile caveats

**Single HA group per RS assumption.** This analysis assumes one HA group per
region server. With one HA group, the `MetricsReplicationLogGroupSource`
metrics emitted from an RS reflect exactly that group's behavior — aggregate
metrics equal per-group metrics. If a deployment ever runs multiple HA groups
on a single RS, the metrics blend distributions across groups, hot-group
detection becomes impossible, and per-group tagging becomes necessary.

**Per-RS percentiles, not fleet percentiles.** Each RS emits its own histogram
locally; HBase's `MutableHistogram` computes percentiles over the scrape
window's samples on that RS. So a metric like `syncTime.p99` is the p99 for
that RS over that window. It is **not** the fleet-wide p99.

**Averaging per-RS p99s does not give fleet p99.** Percentiles are not linear:

- `avg(rs:syncTime_p99)` — wrong, averages percentiles
- `max(rs:syncTime_p99)` — overconservative, shows worst RS but not fleet p99
- `quantile(0.99, rs:syncTime_p99)` — wrong, this is "p99 of the p99 values"

For correct fleet-wide percentiles, the monitoring pipeline must ship
histogram bucket counts (not pre-computed quantiles) and merge them centrally.
Prometheus native histograms or `histogram_quantile()` over `_bucket` time
series do this correctly. JMX-scraped pre-computed quantiles do not.

**Pragmatic approach for perf comparison work:** pick a representative RS and
analyze its per-RS percentiles in isolation. Compare WAL p99 and replication
p99 on the same RS, same window. Spot-check on a few RSes to confirm the
comparison holds. This is what perf debugging actually does and is sufficient
for the WAL vs replication question.

For ongoing dashboards: `avg(rs:p99)` and `max(rs:p99)` are both useful for
trends and alerting even though neither is true fleet p99. Add a per-RS table
or graph for drill-down. If fleet-wide percentiles become a hard requirement,
invest in changing the metric path to ship histogram buckets.

### Metrics already in code

From `MetricsReplicationLogGroupSource.java`:

- `appendTime` — producer-observed time inside `append()`
- `syncTime` — producer-observed end-to-end sync wait
- `ringBufferTime` — time events spend in the ring buffer
- `rotationCount` — total rotations
- `rotationFailures` — failed rotations
- `rotationTime` — time taken per rotation
- `syncToSafTransitions` — SYNC → SAF mode flips
- `modeSyncTime` — pure HDFS fsync cost (added but not yet in prod)
- `batchSize` — events drained per disruptor batch (added but not yet in prod)
- `pendingSyncCount` — sync futures coalesced per fsync (added but not yet in
  prod)

Plus `ReplicationSyncTime` in `IndexRegionObserver` — caller-observed time for
the replication path.

### What to gather from production

**Latency histograms — emit p50, p99, max:**

- `appendTime` — tail tracks producer backpressure
- `syncTime` — caller-experienced sync wait
- `ringBufferTime` — consumer queueing severity
- `modeSyncTime` — HDFS fsync floor
- `ReplicationSyncTime` — end-to-end caller experience

Skip `appendTime` mean (long-tail dominated, not informative). Skip
`ringBufferTime` max (too noisy; p99 captures bursts).

**Distribution metrics — emit mean and p99:**

- `batchSize` — distribution shape matters more than max
- `pendingSyncCount` — fsync amortization

**Counters — emit raw counts, derive rates in monitoring:**

- `appendTime.count` → λ (arrival rate)
- `syncTime.count` → caller sync rate
- `modeSyncTime.count` → fsync rate
- `rotationCount` → rotations/sec (anomaly detection)
- `syncToSafTransitions` → mode flips (anomaly detection)
- consumer events drained per second → μ (most important addition)

### Derivable quantities from counts

```
events_per_fsync         = appendTime.count / modeSyncTime.count
events_per_sync_call     = appendTime.count / syncTime.count
syncs_per_fsync          = syncTime.count / modeSyncTime.count
predicted_queue_depth    = λ × ringBufferTime.mean   (Little's Law check)
```

`syncs_per_fsync` is your fsync amortization factor. If close to 1, every caller
sync triggers its own fsync — bad amortization. If high, fsyncs are shared
across many callers — good amortization.

### Recommended dashboard layout

**Row 1 — caller experience (what users feel):**

- `ReplicationSyncTime` p50, p99, max
- `syncTime` p50, p99, max

**Row 2 — pipeline health (where time goes):**

- `modeSyncTime` p50, p99 (HDFS fsync cost)
- `ringBufferTime` p50, p99 (consumer queueing)
- `appendTime` p99, max (producer backpressure indicator)

**Row 3 — workload shape:**

- `batchSize` mean, p99
- `pendingSyncCount` mean, p99
- counter rates: events drained/sec (μ), appends/sec (λ), syncs/sec,
  rotations/sec

### What you can compute today (without modeSyncTime, batchSize)

Currently deployed: `appendTime`, `syncTime`, `ringBufferTime`,
`ReplicationSyncTime`, plus their counts.

Computable:

- λ from `appendTime.count`
- caller wait from `syncTime`
- producer backpressure from `appendTime` max
- consumer queueing from `ringBufferTime` p99
- events per caller-sync from `appendTime.count / syncTime.count`

Two ratios worth watching:

- `syncTime.p99 / ringBufferTime.p99` — close to 1 means SYNC event is just
  sitting in the queue; much greater than 1 means substantial work after
  consumer pickup (the hidden `modeSyncTime`).
- `ringBufferTime.p50 × λ` — average queue depth via Little's Law.

### What you cannot answer without `modeSyncTime` and `batchSize`

- "Is the cost HDFS or our code?" Without `modeSyncTime`, you cannot separate
  HDFS fsync floor from queueing/drain overhead.
- "Is fsync amortized?" Without an fsync count, you can only see
  events-per-sync-call, not events-per-fsync.
- "How big are disruptor batches?" Without `batchSize`, you cannot tell if the
  consumer is processing one event at a time or coalescing.
- "Where is the consumer spending time?" Without `modeSyncTime`, you cannot
  subtract the fsync portion to estimate per-event append cost.

### Minimum addition

If shipping only one new metric, ship **`modeSyncTime`** first. It directly
answers the WAL-vs-replication comparison and separates HDFS floor from
queueing — the most fundamental partition.

`batchSize` is a close second — without it, when you see high `ringBufferTime`,
you cannot distinguish "consumer is slow per-event" from "consumer drains big
batches efficiently but there's a big batch in flight."

## Real data vs theoretical predictions

This section captures a worked example using production data from one RS over
a 1h 47m test window (2026-05-28 23:54:51 → 2026-05-29 01:42:12 UTC). It tests
the queueing-theory framework against actual measurements and shows what can
be inferred without `modeSyncTime`.

### Inputs

```
Window:                  6,432 seconds
AppendOps:               197,693,721
SyncOps:                 392,115
ReplicationSyncOps:      392,128

Derived rates:
  λ (arrival rate)       30,737 events/sec
  sync_rate              61.0 syncs/sec
  events_per_sync        504

Latency distributions (replication):
  ReplicationSyncTime    p50=3.18 ms,   p99=11.9 ms,   max=188 ms
  syncTime               p50=3.08 ms,   p99=13.9 ms,   max=187 ms
  ringBufferTime         p50=0.49 ms,   p99=4.99 ms
  appendTime             p99=0.3 µs,    max=2.65 ms

HBase WAL on the same RS:
  WAL syncTime           p50=1.00 ms,   p99=7.52 ms
```

### Check 1: Little's Law — queue depth

```
L = λ × W = 30,737 × 0.00049 = ~15 events average queue depth
```

Out of 32,768 ring buffer slots, the buffer is essentially empty.

**Prediction:** producer never sees backpressure → `appendTime` should be
sub-µs.
**Observed:** `appendTime` p99 = 0.3µs.
**Verdict:** matches.

### Check 2: M/M/1 utilization estimate

Inverting `W = ρ / (μ × (1-ρ))` at `W = 490µs` and `λ = 30,737`:

```
μ ≈ 32,800 events/sec
ρ = λ/μ ≈ 0.94
```

The system is operating at **~94% utilization**. High but stable, consistent
with the observed finite queues and no runaway growth.

### Check 3: Per-event consumer cost decomposition

If ρ=0.94, per-event consumer cost ≈ 1/μ ≈ 30.5µs. Cross-check by accounting
for how the consumer spends its time:

```
fsync time per second:    61 fsyncs × ~3 ms ≈ 180 ms/sec
remaining time:           1000 - 180 = 820 ms/sec
time per non-fsync event: 820 ms / 30,737 events ≈ 27 µs/event
fsync share per event:    ~3 ms / 504 events ≈ 6 µs/event
total per-event cost:     27 + 6 = 33 µs
```

Implied μ ≈ 1/33µs ≈ 30,300/sec. Two independent estimates (M/M/1 inversion
and time-budget decomposition) agree within 10%.

### Check 4: M/M/1 tail prediction

For exponential service:

```
p50_wait ≈ 0.69 × mean
p99_wait ≈ 4.6  × mean
```

Mean ringBufferTime ≈ 700µs (from p50=490µs). M/M/1 predicts p99 ≈ 3.2ms.

**Observed p99 = 4.99ms.** ~1.5× higher than M/M/1 predicts.

**Why:** real service time has high variance — block-full syncs and fsyncs
are 100× longer than per-event appends. M/G/1 with high variance predicts
longer tails than M/M/1. The 1.5× excess matches what you'd expect for
moderately bursty service. M/M/1 underestimates the tail, as expected.

### Check 5: syncTime decomposition

Model: `syncTime ≈ ringBufferTime(SYNC) + post-pickup work`, where
post-pickup work = drain of events queued ahead of the SYNC after consumer
picks it up + `processPendingSyncs()` overhead + the actual HDFS fsync
(`modeSyncTime`, currently unmeasured).

This decomposition lets us **infer** the post-pickup work cost from observed
data, not predict it independently:

**At p50:**
```
syncTime         = 3.08 ms (observed)
ringBufferTime   = 0.49 ms (observed)
inferred post-pickup work = 3.08 - 0.49 ≈ 2.6 ms
```

**At p99:**
```
syncTime         = 13.9 ms (observed)
ringBufferTime   = 4.99 ms (observed)
inferred post-pickup work = 13.9 - 4.99 ≈ 8.9 ms
```

These inferred values are **not** independent predictions of the model — they
are residuals after subtracting the measured queue wait. What the model does
do is identify that `syncTime > ringBufferTime` is structural and quantify
how much extra time the caller pays beyond pure queueing.

Comparing the inferred post-pickup work to WAL's `syncTime`:

| Statistic | inferred replication post-pickup | WAL syncTime | gap |
|-----------|----------------------------------|--------------|-----|
| p50       | ~2.6 ms                          | 1.00 ms      | ~1.6 ms |
| p99       | ~8.9 ms                          | 7.52 ms      | ~1.4 ms |

At p99 the gap closes — fsync floor dominates and matches WAL. At p50 the
gap is ~1.6ms, which is either:

- the actual `modeSyncTime` matches WAL (~1ms) and the extra ~1.6ms is
  consumer-side drain/overhead, **or**
- `modeSyncTime` itself is slower than WAL fsync (~2.5ms vs ~1ms)

The data alone cannot distinguish these without `modeSyncTime`. This is the
single most important reason to ship that metric.

### Check 6: fsync amortization

```
events_per_fsync = 504
per-event share of fsync = 3 ms / 504 ≈ 6 µs
```

Healthy. WAL likely amortizes more (multi-handler-per-batch), but 504:1 is
not wasteful.

### Where the model breaks

The 187ms max events. M/M/1 doesn't predict 187ms outliers when p99=14ms —
that's a ~13× tail beyond p99. Sources outside the queueing model:

- HDFS pipeline DataNode hiccup (slow ack, brief network blip)
- Rotation episode (writer creation can take tens of ms on first allocation)
- JVM safepoint or GC pause
- Mode transition

These are rare-event, non-queueing phenomena. They show up as max but don't
materially affect p50/p99.

### Summary table

| Check | Predicted | Observed | Fit |
|-------|-----------|----------|-----|
| Producer backpressure (Little's Law) | Empty buffer | appendTime p99 = 0.3µs | matches |
| Utilization (M/M/1 inversion) | ρ ≈ 0.94 | n/a (consistent) | matches |
| Per-event cost decomposition | 33µs | 30.5µs (M/M/1) | matches within 10% |
| Tail latency (M/M/1) | p99 ≈ 3.2ms | p99 = 4.99ms | 1.5× off (expected for M/G/1) |
| syncTime p50 decomposition | ringBufferTime + post-pickup residual | 0.49 + 2.6 = 3.08ms | identity (residual) |
| syncTime p99 decomposition | ringBufferTime + post-pickup residual | 4.99 + 8.9 = 13.9ms | identity (residual) |
| max outliers | M/M/1 doesn't model | 187ms | outside model |

### Implications for closing the WAL gap

**Important caveat on the comparison.** HBase WAL `syncTime` measures pure
HDFS hsync wall time only (see Observation 5). Phoenix's `syncTime` measures
end-to-end caller wait including ring buffer queueing and consumer drain.
So "Phoenix `syncTime` p50 = 3.08ms vs WAL p50 = 1.00ms" is comparing
end-to-end-wait against pure-fsync — apples-to-oranges. The right
apples-to-apples comparison is `modeSyncTime` (when shipped) ↔ HBase WAL
`syncTime`.

What we can say from current data:

- Phoenix end-to-end caller wait p50 is ~3.08ms.
- HDFS hsync wall time on this RS (per HBase WAL metric) is ~1.00ms.
- Phoenix `ringBufferTime` p50 is 0.49ms — events queue this long on the
  consumer side.
- Inferred post-pickup work p50 ≈ 2.6ms = `modeSyncTime + drain + overhead`,
  all bundled together. We cannot split this without `modeSyncTime`.

Three RSes show the same load-independent ~1.5ms p50 gap between Phoenix
`syncTime` and HBase WAL `syncTime` — across batch sizes from 6.67 to 504
events per sync. Load-independence rules out queueing as the dominant cause
and points toward per-fsync constant overhead, but the constant overhead
could live either at the HDFS layer (Phoenix's `modeSyncTime` > HBase's
fsync) or at the Phoenix consumer layer (per-fsync processing + drain).

Two levers, additive — but their applicability depends on what
`modeSyncTime` shows:

**Per-batch coalescing.** Cuts λ from ~30K/sec to ~61/sec (504× reduction).
Helps if the gap is consumer-side: reduces per-event overhead, reduces drain
of events behind a SYNC. Helps less if the gap is HDFS-side, though it does
reduce per-fsync byte volume by removing per-record framing overhead.

**Decouple fsync from consumer thread (HBase's parallel SyncRunners
pattern).** Lets the consumer keep draining while fsync runs. Helps if the
gap is consumer-side and driven by SYNC events waiting for the previous
fsync to complete. Doesn't help if the gap is HDFS-side per-fsync cost.

### What `modeSyncTime` would confirm or reject

Inferred post-pickup work p50 ≈ 2.6ms = `modeSyncTime + drain + overhead`,
combined. Compared to HBase WAL `syncTime` p50 = 1.00ms (pure HDFS hsync),
the gap is ~1.6ms. `modeSyncTime` would split this into its components:

- **If `modeSyncTime` p50 ≈ 1ms (matches HBase WAL `syncTime`):** Phoenix's
  HDFS fsync is just as fast as HBase's. The entire ~1.6ms gap is on the
  Phoenix consumer path (drain + per-batch processing) → per-batch coalescing
  and/or fsync decoupling are the right fix.
- **If `modeSyncTime` p50 ≈ 2.6ms (slower than HBase WAL):** Phoenix's HDFS
  fsync itself is slower. The gap is at the HDFS layer → investigate output
  stream class (standard `FSDataOutputStream` vs HBase's `AsyncFSOutput`),
  block placement locality, pipeline length, bytes-per-fsync.
- **If somewhere in between:** both layers contribute. Multiple fixes apply
  in proportion.

The cross-RS load-independence of the gap leans toward the second case —
per-fsync HDFS-layer overhead — but only the metric will resolve it
definitively.

## Plan

1. Ship granular metrics (`modeSyncTime`, `batchSize`, `pendingSyncCount`) to
   production.
2. Re-run perf comparison against HBase WAL using `modeSyncTime` for the
   apples-to-apples HDFS durability comparison.
3. If `modeSyncTime` ≈ WAL `syncTime` and `ringBufferTime` is the gap, the
   per-record-vs-per-batch coalescing change is the right next move.
4. If `modeSyncTime` itself is higher than expected, investigate
   bytes-per-fsync, block size, and pipeline contention.

## References

- `phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLogGroup.java`
- `phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLog.java`
- `phoenix-core-server/src/main/java/org/apache/phoenix/replication/StoreAndForwardModeImpl.java`
- `phoenix-core-server/src/main/java/org/apache/phoenix/replication/log/LogFileWriter.java`
- `phoenix-core-server/src/main/java/org/apache/phoenix/replication/log/LogFileWriterContext.java`
- `phoenix-core-server/src/main/java/org/apache/phoenix/replication/metrics/MetricsReplicationLogGroupSource.java`
