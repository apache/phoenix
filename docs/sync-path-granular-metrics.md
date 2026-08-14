# Plan: New metrics for tail-latency diagnosis in `ReplicationLogGroup`

## Context

We discussed how the LMAX Disruptor's batching in `ReplicationLogGroup` is good for throughput but can hurt latency for the *first* sync in a multi-event batch: it pays for every later append in the batch plus one fsync. To know whether observed tail latency comes from batching, from the underlying writer, from rotation, from peer init, or from back-pressure, we need finer-grained timing breakdowns than what's emitted today.

This plan inventories existing metrics and proposes a small, targeted set of new ones aimed at root-causing tail latency. It does **not** attempt full coverage of the subsystem — only metrics that decompose the latency a single `sync()` call traverses.

## Performance goal and the IndexRegionObserver SLI

The user-observable goal: `replicateMutations()` and `doPost()` are launched in parallel on the success path of `postBatchMutateIndispensably` (`IndexRegionObserver.java:2043-2055`):

```java
CompletableFuture<Void> postIndexFuture =
  CompletableFuture.runAsync(() -> doPost(c, context));
long start = EnvironmentEdgeManager.currentTimeMillis();
try {
  replicateMutations(c.getEnvironment(), miniBatchOp, context);
} finally {
  long duration = EnvironmentEdgeManager.currentTimeMillis() - start;
  metricSource.updateReplicationSyncTime(dataTableName, duration);
}
FutureUtils.get(postIndexFuture);
```

Both branches end in a durable WAL append (HBase WAL for the index tables in `doPost` → `postWriter.write`, replication WAL for replication via `group.append` + `group.sync`). The user-visible per-batch latency is therefore `max(postIndexUpdateTime, replicationSyncTime)`. The performance contract we want is:

**`replicationSyncTime ≤ postIndexUpdateTime`** — i.e. replication never becomes the long pole, so the parallelism actually pays off.

The contract only applies when the data table has at least one index. `doIndexWritesWithExceptions` short-circuits on an empty `postIndexUpdates` (`IndexRegionObserver.java:2145-2147`), so for index-less tables `doPost` is effectively free and `postIndexUpdateTime` ≈ 0; the comparison is meaningless there. For those tables, replication is unavoidably the long pole and the only useful signal is `replicationSyncTime` itself plus the RLG decomposition.

Both IRO metrics are emitted per-table by the same metric source today (`updateReplicationSyncTime` at `IndexRegionObserver.java:2052`, `updatePostIndexUpdateTime` at `IndexRegionObserver.java:2129`), so for indexed tables the comparison is already dashboardable — no IRO changes needed. The metrics proposed below sit *inside* `ReplicationLogGroup` and decompose `replicationSyncTime` to explain *why* it is high (whether the contract is violated, or — for index-less tables — simply why replication latency is what it is).

`replicateMutations()` (`IndexRegionObserver.java:2653-2705`) does **N `group.append(...)` + 1 `group.sync()`** for one minibatch. New metrics are **per-haGroup**, not per-table (one haGroup serves many tables). The two layers compose like this:

```
replicationSyncTime (IRO, per-table)
  ≈ N × appendTime (RLG, per-haGroup)        ← producer-side ringBuffer.next + publish
    + syncTime    (RLG, per-haGroup)          ← producer publish + future-await

syncTime (RLG)
  ≈ ringBufferTime              (queue dwell, exists)
    + onEventTime × eventsAhead (consumer processing of events ahead in batch — NEW)
    + modeSyncTime              (the actual fsync — NEW)
    + future-complete fanout    (negligible)
```

So the diagnostic flow becomes:

1. **Alert fires** when `replicationSyncTime > postIndexUpdateTime` for an indexed table (replication is the long pole), or when `replicationSyncTime` is high in absolute terms for an index-less table.
2. Operator looks up which haGroup serves that table.
3. **Decompose** the haGroup's `syncTime` using the new metrics:
   - `ringBufferTime` high → consumer is starved or stuck (look at `onEventTime`, `modeSyncTime`, `rotationStallTime`).
   - `modeSyncTime` high → HDFS hsync is slow (peer NN, datanode acks).
   - `batchSize` and `pendingSyncFanout` high → tail latency is the head-of-batch paying for batch followers; expected under load.
   - `ringBufferClaimWaitTime` non-trivial → producers blocked, ring is full → consumer is the bottleneck.
   - `peerInitTime` / `modeTransitionTime` outliers → cold-start or mode flip on the critical path.
4. The numbers should **roughly add up** to the IRO metric: `replicationSyncTime ≈ N×appendTime + ringBufferTime + (events-ahead × onEventTime) + modeSyncTime`. If they don't, we have an un-instrumented gap to chase.

The IRO metrics (`replicationSyncTime`, `postIndexUpdateTime`) are *not* being changed — they remain the SLI pair the contract is judged against. The new metrics are diagnostic decomposition for when the contract is violated. Per-table tagging stays on the IRO metrics (they tell you *who* is hurt); the RLG metrics stay per-haGroup (they tell you *why*).

## Existing metrics (writer/producer side)

Definitions live in `phoenix-core-server/src/main/java/org/apache/phoenix/replication/metrics/MetricsReplicationLogGroupSource.java`. Implementation in `MetricsReplicationLogGroupSourceImpl.java`.

| Metric | Type | Emitted at | What it captures |
|---|---|---|---|
| `appendTimeMs` (ns) → rename to `appendTime` | Histogram | `ReplicationLogGroup.java:575` | Producer-side wall time of `append()` (ring-buffer publish) |
| `syncTimeMs` (ns) → rename to `syncTime` | Histogram | `ReplicationLogGroup.java:609` | Producer-side wall time of `sync()` (publish + future await) |
| `ringBufferTime` (ns) | Histogram | `ReplicationLogGroup.java:1147` | Time event sat in ring buffer (publish → consumer pickup) |
| `rotationCount` | Counter | `ReplicationLog.java:448` | Successful rotations |
| `rotationFailures` | Counter | `ReplicationLog.java:450` | Failed rotation attempts |
| `syncToSafTransitions` | Counter | `ReplicationModeImpl.java:99` | Mode flips SYNC → SAF |
| `rotationTimeMs` (ns) → rename to `rotationTime` | Histogram | **defined but never emitted** | Should track rotation duration |

**What's missing for tail-latency analysis:** every stage *between* `ringBufferTime` (entry into consumer) and `syncTime` (producer wakeup) is a black box.

**Naming convention cleanup (in scope):** the existing `appendTimeMs`, `syncTimeMs`, and `rotationTimeMs` histograms are nanoseconds despite the `Ms` suffix (constants in `MetricsReplicationLogGroupSource.java:36-43`; values added via `.add(System.nanoTime() - startTime)`). Since the replication-log feature is still in development and these metric *names* have no production consumers yet, rename to drop the misleading suffix as part of this work:

- `appendTimeMs` → `appendTime`
- `syncTimeMs` → `syncTime`
- `rotationTimeMs` → `rotationTime`

The Java constant identifiers (`APPEND_TIME`, `SYNC_TIME`, `ROTATION_TIME`) already have clean names — only the string-literal values change. A repo-wide grep confirms no test references the literal strings `"appendTimeMs"` / `"syncTimeMs"` / `"rotationTimeMs"` as metric names; the only `rotationTimeMs` matches outside the metric source are an unrelated config-interval variable in `ReplicationLog.java:69` and `HAGroupStoreClient.java:124`. After rename, all time histograms in this metric source share one convention (no suffix; nanoseconds).

## Recommended new metrics

All histograms are nanoseconds, sampled with `System.nanoTime()` deltas. Counters are emitted at sites where we currently log + return.

Implementation is **tiered by signal-per-line-of-code**. Tier 1 alone is enough to validate the `replicationSyncTime ≈ N×appendTime + ringBufferTime + (events-ahead × onEventTime) + modeSyncTime` identity in production; Tiers 2–3 land after we see what Tier 1 explains.

### Tier 1 — must have

- **`modeSyncTime`** (histogram, §2): time of `currentModeImpl.sync()` only — wrap `ReplicationLogGroup.java:1035`. Dominant cost on the critical path.
- **`batchSize`** (histogram, §1): events drained per Disruptor batch. Single most important new signal.
- **`pendingSyncFanout`** (histogram, §1): `pendingSyncFutures.size()` at `processPendingSyncs` entry.
- **Wire up the existing `rotationTimeMs`** at the rotation site in `ReplicationLog.java` (`LogRotationTask.run()` body, around `:441-:448`). One-line change; the metric is already declared.

### Tier 2 — high value

- **`onEventTime`** (histogram, §1): wall time of one `LogEventHandler.onEvent` call.
- **`ringBufferClaimWaitTime`** (histogram, §3): time in `ringBuffer.next()` — back-pressure signal.

### Tier 3 — nice to have

- `modeAppendTime`, `rotationStallTime`, `modeTransitionTime`, `peerInitTime`, `ringBufferRemainingCapacity`.

### 1. Decompose consumer-side time per event

Goal: separate "queueing time" (ringBufferTime) from "consumer-thread work time" so we can tell whether the consumer is starved, slow, or just busy.

- **`onEventTime`** (histogram): wall time of one full `LogEventHandler.onEvent` call, measured around the body in `ReplicationLogGroup.java:1144-1192`. Lets us correlate batch size with per-event cost.
- **`batchSize`** (histogram): number of events drained per Disruptor batch. Increment a per-handler counter from each `onEvent`, then snapshot+reset on the single `if (endOfBatch)` branch at `ReplicationLogGroup.java:1172`. Reset must happen in a `finally` that wraps the entire `onEvent` body — the existing `catch (IOException)` at `:1175` and `catch (Throwable)` at `:1186` bypass the `endOfBatch` block, so a counter increment without a finally-reset would leak into the next batch. Answers "are batches large enough that head-of-batch syncs are paying for many followers?"
- **`pendingSyncFanout`** (histogram): `pendingSyncFutures.size()` snapshotted at the moment `processPendingSyncs` is entered (`ReplicationLogGroup.java:1030`), before the `isEmpty()` early-return. Tells us how many sync futures got coalesced into one `currentModeImpl.sync()` call. Distinct from `batchSize`: `batchSize` counts all events (data + sync) drained per Disruptor batch, while `pendingSyncFanout` counts only the sync futures consolidated into one fsync — they diverge whenever producers fan many appends per sync.

### 2. Decompose `processPendingSyncs`

Goal: separate fsync time from "everything else after the fsync" (future completion, mode-switch overhead).

- **`modeSyncTime`** (histogram): time of `currentModeImpl.sync()` only — wrap line `ReplicationLogGroup.java:1035`. This is the dominant cost on the critical path; without it we can't tell HDFS slowness apart from in-process queueing.
- **`modeAppendTime`** (histogram): time of `currentModeImpl.append(record)` per data event — wrap `ReplicationLogGroup.java:1159`. Distinguishes consumer-side append work (block compression, occasional internal sync on block-full) from the explicit sync path.

### 3. Producer back-pressure visibility

Goal: detect when producers block on `ringBuffer.next()` because the consumer can't keep up.

- **`ringBufferClaimWaitTime`** (histogram): time spent in `ringBuffer.next()` itself — measure around the call at `ReplicationLogGroup.java:566` (append) and `:619` (sync). Default cost is ~ns; meaningful values mean the ring is full. Cheap to instrument and a clear back-pressure signal.
- **`ringBufferRemainingCapacity`** (gauge, sampled): periodically read `ringBuffer.remainingCapacity()` from a `MutableGaugeLong` driven by a single-thread `ScheduledExecutorService` ticking at 1s. End-of-batch sampling is rejected because batches *finish* exactly when the ring has drained, biasing the gauge low. Tier-3 because the cost is a thread; if we drop it, `ringBufferClaimWaitTime` is an adequate back-pressure proxy.

### 4. Rotation on the critical path

Goal: rotation can stall the consumer thread. Today we count rotations but don't time them.

- **Wire up the existing `rotationTimeMs`** in `ReplicationLog.LogRotationTask.run()` — `ReplicationLog.java:435-471`. Bracket the `createNewWriter() / pendingWriter.getAndSet(...)` block (success path ends at `:448`, failure path increments at `:450`); emit the histogram in the `finally` so both success and failure are timed.
- **`rotationStallTime`** (histogram): time the consumer thread blocks waiting for a staged writer during the retry path in `ReplicationLog.apply()` — wrap the `latch.await(retryDelayMs, TimeUnit.MILLISECONDS)` call at `ReplicationLog.java:332`. Distinguishes "rotation happens in background" from "consumer is parked on rotation."

### 5. Mode and peer init

Goal: mode transitions and first-time peer init can introduce multi-second outliers.

- **`peerInitTime`** (histogram): time spent in `getOrCreatePeerShardManager()` future-await — wrap `peerShardManagerFuture.get(...)` at `ReplicationLogGroup.java:865`. The cached path returns at `:846-849` before the `synchronized` block, so this metric only fires on the cold call. A single sample is hard to interpret, so add a companion **`peerInitAttempts`** counter incremented every time we enter the `synchronized` block (covers both first-call and retry-after-exception cases).
- **`modeTransitionTime`** (histogram): time of `currentModeImpl.onExit(...)` + `initializeMode(newMode)` measured around `ReplicationLogGroup.java:1054-1056`. Note: `onExit` runs asynchronously via `disruptorExecutor.execute(...)`, so the measured duration covers `submit + initializeMode` only — `onExit` work is off-thread and should be timed separately if needed. Mode-flip cost is currently invisible.

## Out of scope

- Reader-side metrics (`ReplicationLogProcessor`, `ReplicationLogTracker`, `ReplicationLogDiscovery`) — already reasonably covered, and not on the producer-sync tail-latency path.
- Per-mode (SYNC vs SAF) latency disaggregation — useful, but adds tag/label complexity to the metrics source. Defer until the basic decomposition above is in place.
- Replacing `YieldingWaitStrategy` — wait-strategy choice is a separate design discussion; first measure with the new metrics, then decide.

## Files to modify

- `phoenix-core-server/src/main/java/org/apache/phoenix/replication/metrics/MetricsReplicationLogGroupSource.java` — declare new metric names, descriptions, and update methods.
- `phoenix-core-server/src/main/java/org/apache/phoenix/replication/metrics/MetricsReplicationLogGroupSourceImpl.java` — register histograms/counter and implement update methods.
- `phoenix-core-server/src/main/java/org/apache/phoenix/replication/metrics/ReplicationLogMetricValues.java` — extend the immutable snapshot record. The current 7-field constructor (`MetricsReplicationLogGroupSourceImpl.java:97-101`) ripples into every test that builds or asserts against the snapshot; budget for that. Tier-1 alone adds 4 new fields (`modeSyncTime`, `batchSize`, `pendingSyncFanout`, `rotationTimeMs` — already declared but unused in the snapshot).
- `phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLogGroup.java` — emit metrics at the call sites listed above (`:566`, `:619`, `:1030`, `:1035`, `:1054-1056`, `:1144-1192`, `:1159`, `:1172`).
- `phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLog.java` — wire `updateRotationTime` in `LogRotationTask.run()` (`:435-:471`); for Tier 3, emit `rotationStallTime` around `latch.await(retryDelayMs, TimeUnit.MILLISECONDS)` at `:332`.

## Verification

- Unit tests for the metrics impl: locate the existing test of `MetricsReplicationLogGroupSourceImpl` (search `phoenix-core/src/test/java` for `ReplicationLogMetricValues` references) and extend its assertions to cover the new fields. Constructor of `ReplicationLogMetricValues` changes shape — every call site that builds the snapshot must be updated.
- Integration: run an existing replication IT (e.g., `ReplicationLogGroupIT`) with a small custom assertion that `batchSize`, `onEventTime` (Tier 2), and `modeSyncTime` histograms are non-empty after the test.
- Manual: with the existing JMX bean (`RegionServer,sub=ReplicationLogGroup,haGroup=…`), drive a workload and inspect `_max` / `_99thPercentile` for the new metrics; correlate `syncTime_max` with `modeSyncTime_max` + `ringBufferTime_max` + `pendingSyncFanout_max` to confirm the breakdown adds up.
- Spotless: `mvn spotless:apply` before commit.
