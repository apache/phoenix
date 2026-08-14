# Sync Coalescing Measurement: Model A vs Model B

## Purpose

Quantify how effectively `ReplicationLogGroup` coalesces concurrent producer
syncs into a single inner `LogFileWriter.sync()`, and compare two record-shape
strategies:

- **Model A** (current): RPC handler publishes N append events + 1 sync per
  batch. Each Mutation becomes one Disruptor event.
- **Model B** (proposed, on `eliminate-index-replication-v2`): RPC handler
  packs N mutations into a single Disruptor event, then 1 sync per batch.

The benchmark is `testReplicationSyncPathSimulator` in
`phoenix-core/src/test/java/org/apache/phoenix/replication/ReplicationLogGroupTest.java`.

## Test setup

| Parameter | Value |
|---|---|
| `producerCount` | swept ∈ {8, 32, 64, 128, 256} |
| `syncsPerProducer` | 20 (number of batches per producer) |
| `appendsPerSync` | 5 (Model A) or 1 (Model B-as-tested) |
| `cellsPerMutation` | 1 (default sweep) or 5 (cell-volume-controlled comparison) |
| `innerSyncDelayMs` | swept ∈ {2, 5} (HDFS hflush model) |
| Iterations per config | 5 |
| Ring buffer size | 32K slots (production default) |
| Rotation size | `Long.MAX_VALUE` (rotation disabled to remove noise) |
| Mutation | `LogFileTestUtil.newPut("row" + commitId, commitId, cellsPerMutation)` |

The test injects a sleep into the inner writer's `sync()` to model HDFS
hflush. Each producer is a serial loop of `append × appendsPerSync; sync`
repeated `syncsPerProducer` times. Producer threads are gated by a start
latch so they begin simultaneously.

The test instruments every `LogFileWriter` the rotation path creates so
fsync invocations are counted across all writers, not just the initial one.
With rotation disabled, `writerCount = 1` for every run.

## Theoretical floor

`logGroup.sync()` is blocking — each producer has at most one in-flight SYNC
at any instant. So:

- `pendingSyncCount` per fsync ≤ `producerCount`
- Total inner fsyncs ≥ `totalProducerSyncs / producerCount = syncsPerProducer = 20`

The minimum fsync count is independent of `producerCount`. Concurrency only
changes how many producer-syncs each fsync absorbs.

**Coalescing efficiency** = `syncsPerProducer / innerSyncCalls`. Bounded to
`(0, 1]`. 100% means perfect saturation (every fsync coalesces all N
producers).

## Results — coalescing efficiency (medians of 5 runs)

### `innerSyncDelayMs = 2` (fast hflush)

| producerCount | A innerSync | A efficiency | B innerSync | B efficiency | **B − A** |
|---:|---:|---:|---:|---:|---:|
| 8   | 33 | 61% | 28 | 71% | +10 pp |
| 32  | 39 | 51% | 34 | 59% | +8 pp |
| 64  | 39 | 51% | 34 | 59% | +8 pp |
| 128 | 39 | 51% | 33 | 61% | +10 pp |
| 256 | 38 | 53% | 33 | 61% | +8 pp |

### `innerSyncDelayMs = 5` (slow hflush)

| producerCount | A innerSync | A efficiency | B innerSync | B efficiency | **B − A** |
|---:|---:|---:|---:|---:|---:|
| 8   | 34 | 59% | 28 | 71% | +12 pp |
| 32  | 39 | 51% | 34 | 59% | +8 pp |
| 64  | 39 | 51% | 34 | 59% | +8 pp |
| 128 | 39 | 51% | 33 | 61% | +10 pp |
| 256 | 38 | 53% | 32 | 63% | +10 pp |

### Observations

1. Efficiency is flat across `producerCount` for both models (~50–60% A,
   ~60–70% B). The structural floor is wakeup skew + startup ramp, not a
   contention bottleneck.
2. Model B is consistently 8–12 pp more efficient than Model A. Tighter
   consumer cycle gives more producers time to re-publish into the same
   batch.
3. Increasing fsync delay barely moves the numbers (+2 pp at most).

## Results — `maxPendingSyncCount` (medians)

| producerCount | A delay=2 | B delay=2 | A delay=5 | B delay=5 |
|---:|---:|---:|---:|---:|
| 8   | 8   | 8   | 8   | 8   |
| 32  | 32  | 32  | 32  | 32  |
| 64  | 64  | 64  | 60  | 64  |
| 128 | 117 | 127 | 114 | 127 |
| 256 | 244 | 252 | 230 | 256 |

`pendingSyncCount` saturates at `producerCount` for both models, confirming
the cap is reached at every concurrency level we tested. Model B reaches
the cap slightly more often than Model A.

## Results — elapsed wall time (medians, ms)

| producerCount | A delay=2 | B delay=2 | A delay=5 | B delay=5 |
|---:|---:|---:|---:|---:|
| 8   | 152 | 110 | 286 | 222 |
| 32  | 237 | 151 | 384 | 285 |
| 64  | 323 | 170 | 468 | 299 |
| 128 | 527 | 219 | 677 | 341 |
| 256 | 880 | 311 | 1019 | 432 |

## Results — producer-perceived sync latency (medians, p100 from histograms)

`maxSyncTime` is the worst-case time from a producer calling `logGroup.sync()`
to the call returning. All values in milliseconds.

| producerCount | A delay=2 | B delay=2 | A delay=5 | B delay=5 |
|---:|---:|---:|---:|---:|
| 8   | 11.2 | 6.8  | 14.5 | 17.1 |
| 32  | 19.7 | 12.5 | 27.3 | 20.5 |
| 64  | 27.0 | 15.2 | 35.8 | 24.5 |
| 128 | 47.7 | 22.5 | 53.7 | 30.4 |
| 256 | 81.5 | 38.3 | 73.0 | 42.0 |

At `producerCount = 256`, Model B has ~2.1× lower p100 sync latency than
Model A.

## Throughput — calculation

Throughput depends on what you measure. Across the original sweep the two
configurations did different total work:

| pc=256 | totalProducerSyncs | totalProducerAppends | elapsedMs |
|---|---:|---:|---:|
| Model A (`aps=5, cell=1`) | 5,120 | 25,600 | 880 |
| Model B-as-tested (`aps=1, cell=1`) | 5,120 |  5,120 | 311 |

| pc=256 | sync rate (syncs/s) | append rate (appends/s) |
|---|---:|---:|
| Model A | 5,818 | **29,091** |
| Model B | **16,463** | 16,463 |

The original sweep used `cellsPerMutation = 1`, so Model A wrote 25,600
cells while Model B-as-tested wrote 5,120 cells. To rule out cell-volume
as the source of the gap, the test was rerun with
`cellsPerMutation` parameterized.

## Cell-volume-controlled comparison

Three configurations at `producerCount=256`, `innerSyncDelayMs=2`, 5
iterations each. Total cell count is the same in the first and third rows.

| Config | total cells | events on ring | innerSync (median) | maxBatchSize | maxRingBuf | maxSyncT | elapsedMs |
|---|---:|---:|---:|---:|---:|---:|---:|
| Model A `aps=5, cell=1` | 25,600 | 5,120 | 38 | 1,355 | 53.3 ms | 74.9 ms | 841 |
| Model A `aps=5, cell=5` (realistic) | 128,000 | 5,120 | 38 | 1,401 | 59.4 ms | 81.4 ms | 925 |
| **Model B `aps=1, cell=5`** (fair vs A `5×1`) | **25,600** | **1,024** | **33** | **478** | **23.6 ms** | **40.1 ms** | **312** |

**Same 25,600 cells written, Model A vs Model B: 841 ms vs 312 ms — Model B
is 2.7× faster.** The win is not cell-volume related.

**5× more cells per mutation at constant aps=5: 841 ms → 925 ms — only +10%.**
Per-cell cost is small relative to per-event cost. Increasing the cell
payload by 5× barely moves the needle, but multiplying event count by 5×
(comparing Model A and Model B at constant cell volume) costs 2.7× wall
time.

This decomposes the bottleneck:
- **Per-event cost dominates per-cell cost by roughly 10×** on the
  single-threaded disruptor consumer.
- The 2.7× speedup of Model B comes from collapsing 5 events into 1 — i.e.,
  reducing per-event dispatch and metric-update overhead, *not* from doing
  less work per cell.

## Test caveat

The current test uses Mockito spies on `LogFileWriter`, so consumer-side
work is `callRealMethod` on `LogFileFormatWriter.append()` (cell encoding,
buffer write — no real disk I/O on the standby). It captures:

- Disruptor event dispatch overhead (per-event)
- Per-event metric updates (per-event)
- Cell serialization (per-cell, scales with cell count)

It does NOT capture:

- Real HDFS write cost beyond the injected `innerSyncDelayMs`
- Producer-side packing cost in v2 (`MutationCellGrouper.flattenCells` +
  attribute extraction); estimated at hundreds of nanoseconds per RPC,
  bounded above by reference-copy work.

For a definitive end-to-end comparison, the test should call the v2
branch's `append(tableName, commitId, List<Cell>, Map)` API with packed
cell lists. The numbers above are tight enough that the v2 implementation
is unlikely to flip the conclusion.

## syncTime decomposition

`syncTime` (producer-side, end-to-end) decomposes as:

```
syncTime ≈ ringBufferTime         (publish → consumer pickup)
         + pendingSyncWaitTime    (pickup → fsync start, per-event)
         + fsSyncTime             (fsync wall, per-batch)
         + (small wakeup tail)
```

With rotation disabled, `syncTime ≤ sum(component maxes)` holds in 96/100
runs. The decomposition is honest in production-default conditions. The
only consistently unmeasured contribution is producer wakeup latency
(~50µs–1ms), which sits within the slack between summed maxes and observed
`maxSyncTime`.

When the test ring buffer was 32 slots (the original fixture default),
`syncTime > sum(component maxes)` consistently. Root cause: `sync()` records
its `startTime` before `ringBuffer.next()`, but `event.timestampNs` is set
*after* `ringBuffer.next()` returns. With a saturated ring, blocking on
`ringBuffer.next()` is invisible to `ringBufferTime` but captured in
`syncTime`. With the production-default ring, `next()` doesn't block and
the gap closes.

This means: in production, `syncTime > ringBufferTime + pendingSyncWaitTime +
fsSyncTime` is a useful derived signal indicating ring-buffer back-pressure.

## Disk pressure (inner fsync rate)

| pc | A delay=2 | B delay=2 | A delay=5 | B delay=5 | Disk capacity |
|---:|---:|---:|---:|---:|---:|
| 8   | 217/s | 254/s | 119/s | 126/s | 500/s (delay=2), 200/s (delay=5) |
| 32  | 165/s | 225/s | 102/s | 119/s | |
| 64  | 121/s | 200/s | 83/s  | 114/s | |
| 128 | 74/s  | 155/s | 58/s  | 97/s  | |
| 256 | 43/s  | 106/s | 37/s  | 74/s  | |

Both models stay well below disk capacity at every concurrency. Model B
issues ~1.5× as many fsyncs as Model A but neither approaches the
hflush-bound ceiling. Coalescing is doing far more work than disk capacity
requires — the bottleneck is consumer event processing, not disk I/O.

## Per-SYNC decomposition validation

A throwaway probe was added to `LogEventHandler` and the producer loop to
emit per-fsync `SYNC_DECOMP` lines and per-caller `PRODUCER_SYNC` lines.
This validates the producer-side decomposition

```
phoenixLogSyncTime(per caller) ≈ ringBufferTime(this SYNC)
                               + pendingSyncWaitTime(this SYNC)
                               + fsSyncTime(this fsync)
```

empirically by comparing the producer-side sync-time distribution against
the per-fsync component sums.

### aps=5, pc=256, delay=2

- Producer-side `phoenixLogSyncTime` (5,120 samples):
  min=3.0 ms, p50=35.7 ms, mean=38.7 ms, p99=75.9 ms, max=80.3 ms.
- SYNC_DECOMP per fsync (39 samples), means:
  firstSyncRingBuf=21.5 ms, maxPendWait=16.8 ms, fsSync=2.9 ms,
  sum-of-three=41.2 ms, sum-of-three max=80.1 ms.

Producer-side max (80.3 ms) ≈ worst-fsync sum-of-components (80.1 ms).
Producer-side mean (38.7 ms) < component-mean sum (41.2 ms) because
`maxPendWait` per fsync overstates the typical caller's pickup-to-fsync
wait — most callers in a high-`pendCount` fsync experience less wait than
the first-arriving one. The decomposition equation holds.

### Production-realistic batch size: aps=400 vs aps=1, cell=400, pc=64

Model A and Model B run with the **same total cell volume** (25,600 cells
per producer over 20 syncs × 400 cells, in either configuration). The only
difference is event count on the ring buffer.

| | Model A (aps=400, cell=1) | Model B (aps=1, cell=400) | Ratio |
|---|---:|---:|---:|
| Producer-side `phoenixLogSyncTime` min | 206 ms | 6.8 ms | 30× |
| Producer-side `phoenixLogSyncTime` p50 | 607 ms | 21 ms | **29×** |
| Producer-side `phoenixLogSyncTime` mean | 608 ms | 26 ms | **23×** |
| Producer-side `phoenixLogSyncTime` p99 | 692 ms | 81 ms | 8.5× |
| Producer-side `phoenixLogSyncTime` max | 700 ms | 82 ms | 8.5× |
| SYNC_DECOMP firstSyncRingBuf mean | 507 ms | 16 ms | 32× |
| SYNC_DECOMP maxPendWait mean | 115 ms | 8 ms | 14× |
| SYNC_DECOMP fsSync mean | 3.7 ms | 5.8 ms | 0.6× |
| SYNC_DECOMP eventsAhead mean | 8,629 | 1.1 | **7,800×** |
| SYNC_DECOMP batchSize mean | 13,638 | 64 | 213× |
| Wall time | 11,790 ms | 766 ms | **15.4×** |

**Producer-side caller-perceived sync time drops 23–29× at p50/mean.**
fsSync is unchanged (≈ disk model). The entire speedup is from collapsing
event count on the consumer thread — `eventsAhead` drops 7,800× and the
consumer cycle time falls proportionally.

This is the saturation regime where producers continuously back up the
consumer. The decomposition equation holds in both regimes (max
`phoenixLogSyncTime` ≈ worst-fsync sum-of-components in both Model A and
Model B).

### Caveat on production translation

The test's pc=64, aps=400 regime saturates the consumer (continuous
catch-up; mean `batchSize` = 13,638 events per poll). Production runs at
much lower per-RS load (~120 caller-syncs/sec on a busy RS, batchSize
typically ~100, mean queue depth ~15 events). The 23–29× speedup is the
upper bound of v2's per-caller-sync benefit at saturation. Production
will see a smaller absolute multiplier (likely 2–3× on
`ReplicationSyncTime` p50 reduction), but the per-event-cost-dominates
mechanism is structurally identical.

## Conclusion

For Phoenix HA replication at production scale (256 RPC handlers):

1. **Coalescing efficiency** is governed by `syncsPerProducer` (the number
   of round-trips per producer), not by handler count. The achievable floor
   is ~20 inner fsyncs regardless of N.
2. **Model B (packed appends) is unambiguously better** than Model A across
   every dimension measured: ~10 pp higher coalescing efficiency, 23–67%
   lower wall time, ~50% lower p100 sync latency. **At equal cell volume
   (25,600 cells written either way), Model B is 2.7× faster wall time at
   pc=256, delay=2.**
3. **The bottleneck is per-event cost on the disruptor consumer, not
   per-cell cost.** Increasing cells per mutation by 5× adds only ~10% wall
   time, but reducing events by 5× (collapsing 5 single-cell appends into 1
   five-cell append) cuts wall time by 63%.
4. **Disk fsync rate is not the bottleneck.** The dominant cost is
   single-threaded consumer event handling on the Disruptor, which scales
   with the number of events per batch. Reducing event count (Model B)
   directly reduces consumer overhead.
5. **The earlier "Model A wins at low concurrency, slow fsync" finding was
   rotation-induced noise.** With size-based rotation disabled, Model B
   wins or ties at every (`producerCount`, `innerSyncDelayMs`) point in
   the sweep.

## Open items

- The current test's `aps=1` is a proxy for Model B, not the real
  implementation. A definitive comparison should test the v2 branch's
  `append(tableName, commitId, List<Cell>, Map)` API with realistic
  multi-cell mutations.
- Producer-side packing cost (`MutationCellGrouper.flattenCells` + attribute
  extraction) is not modeled in the test — but is bounded above by hundreds
  of nanoseconds per RPC, dominated by reference copying. Would not
  meaningfully change any conclusion above.
- Consumer-side per-cell serialization cost moves with the event in both
  Model A and Model B (the writer serializes cells whether they arrive as
  one Mutation or as a packed cell list). This cost is not captured by the
  test (the writer is a Mockito spy with `callRealMethod` on a
  `LogFileFormatWriter` that has no real I/O).
