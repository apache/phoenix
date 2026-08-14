# Spike verdict: Is the Phoenix HA-log hflush floor stream-parallelizable? — 2026-07-08

**Question.** On a busy RegionServer the Phoenix HA replication-log `FsSyncTime` is the
dominant p50 cost and the serialization floor (single Disruptor consumer calls
`currentModeImpl.sync()` inline, `ReplicationLogGroup.java:1095`). We call **hflush**, not
hsync (`useHsync` default false → `HDFSDataOutput.hflush()`). Would splitting one RS's output
across **N HDFS streams**, switching streams at the sync boundary, and dispatching each
stream's hflush to a per-stream runner reduce that floor — i.e. do N concurrent hflushes get N
independent, overlapping ack pipelines, or do they serialize on a shared resource?

**Method.** Pulled DataNode-side ack/network/flush metrics for the same busy-RS window as
`phoenix-ha-metrics-1783108197` (rs-15 @ 60.4/s, Phoenix FsSync p50 = 3.0 ms), overlaid on the
Phoenix decomposition. Fetch script + raw JSON:
`phoenix-ha-perf/metrics/datanode-1783108197/`. Metrics resolved live from the Argus catalog
(`hbase-contexts/argus/topics/datanode.md`); note the `hadoop.datanode.*` metric prefix lives
under the **`hbase.aws.<inst>.<domain>`** scope, not `hadoop.aws.*`.

## Results (24 DataNodes, window 2026-07-03 19:49–22:15 UTC)

| DataNode metric | value | meaning |
|---|---|---|
| **PacketAckRoundTripTime avg** | **~1.05 ms** (0.86–1.32) | pipeline ack round-trip — network-bound, per-pipeline |
| **FlushNanos avg** (control) | **0.013 ms** | disk is NOT on the hflush path |
| WriteBlockOp avg | 0.049 ms | per-block op cost — negligible |
| **XceiverCount** | **~41** (max 45) | vs 4096 cap → ~99% headroom |
| ack RTT numops | ~11.5M / 5min bucket | DNs busy; RTT stable across all 24 → fabric-wide ~1 ms |
| SendDataPacket net-blocked / transfer | *no data* | did not populate this window (the one gap) |

## Verdict: **GO** (build the design), with a magnitude caveat

The three failure modes that would have killed the design are all disproved:

1. **Not disk-bound.** DataNode `FlushNanos` = 13 µs — the ~3 ms hflush spends effectively
   zero time on disk, exactly as expected for `flush(false)`. N streams therefore cannot
   serialize on shared platters. *This is the decisive number.*
2. **Measurable floor is network ack-RTT** (~1.05 ms) — a *per-pipeline* cost. Independent
   blocks get independent pipelines, so concurrent streams' round-trips overlap.
3. **Resource headroom is abundant.** ~41 xceivers vs a 4096 cap; adding N streams per RS is
   negligible. Phoenix uses plain `DFSOutputStream` (`HDFSDataOutput`), so each stream has its
   own client-side `DataStreamer` thread — no shared client bottleneck (unlike HBase's
   AsyncFSWAL single Netty event loop).

**Nothing in the floor is a shared or serializing resource.** Direction confirmed.

## Caveat — bounds the magnitude, not the direction

DataNode ack-RTT (~1 ms) is only **~1/3 of the 3 ms** Phoenix FsSync. The remaining ~2 ms is
unaccounted from production (and `net_blocked`/`transfer` returned no data this window). That
residual is almost certainly **client-side** — the client→DN1 leg, `DataStreamer` queueing,
and the `flush(false).get()` wait — which is also per-stream and *should* parallelize, but
that is inference, not measurement. Production DN metrics cannot isolate the single Phoenix
stream's client-side portion.

**Consequence:** the *speedup magnitude* is not quantified. To size it before committing
implementation effort, run the microbench fallback from the plan — reuse
`LogFileWriterContext` + `LogFileWriter.init()` (`LogFileWriterTest.java:155-157`) to measure
single-stream vs N-stream concurrent-hflush latency directly against a mini-DFS / real FS.
That closes the one remaining gap (does the ~2 ms client-side residual overlap across
streams?) with a controlled measurement.

## Corroboration at saturation — TOP_ENTITY_1M, July (rs-20 @ 673/s)

Second datapoint: `phoenix-ha-metrics-1783207511` (2026-07-04, coalescing deployed,
PHSCEX.TOP_ENTITY_1M, rs-20 @ **673 sync/s** — 11× the FEED_COMPOSITION spike, ~2× June's
340/s rs-5). DataNode metrics for the same window:

| | FEED_COMP spike (rs-15 @ 60/s) | TOP_ENTITY (rs-20 @ 673/s) |
|---|---|---|
| Phoenix FsSync p50 | 3.0 ms | **1.0 ms** |
| DataNode ack-RTT avg | 1.05 ms | **0.65 ms** |
| DataNode FlushNanos (disk) | 13 µs | **4 µs** |
| ack-RTT ops (5m bucket) | 11.5M | **46.5M** (4×) |
| XceiverCount | 41 | 33 (both ~1% of 4096 cap) |

**The floor scales, it does not degrade.** At 11× the load and 4× the packet ops, ack-RTT
*dropped* (0.65 ms) and disk flush *dropped* (4 µs) — the HDFS pipeline is more efficient when
busy/batched, not congested. The "concurrency just relocates contention to the DataNode"
failure mode is false even at 673/s.

**The magnitude gap largely closes at saturation.** At the spike, ack-RTT (1 ms) was only ⅓
of FsSync (3 ms), leaving ~2 ms unaccounted. Here ack-RTT (0.65 ms) is ~65% of FsSync
(1.0 ms) — the residual is ~0.35 ms. So at the load where the design matters, the DataNode
round-trip *is* the dominant part of the fsync, and parallel streams target the dominant cost.
(`net_blocked`/`pkt_transfer` again returned no data, so the last ~0.35 ms is still not
directly decomposed — but it is now a small inference gap, not ⅔.)

**Regime signature confirms the payoff band.** rs-20 p50 decomposition: **RB 43% / Fs 50%** —
co-dominant, matching the June saturation pattern. This is the band where multi-stream +
async-dispatch pays, because both the consumer-serialization (RB) and single-stream (Fs) costs
are large. Note absolute FsSync at saturation (1.0 ms) is *lower* than at the spike (3.0 ms):
the busy-RS problem is less a slow fsync floor than consumer serialization (RB) + sheer sync
rate — so the win is throughput/concurrency (N syncs' round-trips overlap), consistent with a
673/s regime.

## Correction to prior perf conclusions

Earlier analysis (`docs/replication-perf-baseline-2026-06-11.md`,
`docs/sync-coalescing-measurement.md`, and session notes) treated the fsync floor as
effectively immovable — the reason coalescing/v2/decoupling were said to net ~zero on total
sync time. That holds **only for a single stream**: with one stream the fsync serializes and
the wait removed from RB/PW reappears as fsync-future wait. With **hflush + N parallel
streams** the fsyncs no longer serialize (network-bound, disk-negligible, per-pipeline), so
the floor is **conditionally movable**. This is the one lever that attacks FsSync itself
rather than the event-count side (RingBuffer / PendingSyncWait) that coalescing and v2 target.

## Next step

Proceed to a full implementation-plan session for: N writers per RS, round-robin stream switch
at sync boundary, per-stream async hflush dispatch (HBase `FSHLog SyncRunner` pattern, but N
runners on N streams), rotation / mode-switch / crash-recovery across N writers, per-stream
metrics, and load-adaptive N (quiet RSes must not pay N hflushes for a 1-record batch).
Optionally run the microbench first to quantify the win.
