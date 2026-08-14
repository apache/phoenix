# Pipelined HDFS flushing for Phoenix replication (FSHLog SyncRunner model)

## Context

**Why:** On a table with **no secondary indexes**, the replication sync is fully exposed on the write critical
path: `putBatchTime ≈ walSync + replSync` (no `postIndexUpdate` shadow to hide under). Production shows a
worst case of `2T` per replicated batch, where `T` is a single-digit-ms `hflush`. The `2T` comes entirely
from the **single disruptor consumer thread** doing the blocking `hflush` inline: while flush N is in flight,
the consumer cannot start flush N+1, so a sync arriving just after a flush starts waits `T_remaining + T`.

**Root cause (verified):** `ReplicationLogGroup.LogEventHandler.processPendingSyncs()`
(`ReplicationLogGroup.java:1386-1425`) calls `currentModeImpl.sync()` — the blocking `hflush` — **on the
consumer thread**, then completes all pending sync futures. One thread owns both draining and flushing.

**Why pipelining is the fix, and why the FSHLog model specifically:** HDFS `DFSOutputStream` supports
**multiple outstanding `hflush` calls on a single stream** — verified in `DFSOutputStream.flushOrSync`
(`hadoop .../DFSOutputStream.java:634-695`): the `synchronized(this)` guards only the short packet-enqueue
phase; the blocking ack-wait (`waitForAckedSeqno`) happens outside the monitor on the separate `dataQueue`.
Completion is **monotonic** (`lastAckedSeqno >= seqno`), so a later flush completing implies all earlier data
is durable. Because `DFSOutputStream.hflush()` is **blocking with no future to attach a callback to** (unlike
`AsyncFSOutput`), the only way to pipeline is to move the blocking call onto **dedicated flusher threads** —
exactly what HBase's `FSHLog` does with its round-robin `SyncRunner` pool
(`hbase .../FSHLog.java:535-697`, default 5 threads via `hbase.regionserver.hlog.syncer.count`).

**Intended outcome:** Collapse the `2T` worst case toward `~T` by keeping back-to-back `hflush` calls in
flight across a small flusher pool, while preserving exact durability semantics (no false success, no RPO
loss — the S17b invariant) and today's HA failure behavior (retry-on-fresh-writer + SYNC→S&F fallback).

**Decisions locked with the user:**
- Flusher **pool** (round-robin, N threads), not a single flusher thread (a single thread only decouples
  draining, does not pipeline the flushes themselves, so it would not move `2T`).
- **Replace the sync path unconditionally** — no config gate, no dual code path.
- The **flusher pool lives inside `ReplicationLog`**, scoped to the writer/log lifecycle. On a mode switch
  the new mode's `ReplicationLog` gets its own pool; the old pool is drained and torn down in `onExit`.
  This avoids any cross-mode flush reconciliation.

## Design overview

Split the current "finalize block + block until durable" `sync()` into two phases along the boundary that
already exists in `LogFileFormatWriter.sync()` (`log/LogFileFormatWriter.java:167-178`):

1. **Enqueue phase (stays on the consumer thread, in order):** `closeBlock()` writes the finalized block
   bytes into the `FSDataOutputStream` buffer (synchronous, cheap, no fsync) and `startBlock()` opens the
   next block. This *must* stay serialized on the single consumer thread to keep block framing and stream
   offsets correct — appends and block-close ordering are the one-writer invariant.
2. **Durability phase (moves to a flusher thread):** the blocking `output.sync()` (`hflush`) and the
   completion of the corresponding sync futures.

The consumer, at each `endOfBatch`, hands the batch's finalized position + its pending sync futures to
`flusher[++idx % N]` via a **non-blocking offer**, then returns to draining. A flusher thread performs one
blocking `hflush`, advances a shared monotonic `highestSyncedSeq` (CAS), and completes every pending future
whose sequence is `<= highestSyncedSeq` (group commit). Round-robin keeps flush N (thread A) and flush N+1
(thread B) in flight simultaneously.

### The hard part: durability bookkeeping moves from issue-time to ack-time

Today `ReplicationLog` equates **"`sync()` returned" == "durable"**:
- `append()` adds each record to `currentBatch` (`ReplicationLog.java:392`); `sync()` and the block-full
  inline sync **clear `currentBatch`** (`:406`, `:395`) on the assumption everything appended is now durable.
- On a writer fault, `apply()` rotates to a fresh writer and `replayCurrentBatch()` (`:317-326`) re-appends
  the un-synced records.
- Mode switches (SYNC→S&F) happen only at sync points because "all unsynced appends are flushed" is
  guaranteed there (`ReplicationLogGroup.java:1409-1424`).

With pipelining, **"flush issued" ≠ "durable."** The rework:
- **Do not clear `currentBatch` when the flush is issued.** Retain records (keyed by sequence) until the
  flusher confirms the ack, then trim everything `<= highestSyncedSeq`. A writer fault between issue and ack
  must still be able to replay the un-acked tail — otherwise silent RPO loss.
- **Complete sync futures on the ack** (from the flusher thread via the group-commit release), not on
  `sync()` return.
- **Gate mode switches on the ack**, not on issue: the consumer must observe all in-flight flushes drained
  (`highestSyncedSeq` caught up to the last issued sequence) before switching modes.

### Failure handling (preserve today's HA behavior)

A flusher-thread `hflush` failure cannot itself drive the consumer-thread retry/replay/mode-switch machinery
in `apply()`. Because the pool is **per-`ReplicationLog`**, the flusher signals the failure back to the
consumer (records the throwable + failed sequence on a shared handle), and the consumer thread runs the
existing recovery on its next turn: `apply()` rotates to a fresh writer, `replayCurrentBatch()` replays the
un-acked tail, and — if that is exhausted — the consumer drives SYNC→S&F. This keeps:
- **No false success:** a future is only completed successfully after its sequence is confirmed durable; a
  failed flush completes the covered futures **exceptionally** (mirrors `FSHLog` passing `lastException`
  through `releaseSyncFutures`, `FSHLog.java:677-687`), and the `fatalException` fence
  (`ReplicationLogGroup.java:1320`) still latches on unrecoverable paths.
- **No availability regression:** transient standby-FS blips still recover via rotate-to-fresh-writer and
  the SYNC→S&F fallback, exactly as today.

## Files to modify

**`phoenix-core-server/.../replication/ReplicationLog.java`** (primary — owns the pool and the bookkeeping)
- Add the flusher pool: `N` flusher threads (new inner class, `SyncRunner`-analog), each with a bounded
  `BlockingQueue` of flush handles and a target sequence; a shared `AtomicLong highestSyncedSeq`; round-robin
  index. Start in `init()` (`:141-145`), tear down in `close()` (`:426-446`).
- New config constant + default for pool size (mirror `hbase.regionserver.hlog.syncer.count`=5), added to
  `ReplicationLogGroup`'s constants block alongside the existing `REPLICATION_LOG_*` keys.
- Rework `currentBatch` into a **sequence-keyed** structure of un-acked records (retain until ack, trim
  `<= highestSyncedSeq`). `replayCurrentBatch()` (`:317-326`) replays the un-acked tail.
- Split `sync()`: keep `apply(...)` for the in-order finalize (`closeBlock`/`startBlock` via the writer) but
  hand the blocking durability wait to a flusher; do **not** `clear()` `currentBatch` on issue.
- Handle the **block-full inline sync** in `append()` (`:386-402`, `blockSynced` path) the same way — it is a
  second implicit sync trigger and must also pipeline rather than block the consumer.
- Failure signal path from flusher → consumer, feeding the existing `apply()` rotate/replay + SYNC→S&F.

**`phoenix-core-server/.../replication/log/LogFileFormatWriter.java`**
- Split `sync()` (`:167-178`) into `finalizeBlockForFlush()` (does `closeBlock()`+`startBlock()`, returns the
  stream position to await) and the awaitable `output.sync()` so the durability wait can run on a flusher
  thread while the consumer continues. Preserve `closeBlock`→`write` ordering (no new block's bytes before
  the prior block's bytes are written). `init()`'s header sync (`:64`) stays synchronous on the rotation
  thread (unchanged).

**`phoenix-core-server/.../replication/ReplicationLogGroup.java`**
- `processPendingSyncs()` (`:1386-1425`): stop completing futures inline after a blocking sync; instead pass
  the pending futures + batch sequence down to `ReplicationLog` for the flusher to complete on ack. The
  `PendingSync` holder (`:1306-1314`) already carries `pickupTimeNs`; extend the handoff to carry the
  sequence so the flusher completes the right futures in order.
- Move the mode-switch check (`:1409-1424`) to fire only once in-flight flushes are drained
  (`highestSyncedSeq` == last issued sequence).
- The `fatalException` fence (`:1320`, `setFatalException` `:1331-1337`, checks at `publish` `:859`,
  `sync` `:903`, `onEvent` `:1519`) stays; add the deferred-flush-failure path into it for unrecoverable
  cases.
- Add pool-size constant/default here with the other `REPLICATION_LOG_*` keys (verify default before use).

**`phoenix-core-server/.../replication/ReplicationModeImpl.java`**
- `sync()`/`append()` (`:74-87`) delegation is unchanged in shape, but `onExit` teardown must drain +
  shut down the per-log flusher pool before the mode is discarded (the pool being per-`ReplicationLog`
  is what makes mode-switch clean). `disruptorExecutor.execute(() -> oldModeImpl.onExit(true))`
  (`ReplicationLogGroup.java:1418`) already offloads exit; ensure it awaits pool drain.

## Reusable patterns to follow (do not reinvent)

- **`FSHLog.SyncRunner`** (`hbase .../regionserver/wal/FSHLog.java:535-697`): the flusher-thread loop,
  non-blocking `offer(sequence, futures, count)` (`:559-565`), round-robin `syncRunnerIndex`
  (`:1090-1105`), the pre-flush skip when another runner already covered the sequence (`:652-657`), and
  group-commit release with exception pass-through (`releaseSyncFutures` `:586-599`, `:677-687`).
- **`SyncFuture`** (`hbase .../regionserver/wal/SyncFuture.java:47-204`): the caller handle shape
  (`txid`/`doneTxid`/`throwable` + lock/condition, idempotent `done()`, `get()` rethrows). Phoenix already
  uses `CompletableFuture<Void>` for this; keep `CompletableFuture` but complete it from the flusher via the
  same monotonic-sequence group-commit logic.
- Phoenix's existing **monotonic sequence** is the disruptor ring-buffer sequence already threaded through
  `onEvent`/`processPendingSyncs` — reuse it as the `highestSyncedSeq` analog rather than inventing a txid.

## Risks / invariants to hold

1. **No false success (S17b):** a `CompletableFuture` completes successfully only after its sequence is
   confirmed durable on an ack; any flush failure completes covered futures exceptionally. This is the
   highest-consequence invariant — RPO loss lives here.
2. **Replay correctness:** `currentBatch` must retain un-acked records until ack; a writer fault must replay
   exactly the un-acked tail. Test the fault-between-issue-and-ack case explicitly.
3. **In-order block framing:** `closeBlock`→stream-write ordering stays on the single consumer thread; only
   the durability *wait* moves off-thread.
4. **Mode-switch barrier:** SYNC→S&F only after in-flight flushes drain, else a switch could strand an
   un-acked flush.
5. **Rotation interaction:** `checkAndReplaceWriter`/`pendingWriter` (`ReplicationLog.java:229-241`) and the
   retry latch in `apply()` (`:355-381`) assume the consumer owns writer swaps; a writer cannot be closed
   while it has outstanding flushes on a flusher thread. Drain the pool for the old writer before close.
6. **Backpressure:** bound the per-flusher queue and the number of outstanding flushes (FSHLog uses
   `maxHandlersCount * 3`); the ring buffer (`YieldingWaitStrategy`, size 32768) already backpressures
   producers, but the in-flight-flush depth needs its own cap.
7. **Sync timeout:** `syncFuture.get(syncTimeoutMs)` abort-the-RS path (`ReplicationLogGroup.java:931-945`)
   still applies; "time queued behind other flushes" now counts against that budget — confirm the timeout is
   still generous enough under pipelined depth.

## Verification

- **Unit (JUnit, `phoenix-core`):** extend `ReplicationLogBaseTest`/`ReplicationLogGroupTest`.
  - Group commit: many concurrent `sync()` calls complete on one physical flush.
  - Pipelining: with an artificially slowed `SyncableDataOutput`, assert flush N+1 starts before flush N
    completes (instrument via a test double implementing `SyncableDataOutput`).
  - Fault-between-issue-and-ack: fail `hflush` while a later append is buffered; assert the un-acked tail is
    replayed into the fresh writer and no future false-succeeds.
  - Mode switch with in-flight flushes: assert SYNC→S&F waits for drain and loses no records.
  - Failure propagation: `hflush` throws → covered futures complete exceptionally, `fatalException` latches
    on the unrecoverable path, RS abort fires.
  - Rotation with outstanding flush: writer not closed until its flushes drain.
- **Build:** `mvn install -pl phoenix-core-server -DskipTests` (IT runs against the installed server jar),
  then `mvn spotless:apply`.
- **Integration:** run the replication ITs on a mini-cluster; confirm no regression in the HA failover
  scenarios (S1–S17) that exercise SYNC→S&F and cutover fencing.
- **Perf (the actual goal):** with the phoenix-ha-metrics skill on a no-index table, confirm the
  `replSync`/batch worst case drops from ~`2T` toward `~T`, and `putBatchTime` on no-index tables falls
  correspondingly. Compare fs-sync-time and pending-sync-wait-time distributions before/after.
