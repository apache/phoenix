# Sync Retry False-Success: A Failed `output.sync()` Retries Into a No-Op and Silently Loses a Record

**Status:** FIXED in commit `b5ee5dcac9` ("Fence writer on sync failure to
prevent false-success RPO loss (S17b)", 2026-08-06), and **VERIFIED on the
test-bed** by re-running S17b with the fenced jar (2026-08-07) — zero RPO, count
+ byte-for-byte content parity across the fault. Root cause originally confirmed
from a real test-bed run (S17b, 2026-08-05, kind two-cluster DR test-bed, Phoenix
build `phoenix-server-5.3.0-consistent_failover-14.1.11`). See "The fix
(shipped)" and "Fix verified on the test-bed".
**Companion to:** the S17 / S17b sections of
`Phoenix_HA_Failover_Test_Scenarios.md`, `replication-log-sync-semantics.md`,
and `replication-writer-retry-policy-recommendation.md`.

## TL;DR

`LogFileFormatWriter.sync()` closes the current block **before** it flushes it to
HDFS. `closeBlock()` mutates writer state (increments `blockCount`, nulls
`blockDataStream`, resets the buffer) and only *then* does `output.sync()` push
bytes to the peer DataNode. When `output.sync()` throws — e.g. the peer-DN block
pipeline is dead — `ReplicationLog.apply()` retries `sync()` on the **same**
writer. But the retry re-enters `LogFileFormatWriter.sync()` with
`blockDataStream == null`, so its guard is false and the method **returns success
without doing anything**. The disruptor completes the record's sync future, the
client is ACKed, and `currentBatch` is cleared — even though that block's bytes
never reached the peer. The record is now neither peer-durable nor a replay
candidate. **Permanent single-record RPO loss, with no RS crash and therefore no
`preWALRestore` safety net.**

**Observed:** the active writer counted `recordCount=422`; the standby applied
only **420**. Record 421 (`s17b-1940001`) was ACKed to the client via this
false-success retry and lost; record 422 was still in `currentBatch` and got
re-shipped by the `onFailure`→`replayBatch` path. Loss = exactly the one record
whose `sync()` failed on the first attempt and "succeeded" on the retry.

## The fault that triggered it

Node-level `iptables` silent `DROP` (not `REJECT`) on the kind node's FORWARD
chain, scoped `-s <activeWriterPodIP> -d <peerDN> --dport 9866`, severing the
**block-data** stream from the active RegionServer to the standby cluster's
DataNode. (This is a fault Toxiproxy cannot inject: Toxiproxy fronts only the
peer NameNode `:9000` for metadata RPCs; block data streams RS→DN directly on
`:9866` and is never proxied.) The active cluster's own local WAL DataNode was
left healthy, so local commits kept succeeding while replication block-writes
hung.

## Root cause (code)

`phoenix-core-server/src/main/java/org/apache/phoenix/replication/log/LogFileFormatWriter.java:167-178`:

```java
public void sync() throws IOException {
  // Ensure the current block data is flushed to the FSDataOutputStream.
  if (blockDataStream != null && currentBlockBytes.size() > 0) {   // GUARD
    // Closing the current block forces its header, data, and checksum into output.
    closeBlock();     // :159 blockCount++ ; :164 blockDataStream = null ; :163 buffer.reset()
    // Flush and sync the underlying output.
    output.sync();    // :174 the durability barrier to the peer DN — THROWS here
    // Start a new block for subsequent appends.
    startBlock();     // :176 NEVER reached if output.sync() threw
  }
}
```

`closeBlock()` (`:107-165`) advances `blockCount` at `:159`, then at `:163-164`
resets the buffer and sets `blockDataStream = null` — all **before**
`output.sync()` at `:174`. So once `output.sync()` throws, the block is "closed"
in bookkeeping but its bytes were never durably flushed, and the guard's
precondition (`blockDataStream != null`) is now permanently false for this block.

The retry lives in `ReplicationLog.apply()`
(`phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLog.java:328-359`),
with `maxAttempts = SYNC_RETRIES(1) + 1 = 2` and `retryDelayMs = 100`:

```java
private void apply(Action action) throws IOException {
  for (int attempt = 1; attempt <= maxAttempts; attempt++) {
    checkAndReplaceWriter(true);                 // swaps in pendingWriter IF one was staged
    ...
    try {
      ...
      action.action(currentWriter);              // == currentWriter.sync()
      break;                                     // success → exit loop
    } catch (IOException e) {
      LOG.debug("Attempt {}/{} failed", attempt, maxAttempts, e);   // DEBUG — normally invisible
      if (attempt == maxAttempts) { throw e; }
      CountDownLatch latch = new CountDownLatch(1);
      rotationStagedLatch = latch;
      requestRotation();                         // ask for a fresh writer...
      latch.await(retryDelayMs, TimeUnit.MILLISECONDS);   // ...wait up to 100 ms for it
    }
  }
}
```

The retry's intent is *"run on a fresh writer"* — `requestRotation()` asks the
`LogRotationTask` to stage a `pendingWriter` that `checkAndReplaceWriter()` would
swap in on the next attempt. **But rotation itself is dead in this fault:**
`createNewWriter()` → `LogFileFormatWriter.init()` writes the file header and
does its own `output.sync()` to force the first block allocation — which also
hangs on the dead peer DN. So no `pendingWriter` is staged within the 100 ms
`latch.await`, `checkAndReplaceWriter()` is a no-op, and **attempt 2 runs on the
same writer** whose block was already consumed.

## Trace of the lost record

All on the IP-keyed writer `1785975413179_10.244.2.15,16020,...plog`, block
`blk_1073748591_8548`, in `.../toxiHAGroup/in/shard/016/`. (Pod-log time =
host + 7h.)

```
00:17:41,348  Attempt 1: sync() guard true → closeBlock() (blockCount 420→421,
              blockDataStream=null) → output.sync() THROWS:
              RemoteException(IOException): blk_...8548 is COMPLETE but not
              UNDER_CONSTRUCTION  (from NameNodeRpcServer.updateBlockForPipeline,
              inside DataStreamer.setupPipelineForAppendOrRecovery)
              → consumer logs: WARN [ReplicationLogGroup-toxiHAGroup-0] Error while syncing
              → apply() catches, stages latch, requestRotation(), latch.await(100ms)
              → rotation's header output.sync() ALSO hangs → no pendingWriter staged

~00:17:41,448 Attempt 2: sync() guard FALSE (blockDataStream == null) → NO-OP → returns success
              → apply() breaks on success

00:17:41,451  Client RPC ACKED — responseTooSlow WARN, processingtimems:25574,
              1st row key=s17b-1940001 → sync future completed
              → ReplicationLog.sync() runs currentBatch.clear()  (:381)
```

The **103 ms** gap between the failed `output.sync()` (41,348) and the client ACK
(41,451) ≈ the **100 ms `retryDelayMs`** — the fingerprint of exactly one retry
cycle. (The per-attempt `LOG.debug("Attempt 2/2 failed")` is at DEBUG and is
suppressed at default log level, so the retry is inferred from the timing and the
code path; running `org.apache.phoenix.replication.ReplicationLog` at DEBUG makes
attempt 2 explicit.)

## Why the count proves the loss

```
422  recordCount on the active SYNC writer at close
420  finalized + readable on the peer (60486 bytes; block trailer validated to 420)
---
  2  counted-but-not-peer-durable:
     record 421 → block sealed locally (blockCount 420→421) but output.sync() failed;
                  retry sync() no-op'd (guard false) → FALSE ACK → currentBatch CLEARED
                  → NOT a replay candidate → LOST = s17b-1940001
     record 422 → still in currentBatch (its block was never closed) →
                  onFailure → replayBatch(getCurrentBatch()) re-shipped it to out/017 → delivered
```

The standby only ever sees a finalized block. The `COMPLETE but not
UNDER_CONSTRUCTION` error is the NameNode reporting that the block was already
finalized *while the active writer still held it open and was trying to append* —
the standby replay reader force-recovers the lease on the in-progress `.plog`
every round (`recoverLease` → `internalReleaseLease` → `commitBlockSynchronization`),
sealing the block at whatever length reached the peer DN (here 60486 bytes = 420
records). The reader's finalize races the writer's own block completion and wins.
Records 421 and 422 never reached the peer DN before that finalize, so 420 is all
the standby could apply.

## The defect, precisely

`LogFileFormatWriter.sync()` performs **partial, non-idempotent state mutation**
(`closeBlock()`) *ahead of* the durability barrier (`output.sync()`). This makes
`sync()` unsafe to retry: a first attempt that fails at the barrier has already
advanced `blockCount` and nulled `blockDataStream`, so the retry's guard
short-circuits to a no-op that **reports success without achieving durability**.
Combined with `ReplicationLog.apply()`'s "retry on a fresh writer" contract —
which silently degrades to "retry on the same writer" whenever rotation is also
blocked by the same fault — a single failed block sync is laundered into a
client ACK.

This is distinct from and worse than "sync completed before the peer was durable":
the write path *believes* it retried and succeeded, so no failure is ever
surfaced to the client, the batch is cleared, and no replay is attempted.

## The fix (shipped)

Commit `b5ee5dcac9` ("Fence writer on sync failure to prevent false-success RPO
loss (S17b)"). Rather than reorder `closeBlock()`/`output.sync()` inside
`LogFileFormatWriter`, the fix **fences the writer** on first failure — mirroring
HDFS `DFSOutputStream` single-shot semantics — and makes the retry honest. Two
layers:

1. **`LogFileWriter` — writer-level fence (primary).** A new
   `volatile IOException fault` latches the first `append`/`sync` failure. A new
   `checkWritable()` runs at the top of every `append` and `sync`; once `fault`
   is set it throws `"Writer is faulted by a prior failure"` **before touching
   the underlying stream.** This directly kills the false success: on the retry,
   even though `LogFileFormatWriter.sync()`'s guard would be false (the block was
   already consumed by `closeBlock()` on attempt 1), the call never reaches that
   guard — `checkWritable()` throws first. A partially-written block is never
   re-driven on the same writer.

2. **`ReplicationLog.apply()` — no false second chance.** After
   `requestRotation()` + `latch.await(retryDelayMs)`, if `pendingWriter.get() ==
   null` (no fresh writer was staged — exactly the case where the same peer-DN
   fault also blocks `createNewWriter()`'s header sync), `apply()` now `throw e`
   instead of burning attempt 2 on the fenced writer. It distinguishes the two
   ways this happens (`rotationCompleted` = task ran but `createNewWriter`
   failed, vs `!rotationCompleted` = task did not finish in the window). It also
   upgrades the previously-`LOG.debug` per-attempt line to **`LOG.warn` with the
   cause/stack** — since a successful retry propagates nothing, this is the only
   record of the transient failure (and is why the retry window looked silent in
   the original S17b logs).

**Net:** a failed block sync can no longer be laundered into a client ACK. The
failing event surfaces its IOException; `LogEventHandler#onEvent` logs it and
drives the SYNC→STORE_AND_FORWARD transition. Recovery stays the higher layer's
job — rotate to a fresh writer and replay the unsynced batch; the failing
record itself is recovered via `replayFailedEvent`. Note the original loss
occurred with **no RS crash**, so the `preWALRestore` re-ship net that protects
the S6/S12 crash cases (see `Phoenix_HA_Failover_Test_Scenarios.md`) never
engaged — the fence is what closes this no-crash path.

**Tests:** `LogFileWriterSyncTest` (writer-fence behavior); `ReplicationLogGroupTest`
(no-same-writer retry, and block-full append-failure recovery via
`replayFailedEvent`).

## Fix verified on the test-bed (S17b re-run, 2026-08-07)

Rebuilt the kind image with the fenced jar and re-ran S17b with the identical
fault — node-level `iptables` FORWARD `DROP` on the writer RS's pod IP →
cluster-b DataNodes `:9866` — under sustained HA-connection load on
`toxiHAGroup`. Writer = `regionserver-0` (holds the `PHOENIX_HA_T` region).

The fix fired exactly as designed, and the two log layers make the previously
silent retry window explicit (WARN, with cause):

```
18:21:09,438 WARN  hdfs.DFSClient: Error while syncing
18:21:09,439 WARN  ReplicationLog: Write attempt 1/2 failed on writer ...recordCount=1201...;
                   requesting rotation to retry on a fresh writer
18:21:09,539 WARN  ReplicationLog: Rotation did not complete within 100ms; surfacing the
                   original failure rather than retrying fenced writer ...   ← layer-2 throw e
18:21:09,539 INFO  SyncModeImpl: HAGroup toxiHAGroup mode=SYNC got error
18:21:09,556 INFO  ReplicationLogGroup: HAGroup toxiHAGroup switched from SYNC to STORE_AND_FORWARD
```

The `Rotation did not complete within 100ms; surfacing the original failure
rather than retrying fenced writer` line is the exact spot where the old code
would have run attempt 2 into the false-success no-op and laundered the failure
into a client ACK. Now the IOException surfaces, the group flips
`SYNC → STORE_AND_FORWARD`, and no record is silently dropped.

**Result — zero RPO:**

- 151/151 load rounds `rc=0` (no client-visible write error across the fault).
- On fault removal: `STORE_AND_FORWARD → SYNC_AND_FORWARD` (OUT queue drained to
  0) → `SYNC` (recovered at 18:26:10).
- Count parity on the fresh `s17bfix` prefix: cluster-a **45300** = cluster-b
  **45300**; `COUNT(DISTINCT ID)` = 45300 on both (no holes, no duplicates).
- **Byte-for-byte content parity:** md5 of all `(ID, C)` rows =
  `d123faecc5e5568e6c3743013765b7b9` on **both** clusters.

Contrast the original pre-fix run (writer `recordCount=422` vs peer `420`,
`s17b-1940001` lost). That historical loss is still visible as a residual
delta-of-1 in the *old* `s17b-` prefix (cluster-a 28680 vs cluster-b 28679) — a
fossil of the exact bug this fence closes, left untouched by this run.

## What is NOT the cause (ruled out during analysis)

- **Late writer close / trailer-validation failure.** The active writer's `/in`
  `.plog` close fails **every** round: the standby reader force-recovers the
  lease ~13 s before the writer would rotate, so close throws `Fail to recover
  lease` / `COMPLETE but not UNDER_CONSTRUCTION` and the reader logs `Invalid
  Trailer, proceeding`. This is steady-state design noise, present on lossless
  rounds (e.g. `in/014`, `in/015` → 900/900). Not causal.

- **The `:53` rotation phase offset.** Under load the writer's `/in` file rotates
  ~41–53 s into the round rather than on the 60 s boundary, and never re-aligns.
  This is a one-time ~173 s rotation-thread stall (a `createNewWriter` header
  sync blocked on a transiently-bad peer DN) re-phasing the `scheduleAtFixedRate`
  cadence; it affects both HA groups in lockstep and is benign for correctness
  (the reader's round buffer + lease recovery tolerate any writer phase). It is
  orthogonal to this loss.

- **Duplicate `ReplicationLogGroup` instances** on the RS for the same logical
  group, keyed by different `ServerName` spellings (hostname vs pod IP). Only the
  IP-keyed instance takes writes, so the loss is entirely within one instance.
  A separate, real bug — but not the cause of this RPO loss.