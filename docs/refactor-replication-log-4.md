# Align Writer Rotation with Replication Round Boundaries and Restore Replay

## Summary of improvements from Plan 1 onwards

Starting from commit `64a157c449` where `getWriter()` acquired a `ReentrantLock` on every call from the disruptor consumer thread's hot write path — stalling all write RPC processing during expensive HDFS rotation I/O — we have progressively restructured `ReplicationLog` through four plans:

### Plan 1: Two-Phase Rotation with Lock-Free Fast Path
**Problem:** `rotateLog()` held a `ReentrantLock` during expensive HDFS operations (file create, header write, sync, trailer write, close), stalling all write RPC processing through the disruptor.

**Solution:** Restructured `rotateLog()` into three phases — create new writer (no lock), swap pointer (lock held, no I/O), close old writer (no lock). Made `getWriter()` lock-free since it's only called from the single disruptor consumer thread.

**Removed:** HDFS I/O under lock.

### Plan 2: pendingWriter Staging via AtomicReference
**Problem:** The background `LogRotationTask` called `rotateLog()` which closed the old writer in Phase 3. If the disruptor consumer thread was mid-write, it got an IOException — causing unnecessary error-based rotations and wasted work every rotation cycle.

**Solution:** Background thread only creates new writers and stages them in `AtomicReference<LogFileWriter> pendingWriter`. The disruptor thread owns the full swap lifecycle (drain pendingWriter, swap pointer, close old). Replaced `ReentrantLock` + `volatile boolean closed` with `AtomicBoolean closed`.

**Removed:** `ReentrantLock`, `volatile boolean closed`, `RotationReason` enum, `pendingClose` field, error-based rotation (`rotateLog(ERROR)` in apply's catch block), concurrent rotation detection.

### Plan 3: closeExecutor and Unified Rotation Task
**Problem:** The disruptor consumer thread still performed HDFS I/O (writer close) on the hot event-processing path for both time-based and size-based rotation. Size-based rotation also called `createNewWriter()` on the consumer thread.

**Solution:** Added `closeExecutor` (cached thread pool) for async old writer closes — consumer thread never closes a writer. Unified time-based and size-based rotation through the same `LogRotationTask` via a `rotationRequested` flag. Moved writer swap to batch boundary (inside `sync()` after data is durable).

**Removed:** `rotateLog()` method, `getWriter()` method (replaced by direct `currentWriter` access), `createNewWriter()` on hot path for size rotation.

### Plan 4: Round Alignment and Replay (this document)
**Problem:** (1) Rotation time and round duration were separate configs that could diverge. (2) HDFS lease recovery by the reader breaks `currentWriter` after a bursty-then-idle period, causing unnecessary mode-switch to STORE_AND_FORWARD while a healthy `pendingWriter` sits unused.

**Solution:** Derive `rotationTimeMs` from round duration. Align rotation schedule with round boundaries. Move writer swap from `sync()` into `apply()` (before each attempt) so a broken `currentWriter` is replaced before it's used. Add generation-based replay for mid-batch swaps. Make `getWriter()` drain pending writers so test callers always get the up-to-date writer.

**Removed:** `getRotationCheckInterval()`, separate rotation time config, immediate `rotationExecutor.execute()` for size rotation, `getNextRoundStartTimestamp()` helper.

### Plan 5: On-Demand Rotation for Size Threshold and Error Recovery
**Problem:** (1) Size-based rotation only set a flag, waiting up to a full round duration before the scheduled tick picked it up. (2) Non-transient HDFS errors (lease revoked, stream closed) exhausted all retries on the same broken writer → `closeOnError()` → STORE_AND_FORWARD, even though the error was recoverable with a fresh writer.

**Solution:** Extract `requestRotation()` which sets the `rotationRequested` flag and immediately submits a `LogRotationTask` to the executor. Size-based rotation (`requestRotationIfNeeded()`) calls it after threshold exceeded. Error recovery in `apply()` calls it on `attempt > 1` — first failure retries on the same writer (transient), second failure requests a new writer (non-transient). Simplified `LogRotationTask.run()` by removing the `lastRotationTime` time guard (redundant with `scheduleAtFixedRate`). Collapsed size/time rotation metrics into a single `rotationCount`.

**Removed:** `lastRotationTime` field and time guard, `TIME_BASED_ROTATION_COUNT` / `SIZE_BASED_ROTATION_COUNT` / `ERROR_BASED_ROTATION_COUNT` metrics.

### Net result across all five plans

| Concern | Before (commit 64a157c449) | After (Plan 5) |
|---------|---------------------------|-----------------|
| Lock contention | `ReentrantLock` held during all HDFS I/O | No locks. `AtomicReference` + `AtomicBoolean` |
| HDFS I/O on consumer thread | File create, write, sync, close — all inline | Zero. All creation and close on background threads |
| Writer swap timing | Under lock, any time | In `apply()` before each attempt, guarded by generation |
| Rotation schedule | Arbitrary interval, decoupled from reader | Aligned with round boundaries |
| File placement | Raw `currentTimeMillis()` | Raw `currentTimeMillis()` — round-aligned rotation ensures files close before reader arrives |
| Error recovery | Error-based rotation (create new writer on broken HDFS) | On-demand `requestRotation()` on second failure; drain staged writer on retry |
| Size rotation | Flag-only, waits for next scheduled tick | On-demand `requestRotation()` submits task immediately |
| Config surface | Separate rotation time + round duration | Single source: round duration |

---

## Context

After plans 1-3, the ReplicationLog has a clean design: `LogRotationTask` (background thread) creates writers and stages them in `pendingWriter`; the disruptor consumer thread drains the staged writer via `checkAndReplaceWriter()` at batch boundary (after sync); old writers are closed asynchronously via `closeExecutor`.

Two problems remain:

### Problem 1: Config divergence between rotation time and round duration
`rotationTimeMs` is a separate config from the reader's round duration (`PHOENIX_REPLICATION_ROUND_DURATION_SECONDS`). They can diverge, so there's no guarantee the writer closes before the reader arrives for a given round.

### Problem 2: HDFS lease recovery breaks currentWriter

The reader (`ReplicationLogDiscovery`) processes rounds after `roundDuration + buffer` has elapsed:
```
Reader ready condition: currentTime - lastRoundEnd >= roundDuration + bufferMillis
Default buffer: 15% of roundDuration (e.g. 9 seconds for 60-second rounds)
```

With the writer swap in `sync()` (plan 3's design), the old file stays open until the next sync — which could be arbitrarily delayed under low traffic. This creates a full failure chain:

```
T = R - ε    Sync arrives. checkAndReplaceWriter() → no pendingWriter.
             currentWriter's file is in shard [R-D, R). OPEN. currentBatch cleared.

T = R        LogRotationTask fires at round boundary. Creates new writer
             (stamped into shard [R+D, R+2D)). Stages in pendingWriter.
             currentWriter still points to old writer. File STILL OPEN.

             ... silence (no events) ...

T = R + B    Reader arrives for round [R-D, R).
             Condition: currentTime - (R-D) >= D + B  →  met.
             Reader finds old writer's file, still open.
             HDFS lease recovery yanks the lease.
             currentWriter's FSDataOutputStream is NOW BROKEN.
             pendingWriter (healthy) sits staged, unused.

T = ???      Events arrive.
             apply() → action.action(currentWriter) → IOException (broken stream)
             Retry → same broken currentWriter → IOException
             All retries exhausted → closeOnError() → mode switch to STORE_AND_FORWARD
```

The healthy `pendingWriter` is discarded during `closeOnError()` without ever being used. The system mode-switches to STORE_AND_FORWARD unnecessarily.

**Step 5 fixes this:** With `checkAndReplaceWriter()` at the top of the retry loop in `apply()`, the first attempt drains the healthy `pendingWriter` before touching the broken writer. No mode switch needed.

**Step 4 adds error recovery rotation:** Even without a pre-staged `pendingWriter`, `apply()` now recovers from non-transient errors. On the second failure (`attempt > 1`), `requestRotation()` submits an on-demand `LogRotationTask` to the executor. During the retry sleep window, the background thread creates a fresh writer. The next attempt's `checkAndReplaceWriter()` drains it. First failure retries on the same writer (covers transient errors); second failure requests a new writer (covers non-transient stream errors like lease revocation).

**Step 5 forces step 6:** Once the swap can happen inside `apply()` (before each attempt), it can happen mid-batch — between appends and sync. Records appended to the old writer but not yet synced must be replayed into the new writer for data safety.

**Note:** Moving the swap to `apply()` does NOT help the idle-then-reader case by itself (if no events flow, `apply()` is never called). But it ensures that when events DO arrive after lease recovery, the system recovers gracefully instead of fail-stopping.

### Why replay is separated from checkAndReplaceWriter()

Replay lives in `apply()` guarded by a generation mismatch, not inside `checkAndReplaceWriter()`. This is critical for two reasons:

1. `checkAndReplaceWriter()` is also called from `forceRotation()` (tests) where `currentBatch` is always empty — replay would be a no-op and adds unnecessary coupling.

2. If replay fails, the next retry in `apply()` needs to re-attempt it. With replay inside `checkAndReplaceWriter()`, the second attempt would find `pendingWriter` null (already drained) and skip replay entirely. The generation check retries replay independently of the swap:
   - Attempt 1: `generation=4`, `writer.generation=5` → mismatch → replay fails → generation stays 4
   - Attempt 2: `generation=4`, `writer.generation=5` → mismatch → replay retries → succeeds → `generation=5`
   - If rotation task stages another writer during retry sleep: attempt 2 drains it (`generation=6`), mismatch again, replays into the fresh writer.

---

## Files to modify

1. `phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLog.java` — remove `lastRotationTime`/time guard, extract `requestRotation()`, error recovery in `apply()`, simplify `LogRotationTask`, simplify `forceRotation()`
2. `phoenix-core-server/src/main/java/org/apache/phoenix/replication/metrics/MetricsReplicationLogGroupSource.java` — remove size/time/error rotation metric constants and method declarations
3. `phoenix-core-server/src/main/java/org/apache/phoenix/replication/metrics/MetricsReplicationLogGroupSourceImpl.java` — remove size/time/error metric fields, init, impls, update `getCurrentMetricValues()`
4. `phoenix-core-server/src/main/java/org/apache/phoenix/replication/metrics/ReplicationLogMetricValues.java` — remove size/time/error fields, constructor params, getters
5. `phoenix-core/src/test/java/org/apache/phoenix/replication/ReplicationLogBaseTest.java` — increase default round duration to 60s, remove `stopRotationExecutor()`, add `recreateLogGroup()`, parameterize `waitForRotationTick()`
6. `phoenix-core/src/test/java/org/apache/phoenix/replication/ReplicationLogGroupTest.java` — new tests, update scheduler-dependent tests to use `recreateLogGroup()` with short round duration

Read-only references:
- `ReplicationShardDirectoryManager.java` — provides `getNearestRoundStartTimestamp()`, `getReplicationRoundDurationSeconds()`, `getWriterPath()`
- `ReplicationLogGroup.java` — `LogEventHandler.onFailure()` uses `getCurrentBatch()` for mode-transition replay (unchanged)

---

## Step 1: Derive `rotationTimeMs` from round duration

**File:** `ReplicationLog.java` constructor (lines 96-97)

Replace:
```java
this.rotationTimeMs = conf.getLong(ReplicationLogGroup.REPLICATION_LOG_ROTATION_TIME_MS_KEY,
  ReplicationLogGroup.DEFAULT_REPLICATION_LOG_ROTATION_TIME_MS);
```
With:
```java
this.rotationTimeMs = shardManager.getReplicationRoundDurationSeconds() * 1000L;
```

Eliminates config divergence. The `shardManager` already reads `PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY`.

---

## Step 2: Use raw timestamps in `createNewWriter()` (no snapping)

**File:** `ReplicationLog.java`, `createNewWriter()` (lines 142-153)

`createNewWriter()` uses raw `EnvironmentEdgeManager.currentTimeMillis()` for file timestamps:

```java
protected LogFileWriter createNewWriter() throws IOException {
    long timestamp = EnvironmentEdgeManager.currentTimeMillis();
    Path filePath = replicationShardDirectoryManager.getWriterPath(timestamp,
        logGroup.getServerName().getServerName());
    // ... rest unchanged
}
```

**Why not snap to round boundaries:** An earlier iteration snapped timestamps to the next round boundary via `getNextRoundStartTimestamp()`. This caused filename collisions when `forceRotation()` was called multiple times within the same round in tests, since multiple files got the same snapped timestamp. Raw timestamps are simpler and guarantee unique filenames. The round-aligned rotation executor (Step 3) already ensures writers rotate at round boundaries, so files close before the reader arrives without needing timestamp manipulation.

---

## Step 3: Align rotation schedule with round boundaries

**File:** `ReplicationLog.java`, `startRotationExecutor()` (lines 147-155)

```java
protected void startRotationExecutor() {
    rotationExecutor = Executors.newSingleThreadScheduledExecutor(...);
    long now = EnvironmentEdgeManager.currentTimeMillis();
    long currentRoundStart = replicationShardDirectoryManager.getNearestRoundStartTimestamp(now);
    long nextRoundStart = currentRoundStart + rotationTimeMs;
    long initialDelay = nextRoundStart - now;
    rotationExecutor.scheduleAtFixedRate(new LogRotationTask(), initialDelay,
        rotationTimeMs, TimeUnit.MILLISECONDS);
}
```

Period = `rotationTimeMs` (= round duration). Initial delay aligns the first tick with the next round boundary.

Remove `getRotationCheckInterval()` — no longer needed.

---

## Step 4: On-demand rotation for size threshold and error recovery

**File:** `ReplicationLog.java`

Extract `requestRotation()` which sets the flag and immediately submits a `LogRotationTask`:

```java
private void requestRotation() {
    if (rotationRequested.compareAndSet(false, true)) {
        try {
            rotationExecutor.execute(new LogRotationTask());
        } catch (RejectedExecutionException e) {
            LOG.info("Rotation executor shut down, skipping on-demand rotation", e);
            rotationRequested.set(false);
        }
    }
}
```

`compareAndSet` avoids duplicate submissions. The `RejectedExecutionException` catch handles the race where `close()`/`closeOnError()` shuts down the executor while the consumer thread is still processing events.

**`requestRotationIfNeeded()`** calls `requestRotation()` when the size threshold is exceeded:
```java
private void requestRotationIfNeeded() throws IOException {
    if (shouldRotateForSize()) {
        requestRotation();
    }
}
```

**`apply()` catch block** requests a new writer on the second failure:
```java
} catch (IOException e) {
    if (attempt == maxAttempts) { closeOnError(); throw e; }
    // First failure retries on the same writer (transient). Second failure
    // requests a new writer to recover from non-transient stream errors.
    if (attempt > 1) { requestRotation(); }
    Thread.sleep(retryDelayMs);
}
```

Retry flow with `maxAttempts=6` (default):
- Attempt 1 fails → sleep → retry on same writer (transient)
- Attempt 2 fails → `requestRotation()` + sleep → background thread creates writer during sleep → attempt 3's `checkAndReplaceWriter` drains it
- Attempt 3 on new writer → succeeds (or continues retrying)

**`LogRotationTask.run()`** simplified — remove `lastRotationTime` and time guard:
```java
public void run() {
    if (closed.get()) { return; }
    rotationRequested.compareAndSet(true, false);
    try {
        LogFileWriter newWriter = createNewWriter();
        LogFileWriter undrained = pendingWriter.getAndSet(newWriter);
        if (undrained != null) { closeWriter(undrained); }
        rotationFailures.set(0);
        logGroup.getMetrics().incrementRotationCount();
    } catch (IOException e) { ... }
}
```

Every invocation unconditionally creates a writer. The time guard was redundant with `scheduleAtFixedRate`. This also fixes the flaky `testUndrainedPendingWriterReplaced` (root cause: divergence between `System.nanoTime()` used by the scheduler and `System.currentTimeMillis()` used by the time guard).

**`forceRotation()`** simplified:
```java
protected void forceRotation() {
    new LogRotationTask().run();
    checkAndReplaceWriter(false);
}
```

No `lastRotationTime` manipulation needed — the task is unconditional.

**Metrics simplified:** Removed `TIME_BASED_ROTATION_COUNT`, `SIZE_BASED_ROTATION_COUNT`, `ERROR_BASED_ROTATION_COUNT` and their methods from `MetricsReplicationLogGroupSource`, `MetricsReplicationLogGroupSourceImpl`, and `ReplicationLogMetricValues`. All rotations increment a single `rotationCount`.

---

## Step 5: Move writer swap from `sync()` into `apply()` (before every attempt)

**File:** `ReplicationLog.java`

**`apply()`** (lines 261-284) — call `checkAndReplaceWriter(true)` inside the loop, before each attempt. If the action fails and we sleep during `retryDelayMs`, the rotation task may stage a fresh writer during that window. Draining it before the next attempt gives us a healthy writer to retry on.

```java
private void apply(Action action) throws IOException {
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
        checkAndReplaceWriter(true);
        if (isClosed()) {
            throw new IOException("Closed");
        }
        try {
            action.action(currentWriter);
            requestRotationIfNeeded();
            break;
        } catch (IOException e) {
            LOG.debug("Attempt {}/{} failed", attempt, maxAttempts, e);
            if (attempt == maxAttempts) {
                closeOnError();
                throw e;
            }
            try {
                Thread.sleep(retryDelayMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("Interrupted during retry delay");
            }
        }
    }
}
```

**`sync()`** (lines 296-301) — remove `checkAndReplaceWriter(true)`:

```java
protected void sync() throws IOException {
    apply(LogFileWriter::sync);
    currentBatch.clear();
}
```

The swap now happens at the first event (append or sync) after the rotation task stages a new writer, closing the old file sooner.

**`checkAndReplaceWriter()` stays unchanged** — just swap + close, no replay:

```java
protected void checkAndReplaceWriter(boolean asyncClose) {
    LogFileWriter newWriter = pendingWriter.getAndSet(null);
    if (newWriter != null) {
        LogFileWriter oldWriter = currentWriter;
        currentWriter = newWriter;
        if (asyncClose) {
            submitClose(oldWriter);
        } else {
            closeWriter(oldWriter);
        }
    }
}
```

**`getWriter()` drains pending writers** — test-visible accessor now calls `checkAndReplaceWriter(false)` before returning `currentWriter`, so test callers always get the up-to-date writer regardless of whether a rotation tick has staged a pending writer:

```java
@VisibleForTesting
protected LogFileWriter getWriter() {
    checkAndReplaceWriter(false);
    return currentWriter;
}
```

---

## Step 6: Replay unsynced appends on writer swap via generation check

Since the swap can now happen mid-batch, records appended to the old writer but not yet synced must be replayed into the new writer. The generation check in `apply()` handles this independently of the swap.

**Add tracking field and initialize it in `init()`:**

```java
private long generation;
```

In `init()`, set `lastRotationTime` to the current round start (so the first rotation tick's time check passes), then after `createNewWriter()`, set `generation` to match the first writer:
```java
public void init() throws IOException {
    lastRotationTime.set(
      replicationShardDirectoryManager.getNearestRoundStartTimestamp(
        EnvironmentEdgeManager.currentTimeMillis()));
    startRotationExecutor();
    currentWriter = createNewWriter();
    generation = currentWriter.getGeneration();
}
```

Setting `lastRotationTime` to the round start ensures the first rotation tick at `nextRoundStart` sees `now - lastRotationTime >= rotationTimeMs` and creates a new writer. Setting `generation` avoids a spurious generation mismatch on the first `apply()` call (writer starts at generation 1, field at 0).

**New private method:**

```java
private void replayCurrentBatch() throws IOException {
    if (currentBatch.isEmpty()) {
        return;
    }
    LOG.info("Replaying {} unsynced records into new writer", currentBatch.size());
    for (Record r : currentBatch) {
        currentWriter.append(r.tableName, r.commitId, r.mutation);
    }
}
```

**Update `apply()` — add generation check after `checkAndReplaceWriter()`:**

```java
private void apply(Action action) throws IOException {
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
        checkAndReplaceWriter(true);
        if (isClosed()) {
            throw new IOException("Closed");
        }
        try {
            if (currentWriter.getGeneration() > generation) {
                replayCurrentBatch();
                generation = currentWriter.getGeneration();
            }
            action.action(currentWriter);
            requestRotationIfNeeded();
            break;
        } catch (IOException e) {
            LOG.debug("Attempt {}/{} failed", attempt, maxAttempts, e);
            if (attempt == maxAttempts) {
                closeOnError();
                throw e;
            }
            try {
                Thread.sleep(retryDelayMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("Interrupted during retry delay");
            }
        }
    }
}
```

**Critical: generation is updated AFTER successful replay, not before.** This ensures replay retries on failure:
- Attempt 1: `generation=4`, `writer.generation=5` → mismatch → `replayCurrentBatch()` fails → generation stays 4
- Attempt 2: `generation=4`, `writer.generation=5` → mismatch → `replayCurrentBatch()` retries → succeeds → `generation=5`
- If rotation task stages another writer during retry sleep: attempt 2 drains it (`generation=6`), mismatch again, replays into the fresh writer.

If no new writer arrives and all retries exhaust — `closeOnError()`, mode switch. That's correct behavior: HDFS is truly broken.

**Duplicates are acceptable:** The old writer may flush records on close that also appear in the new writer. The reader handles idempotency.

**`forceRotation()` in tests is unaffected:** Always called after `sync()`, so `currentBatch` is empty and replay is a no-op.

**Important:** `currentBatch` and `getCurrentBatch()` must be preserved unchanged. They are used by `ReplicationLogGroup.LogEventHandler.onFailure()` to fetch unsynced appends from the old mode's `ReplicationLog` and replay them on the new mode's log during mode transitions.

---

## Edge cases analyzed

### Rotation swap + mode switch = double replay (acceptable)

Scenario: rotation replay copies [r1, r2] into the new writer, then sync fails on the new writer, `closeOnError()` triggers, `onFailure()` reads `getCurrentBatch()` → [r1, r2] → replays to STORE_AND_FORWARD log.

Records appear in both the (broken) new writer and the STORE_AND_FORWARD log. This is acceptable: the new writer's data was never synced (it failed), and duplicates are already expected — the plan states the reader handles idempotency. Same guarantee that covers the old writer flushing records on async close.

### closeOnError() from rotationExecutor racing with replayCurrentBatch() (pre-existing)

`closeOnError()` can be called from the rotation executor thread (when `maxRotationRetries` is exceeded) while `replayCurrentBatch()` is executing on the disruptor consumer thread. This could close `currentWriter` mid-replay, causing an IOException.

This is a **pre-existing race** — the same thing can happen today with any append or sync in `apply()`. The handling is correct: `replayCurrentBatch()` throws IOException → caught by `apply()`'s catch block → next retry checks `isClosed()` → true → throws `IOException("Closed")` → mode switch. If `closeOnError()` fires from the rotation executor, HDFS is truly broken and mode-switching is appropriate.

### Idle-then-lease-recovery with empty currentBatch (tested)

```
T=0   Last sync clears currentBatch. System goes idle.
T=R   LogRotationTask stages W2 in pendingWriter. currentWriter = W1 (file still open).
T=R+B Reader arrives, lease recovery breaks W1's stream.
T=??  Events resume. apply() → checkAndReplaceWriter(true) → drains W2 → action on W2.
      No replay needed (currentBatch empty). W1 never touched after break.
```

The reader handles files without a trailer (W1 was never closed by the writer). W2 receives the new events normally. Tested by `testIdleLeaseRecoveryDrainsStagedWriter`.

---

## Step 7: `LogRotationTask` simplified (done in Step 4)

The `lastRotationTime` time guard and size/time metric distinction were removed in Step 4. The task now unconditionally creates a writer on every invocation. `compareAndSet(true, false)` clears the on-demand flag so `requestRotation()` can re-arm.

---

## Step 8: Update test infrastructure

**File:** `ReplicationLogBaseTest.java`

- Remove `TEST_ROTATION_TIME` constant (line 76)
- Change `TEST_REPLICATION_ROUND_DURATION_SECONDS` from `20` to `5` (matches old 5000ms rotation time)
- Remove `conf.setLong(REPLICATION_LOG_ROTATION_TIME_MS_KEY, TEST_ROTATION_TIME)` from `setUpBase()` (line 102)

---

## Step 9: Update existing test assertions

Two behavioral changes affect tests:
- **A.** Writer swap now happens pre-action in `apply()`, not post-sync
- **B.** Size rotation no longer fires immediately — waits for next round tick

---

### `testTimeBasedRotation` (lines 345-383)

**Old behavior:** After rotation time, append+sync still go to old writer; swap happens after sync. Third append+sync go to new writer.

**New behavior:** After rotation time, append's `apply()` drains pending writer before the append. Second append+sync go directly to new writer. No need for a third batch.

Updated assertions:
```java
// First batch: old writer
inOrder.verify(writerBeforeRotation).append(eq(tableName), eq(commitId), eq(put));
inOrder.verify(writerBeforeRotation).sync();
// Second batch: new writer (swap happened before append)
inOrder.verify(writerAfterRotation).append(eq(tableName), eq(commitId + 1), eq(put));
inOrder.verify(writerAfterRotation).sync();
```

Remove third append+sync — no longer needed.

---

### `testSizeBasedRotation` (lines 392-427)

**Old behavior:** `requestRotationIfNeeded` submits immediate `LogRotationTask`. After sleep, second append goes to old writer, sync drains.

**New behavior:** `requestRotationIfNeeded` only sets `rotationRequested` flag. The rotation task picks it up at the next scheduled round boundary tick. The test needs to wait for the scheduled tick (use `forceRotation()` or wait for the round tick) instead of relying on immediate execution + short sleep.

Updated flow:
1. Append 100 records + sync (all go to old writer, flag is set)
2. Force rotation via `lastRotationTime.set(0)` + `new LogRotationTask().run()` (simulates the round boundary tick picking up the size flag)
3. Next append's `apply()` drains pending writer → append goes to new writer
4. Sync on new writer

Updated assertions:
```java
// After forced rotation + drain, new append goes to new writer
verify(writerAfterRotation).append(eq(tableName), eq(commitId), eq(put));
verify(writerAfterRotation).sync();
```

---

### `testRotationTask` (lines 478-505)

**Old behavior:** After rotation time, append+sync go to old writer; sync drains pending writer.

**New behavior:** After rotation time, append's `apply()` drains pending writer. Second append+sync go to new writer.

Updated assertions:
```java
// First batch: old writer
verify(writerBeforeRotation).append(eq(tableName), eq(1L), eq(put));
verify(writerBeforeRotation).sync();
// Second append goes to new writer (swap happened before append)
verify(writerAfterRotation).append(eq(tableName), eq(commitId + 1), eq(put));
verify(writerAfterRotation).sync();
// Old writer closed async
verify(writerBeforeRotation, timeout(5000)).close();
```

---

### `testRotationDuringBatch` (lines 710-747)

**Old behavior:** 5 appends + sync all go to old writer. Swap happens after sync.

**New behavior:** 5 appends are already in the ring buffer and processed before the test thread sleeps. The consumer processes all 5 fast (local FS). Then the test thread sleeps, rotation task fires and stages pending writer. When `sync()` is published and the consumer's `apply()` runs, it drains the pending writer, replays `currentBatch` (5 records) into the new writer, then syncs the new writer.

Updated assertions:
```java
InOrder inOrder = Mockito.inOrder(writerBeforeRotation, writerAfterRotation);
// 5 appends went to old writer (processed before rotation task fired)
for (int i = 0; i < 5; i++) {
    inOrder.verify(writerBeforeRotation).append(eq(tableName), eq(commitId + i), eq(put));
}
// Swap happens before sync action: 5 records replayed into new writer
for (int i = 0; i < 5; i++) {
    inOrder.verify(writerAfterRotation).append(eq(tableName), eq(commitId + i), eq(put));
}
// Sync goes to new writer
inOrder.verify(writerAfterRotation).sync();
// Old writer closed async
verify(writerBeforeRotation, timeout(5000)).close();
```

---

### `testSizeBasedRotationViaFlag` (lines 1086-1118)

**Old behavior:** `requestRotationIfNeeded` submits immediate `LogRotationTask`. After sleep(500), second append goes to old writer, sync drains.

**New behavior:** Flag is only set, not immediately executed. Need to trigger the task (via `forceRotation` or wait for scheduled tick). After staging, the draining append goes directly to the new writer.

Updated flow similar to `testSizeBasedRotation` above.

---

### Tests NOT affected

These tests have `currentBatch` empty at the swap point (swap after sync or during `forceRotation`), so replay is a no-op. The swap-before-action change doesn't alter their assertions.

- `testAppendAndSync` — no rotation
- `testSyncFailureAndRetry` — no rotation
- `testBlockingWhenRingFull` — no rotation
- `testAppendFailureAndRetry` — no rotation
- `testSyncTimeout` — no rotation
- `testConcurrentProducers` — no rotation
- `testClose` — no rotation assertions
- `testEventProcessingException` — closeOnError path
- `testSwitchToStoreAndForwardOnSyncFailure` — mode-switch path
- `testFailToUpdateHAGroupStatusOnSwitchToStoreAndForward` — mode-switch path
- `testFailedRotation` — first rotation fails (no pending writer staged), second succeeds; `currentBatch` is empty at drain point because there's a sync between rotations
- `testTooManyRotationFailures` — uses `forceRotation()` after sync, `currentBatch` empty
- `testRuntimeExceptionDuringLengthCheck` — closeOnError path
- `testAppendAfterCloseOnError` — closeOnError path
- `testSyncAfterCloseOnError` — closeOnError path
- `testUndrainedPendingWriterReplaced` — uses direct `LogRotationTask().run()` + drain; no Disruptor involved
- `testSyncConsolidation` — no rotation
- `testReplicationLogGroupCaching` — no rotation
- `testReplicationLogGroupCacheRemovalOnClose` — no rotation
- `testInFlightAppendsReplayAfterModeSwitch` — mode-switch replay path (uses `getCurrentBatch()` from `onFailure`, not rotation swap)
- `testMetricsCaching` — no rotation
- All `testReadAfter*` tests — use `forceRotation()` after sync, `currentBatch` empty

---

## Step 10: New tests

**File:** `ReplicationLogGroupTest.java`

### `testReplayOnMidBatchSwap`
Verifies that unsynced appends are replayed into the new writer when a swap happens mid-batch.
1. Append 3 records (no sync — they accumulate in `currentBatch`)
2. Directly stage a pending writer via `LogRotationTask` (set `lastRotationTime.set(0)`, run task)
3. Append a 4th record — this triggers `checkAndReplaceWriter` in `apply()`, which drains the pending writer and replays 3 records
4. Sync
5. Assert: old writer has 3 appends (no sync), new writer has 3 replayed appends + 4th append + sync
6. Assert: old writer closed async

### `testRetryPicksUpStagedWriter`
Verifies the lease-recovery scenario.
1. Get the initial writer
2. Stage a pending writer via `LogRotationTask`
3. Configure the initial writer's `sync()` to fail with IOException (simulating stream closed after reader rename)
4. Call `append` + `sync`
5. The first sync attempt fails on the old writer. After retry delay, `checkAndReplaceWriter` before the second attempt drains the staged writer. The second attempt succeeds on the new writer.
6. Assert: old writer received append + failed sync. New writer received replayed append + successful sync.

### `testReplayFailureRetries`
Verifies that a failed replay is retried on the next attempt with the same writer.
1. Append 2 records (no sync — accumulate in `currentBatch`)
2. Stage pending writer via `LogRotationTask`
3. Configure the new writer's first `append()` call to fail with IOException (simulating a transient HDFS error during replay)
4. Call `append(r3)` — triggers swap + replay, replay fails on first record → IOException caught, attempt 1 fails
5. Reset the new writer's `append()` to succeed
6. Attempt 2: `checkAndReplaceWriter()` → no pendingWriter (already drained). Generation mismatch still exists (generation not updated because replay failed) → replays [r1, r2] successfully → generation updated → r3 appended
7. Assert: new writer received 2 failed replay appends (attempt 1) + 2 successful replay appends (attempt 2) + r3 append

### `testRotationScheduleAlignsWithRoundBoundary`
Verifies that the rotation executor fires at the round boundary.
1. Record the `lastRotationTime` after init
2. Wait for the first rotation tick to fire
3. Assert that the rotation happened at or very near the next round boundary (within a tolerance)

---

## Verification

```bash
mvn test -pl phoenix-core -Dtest=ReplicationLogGroupTest
```
