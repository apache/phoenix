# Refactor ReplicationLog: Move Writer Close Off Disruptor Consumer Thread

## Context

The disruptor consumer thread in `ReplicationLog` currently performs HDFS I/O (writer close) on the hot event-processing path. Both time-based rotation (`getWriter()` line 182) and size-based rotation (`rotateLog()` line 241) call `closeWriter(oldWriter)` synchronously on the consumer thread, blocking all ring buffer event processing during NameNode/DataNode RPCs. HBase avoids this by using a dedicated `closeExecutor` thread pool — the consumer thread never closes a writer.

Additionally, size-based rotation calls `createNewWriter()` on the consumer thread (`rotateLog()` line 221), adding more HDFS I/O to the hot path.

Currently `getWriter()` is called from `apply()` on every event (every append and sync), meaning the `pendingWriter` drain and size check happen mid-batch — not just at batch boundaries. This can cause a writer swap between appends and sync, triggering the generation check and batch replay logic.

There is also dead code: the `closeWriter(newWriter)` at line 388 in `TimeBasedLogRotationTask` can never execute because the CAS cannot fail (single-threaded executor, sole writer of `pendingWriter` to non-null).

### Problems addressed

1. **Old writer close blocks disruptor consumer** — both rotation paths close inline
2. **Size-based rotation creates new writer on consumer thread** — HDFS file create, header write, sync
3. **Writer swap happens mid-batch** — `getWriter()` called on every event, not just at batch boundaries after sync
4. **Dead code** — unreachable CAS failure branch in `TimeBasedLogRotationTask`
5. **Undrained pendingWriter during idle** — reader expects files to rotate every ~60s; if no events flow, the staged writer is never drained and the old file stays open too long

### Approaches considered

**Safe point latch (rejected):** Rotation thread creates new writer, sets a two-latch `SafePointLatch`, blocks until consumer finishes current batch, then rotation thread owns the swap and submits old writer to `closeExecutor`. Matches HBase's `SafePointZigZagLatch` pattern.

Rejected because:
- **Idle system is broken** — rotation thread blocks forever waiting for consumer to reach safe point when no events are flowing. Requires timeout/cancel fallback, adding complexity.
- Consumer can't block waiting for the new writer after signaling size rotation — that would put HDFS I/O back on the consumer's critical path indirectly.
- Two-latch coordination adds complexity vs a single `AtomicReference`.

**Staging + closeExecutor (chosen):** Keep the `pendingWriter` staging pattern. Unify both time-based and size-based rotation through the same mechanism. Add a `closeExecutor` thread pool for old writer closes. Rotation task handles undrained writers. Move writer swap to batch boundary (after sync).

## Recommended Approach

### Overview

- **Rotation task** (background thread): creates new writers for both time-based and size-based rotation, stages them in `pendingWriter`
- **Consumer thread**: drains `pendingWriter` at **batch boundary** (inside `sync()` after data is durable), swaps pointer, submits old writer to `closeExecutor`, checks size and signals rotation task
- **`closeExecutor`** (new thread pool): closes old writers asynchronously off the hot path
- **Rotation task** replaces undrained `pendingWriter` to keep files fresh for the reader

### Step 1: Add `closeExecutor` field

**File:** `ReplicationLog.java`

Add a cached thread pool for async writer closes, similar to HBase's `AbstractFSWAL.closeExecutor`:

```java
private final ExecutorService closeExecutor = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("Close-ReplicationLog-Writer-%d").build());
```

### Step 2: Add `rotationRequested` flag for size-based rotation

**File:** `ReplicationLog.java`

Add an `AtomicBoolean` that the consumer sets when size threshold is exceeded, and the rotation task reads:

```java
private final AtomicBoolean rotationRequested = new AtomicBoolean(false);
```

### Step 3: Move writer swap from `getWriter()` into `sync()`

**File:** `ReplicationLog.java`

**Remove** `getWriter()` method entirely (lines 176-188). The `apply()` method uses `currentWriter` directly.

Move the swap + size check logic into `sync()`, after the sync action succeeds and `currentBatch` is cleared. This ensures the swap only happens at batch boundaries when all data is durable and there are no unsynced appends.

```java
protected void sync() throws IOException {
    apply(LogFileWriter::sync);
    currentBatch.clear();
    // Swap and size check at batch boundary — all data is durable
    checkAndSwapWriter();
}

private void checkAndSwapWriter() throws IOException {
    LogFileWriter newWriter = pendingWriter.getAndSet(null);
    if (newWriter != null) {
        LogFileWriter oldWriter = currentWriter;
        currentWriter = newWriter;
        submitClose(oldWriter);
    }
    if (shouldRotateForSize()) {
        rotationRequested.set(true);
    }
}
```

### Step 4: Update `apply()` to use `currentWriter` directly

**File:** `ReplicationLog.java` (lines 270-302)

Replace `LogFileWriter writer = getWriter()` with `LogFileWriter writer = currentWriter`. The generation check and batch replay logic stays as-is — it provides a safety net for edge cases (e.g., mode transitions), though it should not trigger during normal rotation since swaps now happen after sync.

**Important:** `currentBatch` and `getCurrentBatch()` must be preserved unchanged. They are used by `ReplicationLogGroup.LogEventHandler.onFailure()` (line 965) to fetch unsynced appends from the old mode's `ReplicationLog` and replay them on the new mode's log during mode transitions.

### Step 5: Add `submitClose()` helper

**File:** `ReplicationLog.java`

```java
private void submitClose(LogFileWriter writer) {
    if (writer == null) {
        return;
    }
    closeExecutor.execute(() -> closeWriter(writer));
}
```

### Step 6: Refactor `TimeBasedLogRotationTask` → `LogRotationTask`

**File:** `ReplicationLog.java` (lines 367-394)

Rename and refactor to handle both time-based and size-based rotation. The rotation task now:
1. Closes and replaces undrained `pendingWriter` (handles idle system + long batch — keeps files fresh for reader)
2. Creates new writer for time-based rotation OR size-based rotation (when `rotationRequested` is set)
3. Stages new writer in `pendingWriter`

```java
protected class LogRotationTask implements Runnable {
    @Override
    public void run() {
        if (closed.get()) {
            return;
        }

        // Close and discard undrained writer to keep files fresh for reader
        LogFileWriter undrained = pendingWriter.getAndSet(null);
        if (undrained != null) {
            closeWriter(undrained);
        }

        boolean sizeRotation = rotationRequested.compareAndSet(true, false);
        boolean timeRotation = false;
        if (!sizeRotation) {
            long now = EnvironmentEdgeManager.currentTimeMillis();
            long last = lastRotationTime.get();
            timeRotation = (now - last >= rotationTimeMs);
        }

        if (!sizeRotation && !timeRotation) {
            return;
        }

        try {
            LogFileWriter newWriter = createNewWriter();
            pendingWriter.set(newWriter);
            lastRotationTime.set(EnvironmentEdgeManager.currentTimeMillis());
            logGroup.getMetrics().incrementRotationCount();
            if (sizeRotation) {
                logGroup.getMetrics().incrementSizeBasedRotationCount();
            } else {
                logGroup.getMetrics().incrementTimeBasedRotationCount();
            }
        } catch (IOException e) {
            LOG.error("Failed to create new writer for rotation", e);
            if (sizeRotation) {
                logGroup.getMetrics().incrementRotationFailureCount();
                long numFailures = rotationFailures.incrementAndGet();
                if (numFailures >= maxRotationRetries) {
                    LOG.error("Too many rotation failures ({}/{}), closing log",
                        numFailures, maxRotationRetries);
                    closeOnError();
                }
            }
        }
    }
}
```

### Step 7: Remove `rotateLog()` method

**File:** `ReplicationLog.java` (lines 218-243)

No longer needed — size-based rotation is handled by `LogRotationTask` via `rotationRequested`.

### Step 8: Shut down `closeExecutor` in `close()` and `closeOnError()`

**File:** `ReplicationLog.java` (lines 334-357)

Add `closeExecutor` shutdown with a timeout to both methods. In `close()`, await termination so in-flight closes complete. In `closeOnError()`, shutdownNow for fast teardown. Close of `currentWriter` and any staged writer should still be synchronous during shutdown (we're tearing down, not on the hot path).

## Files to modify

1. `phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLog.java` — all changes above
2. `phoenix-core/src/test/java/org/apache/phoenix/replication/ReplicationLogGroupTest.java` — update tests for renamed task class, add test for undrained writer replacement, add test for size-based rotation via flag

## Verification

```bash
mvn test -pl phoenix-core -Dtest=ReplicationLogGroupTest
```

Key tests that must pass:
- `testTimeBasedRotation`
- `testSizeBasedRotation`
- `testRotationTask`
- `testRotationDuringBatch`
- `testFailedRotation`
- `testTooManyRotationFailures`
- `testReadAfterMultipleRotations`
- `testRuntimeExceptionDuringLengthCheck`

New tests to add:
- Test that undrained `pendingWriter` is closed and replaced by rotation task (idle system scenario)
- Test that size-based rotation signals `rotationRequested` and rotation task picks it up
- Test that `closeExecutor` closes old writers asynchronously
- Test that writer swap only happens at batch boundary (after sync), not mid-batch between appends
