# Refactor ReplicationLog Rotation: pendingWriter Design

## Context

The current rotation design has a fundamental cross-thread problem: `TimeBasedLogRotationTask` (background thread) calls `rotateLog()` which closes the old writer in Phase 3. If the disruptor consumer thread is mid-write on that writer, it gets an IOException and must retry — causing unnecessary error-based rotations and wasted work every 60 seconds.

The fix: background thread only creates new writers and stages them. The disruptor thread owns the full swap lifecycle (close old, swap to new). Coordination via a single `AtomicReference`.

---

## Design

### Fields

```java
// ADD:
private final AtomicReference<LogFileWriter> pendingWriter = new AtomicReference<>();
private final AtomicBoolean closed = new AtomicBoolean(false);

// REMOVE:
// - pendingClose field and closePendingWriter() method
// - ReentrantLock lock (no longer needed — rotateLog doesn't use it, close/closeOnError use AtomicBoolean)
```

### TimeBasedLogRotationTask.run()

Background thread creates a writer, stages it, and updates rotation time/metrics at staging time (when the rotation decision is made).

```java
public void run() {
    if (closed.get()) return;
    long now = EnvironmentEdgeManager.currentTimeMillis();
    long last = lastRotationTime.get();
    if (now - last < rotationTimeMs) return;
    if (pendingWriter.get() != null) return; // already staged, don't create another
    try {
        LogFileWriter newWriter = createNewWriter();
        if (pendingWriter.compareAndSet(null, newWriter)) {
            lastRotationTime.set(EnvironmentEdgeManager.currentTimeMillis());
            logGroup.getMetrics().incrementRotationCount();
            logGroup.getMetrics().incrementTimeBasedRotationCount();
        } else {
            closeWriter(newWriter); // lost race, discard
        }
    } catch (IOException e) {
        LOG.error("Failed to create new writer for time-based rotation", e);
    }
}
```

### getWriter()

Disruptor thread drains pendingWriter, does swap+close. No metrics or timestamp here — those belong to the rotation decision point.

```java
protected LogFileWriter getWriter() throws IOException {
    LogFileWriter newWriter = pendingWriter.getAndSet(null);
    if (newWriter != null) {
        LogFileWriter oldWriter = currentWriter;
        currentWriter = newWriter;
        closeWriter(oldWriter);
    }
    if (shouldRotateForSize()) {
        rotateLog();
    }
    return currentWriter;
}
```

### rotateLog()

Only called from disruptor thread for SIZE-based rotation. No lock needed. No concurrent rotation detection needed.

```java
protected LogFileWriter rotateLog() throws IOException {
    LogFileWriter newWriter;
    try {
        newWriter = createNewWriter();
    } catch (IOException e) {
        logGroup.getMetrics().incrementRotationFailureCount();
        long numFailures = rotationFailures.getAndIncrement();
        if (numFailures >= maxRotationRetries) {
            closeOnError();
            throw e;
        }
        return currentWriter;
    }
    LogFileWriter oldWriter = currentWriter;
    currentWriter = newWriter;
    lastRotationTime.set(EnvironmentEdgeManager.currentTimeMillis());
    rotationFailures.set(0);
    logGroup.getMetrics().incrementRotationCount();
    logGroup.getMetrics().incrementSizeBasedRotationCount();
    closeWriter(oldWriter);
    return currentWriter;
}
```

Note: `rotateLog` is only called from the disruptor thread, so closing `oldWriter` here is safe — the disruptor is the one using it and it's done with it before calling rotateLog.

### apply() — KEEP generation check + replay, NO error rotation

On IOException, retry with the same writer. If all retries exhausted, fail-stop. No new writer creation on a broken HDFS (if HDFS is degraded, creating a new writer will likely fail too).

```java
private void apply(Action action) throws IOException {
    LogFileWriter writer = getWriter();
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
        if (isClosed()) {
            throw new IOException("Closed");
        }
        try {
            if (writer.getGeneration() > generation) {
                generation = writer.getGeneration();
                if (!currentBatch.isEmpty()) {
                    LOG.trace("Writer has been rotated, replaying in-flight batch");
                    for (Record r : currentBatch) {
                        writer.append(r.tableName, r.commitId, r.mutation);
                    }
                }
            }
            action.action(writer);
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

The replay protects against the rare case where `closeWriter(oldWriter)` fails and unsynced data is lost.

### close() / closeOnError()

Replace `lock` + `volatile boolean closed` with `AtomicBoolean`:

```java
protected void closeOnError() {
    if (!closed.compareAndSet(false, true)) {
        return;
    }
    stopRotationExecutor();
    LogFileWriter staged = pendingWriter.getAndSet(null);
    if (staged != null) {
        closeWriter(staged);
    }
    closeWriter(currentWriter);
}

public void close() {
    if (!closed.compareAndSet(false, true)) {
        return;
    }
    stopRotationExecutor();
    LogFileWriter staged = pendingWriter.getAndSet(null);
    if (staged != null) {
        closeWriter(staged);
    }
    closeWriter(currentWriter);
}

public boolean isClosed() {
    return closed.get();
}
```

---

## Edge Cases Verified

| Scenario | Outcome |
|----------|---------|
| Rotation between append and sync | Old file closed (data flushed). Replay into new writer ensures safety net. |
| Rotation during sync | Old file closed (data flushed). Sync on new writer is harmless. |
| Idle (no writes after staging) | Old writer stays open (reader handles in-progress files). Next timer tick skips since pendingWriter != null. |
| Size rotation while pendingWriter staged | getWriter() drains pendingWriter first, then triggers size rotation. Both close cleanly. |
| Error in apply() with unsynced batch | Retries with same writer. If all retries fail, closeOnError() fail-stops. Replay on next writer (from time rotation) ensures safety net. |
| Background writer creation fails | Logs error, pendingWriter stays null. System continues with current writer. |
| close()/closeOnError() while pendingWriter staged | Staged writer drained and closed. No leaks. |

---

## What Gets Removed

- `pendingClose` field and `closePendingWriter()` method
- `ReentrantLock lock` field (replaced by `AtomicBoolean closed`)
- `volatile boolean closed` field (replaced by `AtomicBoolean closed`)
- `RotationReason` enum entirely (TIME handled by pendingWriter staging, SIZE handled inline, ERROR removed)
- Lock acquisition in `rotateLog()` (Phase 2 lock)
- Concurrent rotation detection (`currentWriter != expectedOldWriter` check)
- Error-based rotation (`rotateLog(ERROR)` in apply's catch block)
- `RotationReason` parameter on `rotateLog()` (only SIZE remains, no need for a parameter)
- `import java.util.concurrent.locks.ReentrantLock`

## What Stays

- `generation` field and generation check in `apply()`
- `currentBatch` replay logic (safety net for failed close)
- Retry loop in `apply()` (retries with same writer, not a new one)

---

## Files to Modify

1. `phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLog.java` — all changes above
2. `phoenix-core/src/test/java/org/apache/phoenix/replication/ReplicationLogGroupTest.java` — update test comment referencing `shouldRotateForSize()`

---

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
- `testRuntimeExceptionDuringLengthCheck`
