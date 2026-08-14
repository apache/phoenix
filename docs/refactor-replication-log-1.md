# Fix Log Rotation Locking in ReplicationLog.java

## Context

`ReplicationLog.getWriter()` acquires a `ReentrantLock` on every call from the disruptor consumer thread's hot write path. When `LogRotationTask` (background thread) triggers time-based rotation, it holds this same lock during expensive HDFS operations (file create, header write, sync, trailer write, close), stalling all write RPC processing through the disruptor.

## Recommended Approach: Two-Phase Rotation with Lock-Free Fast Path

Restructure `rotateLog()` to move expensive HDFS I/O outside the lock and make `getWriter()` lock-free. Changes are contained within `ReplicationLog` — no new event types, no cross-layer plumbing.

### Step 1: Make `LogFileWriter.closed` volatile

**File:** `LogFileWriter.java` (line 38)

Change `private boolean closed` to `private volatile boolean closed` so the disruptor thread sees the close state when the old writer is closed by `LogRotationTask` after the swap.

### Step 2: Make `getWriter()` lock-free

**File:** `ReplicationLog.java` (lines 178-188)

Remove the lock entirely. `getWriter()` is only called from the single disruptor consumer thread via `apply()`. `currentWriter` is already `volatile`, and `shouldRotate()` reads only volatile/atomic state plus `FSDataOutputStream.getPos()` (local counter, not an RPC). Update `shouldRotate()` Javadoc to remove "Must be called under lock".

### Step 3: Restructure `rotateLog()` into three phases

**File:** `ReplicationLog.java` (lines 243-285)

```
Phase 1 (no lock): Capture expectedOldWriter = currentWriter. Create new writer via createNewWriter() — expensive HDFS ops.
Phase 2 (lock held): If currentWriter == expectedOldWriter, swap pointer + update timestamps/metrics — no I/O. Otherwise discard new writer (concurrent rotation already happened).
Phase 3 (no lock): Close old writer — expensive HDFS ops.
```

### Step 4: Simplify `LogRotationTask`

**File:** `ReplicationLog.java` (lines 421-460)

Remove the `tryLock` mechanism. The time check reads `lastRotationTime` (AtomicLong) and calls `rotateLog()` which handles its own brief locking internally.

### Thread Safety Analysis

| Concern | Handling |
|---|---|
| LogRotationTask closes old writer while disruptor thread still has a reference | Already exists today. `apply()` catches IOException, calls `rotateLog(ERROR)`, retries. Making `LogFileWriter.closed` volatile improves detection. |
| Both threads call `rotateLog()` simultaneously | Compare-and-swap pattern: loser detects `currentWriter` changed, closes its orphaned writer, returns current. |
| `shouldRotate()` reads stale position after swap | Harmless: next call sees correct value; worst case is one extra batch before rotation triggers. |

### Files to Modify

1. `phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLog.java` — main changes
2. `phoenix-core-server/src/main/java/org/apache/phoenix/replication/log/LogFileWriter.java` — make `closed` volatile

## Testing

Existing tests to verify (all must pass):
- `ReplicationLogGroupTest.testTimeBasedRotation`
- `ReplicationLogGroupTest.testSizeBasedRotation`
- `ReplicationLogGroupTest.testRotationTask`
- `ReplicationLogGroupTest.testRotationDuringBatch`
- `ReplicationLogGroupTest.testFailedRotation`
- `ReplicationLogGroupTest.testTooManyRotationFailures`
- `ReplicationLogGroupTest.testReadAfterMultipleRotations`

Run: `mvn test -pl phoenix-core -Dtest=ReplicationLogGroupTest`

## Verification

1. All existing unit tests pass
2. Confirm lock is only held for pointer swap (no HDFS I/O under lock)
3. Confirm `closeWriter(newWriter)` is called in the concurrent-rotation discard path to prevent HDFS file leaks

---

## Rejected Alternative: ROTATE Event via Disruptor Ring Buffer

Explored introducing `EVENT_TYPE_ROTATE` where `LogRotationTask` pre-creates the new writer and publishes a ROTATE event to the ring buffer, with the consumer thread doing only the fast pointer swap.

**Why rejected:**

1. **Mode switching race**: The ring buffer is shared across all replication modes. `LogRotationTask` pre-creates a writer for mode A's `ReplicationLog`. By the time the ROTATE event is consumed, a mode switch (e.g., SYNC → STORE_AND_FORWARD) may have already occurred via `onFailure()`. The `currentModeImpl` now points to mode B with a completely different `ReplicationLog` — the pre-created writer belongs to the wrong log.

2. **Abstraction leak**: `LogFileWriter` is an internal detail of `ReplicationLog`. Routing it through `LogEvent` → `LogEventHandler` → `ReplicationModeImpl` → `ReplicationLog` means `ReplicationLogGroup` now knows about writer internals it shouldn't. Rotation is a concern of `ReplicationLog`, not the event handler layer.

The two-phase approach keeps rotation self-contained within `ReplicationLog` and avoids both problems.
