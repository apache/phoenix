# Plan: Make the scheduled round-boundary rotation non-coalescible

> On execution, copy this plan to `phoenix/plans/` (git-tracked) per repo convention before starting.

## Context

Phoenix replication rotates the active `.plog` writer on two independent triggers that today
share a single `rotationRequested` CAS gate in `ReplicationLog`:

- **Size-based / fault-based** (`requestRotationIfOversized`, `apply()` retry): an *idempotent*
  intent — "ensure a fresh, non-oversized writer exists." Coalescing these is correct and
  necessary: size checks fire after every sync, and during the ~120ms window between requesting a
  rotation and the new writer being staged, `currentWriter` still reports the old over-threshold
  length, so uncoalesced requests would cascade into many tiny files. The `rotationRequested` CAS
  is what closes that window.
- **Time-based** (the `scheduleAtFixedRate` tick, `ReplicationLog.java:175`): a *non-idempotent*
  intent — "this specific round boundary must get its own file," which is the placement unit the
  round-sharded reader is organized around.

Because both share one gate, a scheduled tick that fires while a size/fault rotation holds the CAS
is **coalesced away** (dropped), so that round boundary may not get its own file. In practice the
single-threaded rotation executor makes a true drop rare and near-harmless (a dropped tick's
in-flight task usually stamps a near-boundary file anyway), so this is **not a production bug**.
The reason to fix it is **correctness-by-design**: the boundary guarantee should not depend on an
emergent single-thread timing property that a future refactor (e.g. the pipelined-flusher work, or
moving rotation off the single thread) could silently break.

**Intended outcome:** every scheduled round boundary is honored with its own rotation, while
size/fault rotations keep coalescing. At most one extra (often empty) file per boundary when a
size rotation happened to be in flight — which the reader already tolerates (size rotations
already produce multiple files per round today).

## Design

**Asymmetric coalescing, no new synchronization primitive.** Coalescing direction is the whole
point: an on-demand (size/fault) request *may* be satisfied by an in-flight rotation of any kind
(including a scheduled one — it's already minting a fresh writer), but a scheduled tick must *never*
be satisfied by an in-flight on-demand rotation — a round boundary always gets its own rotation.

This reduces to one rule on the existing `rotationRequested` gate, with **no new state and no
change to `LogRotationTask`** (its `finally` keeps clearing the gate unconditionally, as today):

- **On-demand callers use the CAS** (`compareAndSet(false, true)`): if a rotation already holds the
  gate they coalesce (as today) — now also coalescing into a scheduled rotation.
- **The scheduled tick forces the gate** with `rotationRequested.set(true)` (not CAS) and always
  enqueues its task. `set` instead of `compareAndSet` is the "bypass the CAS check": the tick is
  never turned away, and it *holds* the gate for the duration so concurrent on-demand requests
  coalesce into it.

### Changes in `phoenix-core-server/.../replication/ReplicationLog.java`

1. **`requestRotation()` takes a `scheduled` flag:**
   ```java
   private boolean requestRotation() {           // existing on-demand callers unchanged
     return requestRotation(false);
   }

   private boolean requestRotation(boolean scheduled) {
     if (logGroup.isFailoverPending()) {
       LOG.info("HAGroup {} rotation suspended: failover pending", logGroup);
       return false;
     }
     if (scheduled) {
       rotationRequested.set(true);   // force-hold: a boundary is never coalesced away, and
                                      // holding the gate makes concurrent on-demand requests
                                      // coalesce into this rotation
     } else if (!rotationRequested.compareAndSet(false, true)) {
       return true;                   // on-demand coalesces into the in-flight rotation
     }
     try {
       rotationExecutor.execute(new LogRotationTask());
     } catch (java.util.concurrent.RejectedExecutionException e) {
       LOG.info("Rotation executor shut down, skipping rotation", e);
       rotationRequested.set(false);  // nothing will run to clear it
       return false;
     }
     return true;
   }
   ```

2. **`LogRotationTask` is unchanged** — its `finally` still does `rotationRequested.set(false)`
   unconditionally (`:540`). No constructor parameter.

3. **Route the scheduled tick** (`:175`) to `() -> requestRotation(true)`.
   `apply()`/`requestRotationIfOversized`/`awaitStagedWriter` keep calling the no-arg
   `requestRotation()` (scheduled = false) — unchanged.

### Why this is correct

- **Boundary never dropped:** the scheduled tick always enqueues a task that runs on the
  single-thread FIFO executor.
- **On-demand coalesces into a scheduled rotation:** the tick holds the gate across its own
  `createNewWriter` window, so a size/fault request arriving then loses the CAS and coalesces.
- **No stuck gate:** every enqueued task clears the gate in `finally`; the shutdown path clears it
  in the `RejectedExecutionException` catch.
- **Premature-clear is benign:** when a scheduled and an on-demand task overlap, one task's
  unconditional clear can drop the gate while the other is still queued, at worst spawning one
  extra rotation. The `pendingWriter` orphan-close guard (`:518-521`) keeps only the last-staged
  writer, and the next sync re-checks size — so this only ever costs an extra (usually empty) file,
  never a lost or mis-sharded rotation.

### Why NOT a full "queue every request" model

Honoring every request would cascade the size path into many tiny files during the
create-in-flight window — exactly what the CAS filter prevents. Coalescing is the correct model for
the idempotent fresh-writer intent; only the non-idempotent boundary needs guaranteed delivery.

### Cost

At most one extra (often empty) rotation/file per boundary, and only when a size/fault rotation was
already in flight at that boundary (the orphan-closed writer). On-demand requests arriving during a
scheduled rotation now coalesce, so that direction produces fewer files than the baseline. The
reader already tolerates multiple files per round (`ReplicationLogTracker.getNewFilesForRound`
lists all and filters by timestamp window), so this is benign.

## Files to modify

- `phoenix-core-server/src/main/java/org/apache/phoenix/replication/ReplicationLog.java`
  (`requestRotation()` overload with `scheduled`; tick wiring at `:175`). `LogRotationTask` is
  unchanged.
- Doc comments to reconcile: `requestRotation` javadoc (`:243-257`) and `LogRotationTask` javadoc
  (`:500-506`) — describe the asymmetric coalescing: on-demand callers CAS-and-coalesce; the
  scheduled tick force-holds the gate and always runs, so on-demand coalesces into a scheduled
  rotation but a scheduled boundary is never coalesced away. `testRotationTaskClearsRequestedFlag`
  javadoc (`ReplicationLogGroupTest.java:2513-2516`) still says "both go through requestRotation()"
  — that stays true (both do), just note the scheduled path forces the gate rather than CAS-ing it.

## Tests

- **Add** the *concurrent* cases (currently missing):
  - a scheduled tick while a size rotation is in flight still yields its own boundary writer (mirror
    `testOnDemandRotationDoesNotSuppressScheduledTick:2014` but overlap rather than sequence, via
    `forceRotation`/`TestableLog` seams);
  - the reverse — an oversized check while a scheduled rotation holds the gate coalesces (no extra
    writer produced for the size request).
- **Verify unchanged:** `testRotationTaskClearsRequestedFlag:2518` (task untouched),
  `testSizeRotationDoesNotLoopOnReplay:2459` — the ≤10 writer bound now also absorbs unconditional
  scheduled ticks; confirm empirically it stays under (short round → few ticks; loosen the bound
  only if a real run exceeds it), `testOnDemandRotationDoesNotSuppressScheduledTick`,
  `testRotationScheduleAlignsWithRoundBoundary:2061`.

## Verification

1. `mvn spotless:apply`
2. Unit: `mvn test -pl phoenix-core -Dtest=ReplicationLogGroupTest,ReplicationLogTest`
3. Build server jar for ITs: `mvn install -pl phoenix-core-server -DskipTests`
4. IT (rotation/cutover file-count behavior): `mvn verify -pl phoenix-core -Dit.test=ReplicationLogGroupIT`
5. Confirm no new "boundary file closed late" behavior and rotation-count metrics remain sane.
