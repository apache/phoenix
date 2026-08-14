Exactly right — that's the cleaner analysis than the one I stumbled toward. Let me restate it precisely so we're on the same page before I touch the plan:

### The full picture under Option A (roll writer + propagate, no abort on IOException)

**Timeline of the failing-then-succeeding mutation:**

1. Client sends Put at time `T1`.
2. RS: `preBatchMutate` assigns timestamp `T1` to the cells.
3. RS: HBase WAL append + sync → **WAL entry #1 at timestamp `T1` is durable locally**.
4. RS: SAF sync throws IOException.
5. `apply()` requests writer rotation, propagates IOException.
6. `LogEventHandler.onFailure` → fatal path → `failPendingSyncs` → client sees IOException. **No abort.**
7. Client retries the same logical Put at time `T2`.
8. RS: `preBatchMutate` assigns fresh timestamp `T2`.
9. RS: HBase WAL append + sync → **WAL entry #2 at timestamp `T2` is durable locally**.
10. RS: SAF sync succeeds on the rotated writer → **Phoenix replication entry at `T2` lands in SAF queue**.
11. Client gets success. SAF forwarder ships the `T2` entry to the standby.

**Final state:**
- **HBase WAL (local)**: two entries — `T1` and `T2` — both applied to MemStore. Both visible to reads on the active.
- **Phoenix replication log (standby-bound)**: one entry — only `T2`. The `T1` entry is orphaned from the Phoenix replication pipeline.
- **Standby (after SAF forwarder drains)**: has only the `T2` cell.

**Is this a correctness issue?**

Your point: both WAL entries represent "the same row write with the same user-intent values" — the retry just re-submitted the identical Put with a later timestamp. On the active, HBase has two versions of the cell; on reads, max-version semantics return the most recent (`T2`). On the standby, only `T2` exists.

When the active's MemStore flushes → HFile has both `T1` and `T2`. After max-lookback expiration, compaction will discard the older `T1` version. So the active eventually has only `T2` too — matching the standby.

**For failover scenarios:**
- Automatic failover is blocked while in `DegradedStandby` (same as before), so this is not a concern.
- After SAF drains and we return to `Standby`, both sides have `T2` (the active's `T1` is still there until compaction, but the **user-visible state** — latest cell per row — matches).
- If a failover happens after drain but before active's `T1` is compacted away, the old-active-now-standby has `T1 + T2` and the new-active-old-standby has just `T2`. They agree on the latest cell. When the old active becomes standby, future writes from the new active will flow through replication normally. The `T1` orphan on the old active is irrelevant for correctness.

**When would this actually break?**

Only if the user's Put was **not idempotent** at the cell level — e.g., if the user was doing an Increment. But Phoenix replication handles Increments via the mutation record; the IOException on the first attempt would mean the first Increment *did* run locally (MemStore already has it) but the standby never gets the `T1` version. A client retry lands a **second** Increment at `T2`, and the standby only sees the `T2` increment. Active has total = `initial + 2×delta`; standby has `initial + 1×delta` after failover. That would be a real divergence.

But — HBase doesn't solve this either! HBase's same log-roll-and-retry pattern would produce the same double-increment on the active vs. single-increment on the standby. It's a well-known caveat of "let the client retry" fault tolerance models for non-idempotent operations.

### What the design doc actually says

Going back to the "If in some rare circumstance both Phoenix replication log write methods fail, we can abort the regionserver" line — that's described as one **option**, not a mandate. The doc says "we can abort" specifically to keep `preWALRestore` as a recovery mechanism. The implicit assumption was "orphaned WAL entries need to be shipped." Under your analysis, they don't need to be, because the client retry produces a functionally-equivalent (for Put/Delete) or HBase-equivalent-caveat (for Increment/Append) replicated record.

### Proposal for the plan

If you agree with the analysis:

- **SAF IOException**: request rotation, propagate IOException, **no abort**. Client retries. Fresh writer absorbs the retry.
- **SYNC IOException**: flip to SAF, replay on SAF writer, **if SAF replay succeeds** → client sees success. **If SAF replay also fails IOException** → request rotation + propagate, no abort. Client retries.
- **Abort triggers** — only for the 5-min `syncFuture.get()` timeout (item 2b), which represents "stuck for minutes, something is actually wrong" rather than "transient blip."
- **`StoreAndForwardModeImpl.onFailure()`**: change to **not** call `logGroup.abort()`. Just throw/propagate. (This is the 2a-bis change, but in the direction I originally had — then corrected — then re-flipped based on your clearer framing.)

This matches HBase's availability stance, accepts duplicate-WAL-entry-tolerated-by-idempotency, and reserves abort for genuine stuckness.

### Questions before I touch the plan

1. Is Option A what you want?
2. If yes, do you want item 2a-bis to change `StoreAndForwardModeImpl.onFailure()` to **not abort** (so SAF IOException never aborts)?
3. Any concerns about the Increment/Append caveat that I should document explicitly as a known limitation?

I won't edit the plan until you confirm the shape.
