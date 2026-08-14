# Root-Cause Analysis — Multi-Second PreIndexUpdateTime on FEED_COMPOSITION

**Status:** mechanism established; one sub-question (Phase-A meta-scan latency) pending live
`INDEXLOC` logging. First build deployed 2026-07-10; an **enriched build** (adds `useCache` on
every lock-path line + `MetaCache.getCachedLocation` miss lines) committed 2026-07-11 to settle
the sustained-single-region-miss puzzle (§6.1). Data expected ~2026-07-13.
**Cluster:** dev-phoenix-hbase5a (falcon `dev1-uswest2`, functional domain `core002`)
**Table:** `PHSCEX.FEED_COMPOSITION` + its two global indexes
(`..._LAST_UPDATE`, `..._CREATED_DATE`)
**Symptom:** data-region `PreIndexUpdateTime` / `putBatch` reaches multiple seconds
(July-4: 10–37 s on rs-24/rs-26; July-9/rs-15 and July-10/rs-5: p95 4–5 s sustained ~15 min)
while the index tables' own writes commit in single-digit ms.

---

## 1. Executive summary

`PreIndexUpdateTime` (measured in `IndexRegionObserver.doPre`, timing only `preWriter.write`)
balloons to seconds **not** because index-region writes are slow, and **not** because of
post-split compaction. It balloons because the outbound index write must first **locate the
target index region**, and on a busy RegionServer those location lookups **serialize through a
single non-fair `userRegionLock`** on the shared index-writer connection. Under a high miss
rate the lock develops a standing queue and individual lookups are starved for **seconds**.

**CONFIRMED ROOT CAUSE (2026-07-13, rs-12 enriched `INDEXLOC` burst — see §4.6):** the high
miss rate is a **poisoned MetaCache**. Stale index regions from a *previous table incarnation*
(drop/recreate-same-name) remain resident in the long-lived per-RS index-writer cache with no
TTL. The freshly-recreated index is a single `[EMPTY, EMPTY]` region whose startKey (`byte[0]`)
sorts **first** in the per-table map — so `floorEntry(row)` **always** returns a stale
non-empty-startKey sibling instead, and the fresh region is **never selectable**. Every lookup
therefore misses, re-scans meta, and re-caches a region it can never hit again → a **permanent
100% miss rate** that saturates the single lock. This is invisible to every exception-based
signal (writes still succeed on the correct region returned by the scan, so nothing ever throws
to evict the zombie). This is the "poisoned cache" hypothesis raised early and wrongly rejected
(§8) — the harm is a silent miss-storm, **not** a stale-hit-to-dead-region.

The two phases below are the earlier framing (still real as secondary miss sources) but are
**not** the dominant mechanism; §4.6 supersedes them for the FEED_COMPOSITION perf runs.

- **Phase A — cold-cache warming.** A freshly-recruited RS (one that just opened a split
  DATA daughter) has a cold index-writer MetaCache and must first-touch every index region it
  writes to. Genuine first-attempt misses, on validly-assigned regions, **no exception**.
- **Phase B — assignment-gap re-scans.** During index-split waves, daughters are committed to
  `hbase:meta` with a null serverName for ~0.4–2.9 s. Writes to that key range throw
  `NoServerForRegionException` **before** the location is cached, so every attempt re-scans
  meta for the whole gap.

Both funnel through the **same single per-RS lock**; they differ only in what generates the
miss. That is why the fix is the same for both.

---

## 2. What PreIndexUpdateTime actually measures

`IndexRegionObserver.doPre` (phoenix-core-server) times **only** the outbound index write
(`preWriter.write`). It does **not** include the index region's server-side write. So an
elevated `PreIndexUpdateTime` localized to a subset of RSes cannot be caused by the index
regions themselves (that would be cluster-wide) — it is a **sending-side, per-RS** cost.

The index writes fan out through `TrackingParallelWriterIndexCommitter` — one `Task` per index
table, each doing `table.batch(mutations, null)` on the shared index-writer `Connection`. The
wait is `Futures.successfulAsList` (`WaitForCompletionTaskRunner`), i.e. doPre waits for the
**slowest** index table.

---

## 3. The single-lock bottleneck

There is **one index-writer `Connection` per ConnectionType per RegionServer** → one
`MetaCache` → one `userRegionLock` (`ReentrantLock`, **non-fair**). All index-write region
lookups on that RS serialize through it.

Critically, the lock is taken **only on a cache miss**
(`ConnectionImplementation.locateRegionInMeta`):

- pre-lock cache check (line 983–986): a **hit returns lock-free** — steady state is
  uncontended.
- on miss: `takeUserRegionLock()` (999) → recheck under lock (1006–1009) → meta scan
  (1017–1021) → `cacheLocation` (1068) → return.

`MetaCache` is **unbounded, no TTL, error-driven eviction only** — entries leave *only* via a
`clearCache` triggered by a meta-clearing exception (`NotServingRegionException`,
`RegionOpeningException`). `RegionMovedException` updates in place; `RegionTooBusy` /
`RpcThrottling` are not meta-clearing.

---

## 4. Evidence

### 4.1 jstack signature (rs-15 July-9; rs-5 July-10, 10 dumps 03:05:22–03:10:17)

Every dump shows the identical shape:

- **11–16 threads** parked on the **same** lock object (July-10:
  `<0x00007ef66dd7b2b8>`, `ReentrantLock$NonfairSync`) in `takeUserRegionLock`.
- **exactly one** holder progressed into the meta scan
  (`locateRegionInMeta:1023 → ClientScanner → ScannerCallableWithReplicas`).
- `indexBatchRPC = 0` — **no threads in `table.batch` RPC** (rules out slow index writes).
- `retrySleep = 0`, no `getPauseTime` frames (rules out retry-backoff sleeps during the
  plateau).

Holder full stack (July-10 rs_5): the holder is inside `AsyncProcess.submit → submitAll →
groupAndSendMultiAction → findAllLocationsOrFail → locateRegion → locateRegionInMeta` — i.e.
**region location, before any mutation is sent.** `HTable.batch` has sent nothing.

### 4.2 The queue starves individual threads for seconds (measured, not inferred)

Across rs_1/rs_2/rs_3 (03:05:22 → 03:05:30, ~8 s apart), **4 waiter thread-ids are parked on
the lock in all three dumps** → those threads waited **> 8 s** for a single location lookup;
7 persisted across the first 3 s pair. This is a direct duration measurement from
parked-waiter overlap (no length-bias/sampling fallacy).

Meanwhile the **holder turns over every dump** (10 distinct holder threads, none repeated) →
many scans completed during those 8 s, yet the same 4 threads never won the lock. That is the
signature of a **fast-but-saturated non-fair lock** (barging newcomers starve the queue),
**not** a few slow scans.

> Caveat recorded honestly: a thread dump catching the holder mid-scan does **not** by itself
> prove the scan is slow (length-biased sampling). Whether Phase-A scans are genuinely slow
> (meta contention) or fast-but-saturated is the one open question §6.

### 4.3 Phase-B assignment gap — proven by region-id vs log-time

`NoServerForRegionException` is thrown at `locateRegionInMeta:1058-1061`, **before**
`cacheLocation` (1068) → the region **never caches** → retries re-scan for the whole gap.

rs-5 NSRE log (03:10–03:36). Each region's encoded name embeds its creation timestamp
(region-id); decoding it against the log time it was thrown:

| Index region (id) | created (from id) | first NSRE | gap |
|---|---|---|---|
| CREATED_DATE `7f476f196aa8` (1783653044344) | 03:10:44.344 | 03:10:45.108 | ~0.8 s |
| LAST_UPDATE `99868a44b780` (1783653050091) | 03:10:50.091 | 03:10:50.804 | ~0.7 s |
| LAST_UPDATE `9a071269d1a9` (1783653849389) | 03:24:09.389 | 03:24:10.57 | ~1.2 s |
| CREATED_DATE `4c4a023c0272` (1783653850067) | 03:24:10.067 | 03:24:11.78 | ~1.7 s |
| CREATED_DATE `0b660da9b080` (1783654590338) | 03:36:30.338 | 03:36:31.69 | ~1.4 s |

Every NSRE is for a region **created ~0.7–1.7 s earlier** — a freshly-split daughter caught in
its assignment gap. At 03:10:50.804–.824, **8 NSRE in 20 ms all for the same region**
`99868a44b780` — the pool-wide herd converging on one gap-stuck region. Bursts recur at
**03:10 / 03:24 / 03:36** = generational split waves.

### 4.4 The two phases are temporally distinct

- The dump-window queue (03:05:22–03:10:17) exists with **zero NSRE on rs-5 before 03:10** and
  **all frames first-attempt, `useCache=true`, no exception of any kind.**
- No NSRE before 03:10 ⇒ no assignment gaps in the dump window ⇒ no fresh split-daughters
  being born mid-window ⇒ the dump-window misses are **cold first-touch on validly-assigned
  regions** (Phase A), categorically separate from the 03:10+ NSRE herd (Phase B).

This also matches July-9: dumps 22:45–22:49 had zero in-window splits; NSRE/splits clustered
at 23:04+.

### 4.5 The rs-9 burst — sustained misses on a *single* covering region (unexplained)

The July-11 rs-9 `INDEXLOC` dump (first-build jar) shows a regime the Phase-A/B split does not
fully account for, and it is what motivated the enriched build (§6.1):

- **Fan-in topology confirmed.** Many data regions (rs-1, rs-9, …) write index updates into a
  **single** index region per index — `20f327164cbc` (CREATED_DATE) and `0b3419d7ff36`
  (LAST_UPDATE), both `[EMPTY,EMPTY]` and both hosted on rs-0. A fresh global index starts life
  as one all-encompassing region = a write hotspot and a single cache key.
- **31,060 scan lines, all on those two region hashes**, split ~50/50 across the two indexes;
  `lockWaitMs≥1000` on 515 of them, `100–999` on 1,653 — the lock-saturation harm, sustained.
- **Zero herd, zero noserver, zero evictions, zero splits, zero NSRE in-window.** Every lookup
  goes all the way to a meta scan (`metaScanMs>0`); none finds the entry already warmed on the
  post-lock recheck.

That last point is the puzzle. For a **single `[EMPTY,EMPTY]` region**, the first scan should
cache an entry that covers *every* row, so subsequent lookups should hit the lock-free
fast-path — yet 31 K consecutive lookups all miss and scan. With `useCache=true` and one
covering region, "always scan" should be impossible; one of {cache disabled, entry absent,
entry mismatched} must hold, and the first-build lines cannot say which. See §6.1.

> **Resolved by §4.6.** The premise "single covering region" was the error: the map also holds
> **stale prior-incarnation siblings** with non-empty startKeys. The `[EMPTY,EMPTY]` region is
> real but **unreachable** by `floorEntry` because a stale sibling always sorts closer to the
> row. rs-9 (first build) could not see the stale entries; rs-12 (enriched) prints them.

### 4.6 rs-12 enriched burst — CONFIRMED root cause: poisoned cache, `floorEntry` shadowing

The 2026-07-13 rs-12 dump is from the **enriched build** (`useCache` on every lock-path line +
`INDEXLOC miss` lines from `MetaCache.getCachedLocation` with row + floor `[startKey,endKey]` +
`mapSize`). Parsed by `~/index-delay/analyze_indexloc.py rs-12/rs-12.csv`:

- **Branch A dead, empirically.** `useCache=true` on all 1,407 scan lines (0 false). The
  cache is never disabled — confirms the code trace (the `table.batch` path passes
  `useCache=true` on submit **and** on every retry via `resubmit → groupAndSendMultiAction →
  findAllLocationsOrFail(action, true)`, `AsyncRequestFutureImpl.java:452/545,883`; the only
  `false`, `:519`, is gated on a null-serverName replica and never fired — `noserver=0`).
- **100% of misses are `floor-rejected`** (4,254/4,254; zero `floor=null`). The candidate entry
  *is* in the map; `getCachedLocation` selects it by startKey then rejects it on the endKey
  check (`MetaCache.java:98-100,112`). **The endKey check is not the bug — it is correct** (row
  `1003` genuinely is past a region ending `1002`). The bug is upstream: `getCachedLocation` uses
  a **single** `floorEntry(row)` candidate with **no fallback** to the next-lower entry
  (`MetaCache.java:77` — there is no loop). So when the stale sibling is picked and correctly
  rejected, the method returns null instead of falling back to the `[EMPTY,EMPTY]` region that
  actually covers the row.
- **`mapSize` = 8 and 16, not 1.** The per-table map holds many entries — the fresh index
  region **plus stale prior-incarnation siblings**.
- **Scans are fast: `metaScanMs` 1–4 ms** (one lone 48). This **answers the §6 open question**:
  the plateau is **pure lock saturation from a 100% miss rate**, not slow meta scans.
- **Zero evictions in-window** — the zombie is never cleared.

**The keys prove the mechanism.** A representative CREATED_DATE miss (4,234 of 4,254 share the
identical floor entry):

| | key |
|---|---|
| lookup `row` | `ORG…1003…CON…1331U…` |
| stale floor `startKey` | `ORG…1001NET…1019…` (≤ row → `floorEntry` picks it) |
| stale floor `endKey` | `ORG…1002…1258…` (**≤ row** → correctly rejected → miss) |
| scan always returns | region `82192ab7…` (1,400/1,400) — the real, current region |

The real index region is (per the recreate lineage) a single **`[EMPTY, EMPTY]`** region. Its
startKey is `byte[0]`, which sorts **first** in the `CopyOnWriteArrayMap`
(`Bytes.BYTES_COMPARATOR`). `floorEntry(row)` returns the **greatest** startKey ≤ row, so for
any row that a stale non-empty-startKey sibling precedes (all of them — `ORG…1001`, `ORG…1002`),
`floorEntry` returns the **stale sibling**. Its endKey check then correctly rejects it (the row
really is past `1002`) — but `getCachedLocation` has **no fallback**: it returns null on that one
rejection rather than trying the next-lower entry. The `[EMPTY,EMPTY]` region's entry is thus
**never selected**, and its `EMPTY` endKey — which would guarantee a hit at `MetaCache.java:99` —
is never examined. Re-scanning and re-caching the real region 1,400× changes nothing: it never
wins the single-candidate floor lookup.

```
keyspace →
  Start_real = EMPTY ───────────────────────────────────── End_real = EMPTY  (fresh region)
              Start_stale(1001) ── End_stale(1002)
                                          row(1003) ●   ← floorEntry picks stale, rejects, misses
```

**Why nothing evicts the zombie.** The write is grouped using the **scan result** (the correct
`82192ab7…`), so the RPC goes to the right server and **succeeds**. The stale region's server is
never contacted → no `NotServingRegion`/`RegionMoved` → `updateCachedLocations` never calls
`clearCache`. MetaCache has no TTL and no size bound (`MetaCache.java:52-58`). The stale entry is
immortal. This is exactly why every exception-based signal showed nothing (NSRE ≈ 4/h,
`noserver=0`) — the poison harms via **miss**, never via a bad hit.

**Why `cleanProblematicOverlappedRegions` doesn't remove it** (`MetaCacheUtil.java:61-88`,
HBASE-27650). It runs only inside `cacheLocation`, and only when the insert actually changes the
map: on the first insert of a new startKey (`MetaCache.java:145-151`) or on a changed
`updateLocation` (`:167-169`). Every re-scan of `82192ab7…` finds it **already present and
unchanged** → `oldLocations == updatedLocations` → the clean at `:169` is **skipped**. And the
clean walks *downward* from the new region's endKey and **breaks at the new region's own
startKey** (`MetaCacheUtil.java:69-73`), so it can only sweep entries whose startKey lies in
`(Start_new, End_new)`. When the stale sibling was inserted, its own clean covers only
`(Start_stale, End_stale)` — a range that **excludes** the `[EMPTY,EMPTY]` region (whose startKey
is below `Start_stale`). Neither region's clean can remove the other; they coexist permanently.
(The exact insertion order that lands stale-after-fresh is shown by its *effect* here, not a
timestamp; the rs-12 heap dump's region-ids would confirm the incarnation order directly.)

**Confirmed region identity + the deeper anomaly (§6.2).** The real region `82192ab7…` is
confirmed `[EMPTY, EMPTY]` from the RS open log:
`PHSCEX…CREATED_DATE,,1783969837470.82192ab71bb647220c4c911996d4aba3.  STARTKEY => '', ENDKEY => ''`
(opened 19:10:37 on regionserver-7, region-id `1783969837470`). This is significant because
caching a `[EMPTY,EMPTY]` region is **supposed to self-heal the poison**: with endKey `EMPTY`,
`cleanProblematicOverlappedRegions` takes the `isLast = true` branch (`MetaCacheUtil.java:65`),
walks `cache.lastEntry()` downward and **removes every non-empty-startKey sibling** (`:69-73`).
We *observed this working* at rs-9 (the 17:06 "MetaCacheUtil - Removing" lines wiped 5
prior-incarnation siblings on exactly this trigger). Yet at rs-12 `mapSize` stays at 8/16 with
the siblings persisting, and meta holds only the fresh region so nothing re-adds them. **The
self-heal that should fire on every cache of `82192ab7…` is not firing.** §6.2 (now RESOLVED)
explains why: on the startKey collision, `cacheLocation` takes the range-blind seqNum merge and
the fresh region (openSeqNum **2**, RS log) loses to the stale occupant (seqNum **94284**, heap
dump), so the `[EMPTY,EMPTY]` region never lands and the wipe never fires. Real HBase defect;
prod trigger is `TRUNCATE` (and DROP+CREATE), **not** split/merge or snapshot-restore.

**Net cost.** Every write to any row preceded by a stale sibling misses → takes the single
non-fair `userRegionLock` in `locateRegionInMeta` (`ConnectionImplementation.java:~1005`) →
scans meta (1–4 ms) → re-caches (no-op) → misses again. 86 preWriter threads at a 100% miss rate
build a standing lock queue (jstack: 14 waiters parked, holders at `:1005`, `indexBatchRPC=0`);
individual `PreIndexUpdateTime` stretches to seconds. Scans are cheap; the **saturation** is the
cost.

---

## 5. Necessary vs sufficient

- **Necessary:** a newly-recruited RS with a **cold** (or otherwise un-warmed) index-writer
  MetaCache — the only way to get a burst of genuine misses. An already-warm RS hits the
  lock-free path and never queues, regardless of how hot its data daughter is. Confirmed via
  the DATA-table assignment lineage: elevated RSes (rs-24, rs-26 July-4; rs-15 July-9; rs-5
  July-10) each hosted a freshly-opened DATA daughter, i.e. had just begun index-writing.
- **NOT sufficient by itself:** a cold cache over a *small* region set warms in one scan per
  region and drains in < 1 s, once — invisible. Most fresh-table runs do not plateau.
- **Sufficient** requires the misses to be *expensive* and *sustained*:
  - **Phase A:** the cold set is large (RS newly recruited into an already-well-split index),
    and 256 preWriter threads first-touch it through one lock; **and/or** the meta scans are
    themselves slow (cluster-wide cold-warming after drop/recreate hammering one meta region).
  - **Phase B:** index-split daughters land in **seconds-long** assignment gaps (loaded
    master), so each miss re-scans for the whole gap instead of caching after one scan.

The variable that makes it intermittent is therefore **assignment-gap duration / meta-scan
latency during the write burst** — load-dependent, hence run-to-run variance. This matches the
observed write-phase durations for the paired perf runs (second-of-pair: 47 → 75 → 73 →
**113 min** across Run1–Run4 on 0709).

### Perf-workload amplifier: drop/recreate with same name

The perf harness **drops and recreates** the tables/indexes with the same name between
iterations **without restarting RegionServers**. MetaCache is per-Connection, in-process, with
no cross-connection invalidation and no TTL — so a newly-recruited RS is genuinely cold every
iteration (guaranteeing the Phase-A miss burst), and every RS re-locates the fresh index
region set against the one meta region near-simultaneously (the leading hypothesis for slow
Phase-A scans). This is a real production-shaped condition (single-root split lineage confirms
fresh tables), not merely a test artifact.

---

## 6. Open questions

**6.0 [RESOLVED] Are Phase-A meta scans genuinely slow, or fast-but-saturated?**
**Answer: fast-but-saturated.** rs-12 (§4.6) shows `metaScanMs` = 1–4 ms across the burst (one
lone 48). It is **pure lock saturation** driven by a 100% miss rate; meta-region contention is
not the floor. Only lock contention / miss volume matter — which the root cause (§4.6) explains.

The jstacks cannot answer this (length-bias). A temporary diagnostic patch was added to
`hbase-client` `ConnectionImplementation.locateRegionInMeta` (committed 2026-07-10; **revert
after investigation**) logging every lock-path lookup for any table whose name contains
`FEED_COMPOSITION`:

- `INDEXLOC scan` — genuine meta scan → `metaScanMs` (**the discriminator**), distinct
  `region=` count = Phase-A cold-set size.
- `INDEXLOC herd` — waited for lock, found it already warmed → recheck-herd volume +
  `lockWaitMs` (measured per-lookup wait, replaces the jstack-overlap inference).
- `INDEXLOC noserver` — Phase-B assignment-gap hits with queue depth attached.

All lines carry `lockWaitMs`, `queueLenAtEntry`, `tries`. The lock-free fast-path hit is
deliberately not logged (would flood; it is the uncontended steady state).

### 6.1 Enriched build — resolving the single-region-always-miss puzzle (§4.5)

The first build proved lock saturation but could not explain **why** a single `[EMPTY,EMPTY]`
region produces sustained misses. The enriched build (committed 2026-07-11) adds fields that
force a two-branch decision from one burst:

- **`useCache`** on every `scan`/`herd`/`noserver` line (`ConnectionImplementation`).
  - **`useCache=false`** ⇒ **Branch A**, the cache is being disabled per-lookup: `useCache=false`
    self-clears the row at `locateRegionInMeta:994` before the recheck, and for a single-region
    table that deletes the one shared entry for *all* rows → every lookup re-scans. Next step:
    capture the caller forcing reload (the batch path passes `true`; some retry/relocate does not).
  - **`useCache=true`** ⇒ **Branch B**, the cache is on yet the lookup still misses → the entry
    is absent or mismatched in `MetaCache`.
- **`INDEXLOC miss` lines** from `MetaCache.getCachedLocation` (fire **only on miss**, so silent
  on a healthy hit) discriminate Branch B:
  - **`floor=null`** — no floorEntry at all → the entry was evicted with no logging → widen to
    the `clearCache` call-sites.
  - **`floorStartKey=… floorEndKey=…`** — a stale sibling shadowed the new region at `floorEntry`
    (its `endKey ≤ row`); the printed key range names the exact stale entry the overlap-wipe
    failed to remove.
  - **`mapSize`** on both — should be `1` for a single-region table; `>1` means residual
    prior-incarnation regions, itself a finding.

Both branches converge on the same single instrument-and-read cycle; the burst picks the branch,
the branch picks where any follow-up stack-capture goes. Parser:
`~/index-delay/analyze_indexloc.py <burst.csv>` prints the branch verdict, the miss breakdown,
the distinct shadowing floor keys, and the `mapSize`/lock-wait distributions.

> Outcome (rs-12): Branch B, `floor-rejected` (stale-sibling shadowing). See §4.6.

### 6.2 [RESOLVED] Why doesn't caching the `[EMPTY,EMPTY]` region wipe the stale siblings?

This is the question that decides **HBase bug vs. pure test artifact.** **Verdict: real HBase
defect** — the merge path arbitrates by seqNum ignoring range, so any region **recreated from
empty** (seqid reset to ~1) at a startKey a stale entry still occupies loses the arbitration.
Confirmed by the rs-12 heap dump (`heapdump_20260713_192552.hprof`, 2026-07-13). **Production
exposure is `TRUNCATE` (and DROP+CREATE), NOT split/merge or snapshot-restore** — see the exposure note
below.

Caching `82192ab7…` (`[EMPTY,EMPTY]`) *should* self-heal the poison: endKey `EMPTY` →
`isLast=true` in `cleanProblematicOverlappedRegions` (`MetaCacheUtil.java:65`) → walk
`lastEntry()` down and remove every non-empty-startKey sibling (`:69-73`). This was **observed
working** at rs-9 (17:06 "Removing" lines). At rs-12 it is **not** firing (mapSize stays 8/16).

**Heap-dump evidence (CREATED_DATE cache contents, all 8 entries):**

| regionId (creation ts) | startLen | endLen | seqNum |
|---|---|---|---|
| 1783964514793 | 86 | 86 | 58510 |
| **1783964724361** | **0** | **86** | **94284** |
| 1783964724361 | 86 | 86 | 94284 |
| 1783965167785 | 86 | 86 | 226143 |
| 1783966261321 | 86 | 86 | 422130 |
| 1783966261321 | 86 | 86 | 422130 |
| 1783966197154 | 86 | 86 | 604263 |
| 1783966197154 | 86 | 86 | 604263 |

Two facts settle it: (1) the `EMPTY`-startKey slot (`startLen 0`) is held by a **stale**
region — `regionId 1783964724361`, **non-empty endKey** (`endLen 86`), seqNum `94284`; (2) the
fresh region `1783969837470` (`82192ab7…`, `[EMPTY,EMPTY]` per the RS open log) is **absent**,
and **no** cached entry has `endLen 0`. The self-heal never ran because the `[EMPTY,EMPTY]`
region never landed in the map.

**Mechanism (now traced, not hypothesized):**
1. `cacheLocation` keys the cache by **startKey** (`MetaCache.java:186-188`). Fresh region's
   startKey = `EMPTY` = the stale occupant's startKey → `putIfAbsent` collides → **merge path**
   (`:202-204`).
2. `mergeLocations` is documented *"assuming same range… keeping the most up to date… according
   to seqNum"* (`RegionLocations.java:205-207`) and arbitrates purely by seqNum via
   `selectRegionLocation` → `isGreaterThan(location.seqNum, oldLocation.seqNum)` (`:268`),
   **ignoring endKey/range**.
3. The fresh region opened at **openSeqNum = 2** (RS log: `Opened 82192ab7…; next sequenceid=2`,
   `HRegion.java:1075-1076`; `openSeqNum = initialize()` returns `nextSeqId`, `:948,1088,7296` —
   the logged value is exactly what meta stores in `info:seqnumDuringOpen` and the client loads
   into `HRegionLocation.seqNum`). The stale occupant carries seq **94284** (edits accrued over
   its prior lifetime). `selectRegionLocation` (`RegionLocations.java:268`) keeps the higher
   seqNum: `isGreaterThan(2, 94284)` is false → the merge **keeps the stale region and discards
   the fresh one**. Not a tie — 2 ≪ 94284.
4. The merged (stale) entry retains `endKey = k1` (non-empty) → `isLast = isEmptyStopRow(endKey)`
   is **false** (`MetaCacheUtil.java:65`) → the clean takes the `lowerEntry` walk, not the
   `lastEntry` full-sibling wipe. Self-heal never fires; poison persists.

The observed end-state is the **unique preimage** of this path: had the fresh region won the
merge it would have landed with `endLen 0`, triggered the `isLast` wipe, and self-healed (exactly
what happened at rs-9). It didn't → the fresh region lost the seqNum arbitration (2 vs 94284).

**Precise defect:** `cacheLocation` merges two entries **because their startKeys collide**, but
`mergeLocations` assumes they share a **range** and arbitrates purely by **seqNum**. A stale
`[EMPTY,k1)` (high seqNum) and a fresh `[EMPTY,EMPTY]` (low seqNum) get seqNum-arbitrated; the
stale region wins despite covering a different range. The merge is **range-blind** — that is the
bug.

**Both operands measured** (no inference left): fresh openSeqNum = **2** (RS open log), stale
occupant seqNum = **94284** (heap dump). `isGreaterThan(2, 94284)` is false → stale wins.

**Production exposure — recreate-from-empty, NOT split/merge (corrected).** The bug fires only
when the *new* region at a colliding startKey has a *lower* openSeqNum than the stale cached
entry — a seqNum **inversion**. Whether that inversion happens depends on how the new region's
openSeqNum is derived:

- **Split / merge are SAFE.** On open, `openSeqNum = max(maxSeqId, maxSeqIdFromFile) + 1`
  (`HRegion.java:1047-1053`), where `maxSeqId` comes from the region's store files. Split
  daughters carry **Reference** files to the parent's hfiles; a merged region references **both**
  parents' hfiles — all retaining the parents' accumulated seqids. So the new region opens
  *above* its predecessor and **wins** the arbitration → correct region lands → `isLast` wipe
  fires. (The `openSeqNum = 1` hardcoded for split/merge in meta, `MetaTableAccessor.java:1586,
  1640-1641`, is only a transient placeholder written at the split/merge *plan* step; it is
  overwritten with the real inherited openSeqNum when the region is assigned and opened.) Region
  move and RS-failover reopen are likewise safe — they preserve/advance seqid. This corrects an
  earlier draft that wrongly cited region MERGE (HBASE-27650) as the prod trigger.
- **Recreate-from-empty is the real trigger.** Only a region rebuilt with *no* store-file lineage
  resets seqid to ~1 while a long-lived connection still caches the prior incarnation at the same
  startKey. In production that is **`TRUNCATE TABLE` / `truncate_preserve`**: it deletes the FS
  layout and recreates empty regions (`TruncateTableProcedure.java:113,136,137` →
  `CreateTableProcedure.createFsLayout` → `ModifyRegionUtils.createRegions`). An empty region has
  no store files, so `initializeStores` returns 0 (`HRegion.java:1104,1170`) and
  `getMaxRegionSequenceId` returns -1 (no seqid file, `WALSplitUtil`), giving
  `nextSeqId = max(0,-1)+1 = 1` (`HRegion.java:1047,1053`) → **openSeqNum = 1**. Our DROP+CREATE
  perf case (`next sequenceid=2`) is the same class.
- **Snapshot restore/clone is SAFE (corrected).** Restore/clone-snapshot does *not* create empty
  regions — it creates **HFileLinks** to the snapshot's hfiles (`RestoreSnapshotHelper.java:534-
  536,655-674`). On open, each store file's seqid is read from the hfile `MAX_SEQ_ID_KEY` metadata
  (`HStoreFile.java:424-431`) — the snapshot's *accumulated* seqid, preserved not reset — so the
  restored region opens *above* it and **wins** the merge, like split/merge. (An earlier draft
  wrongly grouped snapshot-restore with truncate.)

So it is a genuine HBase defect, but the correct one-liner is: *seqNum-based merge in
`cacheLocation` is wrong whenever a region is recreated with **no store-file lineage** (seqid
reset to ~1) at a startKey a stale cached entry still occupies* — surfacing in prod via
**`TRUNCATE TABLE`** (and DROP+CREATE) against a long-lived client connection. Split, merge, move,
failover-reopen, and snapshot restore/clone are all safe (they inherit store-file seqids).

**What is NOT the bug:** the `getCachedLocation` endKey check itself is **correct** — it rightly
rejects the stale region (row genuinely past its endKey). The two real defects are (a) this
un-fired sibling wipe, and (b) `getCachedLocation` having **no fallback** past a non-covering
`floorEntry` to a lower enclosing region (`MetaCache.java:77`, single candidate, no loop).

---

## 7. Fix ranking

**Reframed after §4.6.** The dominant mechanism is a poisoned cache producing a **100% miss
rate**, not merely lock throughput. Fixes are now grouped by what they actually address.

**A. Eliminate the poison (attacks the 100% miss rate — the real cause):**
1. **Don't reuse the long-lived index-writer connection's cache across drop/recreate**, or clear
   its region cache for a table on recreate. Kills the zombie at the source. Cheapest, most
   targeted for the perf runs.
2. **Perf-harness: recreate with a *unique* table name per iteration** (same schema). A new
   `TableName` → a fresh per-table cache map → no prior-incarnation siblings → no shadowing.
   Removes the benchmark repro entirely. **Caveat:** masks, does not fix, the production analog
   (see §6.2 — `TRUNCATE` reproduces it without drop/recreate; split/merge and snapshot-restore do not).
3. **HBase-side (§6.2 now RESOLVED — this is a real defect, file upstream):** the merge path
   arbitrates by seqNum on a startKey collision while *assuming* same range
   (`RegionLocations.mergeLocations`, `MetaCache.java:202-204`). Fix options: (a) in
   `cacheLocation`, only take the merge path when the two entries actually share a range —
   otherwise treat the new location as authoritative (it came from a fresh meta scan); or
   (b) make `getCachedLocation` fall back past a non-covering `floorEntry` to a lower enclosing
   region (`MetaCache.java:77`, single candidate, no loop); or (c) a MetaCache TTL / size bound.
   Prod trigger is `TRUNCATE` against a long-lived connection, not split/merge or snapshot-restore —
   see §6.2.

**B. Reduce the cost of misses (helps, but does NOT fix a 100% miss rate):**
4. **Shard the index-writer connection** (N `userRegionLock`s → N× location throughput). Still
   worthwhile for genuine cold-warming (Phase A) and gap re-scans (Phase B), but note: with the
   poison present, every shard still misses 100% and re-scans — sharding spreads the lock, it
   does **not** stop the miss storm. Necessary-but-not-sufficient; do A first.
5. **Reduce cold/gap-stuck regions:** pre-split index tables + constant-size split policy so the
   region set exists up front and daughters aren't born into assignment gaps mid-load (attacks
   Phase B, shrinks Phase-A churn).
6. **pause tuning** — minor. Only touches the Phase-B `NoServerForRegion` retry sleeps.
   `pause=200` risks retry exhaustion since measured assignment gaps reach ~2.9 s.

---

## 8. Theories considered and rejected (with the fact that killed each)

- **Retry-backoff sleeps dominate doPre** — killed by jstack `retrySleep = 0` during plateau.
- **Slow index-write RPC** — killed by jstack `indexBatchRPC = 0` (no threads in
  `table.batch`).
- **Post-split reference-file compaction stealing CPU/IO** (July-4 IT docstring theory) —
  killed for the dump window: the DATA daughter opened ~5 min before the dumps; compaction
  (~26 s) was long finished, and slow compaction would show as slow *RPC*, not a location-lock
  queue. rs-24/rs-26 (July-4) also had freshly-opened daughters, unifying the runs without
  compaction.
- **Cache poisoning by stale entries from the previous iteration** — the *stale-hit* form was
  correctly killed (dead-region write → NSRE → clearCache; NSRE ≈ 4/hour cluster-wide, so no
  stale-hit re-scans are happening). **BUT the poisoning hypothesis itself was right** — see
  §4.6. The error was assuming poison must manifest as a stale *hit*; it actually manifests as a
  permanent *miss* (stale sibling wins `floorEntry`, is correctly rejected on endKey, no fallback,
  no exception ever thrown → never evicted). This entry is retained to record the reasoning error:
  a poisoned entry that is never *contacted* produces no exception and is invisible to
  eviction-count signals.
- **Split-churn eviction driving the dump-window plateau** — killed by the split lineage:
  **zero splits in the clearest dump windows** (July-9 22:45–22:49; July-10 pre-03:10).
- **"Meta scans are slow" asserted from jstack holder** — retracted as a length-biased
  sampling fallacy; downgraded to the open question §6 pending measurement.

---

## 9. Key code references

- `phoenix-core-server/.../hbase/index/IndexRegionObserver.java` `doPre` — times only
  `preWriter.write`.
- `phoenix-core-server/.../hbase/index/write/TrackingParallelWriterIndexCommitter.java:224` —
  `table.batch` per index table.
- `phoenix-core-server/.../hbase/index/write/WaitForCompletionTaskRunner.java:49` —
  `Futures.successfulAsList` (waits for slowest index task).
- hbase `ConnectionImplementation.locateRegionInMeta` — cache check 983-986; `takeUserRegionLock`
  999; recheck-under-lock 1006-1009; meta scan 1017-1021; `NoServerForRegionException` 1058-1061
  (before cache); `cacheLocation` 1068. **TEMP `INDEXLOC` diagnostic added here 2026-07-10;
  `useCache` field + `updateCachedLocations` evict logging added 2026-07-11.**
- hbase `MetaCache` — unbounded ConcurrentHashMap, no eviction except `clearCache`.
  **TEMP `INDEXLOC miss` diagnostic added to `getCachedLocation` 2026-07-11** (floor-null /
  floor-rejected branches, prints row + floor `[startKey,endKey]` + mapSize). **Revert with the
  `ConnectionImplementation` patch.**
- hbase `MetaCache.getCachedLocation:74-113` — single `floorEntry(row)` candidate (`:77`),
  endKey check (`:98-100`), no fallback loop → miss on `:112`. The shadowing site.
- hbase `MetaCache.cacheLocation:184-209` — `putIfAbsent` by startKey (`:188`); new-entry clean
  (`:195`); **merge path** on collision (`:202-204`, `mergeLocations`).
- hbase `RegionLocations.mergeLocations:212-256` + `selectRegionLocation:258-272` — merge keeps
  the higher-**seqNum** entry, ignoring key range. Suspected §6.2 culprit.
- hbase `MetaCacheUtil.cleanProblematicOverlappedRegions:61-88` — `isLast` (`:65`) EMPTY-endKey
  full sibling wipe (`:69-73`); HBASE-27650.
- rs-12 confirmed real region: `PHSCEX…CREATED_DATE,,1783969837470.82192ab71bb647220c4c911996d4aba3.`
  `STARTKEY=''  ENDKEY=''`, opened 19:10:37 regionserver-7 (RS open log). region-id `1783969837470`.
- rs-12 artifacts: `~/index-delay/rs-12/rs-12.csv` (enriched burst),
  `heapdump_20260713_192552.hprof` (3.3 GB — §6.2 seqNum check),
  `rs_dump_20260713_1921*.txt` (jstacks: 14 lock waiters, `indexBatchRPC=0`).
- hbase `ClientExceptionsUtil.isMetaClearingException` — NServing / RegionOpening / non-special.
- Lineage / assignment sources (hbase-expert):
  `dev-phoenix-hbase5a-split-lineage-20260709-2235.md`,
  `dev-phoenix-hbase5a-datatable-assignments-20260709-2235.md`,
  `dev-phoenix-hbase5a-wave2-split-lineage-20260704.md`.
- Perf-run windows (0709, ms epoch): Run2(b) `1783636528307–1783641047436`
  (= 22:35 lineage run); Run4(b) `1783650650692–1783657406411` (= 03:10:50–05:03:26 UTC,
  the rs-5 dump/NSRE run, worst at 112 min).
