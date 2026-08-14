# Region-level CPU-gated scan admission control for Phoenix

## Context

A RegionServer (fkp-hbase perf, us-east EKS) hit ~1181% CPU (~12 cores). A jstack
dump showed the burn was entirely in the **read path**: 22 concurrent scan handlers
RUNNABLE in `StoreScanner` seek/skip + `CellComparator` + FAST_DIFF `decodeNext`,
all funneling through Phoenix's `TTLRegionScanner → PagingRegionScanner` chain
evaluating `MultiKeyValueComparisonFilter` (a `WHERE` over non-key columns). Root
cause confirmed by the team: a **bad Phoenix query doing a full-table scan with heavy
server-side filtering**, issued concurrently by many clients against a **single
region**. The incident was mitigated by disabling the query client-side; there is no
server-side guardrail, and the pattern has recurred.

Two facts make the usual defenses useless here and shaped the design:
- **Handlers were never saturated** (22 of ~257 scan handlers busy). So queue-depth /
  `CallQueueTooBig` never triggered — the bottleneck was CPU, not the handler queue.
- **Phoenix paging** slices each scan into short back-to-back RPCs, so **queue-latency
  shedding (HBase CoDel) is structurally blind** — a paged scan never dwells in the
  queue.

Prior-art survey (CockroachDB elastic-CPU, ScyllaDB scheduling groups, DynamoDB/Bigtable
per-range isolation, TiDB runaway-query, Oracle/Spanner "enforce only under contention")
pointed to the same conclusion: meter something that **persists across page re-issues**
(CPU / concurrency), enforce **per key-range (region)**, and **only under real CPU
contention**. Goal: a server-side guardrail so the next such query is contained to its
region instead of pegging the node — without penalizing healthy traffic.

## Approach (first cut = Tier 1: reject at scanner-open)

Classify each scan at open; if it is an **expensive** scan, the **region** already has too
many concurrent expensive scans, **and** the RS is **CPU-hot**, reject the scanner-open
with a retriable exception so the client backs off. Enforcement is per-region, matching
where hotspotting occurs (the same granularity DynamoDB/Bigtable isolate at). No effect
when the box is healthy or the region is not contended.

### 1. Scan cost classifier (read-only, from the `Scan` alone)
New static helper in `ScanUtil` (`phoenix-core-client/.../util/ScanUtil.java`, alongside
`isAnalyzeTable`/`removeSkipScanFilter`). A scan is **expensive** iff:
- **Full scan** — empty start row AND empty stop row (and not `isGetScan()`), OR
- **Range scan carrying a non-key expression filter** — the (unwrapped) filter is, or
  contains, any `BooleanExpressionFilter` subclass
  (`MultiKeyValueComparisonFilter`, `MultiCQKeyValueComparisonFilter`,
  `SingleKeyValueComparisonFilter`, `SingleCQKeyValueComparisonFilter` —
  `phoenix-core-client/.../filter/`).

Exempt (cheap): `isGetScan()` point lookups, top-level `SkipScanFilter`
(incl. `isMultiKeyPointLookup()`), `FirstKeyOnlyFilter`, plain range scans with no
expression filter. Must **unwrap `PagingFilter` and `FilterList`** to inspect the real
filter — reuse the unwrap pattern in `ScanUtil.removeSkipScanFilterFromFilterList`
(`ScanUtil.java:1937`). Classify **early in `preScannerOpen`, before** the `PagingFilter`
wrap at `BaseScannerRegionObserver.java:183` so the filter is still raw.

### 2. Admission controller (per-region counters + RS-wide CPU sampler)
New class `ScanAdmissionController` (`phoenix-core-server/.../coprocessor/` or a new
`replication`-sibling `admission` package):
- `ConcurrentHashMap<String encodedRegionName, AtomicInteger>` of in-flight expensive scans.
- **RS-wide CPU sampler**: one scheduled task (~1s) caches
  `com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()` into a `volatile double`.
  Read the cached value in the hot path — **never call `getProcessCpuLoad()` inline**
  (needs an interval between reads; costly). `getAvailableProcessors()` (cgroup-aware in
  JDK 17 — **must be validated on the EKS pods**) gives core count for cap scaling.
- `isHot()` = cached load > threshold. `capFor(region)` = base cap, optionally × cores.

### 3. Integration hook
In `BaseScannerRegionObserver` (`phoenix-core-server/.../coprocessor/BaseScannerRegionObserver.java`):
- **`preScannerOpen` (:155)** — if `enabled` && `classifier.isExpensive(scan)`:
  - if `isHot()` && `inFlight(region) >= capFor(region)` → **throw
    `RegionTooBusyException`** (canonical HBase retriable/backoff signal; refuses the open).
  - else `inFlight.incrementAndGet()` and mark the scan with a
    `EXPENSIVE_SCAN_COUNTED` attribute (new constant in
    `BaseScannerRegionObserverConstants`).
- **`postScannerClose` (new `RegionObserver` override)** — if the closing scanner was
  counted, `inFlight.decrementAndGet()`. Release is reliable: `postScannerClose` fires on
  normal close (`RSRpcServices:3830`) **and on lease expiry**
  (`ScannerListener.leaseExpired()` → `closeScanner` → `RSRpcServices:552`), so an
  abandoned client cannot leak a slot.

**Counting must be idempotent and correctly paired:**
- The `EXPENSIVE_SCAN_COUNTED` scan attribute both (a) dedupes accounting when multiple
  `BaseScannerRegionObserver` subclasses run `preScannerOpen` for one scan (second
  observer sees the marker, skips), and (b) tells `postScannerClose` whether to decrement.
- **Reject path takes no slot** (throw before increment). **Any exception after increment
  but before a scanner is returned must decrement** (try/finally around the open path).
- Recommended: consider isolating this in a **dedicated `RegionObserver`** rather than the
  shared base to sidestep multi-observer accounting entirely — tradeoff is coprocessor
  rollout (Phoenix must install it on its tables; existing tables pick it up on
  upgrade/alter). If folded into the base instead, the idempotent marker is mandatory.

### 4. Config (new keys, default **disabled** — opt-in for safety)
Define constants (server-side; mirror the `phoenix.server.paging.*` precedent):
- `phoenix.region.scan.admission.enabled` (default `false`)
- `phoenix.region.scan.admission.cpu.threshold` (default e.g. `0.85`)
- `phoenix.region.scan.admission.max.concurrent.expensive.per.region` (base cap; verify a
  sane default before shipping — do not guess)
- `phoenix.region.scan.admission.cpu.sample.interval.ms` (default `1000`)

## Critical files
- `phoenix-core-client/.../util/ScanUtil.java` — classifier helper (+ unwrap reuse `:1937`).
- `phoenix-core-client/.../filter/BooleanExpressionFilter.java` and subclasses — the
  "expensive filter" marker type.
- `phoenix-core-client/.../coprocessorclient/BaseScannerRegionObserverConstants.java` —
  new `EXPENSIVE_SCAN_COUNTED` attribute + config-key constants.
- `phoenix-core-server/.../coprocessor/BaseScannerRegionObserver.java` — `preScannerOpen`
  (:155) acquire/reject; new `postScannerClose` release.
- `phoenix-core-server/.../coprocessor/ScanAdmissionController.java` — **new**: per-region
  counters + CPU sampler.

## Verification
1. **Unit** — classifier: point lookup / skip-scan / plain range / full scan / range +
   `MultiCQKeyValueComparisonFilter`, each wrapped in `PagingFilter` and `FilterList`, map
   to the expected expensive/cheap verdict.
2. **Unit** — controller: increments per expensive open, decrements on close; rejects only
   when `isHot()` && at cap; reject takes no slot; error-after-increment decrements
   (try/finally); multi-observer double-`preScannerOpen` counts once.
3. **IT** (extend `BaseTest`/an existing scan IT) — with admission enabled, a stubbed
   always-hot CPU signal and cap=N: open N expensive scans on one region (held open across
   pages), assert the (N+1)th fails with a **retriable** `RegionTooBusyException` and that
   the Phoenix client retries with backoff; assert point lookups and scans on other regions
   are unaffected; assert slots free after close and after lease expiry.
4. **Manual/perf** — reproduce the incident (full-scan-with-filter at concurrency on a
   single region); confirm CPU is bounded to ~cap concurrent scans and the node stays
   responsive to other traffic. Validate `getAvailableProcessors()`/`getProcessCpuLoad()`
   report correctly inside the EKS cgroup.
5. `mvn spotless:apply`; build with both HBase profiles (2.5, 2.6).

## Tier 2 — cooperative CPU pacing (documented fast-follow, NOT in first cut)
Only if data shows the headcount cap is too coarse or legitimate heavy scans are being
rejected. Model: CockroachDB elastic-CPU. Insert a `DelegateRegionScanner` wrapper above
`PagingRegionScanner` (`BaseScannerRegionObserver.java:348-357`); meter per-scan CPU with
`ThreadMXBean.getThreadCpuTime` (persists across pages like `TTLRegionScanner.rowCount`);
at each page boundary check a per-region/per-class **token bucket** whose refill rate is
`target_utilization × cores`; when over budget, yield via
`PhoenixScannerContext.setReturnImmediately(...)`. **Open design question to resolve
then:** where the wait happens — blocking the handler thread (risks pool exhaustion under
broad load) vs. yield-and-client-reissue (latency, not true backpressure). Deferred
deliberately.

## Out of scope
- Handler-**pool** isolation by scan class: impossible from `preScannerOpen` (routing is
  fixed at enqueue in `scheduler.dispatch`; no re-dispatch). Would require a client-set
  priority hint (as Phoenix does for index/metadata pools via `PhoenixRpcScheduler`) or a
  custom scheduler. Not pursued.
- Fingerprint/watch-list auto-demotion of repeat offenders (TiDB `QUERY WATCH` style) —
  possible later enhancement on top of the classifier.