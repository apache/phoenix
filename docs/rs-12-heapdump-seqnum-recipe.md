# rs-12 heap-dump recipe — settle §6.2 (merge-vs-clean seqNum collision)

**Goal.** Read the entries in the index-writer `MetaCache` for
`PHSCEX.PHSCEX_FEED_COMPOSITION_CREATED_DATE` and answer one question:

> Does the `EMPTY`-startKey slot hold a **stale** region (non-empty endKey) with a **higher
> seqNum** than the fresh `[EMPTY,EMPTY]` region `82192ab7…` (region-id `1783969837470`)?

- **YES** → confirms the leading hypothesis: `mergeLocations` kept the stale region by seqNum,
  the `isLast` sibling-wipe never fired, `82192ab7…` never landed → **latent HBase bug**
  (region merges reproduce it in prod). File an HBase issue.
- **NO** (EMPTY slot holds `82192ab7…`, or is absent) → the clean was skipped for another reason;
  likely test-specific. Re-open the trace.

## Inputs
- Heap: `~/index-delay/rs-12/heapdump_20260713_192552.hprof` (3.1 GB, `JAVA PROFILE 1.0.2`).
- Tool: `jhat` from the JDK8 install (`$JAVA_HOME/bin/jhat`) — ships OQL + web UI. MAT works too.

## Step 1 — launch jhat (long-running; run it yourself)
jhat holds the whole heap in memory — give it headroom and expect a few minutes to parse.

```
! $JAVA_HOME/bin/jhat -J-Xmx12g -port 7401 ~/index-delay/rs-12/heapdump_20260713_192552.hprof
```

Wait for `Server is ready.` then browse to `http://localhost:7401`.

## Step 2 — OQL to dump the CREATED_DATE cache entries

**Do NOT call `Bytes.toStringBinary` (or any Java static) from OQL.** jhat's Nashorn engine
resolves a trailing identifier after `Packages.…` as a *class name*, so
`Packages.org.apache.hadoop.hbase.util.Bytes.toStringBinary(x)` throws
`ClassNotFoundException: …Bytes.toStringBinary`. The recipe therefore uses **only heap
field reads** — which is sufficient, because the hypothesis only needs to know *which region
has an empty startKey* and *whether that region's endKey is empty*, i.e. `byte[].length == 0`.

### Query 1 — smoke test (confirms class + field access)
Open `http://localhost:7401/oql/` and run:

```
select { encoded: r.encodedName, id: r.regionId, tbl: r.tableName.qualifierAsString }
from org.apache.hadoop.hbase.client.MutableRegionInfo r
```

Returns one row per cached region (`RegionInfo`'s impl class is `MutableRegionInfo`, not
`RegionInfoImpl`). If this errors, click any `MutableRegionInfo` instance in the object browser
and adjust field names.

### Query 2 — the answer: every CREATED_DATE region, keys by length
`startKey.length == 0` marks the EMPTY-startKey slot; `endKey.length == 0` marks a
`[…,EMPTY]` (last) region. No byte decoding needed.

```
select {
  encoded:  r.encodedName,
  regionId: r.regionId,
  startLen: r.startKey.length,
  endLen:   r.endKey.length
}
from org.apache.hadoop.hbase.client.MutableRegionInfo r
where /CREATED_DATE/.test(r.tableName.qualifierAsString)
```

(`qualifierAsString` is a `final` String always set in the `TableName` ctor — safe to match.)

### Query 3 — seqNum for a specific region (the clincher)
`seqNum` lives on `HRegionLocation`, not the region. Once Query 2 gives the encoded name in the
EMPTY-startKey slot, read its seqNum (substitute the two encoded names):

```
select { seq: h.seqNum, srv: h.serverName.hostnameOnly, enc: h.regionInfo.encodedName }
from org.apache.hadoop.hbase.HRegionLocation h
where h.regionInfo.encodedName == "82192ab71bb647220c4c911996d4aba3"
   || h.regionInfo.encodedName == "PASTE_EMPTY_SLOT_ENCODED_NAME"
```

(If `serverName.hostnameOnly` errors, drop the `srv` field — it is not needed for the verdict.)

## Step 3 — read the answer
From Query 2, find the row with `startLen == 0` (the EMPTY-startKey slot):

- If `encoded == 82192ab71bb647220c4c911996d4aba3` (regionId `1783969837470`) and `endLen == 0`
  → the fresh region **is** in the EMPTY slot → hypothesis **FALSE**, re-open the trace.
- If `endLen != 0` and `encoded != 82192ab7…` → a **stale** region occupies the EMPTY slot. Run
  Query 3 for both encoded names; **stale seqNum > fresh (`82192ab7…`) seqNum** → hypothesis
  **CONFIRMED** (`mergeLocations` kept the stale region by seqNum, `isLast` wipe never fired).
- Also note the CREATED_DATE row count from Query 2 (expect 8 or 16, matching `mapSize` in the
  burst) and the full `startLen/endLen` list — the exact stale-sibling set for the HBase issue.

Note: Query 2 enumerates every `MutableRegionInfo` still reachable in the heap, not only the ones
currently in the cache map. If the count exceeds `mapSize`, some are unreferenced prior-incarnation
regions awaiting GC — the EMPTY-slot / seqNum verdict is unaffected, but to read the *live* cache
contents exactly, use the MAT fallback below.

## Fallback
If Query 2's enumeration is ambiguous (e.g. more `MutableRegionInfo` instances than the live
cache holds), load the same hprof in **Eclipse MAT**, open the `MetaCache` instance for the
index-writer connection, and use "List objects → with outgoing references" on
`cachedRegionLocations` → the CREATED_DATE submap to read `startKey`/`endKey`/`regionId`/`seqNum`
straight from the object inspector (no scripting).

## Cleanup
Kill jhat (`Ctrl-C` on the `!` process) when done. Do not commit the hprof.
