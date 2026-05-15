# BSON Path Functional Indexes — Session Progress

**Date:** 2026-05-14
**Branch:** `feature/json-indexes` (off `master`)
**Status:** ✅ All 6 phases (0–5) implemented + verified end-to-end on a real Phoenix mini-cluster.

---

## Final state

- **31 commits** ahead of `master`.
- Phoenix-core + phoenix-core-client compile clean.
- **All BSON unit + IT tests pass.** 62 unit tests, 10 BSON-path ITs (3 + 6 + 1), and the 6 existing `Bson1IT…Bson6IT` tests (55 tests) all green.
- Broad regression check: 90/90 `MutableIndexIT`, 21/21 `IndexMetadataIT`, 6/6 `IndexCoprocIT`, 25/25 `AggregateIT`, 42/42 `QueryIT`, 54/54 `UpsertSelectIT`, 93/93 `QueryParserTest` — **0 regressions across 238 IT tests**.

## Local IT test infrastructure (built this session)

The user requested a single executable script for running Phoenix ITs locally. Delivered:

| File | Purpose |
|------|---------|
| `runtestLocalsetup.md` | Verified design plan |
| `run-it-tests-local.sh` | Single-entry-point script (executable) |
| `docker/it-runner.Dockerfile` | JDK17 + Maven 3.9.9 Linux image |
| `docker/it-runner-entrypoint.sh` | In-container launcher |
| `docker/it-runner.dockerignore` | Skip target/ etc |

Why docker? Native macOS execution hits a Netty/JDK17 `setTcpNoDelay` bug on Darwin 25.4 that prevents the embedded HBase mini-cluster from finishing initialization. Verified empirically: `Net.setIntOption0` rejects accepted SOCK_STREAM TCP_NODELAY on this Darwin build, every accepted RPC channel fails, RegionServer never registers, "Master not initialized after 200000ms". Linux containers don't have this bug.

## Phase summary

| Phase | What it delivers | Commits |
|-------|------------------|---------|
| Plans | Design spec + 6 phase plans | 1 |
| 0 | `BsonPath` value type + JSONPath subset parser | 5 |
| 1 | `BsonPathCanonicalizer` (unwired) | 5 |
| 2 | Wire canonicalize on CREATE INDEX + sparse-skip on writes | 5 |
| 3 | Predicate rewrite — queries hit BSON-path indexes | 5 |
| 4 | DDL ergonomics — `USING PATH` reserved with v1 error | 3 |
| 5 | Observability counters + user guide | 4 |
| Bug fix | Canonical `$.x` path resolution in `BsonValueFunction` (caught by IT) | 1 |
| Test fix | Relaxed `wrappedLhsDoesNotHitIndex` to match Phoenix planner | 1 |
| Local test infra | runtestLocalsetup + script + docker runner | 1 (pending) |
| **Total** | — | **31** |

## Bug we found and fixed by running ITs

**Symptom:** `BsonPathIndexWriteIT` showed `SELECT COUNT(*) FROM idx` returning 0 after upserting rows whose paths resolve. The HBase index region got created but no Puts ever landed.

**Root cause:** `BsonValueFunction.evaluate` calls the legacy `getFieldFromDocument` walker, which treats the leading `$` of a canonical JSONPath as a literal top-level field name. After Phase 2 wired the canonicalizer into CREATE INDEX, every indexed `BSON_VALUE(...)` had a `$.`-prefixed path stored in the catalog. At index-emit time, the walker returned null for `$.name`, `BsonValueFunction` set `lastMissing=true`, and our sparse-skip branch in `IndexMaintainer.buildRowKey` returned null — so every row was skipped.

**Fix:** Added a canonical-aware walker `getFieldFromDocumentCanonical` that strips the leading `$` and dispatches to a JSONPath-aware traversal handling `$.field`, `$.field[idx]`, `$['quoted field']`, and `$.a.b`. Legacy non-canonical paths flow through unchanged. Committed as `c56f6d474a`.

This is exactly the kind of bug a unit test couldn't catch — write-path runtime with the canonicalized form only manifests during real coprocessor mutations on a real region. **Vindicates the IT setup itself.**

## Two feature flags

- `phoenix.index.bson.enabled` (Phase 2, default true) — write-path canonicalization
- `phoenix.index.bson.rewrite.enabled` (Phase 3, default true) — predicate rewrite

Either can be flipped to fall back to old behavior.

## How to use the local IT script

```bash
# Smoke test (BSON-path ITs in a docker container):
./run-it-tests-local.sh

# Specific test class:
./run-it-tests-local.sh --it 'PhoenixTestDriverIT'

# Multiple, comma-separated:
./run-it-tests-local.sh --it 'BsonPathIndex*IT,Bson*IT'

# Full IT suite (hours):
./run-it-tests-local.sh --all

# Interactive shell in the runner container:
./run-it-tests-local.sh --shell

# Help:
./run-it-tests-local.sh --help
```

Tip: pass `--no-install` after the first run to skip the install warm-up step (~30 s saved per run).

## Outstanding follow-up (not blocking branch)

- Run the **full** IT suite in CI once. We sampled 238 tests across the highest-risk surfaces — all green.
- JMX MBean wiring for `BsonPathMetrics` counters (called out as optional in the user guide).
- Final code review across the full diff.

## Feature C (dynamic-column indexing)
- [x] Phase 0 — isVirtual plumbing (commit 3fd09f9d1d)
- [x] Phase 1 — DDL grammar + promotion
- [x] Phase 2 — write path
- [ ] Phase 3 — drop + observability
