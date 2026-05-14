# BSON Path Functional Indexes — Session Progress

**Date:** 2026-05-14
**Branch:** `feature/json-indexes` (off `master`)
**Status:** ✅ All 6 phases (0–5) implemented and committed. 28 commits ahead of master.

---

## Final state

- 28 commits on `feature/json-indexes` ahead of `master`.
- Phoenix-core + phoenix-core-client compile clean.
- All 62 new BsonPath unit tests pass.
- Broader regression sweep: `QueryParserTest` 93/93 pass — grammar change for `USING PATH` reservation did not regress anything.
- Integration tests authored and committed; **deferred from execution on this host** because the embedded HBase mini-cluster fails to start (`Master not initialized after 200000ms`) — environmental, not code. ITs need a follow-up CI run on a host where the mini-cluster boots.

## Phase summary

| Phase | What it delivers | Commits | Tests added |
|-------|------------------|---------|-------------|
| Plans | Design spec + 6 phase plans | 1 | — |
| 0 | `BsonPath` value type + JSONPath subset parser | 5 | 34 unit |
| 1 | `BsonPathCanonicalizer` (unwired) | 5 | 17 unit |
| 2 | Wire canonicalize on CREATE INDEX + sparse-skip on writes | 5 | 7 unit + 1 IT |
| 3 | Predicate rewrite — queries actually hit BSON-path indexes | 5 | 2 ITs |
| 4 | DDL ergonomics — `USING PATH` reserved with v1 error | 3 | 3 unit |
| 5 | Observability counters + user guide | 4 | 1 unit |
| **Total** | — | **28** | **62 unit + 3 ITs** |

## Commit log (newest first)

```
f8932b4962 PHOENIX BsonPath: user guide for v1
5da2ba4adf PHOENIX BsonPath: increment rewrite hit/miss counters + IT assertion
82beabf77c PHOENIX BsonPath: increment sparse-skip counter on missing path
7e3eb4c097 PHOENIX BsonPath: add BsonPathMetrics counters
005f12dbe6 PHOENIX BsonPath: parser test for USING PATH reservation
69ccc7227b PHOENIX BsonPath: reserve USING PATH clause on CREATE INDEX (v1 rejects)
c0bda2970f PHOENIX BsonPath: reserve BSON_PATH_INDEX_NOT_SUPPORTED error code
18a113d250 PHOENIX BsonPath: randomized index/no-index consistency IT
305690f320 PHOENIX BsonPath: query-side IT covering eq, IN, BETWEEN, fallback
8640ca3d18 PHOENIX BsonPath: phoenix.index.bson.rewrite.enabled feature flag
430dfaf179 PHOENIX BsonPath: canonicalize WHERE expression before index match
2fbe204c71 PHOENIX BsonPath: canonicalize indexed expression on rewriter load
fa48dfb062 PHOENIX BsonPath: write-path IT covering populate, sparse-skip, dedupe
64149fcd86 PHOENIX BsonPath: sparse-skip rows where indexed BSON path is missing
5d2e4c65a3 PHOENIX BsonPath: canonicalize index expression on CREATE INDEX + feature flag
fd13a16073 PHOENIX BsonPath: add BsonIndexUtil helpers
83ae9e2a28 PHOENIX BsonPath: add phoenix.index.bson.enabled feature flag
30f0d47c8d PHOENIX BsonPath: extractPath helper coverage
2906aed6c8 PHOENIX BsonPath: canonicalizer recurses into compound nodes
af5a42be1e PHOENIX BsonPath: canonicalizer rewrites JSON_VALUE to BSON_VALUE
bff868ca7f PHOENIX BsonPath: canonicalize BSON_VALUE path arg + type case
3b9ed682b8 PHOENIX BsonPath: canonicalizer skeleton (identity rewrite)
62a019689e PHOENIX BsonPath: parser fuzz test (5k random inputs, no crashes)
83561fe456 PHOENIX BsonPath: parser rejects unsupported JSONPath features
f66a1f8ce9 PHOENIX BsonPath: add JSONPath-subset parser (happy path)
98b3178dc9 PHOENIX BsonPath: add immutable BsonPath value type
8153debc89 PHOENIX BsonPath: add exception type for path parser (Phase 0/1)
72a1b033a2 PHOENIX BsonPath: design spec + 6 phase implementation plans
```

## Notable deviations from plans (all documented in commits + plans)

1. **Phase 1 — `canonicalizesQuotedKey` test reshape.** Phoenix parses `"['weird key']"` as an identifier, not a string literal, so the canonicalizer correctly skips it. Test was rewritten to assert input-unchanged on that input.
2. **Phase 2 — duplicate-index collision test.** Connectionless driver doesn't raise duplicate-index errors, so the collision test was rewritten to assert canonical form is what gets persisted on the indexed PColumn name (direct evidence of canonicalization at CREATE).
3. **Phase 2 — sparse-skip null propagation widened.** Plan flagged this; eight call sites of `IndexMaintainer.buildRowKey` now handle null returns (rebuild + observer + DeleteCompiler + IndexTool).
4. **Phase 4 — `USING PATH` soft-keyword strategy.** The naïve token-add approach pushed the generated parser past Java's 64KB method-size limit. Implemented as a generic `NAME` match with a runtime guard in the rule action; same semantics, no parser-size growth.
5. **Phase 5 — JMX MBean adapter not added.** Plan called it optional; counters are static `AtomicLong`s with getters, easy to wire to JMX later if operators want it. Documented in the user guide.

## Outstanding follow-up before this branch ships

- **Run integration tests on a host where the HBase mini-cluster starts.** New ITs:
  - `BsonPathIndexWriteIT` (Phase 2) — populate/sparse-skip/dedupe on writes.
  - `BsonPathIndexQueryIT` (Phase 3) — eq, IN, BETWEEN, fallback when no index.
  - `BsonPathIndexConsistencyIT` (Phase 3) — randomized index vs no-index parity.
  - Existing `Bson1IT…Bson6IT` regression check.
  - `IndexMaintenanceIT` regression check (sparse-skip null-propagation in rebuild paths is the highest-risk surface).
- **JMX MBean wiring** for `BsonPathMetrics` counters — optional, mechanical.
- **Final code review** across the full diff (`git diff master..feature/json-indexes`).

## How to resume

1. `cd /Users/nlakshmanan/git/phoenix && git checkout feature/json-indexes`
2. `git log --oneline -1` should be `f8932b4962 PHOENIX BsonPath: user guide for v1`.
3. Run the deferred ITs on a CI host or a workstation where mini-cluster boots cleanly.
4. Once green, use `superpowers:finishing-a-development-branch` to merge / open the PR.

## Important notes

- **Commit signing must stay disabled.** All 28 commits used `--no-gpg-sign`.
- **Two feature flags ship off-the-shelf:**
  - `phoenix.index.bson.enabled` (Phase 2, default true) — controls write-path canonicalization.
  - `phoenix.index.bson.rewrite.enabled` (Phase 3, default true) — controls predicate rewrite.
  Either can be flipped to fall back to old behavior if a regression appears in production.
