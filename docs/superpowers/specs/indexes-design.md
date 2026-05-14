# Final Design: Expression-Based Secondary Indexes on BSON/JSON Paths

**Status:** Final draft, grounded in source verification
**Date:** 2026-05-05
**Supersedes:** `2026-05-05-bson-path-functional-indexes-design.md`
**Scope:** Apache Phoenix master branch

## 1. What the verification revealed

The original design was built on several assumptions that do not survive contact with
`master`. The feedback review flagged most of them; some flags were themselves off. Here
is what is actually true, with direct code references:

### Already works today

- **BSON-path functional indexes exist and ship in master.** `Bson5IT.java:111-117` creates
  `CREATE UNCOVERED INDEX … ON t(BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR'))
  WHERE BSON_VALUE(COL, 'rather[3].outline.clock', 'VARCHAR') IS NOT NULL CONSISTENCY =
  EVENTUAL` and then runs a `SELECT` with the same BSON_VALUE expression in the WHERE
  clause — and the plan is `RANGE SCAN` over the index (`Bson5IT.java:172`). End-to-end
  working feature.
- **`BSON_VALUE(doc, 'path', 'TYPE'[, 'default'])` already takes type as an argument.**
  `BsonValueFunction.java:67-75` declares three required args plus an optional default.
  Type coercion at eval time is already implemented across VARCHAR, INTEGER, LONG,
  DOUBLE, DECIMAL, BOOLEAN, VARBINARY, DATE, BSON.
- **Null / missing-path is already handled correctly.** `BsonValueFunction.evaluate`
  calls `returnDefaultValue(ptr, type)` when the path is missing. With the default
  `'null'` (string) it sets `EMPTY_BYTE_ARRAY`. Phoenix's index-key encoder in
  `IndexMaintainer.buildRowKey` (around the separator-byte logic at
  `IndexMaintainer.java:890`) already honors `ptr.getLength() == 0` as a null marker and
  strips trailing nulls.
- **Partial index (`WHERE path IS NOT NULL`) is the existing idiom for sparse-index
  semantics** — visible in `Bson5IT.java:112,116`. Users who want "only index rows where
  the path is present" write the partial-index `WHERE` clause today.
- **Pre-image / post-image diff is already implemented** in
  `IndexRegionObserver.generateIndexMutationsForRow` (L1265-1310). It computes
  `buildRowKey(currentDataRowVG, …)` and `buildRowKey(nextDataRowVG, …)` and emits a
  delete for the old key when the new key differs. Update, delete, and absence-transition
  transitions work for expression indexes out of the box.
- **Both STRONG (sync) and EVENTUAL consistency modes exist** — `IndexConsistency.java`
  and used in prod IT with `CONSISTENCY = EVENTUAL`.
- **`INCLUDE(doc)` syntax already honored** on BSON-path indexes (`Bson5IT.java:378`).
- **Index types:** `GLOBAL`, `LOCAL`, `UNCOVERED_GLOBAL` (`MetaDataClient.java:1446,
  PTable.IndexType`). BSON-path indexes in prod tests use `UNCOVERED INDEX` with
  `CONSISTENCY = EVENTUAL`.
- **The `isJsonFragment` guard** (`ExpressionCompiler.java:313-314`) triggers only on
  `JsonQueryParseNode` and `JsonModifyParseNode`, not on `BsonValueFunction`,
  `BsonValueTypeFunction`, or `JsonValueFunction`. It was never a blocker for BSON path
  indexes. The original spec's Motivation #1 was wrong.
- **Determinism and stateless gates pass naturally.** `BsonValueFunction` extends
  `ScalarFunction → FunctionExpression → BaseCompoundExpression`, whose default
  `getDeterminism()` combines children (→ `Determinism.ALWAYS`) and whose default
  `isStateless()` is AND of children (→ `false`, because the BSON column reference is
  non-stateless). Both gates in `MetaDataClient.createIndex` (the determinism check and
  the stateless check) already pass for BSON path expressions.

### Real gaps that remain

These are the verified gaps — the legitimate feature work:

1. **No path-string canonicalization.** `IndexExpressionParseNodeRewriter.leaveCompoundNode`
   matches ParseNode by `equals()`. `FunctionParseNode.equals` compares name + children;
   a path literal child is a `LiteralParseNode` whose `equals` is byte-for-byte on the
   value. So `BSON_VALUE(doc, 'a.b', 'VARCHAR')` and `BSON_VALUE(doc, '$.a.b', 'VARCHAR')`
   — which are semantically identical — do **not** match the same index. A user whose
   query spells the path one way misses an index created with the other spelling.
2. **Predicate matching limited to exact equality in practice.** In-tree IT coverage
   (`Bson5IT`) shows `BSON_VALUE(COL, p, t) = ?` hitting the index. Range (`<`, `<=`,
   `>`, `>=`), `BETWEEN`, and `IN` are not exercised — and because the rewriter relies on
   AST equality of the indexed expression against the predicate LHS, range predicates
   *will* rewrite correctly for the indexed column (that part is generic scan-range
   derivation), but canonicalization differences will still cause misses.
3. **Sort order for typed numeric paths is likely wrong with the default index column
   type.** `BSON_VALUE(doc, 'x', 'VARCHAR')` returns VARCHAR bytes — fine. But
   `BSON_VALUE(doc, 'x', 'DOUBLE')` at the write path sets `ptr` to
   `PDouble.INSTANCE.toBytes(double)` (IEEE 754 bits), which is **not** order-preserving
   under unsigned byte comparison for negative values. Range scans on such indexes will
   return incorrect ordering across sign boundaries. Must be verified; if confirmed,
   fixed at index-key time with a sign-flip or by routing through Phoenix's fixed-width
   numeric encoders that already provide order-preserving bytes.
4. **Rewriter has no fast-path.** `IndexStatementRewriter.translate` runs on every
   `SELECT`, walking the index column list and parsing every indexed expression. On
   tables with no BSON columns and no BSON indexes, this is pure overhead. It does not
   scale as BSON indexes proliferate.
5. **No operator sugar.** Phoenix exposes BSON navigation only through `BSON_VALUE`,
   `BSON_VALUE_TYPE`, `BSON_CONDITION_EXPRESSION`. There is no `->` or `->>` in the
   grammar (`grep` of `PhoenixSQL.g`, `PhoenixBsonExpression.g`: no hits). Adding them is
   a self-contained grammar change independent of the indexing work.
6. **Observability is thin.** No per-BSON-index metrics for rewrite hits/misses, no
   EXPLAIN hint showing which canonical path matched, no counter for partial-index
   skips. Operators debugging "why didn't my query hit the index" have no signal.

## 2. What this design delivers

A focused, incremental enhancement program that closes the five real gaps above without
inventing new infrastructure where Phoenix already has working infrastructure.

### Non-goals (unchanged from prior spec, reaffirmed)

- Dynamic-column (per-row, non-schema) indexing.
- GIN-style multi-valued path indexes.
- Containment (`@>`) predicates.
- Wildcards, filter expressions, recursive descent in JSONPath.

### Goals, restated

1. Queries that spell a BSON path in a different-but-equivalent way must hit the same
   index as the DDL that created it.
2. Range (`<`, `<=`, `>`, `>=`), `BETWEEN`, and `IN` predicates on BSON-path expressions
   must use the index correctly, including with correct sort order for numeric types.
3. The rewriter must add negligible overhead on queries against tables with no BSON
   indexes.
4. Operators must be able to tell whether and why an index was used.
5. The DDL / query experience must remain backwards-compatible — nothing in `Bson5IT`
   breaks.

## 3. Architecture

Two modest additions, both client-side; no on-disk format change; no coprocessor change.

```
          ┌───────────────────────────────────────────────┐
  DDL     │ Parser ─► BsonPathNormalizer (new) ─► compile │
          │            │                                  │
          │            ▼                                  │
          │  canonical path literal stored in             │
          │  PColumn.getExpressionStr() (SYSTEM.CATALOG)  │
          └───────────────────────────────────────────────┘

          ┌───────────────────────────────────────────────┐
  Query   │ Parser ─► BsonPathNormalizer (same rewriter)  │
          │            │                                  │
          │            ▼                                  │
          │ IndexExpressionParseNodeRewriter              │
          │  (fast-path: bail if no BSON indexes)         │
          │            │                                  │
          │            ▼                                  │
          │  existing exact-AST match ─► scan ranges      │
          └───────────────────────────────────────────────┘
```

`BsonPathNormalizer` is applied at two points: (a) during `CREATE INDEX` compilation,
before the expression string is persisted into SYSTEM.CATALOG, so equivalent paths
produce identical expression strings; (b) during query rewrite, to the WHERE-clause
parse nodes, so a differently-spelled path rewrites to the canonical form and then
matches by `ParseNode.equals`.

Everything else — `IndexMaintainer.buildRowKey`, `IndexRegionObserver.preBatchMutate`,
partial-index `WHERE` compilation, `CONSISTENCY` modes, `INCLUDE` semantics — is reused
without modification.

## 4. Components

### 4.1 `BsonPathNormalizer`

- Location: `phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/`.
- A pure function that walks a `ParseNode` tree and, whenever it finds a
  `BsonValueParseNode` whose **path literal** (second argument) is a constant string,
  replaces that literal with the canonical form of the same path.
- Normalization rules (v1, JSONPath subset):
  - Strip a leading `$.` or `$` if present (Phoenix's `BSON_VALUE` uses paths *without*
    the `$.` prefix — confirmed by `Bson5IT.java:111` which uses `'rather[3].outline.
    clock'`, no leading `$`). Accept both forms on input; emit no-prefix form.
  - Collapse redundant whitespace inside segments (none legal today, so mostly a no-op).
  - Normalize quoted keys: if a quoted key matches the unquoted-key regex, drop the
    quotes (`['a']` → `.a`); otherwise keep exact bracketed form.
  - Reject wildcards (`*`), filter expressions (`[?(...)]`), recursive descent (`..`),
    and slice syntax (`[a:b]`) with a clear SQLException that identifies the offending
    segment.
- Does **not** touch the first argument (the BSON column) or the third/fourth arguments
  (type, default). Those must match byte-for-byte between DDL and query, as today.
- Does **not** consult schema. Pure syntactic.
- Unit-tested with golden files covering equivalent path pairs.

### 4.2 Fast-path guard in `IndexExpressionParseNodeRewriter`

`IndexExpressionParseNodeRewriter`'s constructor today parses every index column's
expression string (`IndexExpressionParseNodeRewriter.java:62-75`). For a table with ten
BSON-path indexes, that's ten `SQLParser.parseCondition(...)` calls per query.

Change:

- Add a `hasBsonIndex` check on the index `PTable` at construction time. If no index
  column has a `BsonValueParseNode`-rooted expression, skip the BSON normalizer
  invocation on the WHERE clause entirely. This is the M6 fast-path from review.
- Populate the existing `indexedParseNodeToColumnParseNodeMap` with the *already-canonical*
  ParseNode (see §4.1); when normalizing the WHERE clause, look up by canonical form.

### 4.3 Numeric sort order verification + fix

This is the one place where existing production behavior is likely wrong and we must
change real code.

For `BSON_VALUE(doc, 'x', 'DOUBLE')` used as an index key, the write path currently
routes through `BsonValueFunction.evaluate` → `PDouble.INSTANCE.toBytes(double)`. This
uses `Double.doubleToLongBits` raw bytes; they are **not** order-preserving under
unsigned byte comparison across sign boundaries (negatives sort *after* positives). The
same issue applies to `PFloat`. `PInteger`, `PLong`, `PSmallint`, `PTinyint` use
Phoenix's offset-encoded integers which *are* order-preserving. `PDecimal` uses its own
encoding which is order-preserving.

**Action:**

1. Write a unit test that creates a BSON-path index on a DOUBLE path with rows spanning
   negatives and positives, runs `BSON_VALUE(...) BETWEEN -1 AND 1`, and asserts correctness.
2. If the test fails (expected), fix by routing DOUBLE/FLOAT index-key bytes through
   Phoenix's existing order-preserving encoder for those types. This may already exist
   in `IndexUtil.getIndexColumnDataType` / `PDataType.coerceBytes` — must be traced on
   actual execution paths, not assumed.
3. Add an IT that covers all `BSON_VALUE` type codes in a range predicate.

This is the one fix that must happen regardless of everything else — it is a latent
correctness bug, not a feature gap.

### 4.4 Predicate rewrite coverage for range / BETWEEN / IN

Phoenix's scan-range derivation over an indexed column already supports all of `=`, `<`,
`<=`, `>`, `>=`, `BETWEEN`, `IN`, and `!=` — see `WhereCompiler`. The machinery works
once the LHS of the predicate is matched to an index column, which the
`IndexExpressionParseNodeRewriter` does today.

So the work here is not new rewrite code; it is **test coverage and verification** that
canonicalized BSON path predicates flow through existing scan-range derivation for all
predicate forms. Concretely:

- `BsonPathIndexPredicateIT`: for each of `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN`,
  `!=`, assert the plan uses the index and the result set matches a no-index baseline.
  Cover VARCHAR, BIGINT, DOUBLE, DECIMAL, DATE, BOOLEAN paths.
- Known non-matches (must be documented, not fixed in v1): `LIKE`, `IS NULL` / `IS NOT
  NULL` (the latter works today via partial index, *not* via rewrite — a user's explicit
  `IS NOT NULL` predicate hits the index because Phoenix's scan machinery treats
  non-empty key as present; this is the existing behavior in `Bson5IT`), `CAST` wrapping
  the BSON_VALUE on the query side, arithmetic wrappers.

### 4.5 Observability

- `phoenix.index.bson.rewrite.hit` and `phoenix.index.bson.rewrite.miss` — client-side
  counters tagged with `table_name`, `index_name`. Incremented whenever the rewriter
  runs against a table with BSON indexes.
- EXPLAIN plan suffix: when a BSON-path index is matched, append
  ` [BSON path: <canonical-path>, type: <TYPE>]` to the existing RANGE SCAN plan line.
  The existing code path for plan-line generation lives in
  `ExplainPlan` / `ScanPlan.getExplainSteps()`; adding a suffix from IndexMaintainer
  metadata is a small change.
- No new coprocessor counters — nothing new happens on the server side.

### 4.6 Operator sugar (`->` and `->>`), optional separate phase

The reviewer correctly noted this was smuggled in. Separated out: add `->` and `->>`
operators to the ANTLR grammar (`PhoenixSQL.g`), with PG-equivalent semantics:

- `bson_col -> 'field'` → `BSON_VALUE(bson_col, 'field', 'BSON')` (returns sub-document)
- `bson_col ->> 'field'` → `BSON_VALUE(bson_col, 'field', 'VARCHAR')` (returns scalar as
  string — matches PG `->>` behavior)
- Chained: `bson_col -> 'a' -> 'b' ->> 'c'` → `BSON_VALUE(bson_col, 'a.b.c', 'VARCHAR')`.
  Desugaring happens in the parse-tree phase, producing canonical `BsonValueParseNode`.

Owns its own ticket and grammar-review cycle. Not blocking the indexing improvements.

## 5. What does **not** change

- `IndexMaintainer.buildRowKey` — unchanged. No new "is_bson_path" protobuf flag (M6 of
  the review: redundant). No sparse-null skip branch — the existing null-in-index-key
  encoding plus partial-index `WHERE` already gives users both dense and sparse options.
- `IndexRegionObserver` — unchanged. Existing pre-image/post-image logic is already correct.
- `MetaDataClient.createIndex` — unchanged. The `isJsonFragment` guard does not block
  BSON and does not need relaxation. The determinism and stateless gates pass today.
- `SYSTEM.CATALOG` schema — unchanged.
- DDL grammar for `CREATE INDEX` — unchanged. No mandatory `AS <type>` (type is already
  an argument of `BSON_VALUE`). No reserved `USING PATH` keyword in this scope (GIN is a
  separate design; reserve in that design if needed).
- On-disk index format — unchanged.

## 6. Error handling and edge cases

| Situation | Behavior |
|---|---|
| BSON column missing | Existing: `BSON_VALUE` returns default; index encodes null; behavior matches today |
| Path missing in row | Existing: `returnDefaultValue` → empty `ptr`; index encodes null; if user has partial-index `WHERE ... IS NOT NULL`, row is skipped from index |
| Path present, wrong type | Existing: `BsonValueFunction.evaluate` throws `IllegalArgumentException("function data type does not match with actual data type")`. **This aborts the mutation.** (Verified at `BsonValueFunction.java:164-165`.) |
| Unparseable JSONPath at DDL | New: `BsonPathNormalizer` throws SQLException pointing at offending segment |
| Wildcard / filter / recursive descent | New: reject at DDL with SQLException |
| Two indexes defined on equivalent paths (`'a.b'` vs `'$.a.b'`) | New after canonicalization: both canonicalize to `'a.b'`; second `CREATE INDEX` gets existing duplicate-index error |
| Pre-existing index with non-canonical path literal (Mod1 upgrade hazard) | Canonicalize only on new `CREATE INDEX`; leave existing catalog rows alone. Queries still match the non-canonical path string byte-for-byte. |
| Predicate shapes the rewriter doesn't handle | Full scan; document the list explicitly |

The mutation-aborting behavior on type mismatch is a **latent surprise** that the
reviewer flagged (as part of Mod4). Filed as a separate issue to decide whether to keep
throwing, coerce-to-null, or add a new `BSON_VALUE` overload with lenient semantics.
Out of scope for this design — do not change `BsonValueFunction` behavior here.

## 7. Phased delivery

Each phase is one PHOENIX JIRA ticket, mergeable independently, passing all existing
tests. Master is coherent after each phase.

### Phase 0 — Verify the numeric sort-order correctness bug

- Write the reproducer IT described in §4.3. No fix yet.
- If it passes, the bug is not there — update §4.3 to note what's actually happening.
- If it fails, file and prioritize the correctness ticket.
- **Exit:** conclusive pass/fail result in a JIRA, documented.
- **Risk:** none — test-only addition.

### Phase 1 — `BsonPathNormalizer` (unwired)

- Add the normalizer class under `parse/bson/`, package-private.
- Unit tests covering: canonical no-op, `$.` stripping, bracketed/dot form equivalence,
  rejection of unsupported syntax.
- **Exit:** 100% branch coverage on normalizer; zero production callers.
- **Risk:** minimal, additive.

### Phase 2 — Fix numeric sort-order (if Phase 0 confirmed it)

- Route DOUBLE/FLOAT index-key encoding through an order-preserving transform.
- Add per-numeric-type range IT under `BsonPathIndexPredicateIT`.
- Document upgrade implications: existing DOUBLE-path indexes will need a rebuild to
  produce correct scan results. Provide an `ALTER INDEX ... REBUILD` note.
- **Exit:** all-type range-predicate IT passes; rebuild-from-upgrade IT passes.
- **Feature flag:** not applicable — this is a bug fix; gate on a one-time upgrade
  migration that marks existing DOUBLE-path indexes as requiring rebuild.

### Phase 3 — Wire the normalizer into DDL and query rewrite

- `MetaDataClient.createIndex`: call `BsonPathNormalizer` on each indexed parse-node
  before computing `expressionStr`.
- `IndexExpressionParseNodeRewriter`: call `BsonPathNormalizer` on each indexed
  expression after parsing, and on the WHERE clause before map lookup. Add the
  `hasBsonIndex` fast-path guard.
- Existing `Bson5IT` must still pass without modification — its paths already round-trip
  through a no-op canonicalization.
- Add `BsonPathCanonicalizationIT`: same index created two ways (`'a.b'` vs `'$.a.b'`)
  → second fails as duplicate; query with either spelling hits the same index.
- **Feature flag:** `phoenix.index.bson.normalize.enabled`, default `true`. Flip off to
  revert to byte-for-byte matching if the normalizer misbehaves.
- **Exit:** `Bson5IT` green; `BsonPathCanonicalizationIT` green; no perf regression on
  non-BSON-table query benchmarks.

### Phase 4 — Predicate coverage for range / BETWEEN / IN

- `BsonPathIndexPredicateIT`: exhaustive matrix of (predicate type) × (BSON_VALUE output
  type). Assert plan uses index and results match no-index baseline.
- No production code changes expected — existing scan-range derivation handles these
  once the LHS matches. If any predicate form is silently not matching, this phase
  files a follow-up ticket rather than forcing a v1 fix.
- **Exit:** matrix green; documented list of known-non-matching predicate forms.

### Phase 5 — Observability

- Client-side metrics: `phoenix.index.bson.rewrite.hit` / `.miss`, tagged per index.
- EXPLAIN plan suffix: `[BSON path: <path>, type: <TYPE>]` on RANGE SCAN lines over a
  BSON-path index.
- `phoenix-pherf` scenario: write+read mix against a BSON-path index; publish a baseline
  report as an artifact on the phase JIRA.
- **Exit:** metrics surfaced in JMX; EXPLAIN assertions in `BsonPathIndexPredicateIT`;
  perf report attached.
- **Budget:** < 5% query p99 overhead on queries against tables with no BSON indexes
  (the cost of the `hasBsonIndex` check). < 10% write-path p99 overhead on a workload
  with one BSON-path index over a 4KB document. If exceeded, revisit the fast-path.

### Phase 6 (optional, separate ticket) — Operator sugar `->` / `->>`

- Grammar addition in `PhoenixSQL.g`.
- Desugar to `BsonValueParseNode` at parse time; then canonicalization and everything
  else works unchanged.
- Add PG-parity IT.
- **Exit:** operator IT green; grammar ambiguity (overload with arithmetic `>`) resolved
  in the parser.

### Phase 7 (out of scope for this spec)

- GIN-style multi-valued path indexes (separate design).
- Dynamic-column indexing (separate design).
- Optional BSON_VALUE leniency mode for type mismatches (separate ticket).

## 8. Rollback strategy

- Phase 2 (numeric fix): gated on a per-index rebuild. If the fix itself is buggy,
  operators can `ALTER INDEX … DISABLE` and fall back to full scan.
- Phase 3 (normalizer): flag `phoenix.index.bson.normalize.enabled=false` reverts to
  byte-for-byte matching. Existing indexes stay correctly maintained either way.
- Phase 4 (tests): test-only.
- Phase 5 (observability): cosmetic / operator-facing, no runtime impact on correctness.
- Phase 6 (operator sugar): gated on successful ANTLR regeneration; operators can stay
  on master without the grammar bump until confident.

## 9. Testing strategy

- **Unit:** `BsonPathNormalizer` covered by golden-file tests; fast-path check in
  `IndexExpressionParseNodeRewriter` has dedicated tests for the no-BSON-index short
  circuit.
- **Integration:** `BsonPathCanonicalizationIT` (Phase 3), `BsonPathIndexPredicateIT`
  (Phase 4), `BsonPathNumericSortOrderIT` (Phase 0/2), plus continued passage of
  existing `Bson5IT`.
- **Correctness invariant:** for any query `Q` and matching BSON-path index `I`, the
  result set with `I` enabled must equal the result set after `ALTER INDEX I DISABLE`.
  Encoded as a randomized IT.
- **Upgrade test:** create indexes on pre-change master, bounce to post-change master,
  verify queries still match; DOUBLE-path indexes are marked for rebuild.
- **Perf test:** `phoenix-pherf` scenarios for (a) write-path overhead with one BSON
  index on a 4KB doc, (b) query-path overhead on a table with no BSON indexes.

## 10. Compatibility matrix

| Dimension | Supported in v1 | Tested in v1 | Notes |
|---|---|---|---|
| Global index (STRONG) | Yes | Yes (existing) | Default today |
| Global index (EVENTUAL) | Yes | Yes (existing `Bson5IT`) | Production usage today |
| Uncovered global | Yes | Yes (existing) | `Bson5IT` uses this |
| Local index | Yes (behavior unchanged) | New IT | Should work — routing doesn't touch IndexMaintainer |
| Salted tables | Yes (behavior unchanged) | New IT | Salting happens inside `buildRowKey`, unchanged |
| Multi-tenant views | Yes (behavior unchanged) | New IT | No interaction with tenant ID encoding |
| Transactional tables | Yes (behavior unchanged) | New IT | Follows existing Omid flow |
| Covered (`INCLUDE`) | Yes | Yes (existing) | `Bson5IT.java:378` |
| Partial (`WHERE path IS NOT NULL`) | Yes | Yes (existing) | Preferred sparse-index idiom |
| CDC interaction | Yes (behavior unchanged) | Yes (existing) | `Bson5IT` exercises this |

## 11. Open questions

1. Is the DOUBLE/FLOAT sort-order issue actually present? Phase 0 resolves. If yes, we
   are shipping a bug fix, not a feature.
2. Should `BsonPathNormalizer` be applied retroactively to existing catalog rows at
   upgrade time, or only to new indexes? Proposal: only to new indexes. Offer a manual
   rebuild admin command for users who want deduplication of existing equivalent indexes.
3. Should path-match go beyond string canonicalization to semantic equivalence (e.g.,
   indexing `"a.b"` and querying `"['a']['b']"`)? Proposal: yes, within the JSONPath
   subset. That's exactly what normalization delivers.
4. For the operator sugar in Phase 6, is PG's `->>` semantics (scalar-to-string) correct
   for Phoenix's BSON type system, or do we want `->>` to return the natural SQL type
   (i.e., always route through the three-arg BSON_VALUE with an inferred type)? Needs a
   separate discussion on the mailing list.

## 12. Key file references

(Pinned to method names rather than line numbers where possible, since line numbers rot.)

- `phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java` —
  `createIndex`; the `isJsonFragment` check **need not** be relaxed.
- `phoenix-core-client/src/main/java/org/apache/phoenix/compile/ExpressionCompiler.java`
  — `visitEnter(FunctionParseNode)` sets `isJsonFragment`; only triggers on
  `JsonQueryParseNode` / `JsonModifyParseNode`.
- `phoenix-core-client/src/main/java/org/apache/phoenix/expression/function/BsonValueFunction.java`
  — four-arg signature `(doc, path, type [, default])`; already handles type coercion
  and missing-path defaults.
- `phoenix-core-client/src/main/java/org/apache/phoenix/parse/BsonValueParseNode.java`
  — target node for canonicalization.
- `phoenix-core-client/src/main/java/org/apache/phoenix/index/IndexMaintainer.java` —
  `buildRowKey` already supports null index keys; do not modify.
- `phoenix-core-server/src/main/java/org/apache/phoenix/hbase/index/IndexRegionObserver.java`
  — `generateIndexMutationsForRow` already handles pre-image/post-image diff; do not
  modify.
- `phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexExpressionParseNodeRewriter.java`
  — hook for new canonicalization + fast-path guard.
- `phoenix-core-client/src/main/java/org/apache/phoenix/compile/IndexStatementRewriter.java`
  — downstream consumer of the parse-node map.
- `phoenix-core-client/src/main/java/org/apache/phoenix/schema/types/IndexConsistency.java`
  — STRONG / EVENTUAL enum; both supported.
- `phoenix-core/src/it/java/org/apache/phoenix/end2end/Bson5IT.java` — must continue to
  pass unchanged as the regression reference.
- `phoenix-core-client/src/main/antlr3/PhoenixSQL.g` — touched only by Phase 6 (operator
  sugar).
