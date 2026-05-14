# Design: Functional Secondary Indexes on BSON/JSON Path Expressions

**Status:** Draft
**Date:** 2026-05-05
**Author:** nlakshmanan (persona brainstorm)
**Scope:** Apache Phoenix 5.x — master branch

## Summary

Enable Phoenix users to create secondary indexes over BSON path expressions — analogous
to PostgreSQL expression indexes on `jsonb` columns — and make the query optimizer
actually use them by canonicalizing equivalent path-expression forms in `WHERE` clauses
before matching them against indexed expressions.

Goal in one line: `CREATE INDEX idx ON t(BSON_VALUE(doc, '$.a.b') AS VARCHAR)` should
cause `SELECT * FROM t WHERE doc->>'a.b' = 'x'` to do an index lookup, not a full scan.

## Non-Goals (v1)

- **Dynamic-column indexing** — indexes over per-row dynamic columns that are not in the
  base schema. Deferred to a separate design.
- **Multi-valued / GIN-style path indexes** — one BSON document producing many index
  entries. DDL surface is reserved (`USING PATH`) but the feature is not implemented.
- **Containment (`@>`) predicates.** B-tree-style typed path indexes only.
- **Async-build, local, and eventually-consistent variants.** v1 supports synchronous
  global indexes only. Other consistency modes follow in later tickets.
- **Wildcards, filter expressions, or recursive descent in JSONPath.** See "Path Language."
- **`IS NULL` predicate rewrite.** Sparse index semantics mean missing paths have no
  index entry; `IS NULL` cannot be served by the index.

## Motivation

Phoenix already supports expression-based index keys (`IndexKeyConstraint` carries a list
of `ParseNode`s), already ships a BSON type (`PBson`) and path-navigation builtins
(`BSON_VALUE`, `BSON_VALUE_TYPE`, `->`, `->>`). Three gaps block the feature today:

1. `MetaDataClient.createIndex` rejects any index expression containing a "JSON fragment"
   (`MetaDataClient.java:1735`). This blanket guard is the first blocker.
2. The predicate-to-index matcher (`IndexExpressionParseNodeRewriter`) is exact-AST-match.
   A user's `doc->>'a.b'` will not match an index defined on `BSON_VALUE(doc, '$.a.b')`,
   even though they are semantically identical.
3. There is no canonical internal representation of a "BSON path," so the same path can be
   spelled many ways and catalog metadata cannot deduplicate equivalent expressions.

This design closes all three gaps using the existing sync-global index machinery and adds
one new internal value type (`BsonPath`) that serves as the canonical form.

## Design Decisions (recorded from brainstorming)

| Decision | Choice |
|---|---|
| Scope | JSON/BSON expression indexes + predicate rewrite. Dynamic-column indexes deferred. |
| Index-key model | Typed single-valued (B-tree-like) in v1; `USING PATH` grammar reserved for a future multi-valued variant. |
| Predicate rewrite aggressiveness | Canonicalize operator sugar + path string; support `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN`. No algebraic simplification. No `IS NULL` rewrite. |
| Null / missing-path handling | Sparse: if the indexed expression evaluates to `null`, no index entry is written for that row. |
| Type mismatch handling | Same as null (sparse skip). No UPSERT failure — semi-structured data tolerates heterogeneity. |
| Consistency mode | Sync global only in v1. |
| Covering | Honor existing `INCLUDE` clause. Uncovered by default. |
| Type declaration | **`AS <type>` is mandatory** on BSON-path index expressions. No defaulting. |
| Path language | JSONPath subset: dot-notation, array index, quoted keys. No wildcards, filters, or recursive descent. |

## Architecture

```
                    ┌────────────────────────────────────────┐
     DDL / DML      │  Parser ──► BsonPathCanonicalizer ──►  │
     queries        │    │                 │                 │
                    │    ▼                 ▼                 │
                    │  ParseNode tree   Canonical BsonPath ──┼──► stored in SYSTEM.CATALOG
                    │                                        │        (PColumn.expressionStr,
                    │                                        │         normalized)
                    └────────────────────────────────────────┘
                                         │
                                         ▼
                         ┌──────────────────────────────────┐
   Write path (UPSERT)   │  IndexMaintainer.buildRowKey     │
                         │   evaluate(expression, row)      │
                         │   null ⇒ sparse-skip             │
                         └──────────────────────────────────┘

                         ┌──────────────────────────────────┐
   Read path (SELECT)    │  QueryOptimizer                  │
                         │   IndexStatementRewriter         │
                         │     ├─ canonicalize WHERE clause │
                         │     └─ AST-match vs. index expr  │
                         └──────────────────────────────────┘
```

The write path, on-disk format, and coprocessor contracts are unchanged. The feature is
implemented entirely as (a) a canonical form for BSON path expressions, (b) a tightly
scoped relaxation of the JSON guard in `MetaDataClient`, (c) a one-line sparse-null branch
in `IndexMaintainer.buildRowKey()`, and (d) a canonicalization pass in
`IndexStatementRewriter`.

## Components

### 1. `BsonPath` — internal canonical path type

- **Location:** `phoenix-core-client/src/main/java/org/apache/phoenix/schema/types/BsonPath.java`.
- **Nature:** Immutable value class. Parsed from a path string (`$.a.b[0]['k']`). Carries a
  structural representation (list of segments: field, array-index, quoted field).
- **Not a SQL column type.** It is never exposed as a user-visible `PDataType`. It exists
  to serve as the canonical form inside `BsonPathParseNode` and as the argument normalizer
  for `BSON_VALUE`.
- **Equality:** structural. `$.a.b` and `$."a"."b"` compare equal. `toString()` emits the
  canonical spelling (dot-notation where legal, bracket-quoted otherwise).

### 2. `BsonPathParser`

- **Location:** `phoenix-core-client/src/main/java/org/apache/phoenix/parse/bson/`.
- Accepts the JSONPath subset: dot segments (`$.a`), array indices (`[0]`), quoted keys
  (`['weird key']`, `["esc\"key"]`). Leading `$.` optional.
- Rejects with a specific `SQLException`: wildcards (`$.*`, `$[*]`), filters (`$[?(...)]`),
  recursive descent (`$..x`), slice syntax (`$[0:2]`).

### 3. `BsonPathCanonicalizer`

- **Location:** `phoenix-core-client/src/main/java/org/apache/phoenix/compile/`.
- **Signature:** `ParseNode rewrite(ParseNode input)`.
- **Inputs recognized (all rewritten to `BSON_VALUE(doc, BsonPath('$.a.b'))`):**
  - `BSON_VALUE(doc, '$.a.b')` — no-op, validate path.
  - `BSON_VALUE(doc, 'a.b')` — missing leading `$.`.
  - `doc -> 'a' -> 'b'` and `doc -> 'a' ->> 'b'`.
  - `doc ->> 'a.b'` (Postgres-style single-string path).
- **Contract:** pure function; does not consult schema; safe to run on any `ParseNode`.
- **Non-recognized shapes** (function-wrapped LHS, `CAST(BSON_VALUE(...) AS ...)` on the
  WHERE side) pass through unchanged — the query will simply not match a BSON-path index.

### 4. DDL relaxation — `MetaDataClient.createIndex`

- Replace the blanket `isJsonFragment` rejection at `MetaDataClient.java:1735` with a
  **deterministic-function allowlist**:
  - Allowed on index expressions: `BSON_VALUE`, `BSON_VALUE_TYPE`, `->`, `->>`, and their
    canonical form.
  - Any other JSON/BSON function: rejected with the existing error.
- Determinism check at `MetaDataClient.java:1741` is unchanged — `BSON_VALUE` and operator
  sugar are `PER_ROW` deterministic, which passes the existing gate.
- Canonicalization: the compiled `Expression` for the index column is rewritten through
  `BsonPathCanonicalizer` before being persisted into `SYSTEM.CATALOG`. This guarantees
  catalog-level deduplication of equivalent expressions.

### 5. DDL grammar — mandatory `AS <type>` and reserved `USING PATH`

- Extend the `indexed_column` grammar rule so that when the expression contains a
  recognized BSON path operator/function at the top level, an `AS <SQL type>` clause is
  **required**. Missing the clause raises a compile-time error:
  `"BSON path index expressions require explicit type: AS VARCHAR|BIGINT|..."`.
- `AS <type>` applies a `CAST` to the evaluated expression before it becomes the index key.
  Coerce failures fall through to sparse-null handling (see §6).
- Parse but reject `CREATE INDEX ... USING PATH (...)`: reserved for a future multi-valued
  variant. Error: `"USING PATH is reserved for a future release."`

### 6. Sparse-null in `IndexMaintainer.buildRowKey`

- In the existing per-expression evaluation loop (around `IndexMaintainer.java:840-870`),
  after `expression.evaluate(...)` returns, if the resulting `ptr.getLength() == 0` AND the
  expression is marked as a BSON-path expression (a new per-column flag on
  `IndexMaintainer`'s serialized metadata), **skip this row's index update** — do not emit
  a put or a delete to the index region.
- Why gated on a flag: today a null result in a normal expression index produces a
  `null`-keyed entry. We preserve that behavior for non-BSON expressions (backward
  compatibility).

### 7. Predicate rewrite — `IndexStatementRewriter`

- Before `IndexStatementRewriter` runs its exact-AST map lookup, invoke
  `BsonPathCanonicalizer` on both (a) every indexed expression in the index metadata
  (once, on load) and (b) each predicate `ParseNode` in the `WHERE` clause.
- Canonical LHS → look up in existing `indexedParseNodeToColumnParseNodeMap`.
- Supported predicate shapes in v1 (RHS must be a literal or bind parameter):
  - `<canonical_lhs> = <literal>`
  - `<canonical_lhs> IN (<literal>, ...)`
  - `<canonical_lhs> <|<=|>|>= <literal>`
  - `<canonical_lhs> BETWEEN <literal> AND <literal>`
- Unsupported shapes fall through to existing behavior (full scan).
- Feature flag: `phoenix.index.bson.rewrite.enabled` (default `true`).

### 8. Metadata & on-disk format

- **No change** to `SYSTEM.CATALOG` schema.
- `PColumn.getExpressionStr()` stores the canonicalized BSON-path expression string. Two
  indexes defined on `doc->>'a.b'` and `BSON_VALUE(doc, '$.a.b')` will collide on DDL (both
  canonicalize to the same string) — this is the correct behavior; user gets a duplicate-
  index error.
- IndexMaintainer protobuf: add one boolean per indexed expression (`is_bson_path`) to
  drive sparse-null handling. Adding an optional protobuf field is backward-compatible.

## Data flow

### CREATE INDEX

```
SQL ──► ANTLR parser ──► CreateIndexStatement (raw ParseNode)
         │
         ▼
    MetaDataClient.createIndex
         │  ├─ allowlist check (BSON_VALUE / -> / ->>)
         │  ├─ determinism check (unchanged)
         │  ├─ AS <type> presence check
         │  ├─ BsonPathCanonicalizer.rewrite(indexed expression)
         │  ├─ compile to Expression (CAST to declared type)
         │  └─ persist canonical expressionStr + is_bson_path=true to SYSTEM.CATALOG
```

### UPSERT (index maintenance)

```
Client mutation ──► RegionServer (IndexRegionObserver)
   │
   ▼
IndexMaintainer.buildRowKey(row)
   for each indexedExpression:
       ptr ← expression.evaluate(row)
       if (is_bson_path AND ptr.length == 0)  ── skip row for this index
       else append ptr bytes to index row key
```

### SELECT with WHERE on BSON path

```
SQL ──► parser ──► QueryCompiler
                      │
                      ▼
                QueryOptimizer
                      │
                      ▼
              IndexStatementRewriter
                 ├─ canonicalize WHERE parse nodes
                 └─ match canonical LHS against indexedParseNodeToColumnParseNodeMap
                      │
                      ▼
                 build index scan ranges (existing)
```

## Error handling

| Situation | Behavior |
|---|---|
| Unparseable JSONPath at DDL | Compile error, specific message pointing to the offending segment |
| Wildcard / filter / recursive descent in path at DDL | Compile error, "feature not supported in v1" |
| Missing `AS <type>` on BSON-path index expression | Compile error |
| `USING PATH` DDL clause | Compile error, "reserved for future release" |
| Path absent in a row at UPSERT | Row is sparse-skipped in that index; UPSERT succeeds |
| Value at path cannot coerce to declared type at UPSERT | Row is sparse-skipped in that index; UPSERT succeeds |
| Duplicate index: two expressions canonicalize to same string | Existing "index already exists" path — correct behavior |
| Rewriter bug makes index miss a query | Feature flag `phoenix.index.bson.rewrite.enabled=false` disables rewrite; queries fall back to full scan; index remains maintained and correct |

## Observability

- `phoenix.index.bson.sparse_skips` — coprocessor-side counter, per index table, for rows
  whose expression evaluated to null/failed-coerce and were skipped.
- `phoenix.index.bson.rewrite.hits` and `.misses` — client-side counter incremented when
  the rewriter attempts canonical matching.
- EXPLAIN plan output: when a query hits a BSON-path index, the plan line should say
  `CLIENT PARALLEL N-WAY RANGE SCAN OVER <INDEX> [BSON path: $.a.b]`.

## Phased Delivery Plan

Each phase is a separate PHOENIX JIRA ticket, mergeable independently, with its own test
suite. The master branch is in a coherent state after each phase.

### Phase 0 — `BsonPath` value type + `BsonPathParser`

- Add `BsonPath` class and `BsonPathParser` with package-private visibility.
- Unit tests: parser accepts all valid forms; rejects wildcards / filters / recursive
  descent; fuzz test ~10k random strings for no crashes.
- **Exit criteria:** 100% branch coverage on parser; zero production callers.
- **Risk:** minimal — additive code, no wiring.

### Phase 1 — `BsonPathCanonicalizer` (unwired)

- Pure rewriter: `ParseNode → ParseNode`.
- Handles `->`, `->>`, `BSON_VALUE`, mixed forms.
- **Exit criteria:** ~50 golden-file unit tests covering operator sugar, nested paths,
  array indexing, quoted keys, no-op cases; still not invoked from any compile path.

### Phase 2 — Enable BSON-path functional indexes for **writes**

- Relax `MetaDataClient.createIndex` guard to the allowlist.
- Require `AS <type>` on BSON-path index expressions.
- Canonicalize on DDL; persist canonical expression string.
- Add `is_bson_path` protobuf field on IndexMaintainer metadata.
- Add sparse-null branch in `IndexMaintainer.buildRowKey()`.
- Feature flag: `phoenix.index.bson.enabled` (default `true`).
- **User-visible state after Phase 2:** `CREATE INDEX` on a BSON path succeeds; UPSERTs
  correctly populate the index; **queries do not yet use it** (rewriter not wired).
- **Exit criteria:**
  - `BsonPathIndexWriteIT`: create index, upsert rows with varying path presence/types,
    direct-scan the index table, assert expected rows/skips.
  - Consistency invariant: index content matches a from-scratch `ALTER INDEX REBUILD`.
  - Rollback test: flip the flag off, create index fails with existing error.

### Phase 3 — Wire canonicalizer into predicate rewrite (queries hit indexes)

- Invoke `BsonPathCanonicalizer` in `IndexStatementRewriter` before AST matching.
- Support `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN`.
- Feature flag: `phoenix.index.bson.rewrite.enabled` (default `true`).
- **User-visible state after Phase 3:** feature works end-to-end.
- **Exit criteria:**
  - `BsonPathIndexQueryIT`: EXPLAIN assertions for each predicate form.
  - Randomized correctness IT (`BsonPathIndexConsistencyIT`): same queries with/without
    the index must return identical result sets across 1k random queries.
  - Negative tests: `LIKE`, `CAST` on LHS, non-literal RHS → full scan, index not used.

### Phase 4 — DDL ergonomics + v2 reservation

- Polish the `AS <type>` error messages.
- Reserve `USING PATH` keyword in the grammar with a specific compile-time rejection.
- Update `phoenix-core/src/it/resources/.../explain/*.md` goldens.
- **Exit criteria:** grammar test coverage; rejection test for `USING PATH`.

### Phase 5 — Observability + operator polish

- Metrics: `phoenix.index.bson.sparse_skips`, `.rewrite.hits`, `.rewrite.misses`.
- `phoenix-pherf` scenario for BSON path index write/read mix; publish a baseline report.
- Docs page on phoenix.apache.org with examples and PG JSONB parity table.
- **Exit criteria:** perf report attached to the phase JIRA showing write-path overhead
  within agreed budget on a representative scenario.

### Phase 6 (future, out of scope for this spec)

Multi-valued / GIN-style path indexes delivered via the `USING PATH` DDL reserved in
Phase 4. Requires new IndexMaintainer fan-out semantics and is a standalone design.

## Rollback strategy

- Phase 2: `phoenix.index.bson.enabled=false` — new BSON-path `CREATE INDEX` rejected;
  existing such indexes (if any) continue to be maintained by the coprocessor.
- Phase 3: `phoenix.index.bson.rewrite.enabled=false` — indexes stay maintained; queries
  do not use them. No data loss.
- Phase 4/5: cosmetic; no runtime impact.

## Testing strategy

- **Unit:** Phases 0, 1 are almost entirely unit-tested. Canonicalizer is a pure function
  and gets golden-file coverage.
- **Integration (IT):** `BsonPathIndexWriteIT`, `BsonPathIndexQueryIT`,
  `BsonPathIndexConsistencyIT` under `phoenix-core/src/it/`.
- **Correctness invariant** (checked in Phase 2 onward): for any query `Q`, the result set
  with the BSON index enabled equals the result set after `ALTER INDEX idx DISABLE`.
  Encoded as a randomized test generating random BSON shapes, random queries, comparing
  result sets.
- **EXPLAIN plan assertions:** Phase 3 tests assert the plan line names the BSON index and
  shows the canonical path.
- **Upgrade test:** create the index on master, bounce, verify maintained correctly and
  can be rebuilt after a rolling restart.
- **Negative tests:** every rejected DDL shape, every predicate shape that should NOT use
  the index.

## Open Questions

1. Should `AS DECIMAL(p, s)` be supported in the mandatory `AS <type>` clause, or only
   primitive SQL types? — proposed: yes, any `PDataType` that BSON_VALUE's result can
   coerce to.
2. Should the rewriter handle `CAST(doc->>'a' AS BIGINT) = 123` in addition to
   `doc->>'a' = '123'`? — proposed: no in v1. Users must align WHERE-side types with the
   indexed expression's declared type, same as existing Phoenix functional indexes.
3. How do we surface the canonicalizer's internal decisions in EXPLAIN? — proposed: one
   new line prefix `CLIENT BSON PATH MATCH:` when the rewriter normalizes a predicate.

## Appendix — Key file references

- `phoenix-core-client/src/main/java/org/apache/phoenix/parse/CreateIndexStatement.java`
- `phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexKeyConstraint.java`
- `phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java` — `createIndex` around L1720–L1786; JSON-fragment rejection at L1735
- `phoenix-core-client/src/main/java/org/apache/phoenix/index/IndexMaintainer.java` — `buildRowKey` around L770–L870
- `phoenix-core-client/src/main/java/org/apache/phoenix/compile/IndexExpressionParseNodeRewriter.java`
- `phoenix-core-client/src/main/java/org/apache/phoenix/compile/IndexStatementRewriter.java`
- `phoenix-core-client/src/main/java/org/apache/phoenix/schema/types/PBson.java`
- `phoenix-core-client/src/main/java/org/apache/phoenix/expression/function/BsonValueFunction.java`
- `phoenix-core-server/src/main/java/org/apache/phoenix/hbase/index/IndexRegionObserver.java`
- `phoenix-core-client/src/main/java/org/apache/phoenix/optimize/QueryOptimizer.java`
