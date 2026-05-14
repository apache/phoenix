# Design Review Feedback — BSON/JSON Path Functional Indexes

**Reviewed spec:** `docs/superpowers/specs/2026-05-05-bson-path-functional-indexes-design.md`
**Reviewer:** Senior Apache Phoenix / HBase committer perspective
**Date:** 2026-05-05

The core architecture — canonical `BsonPath` + existing `IndexMaintainer` + rewriter-side
canonicalization — is sound and worth keeping. The spec just needs to be grounded in
actual Phoenix behavior rather than assumed behavior before it's ready for dev@.

Feedback is ordered by severity.

---

## Major problems

### M1. The motivation's cornerstone claim is wrong

The spec says `MetaDataClient.createIndex` rejects "any index expression containing a JSON
fragment" at L1735 (motivation #1). Verified against
`ExpressionCompiler.java:313-315`:

```java
if (node instanceof JsonQueryParseNode || node instanceof JsonModifyParseNode) {
  this.isJsonFragment = true;
}
```

The guard triggers **only** on `JSON_QUERY` and `JSON_MODIFY` — **not** on
`BsonValueFunction`, `JsonValueFunction`, `BsonValueTypeFunction`, or any other BSON
function. So the premise that a blanket guard blocks BSON-path indexes today is
incorrect; `CREATE INDEX ... (BSON_VALUE(doc, '$.a.b'))` probably already parses and
compiles. The real motivation is different: **there is no canonical form for
predicate-matching, and there is no type-safe key-extraction surface**. The spec must be
re-anchored on those, not on a guard that doesn't exist.

Fix: rewrite Motivation #1. Keep the allowlist idea (for `JSON_QUERY` / `JSON_MODIFY`,
which really are blocked), but stop calling it "the first blocker."

### M2. Phoenix has no `->` or `->>` operators

The spec repeatedly treats `doc->'a'->'b'` and `doc->>'a.b'` as user-facing syntax Phoenix
already accepts. Grepping the ANTLR grammar and `FunctionParseNode` / operator classes
returns nothing for arrow operators. Phoenix surfaces BSON navigation only through
`BSON_VALUE(doc, path)` / `BSON_VALUE_TYPE` / `BSON_CONDITION_EXPRESSION`.

Consequence: half of the `BsonPathCanonicalizer`'s declared inputs are grammar-extensions
in disguise, not canonicalization. Adding `->` / `->>` is a separate grammar change with
real design questions (operator precedence, overload with arithmetic `>`, PG-compat
semantics, dictionary-vs-array semantics of `->>`).

Fix: either (a) drop `->` / `->>` from v1 scope and canonicalize only across `BSON_VALUE`
call-shape variants (path with/without `$.`, equivalent quoting), or (b) add an explicit
"Grammar additions" section that owns the arrow-operator design. Don't hide it inside the
canonicalizer.

### M3. Sparse-null semantics are incorrect for UPDATEs and DELETEs

Section 6 ("Sparse-null in buildRowKey"):

> if the resulting `ptr.getLength() == 0` ... **skip this row's index update** — do not
> emit a put or a delete to the index region.

This is a correctness bug. Scenario:

1. Row has `doc = {"a":{"b":"x"}}`. Index has entry `("x", pk)`.
2. UPSERT changes `doc` to `{"a":{"c":"y"}}` — path `$.a.b` is now absent.
3. Under the spec's rule: we see empty `ptr`, emit no delete → **stale index entry
   `("x", pk)` is left behind forever.**

Correct semantic: on sparse transition to absent, the index entry from the pre-image
**must** be deleted. For transition from absent to present, emit a put. The write path
must read the pre-image (which sync global already does in `IndexRegionObserver`),
evaluate the path on both pre- and post-image, and emit:

| Pre-image | Post-image | Action |
|---|---|---|
| present, value v1 | present, value v2 (≠ v1) | delete v1, put v2 |
| present, value v | present, value v | no-op |
| present, value v | absent | delete v |
| absent | present, value v | put v |
| absent | absent | no-op |

Spec needs a new subsection "Index maintenance under update/delete" that spells this out,
and the phase-2 IT must cover all five transitions.

### M4. `ptr.getLength() == 0` is the wrong null signal

In Phoenix's scan machinery an empty `ImmutableBytesWritable` is sometimes null and
sometimes a legitimate zero-length value (e.g., empty `VARCHAR`). Using length 0 to mean
"absent path" loses the distinction between `$.a = ""` and `$.a` missing, and the
canonical form's return type (VARCHAR) makes empty-string a legitimate row that the index
should cover.

Fix: use an explicit `NULL` tri-state. `BsonValueFunction.evaluate` needs to distinguish
"path missing / coerce failed" (→ don't index) from "path resolved to empty string" (→
index as empty string). Currently `BsonValueFunction` doesn't do that — another reason
the write path is more than a "one-line branch."

### M5. PostgreSQL `->>'a.b'` is not a nested path

The canonicalizer claims `doc ->> 'a.b'` canonicalizes to `BSON_VALUE(doc, '$.a.b')`. In
PG, `->>` takes a single key, so `doc->>'a.b'` looks up a field whose literal name is
`a.b`, not a nested path. If Phoenix adopts `->>` with PG semantics (which M2 hasn't
decided), this rewrite is semantically wrong; if Phoenix invents its own `->>` that splits
on `.`, that's fine but must be documented as a deliberate divergence. Either way the
current spec conflates the two.

### M6. Predicate rewriter runs on every query's WHERE — latency tax

`IndexStatementRewriter` fires on every select. Adding BSON canonicalization to the hot
path means every query pays a cost for a feature most tables don't use.

Fix: fast-path — skip canonicalization if the statement's resolved tables have no BSON
columns, or no BSON-path indexes. Spec should acknowledge this and call it out as a
required optimization, not "wire it in."

### M7. `isStateless()` and `getDeterminism()` are unverified assumptions

Spec: "`BSON_VALUE` and operator sugar are `PER_ROW` deterministic." `BaseExpression
.getDeterminism()` defaults to `Determinism.ALWAYS` and `BsonValueFunction` doesn't
override. `ALWAYS` passes the gate, but the spec's wording ("PER_ROW deterministic") is
factually off and also suggests the author didn't check. More important:
`MetaDataClient:1751` also rejects `expression.isStateless()` expressions. A `BSON_VALUE
(doc, '$.a.b')` with the path as a literal string — is the expression itself considered
stateless by Phoenix's definition? Needs verification. If it's stateless, the allowlist
relaxation isn't enough.

Fix: run a 20-minute spike — try `CREATE INDEX idx ON t(BSON_VALUE(doc, '$.a.b'))` today,
see what actually fails. Rewrite the motivation based on what actually fails, not what the
spec assumes.

---

## Moderate problems

### Mod1. Duplicate-index deduplication is an upgrade hazard

Because `PColumn.getExpressionStr()` stores the canonical form, two pre-existing indexes
that today have distinct expression strings (e.g., one created with `'a.b'`, one with
`'$.a.b'`) will, after upgrade, canonicalize to the same string. Behavior is unspecified:
SYSTEM.CATALOG PK collision, silent drop of the second, or a new deduplication error on
upgrade?

Fix: describe the upgrade path explicitly. Options: (a) canonicalize only on new CREATE
INDEX, leave old entries alone; (b) run a one-shot metadata migration at upgrade time
with conflict detection.

### Mod2. Index maintenance read amplification for partial UPSERTs

Phoenix UPSERTs can touch a subset of columns. If the user UPSERTs a column other than
`doc`, index maintenance still needs to know whether `doc`'s current value at the path
matches the previous index entry. Sync global does pre-image reads for expression indexes
today — but BSON pre-image reads fetch potentially large cells per index. The spec
doesn't cost this or discuss the pattern. Users will see p99 write latency rise on wide
BSON documents.

Fix: add a "Write-path performance" subsection. At minimum, name the read amplification;
at best, provide an INCLUDE-style opt-out (maintain lazily via async rebuild when the
BSON cell isn't in the mutation).

### Mod3. INCLUDE of a large BSON doc isn't free

Section 8 says "honor existing INCLUDE clause." INCLUDE copies the cell bytes into the
index region. For a 1MB BSON doc with 10 BSON-path indexes, that's 10MB of index payload
per row. Phoenix has no existing size guardrail.

Fix: spec should either warn + cap, or say explicitly "this is the user's choice" and
provide a `phoenix.index.max_include_cell_size` safety valve.

### Mod4. Sort order mismatch between SQL type and BSON native type

The user-declared `AS BIGINT` or `AS VARCHAR` drives comparison, but the underlying BSON
value at the path may not be that type. With sparse-skip on coerce failure, correctness is
preserved — but users writing `WHERE BSON_VALUE(doc, '$.x') > 10` on an `AS VARCHAR`
index will get string lexicographic order, not numeric order, and queries like `WHERE
... > '9'` will return rows where `$.x = '11'`. This is PG-compatible behavior but users
are going to file it as a bug.

Fix: spec should mandate a warning in the EXPLAIN output when an inequality predicate
runs over a VARCHAR-typed BSON path index; and/or recommend indexing numeric paths
explicitly as BIGINT / DECIMAL.

### Mod5. Feature-flag granularity

`phoenix.index.bson.rewrite.enabled` as a global flag. One rewriter bug disables the
feature cluster-wide. A per-index property (stored in `SYSTEM.CATALOG` as a table-level
option like `DISABLE_ON_REWRITE='TRUE'`) lets an operator surgically park one index.

### Mod6. `is_bson_path` protobuf flag is redundant

If the `Expression` in IndexMaintainer is known to be rooted at `BsonValueFunction`
(post-canonicalization), the IndexMaintainer can inspect the expression directly. Adding
a protobuf bit adds a second source of truth that can drift. Simpler: make the behavior
fall out of `expression instanceof BsonValueFunction` (or a marker interface
`ISparseIndexed`).

---

## Minor / nits

### N1. Line numbers will rot

`MetaDataClient.java:1735`, `IndexMaintainer.java:840-870` — both correct as of today's
master, but specs age poorly when pinned to line numbers. Reference by method name
(`MetaDataClient.createIndex` — the `isJsonFragment` check) instead.

### N2. `BsonPath` location

`phoenix-core-client/.../schema/types/` is a `PDataType` home. `BsonPath` is not a
`PDataType`. Move to `parse/bson/` (where the parser lives) or `util/bson/`.

### N3. `AS <type>` is grammar, not "extending indexed_column rule"

Section 5 calls it "extending the rule." Be explicit: this is a grammar change to
`CreateIndexStatement.g` (or equivalent) that introduces a new optional (for regular
columns, mandatory for BSON-path expressions) `AS <PDataType>` production.

### N4. Phase 4 reserves `USING PATH` after Phase 3 has already shipped the feature

Reserving a keyword after users are live is mildly risky (compatibility surface). Move
keyword reservation into Phase 2 so it lands with the DDL changes, before anyone writes
queries against the feature.

### N5. Phase 5 "within agreed budget" with no budget

Pick a number now. Suggest: < 10% write-path p99 overhead on a workload with a single
BSON-path index on a 4KB BSON doc, and < 5% query p99 overhead on queries against
non-BSON-indexed tables (the tax from M6).

### N6. No mention of salted tables, multi-tenant views, transactional tables, local indexes

Phoenix has four orthogonal index modes + salting + tenant views. Spec needs a
compatibility matrix even if the answer for several is "not supported in v1" — currently
they're simply invisible.

### N7. `phoenix.index.bson.sparse_skips` needs a per-index dimension

A single cluster-wide counter doesn't help an operator debugging one misbehaving index.
Tag the metric with `table` and `index`.

### N8. Diagrams repeated

The Architecture diagram and the Data-flow diagrams say the same thing twice. Collapse.

### N9. "Zero production callers" as Phase 0 exit criteria

Good, but also add: no references from `main` source at all — the class should be
package-private and unused until Phase 2.

### N10. No rollback story for Phase 2 past the flag

If a bad canonicalization in Phase 2 writes garbage into `SYSTEM.CATALOG.EXPRESSION_STR`,
flipping the flag off doesn't heal the catalog. State the recovery: drop and re-create
the index, or run an admin tool.

---

## What the spec got right

- **Scope discipline:** explicitly deferring dynamic-column indexes and GIN-style is the
  right call.
- **Incremental phasing** with per-phase feature flags and IT coverage is the kind of
  thing reviewers will accept on the dev list.
- **Reusing `IndexMaintainer` / sync-global machinery** is correct — the approach is
  Phoenix-idiomatic.
- **Mandatory `AS <type>`** is a strong choice; avoids PG's "surprised by string
  comparison on jsonb_path_ops" footgun.

---

## Recommended revision order

1. **Run the 20-minute spike (M1, M7).** Try `CREATE INDEX idx ON t(BSON_VALUE(doc,
   '$.a.b'))` on master. Find out what actually fails. Rewrite Motivation and Section 4
   on the basis of facts.
2. **Fix the write-path semantics (M3, M4, Mod2).** Add the pre-image/post-image
   transition table and the correct null signal.
3. **Decide arrow operators explicitly (M2, M5).** Either drop from scope or carve a
   separate "grammar additions" section.
4. **Add the fast-path optimization for the rewriter (M6).**
5. **Add the upgrade/migration section (Mod1).**
6. **Fix the minors.**
