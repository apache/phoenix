# Phase 5 — Observability + Docs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add lightweight client-side counters so operators can see when BSON-path index
canonicalization fires, when sparse-skip happens, and write a short user-facing doc page.

**Architecture:** Two `AtomicLong` counters in a static `BsonPathMetrics` class, wired to (a) the
predicate-rewrite hit/miss path in `IndexExpressionParseNodeRewriter`, and (b) the sparse-skip
branch in `IndexMaintainer.buildRowKey`. Counters are best-effort, JMX-discoverable via a simple
MBean registration. (We deliberately do **not** plumb into Phoenix's `MetricInfo` enum — that's
a heavier change and needs design alignment with the metrics owner.)

**Tech Stack:** Java `java.util.concurrent.atomic.AtomicLong`, optional JMX `MBeanServer`
registration, Markdown docs.

---

## Calibration vs. spec

- The spec called out coprocessor-side metrics (`phoenix.index.bson.sparse_skips`). Phoenix's
  current write path runs `IndexMaintainer` on the client (when the user is using sync global
  indexes via Phoenix's `IndexCommitter`), so a client-side counter is appropriate. If the
  metrics owner wants to promote these to `MetricInfo` later, it's mechanical.
- The "perf scenario via phoenix-pherf" is descoped from this plan because it requires its own
  scenario design, baseline run, and a non-trivial review. We document the missing piece in the
  follow-on note.
- Documentation goes into `docs/` (the existing markdown landing area), not the Phoenix Apache
  site theme — site work is owned elsewhere.

---

## File Structure

- **Create** `phoenix-core-client/src/main/java/org/apache/phoenix/monitoring/BsonPathMetrics.java`
  — counters + `getSparseSkips()`, `getRewriteHits()`, `getRewriteMisses()`.
- **Modify** `phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexExpressionParseNodeRewriter.java`
  — increment hit/miss in `leaveCompoundNode`.
- **Modify** `phoenix-core-client/src/main/java/org/apache/phoenix/index/IndexMaintainer.java`
  — increment sparse-skip in the new sparse-null branch from Phase 2.
- **Create** `docs/superpowers/specs/2026-05-14-bson-path-indexes-user-guide.md` — short user
  guide.
- **Create** `phoenix-core/src/test/java/org/apache/phoenix/monitoring/BsonPathMetricsTest.java`
  — unit test for counter increments.

---

## Task 1: `BsonPathMetrics` class + unit test

**Files:**
- Create: `phoenix-core-client/src/main/java/org/apache/phoenix/monitoring/BsonPathMetrics.java`
- Create: `phoenix-core/src/test/java/org/apache/phoenix/monitoring/BsonPathMetricsTest.java`

- [ ] **Step 1: Write failing test**

```java
package org.apache.phoenix.monitoring;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class BsonPathMetricsTest {

  @Test
  public void countersStartAtZeroAndIncrement() {
    long sparse0 = BsonPathMetrics.getSparseSkips();
    long hits0 = BsonPathMetrics.getRewriteHits();
    long misses0 = BsonPathMetrics.getRewriteMisses();

    BsonPathMetrics.incrementSparseSkips();
    BsonPathMetrics.incrementRewriteHits();
    BsonPathMetrics.incrementRewriteMisses();
    BsonPathMetrics.incrementRewriteMisses();

    assertEquals(sparse0 + 1, BsonPathMetrics.getSparseSkips());
    assertEquals(hits0 + 1, BsonPathMetrics.getRewriteHits());
    assertEquals(misses0 + 2, BsonPathMetrics.getRewriteMisses());
    assertTrue(BsonPathMetrics.getSparseSkips() >= 1);
  }
}
```

- [ ] **Step 2: Run, expect compile failure**

- [ ] **Step 3: Implement**

```java
package org.apache.phoenix.monitoring;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight counters for BSON-path index activity. Best-effort, client-process-local.
 * Counters are static so they aggregate across all connections in this JVM.
 */
public final class BsonPathMetrics {

  private static final AtomicLong SPARSE_SKIPS = new AtomicLong();
  private static final AtomicLong REWRITE_HITS = new AtomicLong();
  private static final AtomicLong REWRITE_MISSES = new AtomicLong();

  private BsonPathMetrics() {}

  public static void incrementSparseSkips() { SPARSE_SKIPS.incrementAndGet(); }
  public static void incrementRewriteHits() { REWRITE_HITS.incrementAndGet(); }
  public static void incrementRewriteMisses() { REWRITE_MISSES.incrementAndGet(); }

  public static long getSparseSkips() { return SPARSE_SKIPS.get(); }
  public static long getRewriteHits() { return REWRITE_HITS.get(); }
  public static long getRewriteMisses() { return REWRITE_MISSES.get(); }

  /** Reset all counters; for use in tests only. */
  public static void resetForTest() {
    SPARSE_SKIPS.set(0);
    REWRITE_HITS.set(0);
    REWRITE_MISSES.set(0);
  }
}
```

- [ ] **Step 4: Run, expect PASS**

```
mvn -pl phoenix-core -am -Dtest=BsonPathMetricsTest test
```

- [ ] **Step 5: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/monitoring/BsonPathMetrics.java \
        phoenix-core/src/test/java/org/apache/phoenix/monitoring/BsonPathMetricsTest.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: add BsonPathMetrics counters"
```

---

## Task 2: Wire sparse-skip counter

**Files:**
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/index/IndexMaintainer.java`

- [ ] **Step 1: Locate Phase 2's sparse-null branch**

You added (in Phase 2 Task 4):

```java
            if (BsonIndexUtil.isBsonPathExpression(expression) && ptr.getLength() == 0) {
              return null;
            }
```

(Or the variant using `lastEvaluationWasMissingPath()`.)

- [ ] **Step 2: Insert increment**

```java
            if (BsonIndexUtil.isBsonPathExpression(expression) && ptr.getLength() == 0) {
              org.apache.phoenix.monitoring.BsonPathMetrics.incrementSparseSkips();
              return null;
            }
```

- [ ] **Step 3: Compile**

```
mvn -pl phoenix-core-client -am -DskipTests compile
```

- [ ] **Step 4: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/index/IndexMaintainer.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: increment sparse-skip counter on missing path"
```

---

## Task 3: Wire rewrite hit/miss counters

**Files:**
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexExpressionParseNodeRewriter.java`

- [ ] **Step 1: Update `leaveCompoundNode`**

The current implementation (post-Phase 3):

```java
    if (indexedParseNodeToColumnParseNodeMap.containsKey(candidate)) {
      return indexedParseNodeToColumnParseNodeMap.get(candidate);
    }
    return super.leaveCompoundNode(node, children, factory);
```

Change to:

```java
    if (indexedParseNodeToColumnParseNodeMap.containsKey(candidate)) {
      if (canonicalizeBson) {
        org.apache.phoenix.monitoring.BsonPathMetrics.incrementRewriteHits();
      }
      return indexedParseNodeToColumnParseNodeMap.get(candidate);
    }
    if (canonicalizeBson && org.apache.phoenix.util.BsonIndexUtil.containsBsonExpression(node)) {
      // Tracked only when the user-facing predicate names a BSON path; otherwise we'd flood
      // the counter on every non-BSON expression in the tree.
      org.apache.phoenix.monitoring.BsonPathMetrics.incrementRewriteMisses();
    }
    return super.leaveCompoundNode(node, children, factory);
```

- [ ] **Step 2: Compile**

```
mvn -pl phoenix-core-client -am -DskipTests compile
```

- [ ] **Step 3: Quick IT check**

The Phase 3 IT `BsonPathIndexQueryIT` should still pass and the rewrite-hit counter should
increment. Add an assertion to `BsonPathIndexQueryIT.canonicalEqualityHitsIndex` (modify
existing file, do not duplicate):

```java
    long before = org.apache.phoenix.monitoring.BsonPathMetrics.getRewriteHits();
    // ... existing test body ...
    long after = org.apache.phoenix.monitoring.BsonPathMetrics.getRewriteHits();
    assertTrue("expected rewrite hit counter to increase", after > before);
```

- [ ] **Step 4: Run**

```
mvn -pl phoenix-core -Dit.test=BsonPathIndexQueryIT verify
```

- [ ] **Step 5: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexExpressionParseNodeRewriter.java \
        phoenix-core/src/it/java/org/apache/phoenix/end2end/index/BsonPathIndexQueryIT.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: increment rewrite hit/miss counters + IT assertion"
```

---

## Task 4: User guide

**Files:**
- Create: `docs/superpowers/specs/2026-05-14-bson-path-indexes-user-guide.md`

- [ ] **Step 1: Write user guide**

```markdown
# BSON Path Functional Indexes — User Guide

This is a short companion to the design spec at
`docs/superpowers/specs/2026-05-05-bson-path-functional-indexes-design.md`.

## What you can do today

Define a secondary index on a path inside a `BSON` column:

    CREATE TABLE orders (
      id   VARCHAR PRIMARY KEY,
      doc  BSON
    );

    CREATE INDEX idx_orders_customer
      ON orders (BSON_VALUE(doc, '$.customer.id', 'VARCHAR'));

Queries that name the same canonical BSON path will use the index automatically:

    SELECT id FROM orders WHERE BSON_VALUE(doc, '$.customer.id', 'VARCHAR') = 'C-42';
    SELECT id FROM orders WHERE BSON_VALUE(doc, 'customer.id', 'VARCHAR')   = 'C-42';
    SELECT id FROM orders
       WHERE BSON_VALUE(doc, '$.customer.id', 'VARCHAR') IN ('C-42', 'C-43');

Both forms canonicalize to `BSON_VALUE(DOC, '$.customer.id', 'VARCHAR')` and hit the index.

## Sparse semantics

If a row's BSON document does not contain the indexed path, **no index entry is written for
that row** (sparse index). Consequence: you cannot use a BSON path index to find missing-path
rows via `IS NULL`.

## Type contract

`BSON_VALUE`'s third argument fixes the SQL type of the indexed key. Match the WHERE clause to
the same type: index built `AS BIGINT` requires the predicate to be a numeric literal, not a
string. v1 does not yet rewrite `CAST(BSON_VALUE(...) AS BIGINT) = 1` for you.

## Predicate forms that hit the index

| Form | Uses index? |
|---|---|
| `BSON_VALUE(doc, p, 'VARCHAR') = 'x'` | Yes |
| `BSON_VALUE(doc, p, 'VARCHAR') IN (...)` | Yes |
| `BSON_VALUE(doc, p, 'VARCHAR') BETWEEN ...` | Yes |
| `BSON_VALUE(doc, p, 'VARCHAR') > 'x'` | Yes |
| `UPPER(BSON_VALUE(doc, p, 'VARCHAR')) = 'X'` | No |
| `BSON_VALUE(doc, p, 'VARCHAR') LIKE 'a%'` | No |
| `BSON_VALUE(doc, p, 'VARCHAR') IS NULL` | No (sparse) |

## Path language supported in v1

| Form | Example | Supported |
|---|---|---|
| Dot | `$.a.b.c` | Yes |
| Array index | `$.a[0]`, `$.a[10][3]` | Yes |
| Quoted key | `$['weird key']`, `$["odd"]` | Yes |
| Bare path | `a.b`, `a[0]` (canonicalized to `$.a.b`) | Yes |
| Wildcards | `$.*`, `$[*]` | No |
| Filters | `$[?(@.x>1)]` | No |
| Recursive descent | `$..x` | No |
| Slice | `$[0:2]` | No |

## Feature flags

| Flag | Default | Effect when `false` |
|---|---|---|
| `phoenix.index.bson.enabled` | `true` | `CREATE INDEX` on BSON paths is rejected |
| `phoenix.index.bson.rewrite.enabled` | `true` | Indexes still maintained; queries don't use them |

## Observability

Client-process counters in `org.apache.phoenix.monitoring.BsonPathMetrics`:

- `getSparseSkips()` — number of UPSERT rows that hit a missing-path branch and were
  skipped from the index.
- `getRewriteHits()` — number of WHERE-clause sub-expressions that matched a BSON path index
  after canonicalization.
- `getRewriteMisses()` — number of BSON-path WHERE expressions that did not match any indexed
  expression (typically: wrapped LHS, or no relevant index defined).

## What's not yet supported

- Multi-valued (GIN-style) BSON path indexes — DDL keyword `USING PATH` is reserved but not
  implemented.
- Local BSON path indexes, async-build, eventually-consistent BSON path indexes.
- `IS NULL` rewrite, `LIKE`, function-wrapped LHS.
- `->` / `->>` operator sugar.
```

- [ ] **Step 2: Commit**

```
git add docs/superpowers/specs/2026-05-14-bson-path-indexes-user-guide.md
git commit --no-gpg-sign -m "PHOENIX BsonPath: user guide for v1"
```

---

## Local testing plan for Phase 5

| What | Command |
|---|---|
| Compile | `mvn -pl phoenix-core-client -am -DskipTests install` |
| Metrics unit test | `mvn -pl phoenix-core -Dtest=BsonPathMetricsTest test` |
| Phase 3 query IT (now also asserts counters) | `mvn -pl phoenix-core -Dit.test=BsonPathIndexQueryIT verify` |
| Phase 2 / 3 regression | `mvn -pl phoenix-core -Dit.test='BsonPathIndex*IT' verify` |

---

## Self-review checklist

- [ ] Counters increment when expected; existing ITs still green.
- [ ] User guide covers DDL, sparse semantics, supported predicates, path language, flags,
      observability.
- [ ] No coprocessor / server-side wiring (deliberately deferred — note in the user guide).
