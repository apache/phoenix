# Phase 3 — Predicate Rewrite (Queries Hit Indexes) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a user has created a BSON-path index, queries containing equivalent BSON-path
predicates must hit the index. Specifically, `WHERE BSON_VALUE(doc, 'a.b', 'VARCHAR') = 'x'` and
`WHERE BSON_VALUE(doc, '$.a.b', 'VARCHAR') = 'x'` should both match the index defined as
`BSON_VALUE(doc, '$.a.b', 'VARCHAR')`.

**Architecture:** Hook the canonicalizer into the predicate-rewriter pass so that **both** the
indexed expression in the catalog AND the WHERE-clause expression are normalized to the same
canonical `ParseNode` form before `IndexExpressionParseNodeRewriter` does its
`indexedParseNodeToColumnParseNodeMap` lookup.

**Tech Stack:** Java 8, Phoenix's existing `IndexExpressionParseNodeRewriter` and `QueryOptimizer`.

---

## Calibration vs. spec

- The "AST-exact map lookup" the spec referenced lives in `IndexExpressionParseNodeRewriter`
  (`phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexExpressionParseNodeRewriter.java`,
  L43-87). The matching is `Map<ParseNode, ParseNode>`. Phoenix's `ParseNode.equals` is
  reference-based by default, but `FunctionParseNode` and friends override `equals`/`hashCode`
  structurally (verify; if not, this whole design pivots — see Task 1's verification step).
- The spec also referenced `IndexStatementRewriter` for the rewrite point.
  `IndexStatementRewriter` rewrites column references (`ColumnParseNode` → indexed-table column),
  not expressions. The expression rewrite happens in `IndexExpressionParseNodeRewriter`. Phase 3
  inserts canonicalization there.
- Predicate forms supported in v1: `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, `IN`. Implementation: by
  canonicalizing the LHS we let Phoenix's existing comparison-operator handling work unmodified.
  Anything wrapping the LHS in a function (`UPPER(BSON_VALUE(...))`) or coercing it via `CAST`
  falls through to non-indexed plan.
- Feature flag: `phoenix.index.bson.rewrite.enabled` (default `true`).

---

## File Structure

- **Modify** `phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexExpressionParseNodeRewriter.java`
  — in the constructor (L47-78), call `BsonPathCanonicalizer.rewrite` on the indexed-expression
  parse node before adding it to the map. Add an override `enterParseNode` /
  `leaveCompoundNode` that canonicalizes incoming WHERE-side nodes before lookup.
- **Modify** `phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServices.java` — add
  `BSON_INDEX_REWRITE_ENABLED_ATTRIB`.
- **Modify** `phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServicesOptions.java`
  — add `DEFAULT_BSON_INDEX_REWRITE_ENABLED = true` and getter/setter.
- **Modify** `phoenix-core-client/src/main/java/org/apache/phoenix/optimize/QueryOptimizer.java`
  — feed the feature-flag value into `IndexExpressionParseNodeRewriter`'s constructor (or skip the
  canonicalize step internally when the flag is off).
- **Create** `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/BsonPathIndexQueryIT.java`
  — full end-to-end IT: create index, upsert, query with EXPLAIN-plan assertions and result-set
  equality checks.
- **Create** `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/BsonPathIndexConsistencyIT.java`
  — randomized correctness IT: same set of generated queries returns the same results with the
  index enabled vs. disabled (`ALTER INDEX ... DISABLE`).

---

## Task 1: Verify ParseNode equality semantics

**Files:** none (investigation only).

- [ ] **Step 1: Run a one-off test**

Write a throwaway test (delete it after) in `phoenix-core/src/test/java/org/apache/phoenix/parse/_BsonProbeTest.java`:

```java
package org.apache.phoenix.parse;

import org.junit.Test;
import static org.junit.Assert.*;

public class _BsonProbeTest {
  @Test
  public void parseNodesWithSameStringMustBeEqual() throws Exception {
    ParseNode a = new SQLParser("BSON_VALUE(doc, '$.a.b', 'VARCHAR')").parseExpression();
    ParseNode b = new SQLParser("BSON_VALUE(doc, '$.a.b', 'VARCHAR')").parseExpression();
    assertEquals("expected structural equality", a, b);
    assertEquals("expected hashCode equality", a.hashCode(), b.hashCode());
  }
}
```

Run: `mvn -pl phoenix-core -Dtest=_BsonProbeTest test`.

- [ ] **Step 2: Branch on result**

If the assertion **passes**: ParseNode equality is structural enough — proceed to Task 2.

If the assertion **fails** (Phoenix's ParseNode.equals is reference-equality): we cannot reuse the
existing `Map<ParseNode, ParseNode>`. Pivot: change the map's key type to `String` (canonical
toString), and look up by `node.toString()` after canonicalization. Update Task 2's instructions
accordingly. The rest of the plan is unchanged.

- [ ] **Step 3: Delete the probe test** (commit deletion or never commit it).

---

## Task 2: Canonicalize indexed expression in `IndexExpressionParseNodeRewriter` constructor

**Files:**
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexExpressionParseNodeRewriter.java`

- [ ] **Step 1: Add canonicalize call**

In the constructor at L47-78, after `expressionParseNode = SQLParser.parseCondition(expressionStr)`
(L64), insert:

```java
      expressionParseNode = BsonPathCanonicalizer.rewrite(expressionParseNode);
```

Add `import org.apache.phoenix.compile.BsonPathCanonicalizer;`.

- [ ] **Step 2: Compile**

```
mvn -pl phoenix-core-client -am -DskipTests compile
```

- [ ] **Step 3: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexExpressionParseNodeRewriter.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: canonicalize indexed expression on rewriter load"
```

---

## Task 3: Canonicalize WHERE-clause expression before map lookup

**Files:**
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexExpressionParseNodeRewriter.java`

- [ ] **Step 1: Override `leaveCompoundNode` to canonicalize before lookup**

The current implementation:

```java
  @Override
  protected ParseNode leaveCompoundNode(CompoundParseNode node, List<ParseNode> children,
      CompoundNodeFactory factory) {
    return indexedParseNodeToColumnParseNodeMap.containsKey(node)
        ? indexedParseNodeToColumnParseNodeMap.get(node)
        : super.leaveCompoundNode(node, children, factory);
  }
```

Replace with:

```java
  @Override
  protected ParseNode leaveCompoundNode(CompoundParseNode node, List<ParseNode> children,
      CompoundNodeFactory factory) {
    ParseNode candidate = node;
    try {
      ParseNode canonical = BsonPathCanonicalizer.rewrite(node);
      if (canonical != null) {
        candidate = canonical;
      }
    } catch (java.sql.SQLException ignored) {
      // canonicalizer should not throw on well-formed input; if it does, fall back to the
      // original node and let the existing matcher do its thing.
    }
    if (indexedParseNodeToColumnParseNodeMap.containsKey(candidate)) {
      return indexedParseNodeToColumnParseNodeMap.get(candidate);
    }
    return super.leaveCompoundNode(node, children, factory);
  }
```

- [ ] **Step 2: Compile**

```
mvn -pl phoenix-core-client -am -DskipTests compile
```

- [ ] **Step 3: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexExpressionParseNodeRewriter.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: canonicalize WHERE expression before index match"
```

---

## Task 4: Feature flag `phoenix.index.bson.rewrite.enabled`

**Files:**
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServices.java`
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServicesOptions.java`
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexExpressionParseNodeRewriter.java`

- [ ] **Step 1: Add flag**

In `QueryServices.java` near `BSON_INDEX_ENABLED_ATTRIB` (added in Phase 2):

```java
  public static final String BSON_INDEX_REWRITE_ENABLED_ATTRIB = "phoenix.index.bson.rewrite.enabled";
```

In `QueryServicesOptions.java`:

```java
  public static final boolean DEFAULT_BSON_INDEX_REWRITE_ENABLED = true;

  public boolean isBsonIndexRewriteEnabled() {
    return config.getBoolean(BSON_INDEX_REWRITE_ENABLED_ATTRIB, DEFAULT_BSON_INDEX_REWRITE_ENABLED);
  }
```

(Add the corresponding `import static`.)

- [ ] **Step 2: Read the flag in `IndexExpressionParseNodeRewriter`**

The constructor receives a `PhoenixConnection`. Read the flag once into a `final boolean
canonicalizeBson;` field. Apply the canonicalize call from Task 2/3 only when `canonicalizeBson`
is `true`.

```java
  private final boolean canonicalizeBson;

  // in constructor, after the connection is captured:
  this.canonicalizeBson = connection.getQueryServices().getProps().getBoolean(
      QueryServices.BSON_INDEX_REWRITE_ENABLED_ATTRIB,
      QueryServicesOptions.DEFAULT_BSON_INDEX_REWRITE_ENABLED);
```

Then guard both canonicalize sites with `if (canonicalizeBson) { ... }`.

- [ ] **Step 3: Compile + commit**

```
mvn -pl phoenix-core-client -am -DskipTests compile
git add phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServices.java \
        phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServicesOptions.java \
        phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexExpressionParseNodeRewriter.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: phoenix.index.bson.rewrite.enabled feature flag"
```

---

## Task 5: Query-side IT — index hits

**Files:**
- Create: `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/BsonPathIndexQueryIT.java`

- [ ] **Step 1: Write the IT**

```java
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class BsonPathIndexQueryIT extends ParallelStatsDisabledIT {

  private String tbl;
  private String idx;

  private void setupSchema(Connection conn) throws Exception {
    tbl = generateUniqueName();
    idx = generateUniqueName();
    conn.createStatement().execute(
        "CREATE TABLE " + tbl + " (PK VARCHAR PRIMARY KEY, DOC BSON)");
    conn.createStatement().execute(
        "CREATE INDEX " + idx + " ON " + tbl + "(BSON_VALUE(DOC, '$.name', 'VARCHAR'))");
    try (PreparedStatement ps = conn.prepareStatement(
        "UPSERT INTO " + tbl + " VALUES (?, ?)")) {
      ps.setString(1, "k1"); ps.setObject(2, BsonDocument.parse("{\"name\":\"alice\"}")); ps.execute();
      ps.setString(1, "k2"); ps.setObject(2, BsonDocument.parse("{\"name\":\"bob\"}"));   ps.execute();
      ps.setString(1, "k3"); ps.setObject(2, BsonDocument.parse("{\"name\":\"carol\"}")); ps.execute();
      ps.setString(1, "k4"); ps.setObject(2, BsonDocument.parse("{\"other\":\"x\"}"));    ps.execute();
    }
    conn.commit();
  }

  private static String explain(Connection conn, String sql) throws Exception {
    try (ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + sql)) {
      StringBuilder sb = new StringBuilder();
      while (rs.next()) sb.append(rs.getString(1)).append('\n');
      return sb.toString();
    }
  }

  @Test
  public void canonicalEqualityHitsIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      setupSchema(conn);
      String sql = "SELECT PK FROM " + tbl
          + " WHERE BSON_VALUE(DOC, '$.name', 'VARCHAR') = 'alice'";
      String plan = explain(conn, sql);
      assertTrue("expected index in plan: " + plan, plan.contains(idx));
    }
  }

  @Test
  public void barePathEqualityHitsIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      setupSchema(conn);
      String sql = "SELECT PK FROM " + tbl
          + " WHERE BSON_VALUE(DOC, 'name', 'VARCHAR') = 'bob'";
      String plan = explain(conn, sql);
      assertTrue("expected index in plan (bare path): " + plan, plan.contains(idx));
      try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
        assertTrue(rs.next());
        assertEquals("k2", rs.getString(1));
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void inHitsIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      setupSchema(conn);
      String sql = "SELECT PK FROM " + tbl
          + " WHERE BSON_VALUE(DOC, 'name', 'VARCHAR') IN ('alice','carol')";
      String plan = explain(conn, sql);
      assertTrue("expected index in plan (IN): " + plan, plan.contains(idx));
    }
  }

  @Test
  public void rangeHitsIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      setupSchema(conn);
      String sql = "SELECT PK FROM " + tbl
          + " WHERE BSON_VALUE(DOC, '$.name', 'VARCHAR') BETWEEN 'b' AND 'm'";
      String plan = explain(conn, sql);
      assertTrue("expected index in plan (BETWEEN): " + plan, plan.contains(idx));
    }
  }

  @Test
  public void wrappedLhsDoesNotHitIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      setupSchema(conn);
      String sql = "SELECT PK FROM " + tbl
          + " WHERE UPPER(BSON_VALUE(DOC, '$.name', 'VARCHAR')) = 'ALICE'";
      String plan = explain(conn, sql);
      // Wrapped LHS is intentionally not supported in v1 — must NOT hit the index.
      assertFalse("did not expect index for UPPER(BSON_VALUE(...)): " + plan, plan.contains(idx));
    }
  }

  @Test
  public void rewriteFlagOffFallsBackToFullScan() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    props.setProperty("phoenix.index.bson.rewrite.enabled", "false");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      setupSchema(conn);
      String sql = "SELECT PK FROM " + tbl
          + " WHERE BSON_VALUE(DOC, 'name', 'VARCHAR') = 'alice'";
      String plan = explain(conn, sql);
      assertFalse("rewrite-disabled plan should not use index: " + plan, plan.contains(idx));
    }
  }
}
```

- [ ] **Step 2: Run**

```
mvn -pl phoenix-core -Dit.test=BsonPathIndexQueryIT verify
```

Expected: 6 tests, all PASS.

If `canonicalEqualityHitsIndex` fails because EXPLAIN does not contain the index name, this means
the canonicalizer's output and the catalog-stored expressionStr produced different parse-trees
when re-parsed. Inspect `IndexUtil.getIndexColumnExpressionStr(column)`'s output for a
BSON-path index column; what gets stored should already be canonical (Phase 2). If not, return
to Phase 2 Task 3 and verify the canonicalize-then-`toSQL` order.

- [ ] **Step 3: Commit**

```
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/index/BsonPathIndexQueryIT.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: query-side IT covering eq, IN, BETWEEN, fallback"
```

---

## Task 6: Randomized correctness IT

**Files:**
- Create: `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/BsonPathIndexConsistencyIT.java`

- [ ] **Step 1: Write the IT**

```java
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.bson.BsonDocument;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class BsonPathIndexConsistencyIT extends ParallelStatsDisabledIT {

  private static final long SEED = 0xC0FFEEL;

  @Test
  public void resultsMatchWithAndWithoutIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tbl = generateUniqueName();
    String idx = generateUniqueName();
    Random rng = new Random(SEED);

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE " + tbl + " (PK VARCHAR PRIMARY KEY, DOC BSON)");
      // Insert 200 rows; ~20% are missing the indexed path.
      try (PreparedStatement ps = conn.prepareStatement(
          "UPSERT INTO " + tbl + " VALUES (?, ?)")) {
        for (int i = 0; i < 200; i++) {
          String name = "n" + (rng.nextInt(40));
          BsonDocument d = (rng.nextDouble() < 0.2)
              ? BsonDocument.parse("{\"other\":\"x\"}")
              : BsonDocument.parse("{\"name\":\"" + name + "\"}");
          ps.setString(1, "k" + i);
          ps.setObject(2, d);
          ps.execute();
        }
      }
      conn.commit();

      conn.createStatement().execute(
          "CREATE INDEX " + idx + " ON " + tbl + "(BSON_VALUE(DOC, '$.name', 'VARCHAR'))");

      List<String> queries = sampleQueries(tbl, rng, 100);

      // 1) Run all queries with index enabled.
      List<List<String>> indexed = runAll(conn, queries);

      // 2) Disable index, run again.
      conn.createStatement().execute("ALTER INDEX " + idx + " ON " + tbl + " DISABLE");
      List<List<String>> baseline = runAll(conn, queries);

      assertEquals("query count", indexed.size(), baseline.size());
      for (int i = 0; i < indexed.size(); i++) {
        assertEquals("mismatch on query: " + queries.get(i),
            new TreeSet<>(baseline.get(i)), new TreeSet<>(indexed.get(i)));
      }
    }
  }

  private static List<String> sampleQueries(String tbl, Random rng, int n) {
    List<String> qs = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      String pathForm = rng.nextBoolean() ? "$.name" : "name";
      int kind = rng.nextInt(4);
      switch (kind) {
        case 0:
          qs.add("SELECT PK FROM " + tbl + " WHERE BSON_VALUE(DOC, '" + pathForm
              + "', 'VARCHAR') = 'n" + rng.nextInt(40) + "'");
          break;
        case 1:
          qs.add("SELECT PK FROM " + tbl + " WHERE BSON_VALUE(DOC, '" + pathForm
              + "', 'VARCHAR') IN ('n" + rng.nextInt(40) + "', 'n" + rng.nextInt(40) + "')");
          break;
        case 2:
          qs.add("SELECT PK FROM " + tbl + " WHERE BSON_VALUE(DOC, '" + pathForm
              + "', 'VARCHAR') > 'n" + rng.nextInt(40) + "'");
          break;
        case 3:
          qs.add("SELECT PK FROM " + tbl + " WHERE BSON_VALUE(DOC, '" + pathForm
              + "', 'VARCHAR') BETWEEN 'n0' AND 'n" + rng.nextInt(40) + "'");
          break;
      }
    }
    return qs;
  }

  private static List<List<String>> runAll(Connection conn, List<String> queries) throws Exception {
    List<List<String>> out = new ArrayList<>();
    for (String q : queries) {
      List<String> rows = new ArrayList<>();
      try (ResultSet rs = conn.createStatement().executeQuery(q)) {
        while (rs.next()) rows.add(rs.getString(1));
      }
      Collections.sort(rows);
      out.add(rows);
    }
    return out;
  }
}
```

- [ ] **Step 2: Run**

```
mvn -pl phoenix-core -Dit.test=BsonPathIndexConsistencyIT verify
```

Expected: PASS — same result set with and without the index across 100 random queries.

- [ ] **Step 3: Commit**

```
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/index/BsonPathIndexConsistencyIT.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: randomized index/no-index consistency IT"
```

---

## Local testing plan for Phase 3

| What | Command |
|---|---|
| Compile | `mvn -pl phoenix-core-client -am -DskipTests install` |
| Probe ParseNode equality | `mvn -pl phoenix-core -Dtest=_BsonProbeTest test` (if you wrote the throwaway) |
| Query IT | `mvn -pl phoenix-core -Dit.test=BsonPathIndexQueryIT verify` |
| Consistency IT | `mvn -pl phoenix-core -Dit.test=BsonPathIndexConsistencyIT verify` |
| Phase 2 ITs still pass | `mvn -pl phoenix-core -Dit.test=BsonPathIndexWriteIT verify` |
| Existing index ITs sanity | `mvn -pl phoenix-core -Dit.test=IndexMaintenanceIT verify` |
| Existing BSON ITs sanity | `mvn -pl phoenix-core -Dit.test='Bson?IT' verify` |

---

## Rollback

Set `phoenix.index.bson.rewrite.enabled=false`. Indexes remain maintained; queries do **not** use
them and fall back to full scan. Zero data loss.

---

## Self-review checklist

- [ ] All 6 tasks committed in order.
- [ ] ParseNode equality assumption verified at Task 1.
- [ ] Indexed-side and WHERE-side both canonicalize before map lookup.
- [ ] Feature flag works end-to-end (last test in `BsonPathIndexQueryIT` validates).
- [ ] Wrapped LHS (`UPPER(BSON_VALUE(...))`) explicitly does not hit the index.
- [ ] Phase 2 ITs still pass.
- [ ] Existing index/BSON ITs still pass.
- [ ] Consistency IT passes — index and no-index results identical.
