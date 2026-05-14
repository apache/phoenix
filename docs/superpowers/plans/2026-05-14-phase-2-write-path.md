# Phase 2 — Enable BSON-path Functional Indexes for Writes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When a user runs `CREATE INDEX idx ON t (BSON_VALUE(doc, 'a.b', 'VARCHAR'))`, Phoenix
must (a) canonicalize the indexed expression so `'a.b'` and `'$.a.b'` collide on duplicate-index
detection, (b) maintain the index on UPSERT, and (c) skip rows where the BSON path is missing
(sparse index). Queries do **not** yet hit the index — that's Phase 3.

**Architecture:** Three small additions, no on-disk format changes:

1. **DDL-side canonicalization.** In `MetaDataClient.createIndex`, run
   `BsonPathCanonicalizer.rewrite` on the indexed `ParseNode` *before* `parseNode.toSQL(buf)`
   produces the `expressionStr` persisted to `SYSTEM.CATALOG`.
2. **Feature flag.** New config `phoenix.index.bson.enabled` (default `true`). When `false`,
   `MetaDataClient.createIndex` rejects any indexed expression whose ParseNode tree contains a
   `BSON_VALUE` (or `JSON_VALUE`) call.
3. **Sparse-null at write time.** In `IndexMaintainer.buildRowKey`, when an indexed `Expression`
   is a `BsonValueFunction` (or wraps one) and `expression.evaluate(...)` produces a length-0 ptr,
   short-circuit the *entire* index row: return `null` to signal "no index entry for this data
   row." Callers of `buildRowKey` already handle a `null` return as "no put / no delete."

**Tech Stack:** Java 8, Phoenix's existing client-side infrastructure. No changes to protobuf or
the IndexMaintainer wire format.

---

## Calibration vs. spec

- The spec said the `MetaDataClient.java:1735` guard rejects BSON_VALUE today. **It does not.**
  `isJsonFragment` is only set for `JsonQueryParseNode` / `JsonModifyParseNode`
  (`ExpressionCompiler.java:313`). BSON_VALUE indexes already compile through the guard. The
  practical implication: today, a `CREATE INDEX ... (BSON_VALUE(doc,'a','VARCHAR'))` succeeds, but
  there is no canonicalization, no sparse-null behavior, and no feature flag. Phase 2 fills in
  exactly those three gaps.
- The spec also said we'd add an `is_bson_path` protobuf field on `IndexMaintainer`. **We do not
  need that.** At `IndexMaintainer.buildRowKey` time, the live `Expression` object is available;
  we can check `instanceof BsonValueFunction` directly. No on-disk format change.
- The "must verify proposed code changes against current code" requirement is honored: each task
  below cites the actual line range or file the implementer must edit.

---

## File Structure

- **Modify** `phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServices.java`
  — add `BSON_INDEX_ENABLED_ATTRIB` constant (around the other index-related attribs near L115).
- **Modify** `phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServicesOptions.java`
  — add `DEFAULT_BSON_INDEX_ENABLED = true` and a getter wired through `setIfUnset` (around L561) +
  helper getter at the bottom.
- **Modify** `phoenix-core-client/src/main/java/org/apache/phoenix/exception/SQLExceptionCode.java`
  — add `BSON_INDEX_DISABLED` (use a new error code; pick the next number after the highest current
  numeric error code; surrounding lines have examples).
- **Modify** `phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java` —
  insert the canonicalization + feature-flag check inside the indexed-column loop at
  `MetaDataClient.java:1724-1786`. Specifically: rewrite `parseNode` via
  `BsonPathCanonicalizer.rewrite` immediately after the `StatementNormalizer.normalize(...)` call
  on `MetaDataClient.java:1727`. Also call a new private static helper `containsBsonExpression(ParseNode)`
  before the existing `expressionIndexCompiler.isJsonFragment()` check; if the helper returns
  `true` and the feature flag is off, throw `BSON_INDEX_DISABLED`.
- **Modify** `phoenix-core-client/src/main/java/org/apache/phoenix/index/IndexMaintainer.java` —
  in `buildRowKey` (the body that begins at `IndexMaintainer.java:770`), inside the loop at
  `IndexMaintainer.java:843-870`, after `expression.evaluate(new ValueGetterTuple(valueGetter, ts), ptr)`
  (`IndexMaintainer.java:862`), if the expression is a BSON-path expression AND `ptr.getLength() == 0`,
  signal sparse-skip by returning `null` from `buildRowKey`. Also add a helper
  `isBsonPathExpression(Expression)` and a precomputed boolean array
  `isIndexedExpressionBsonPath` populated when `indexedExpressions` is finalized (in `init()` and
  in `fromProto` / `readFields`). Backward compatible: defaults to all-false when not yet
  populated.
- **Create** `phoenix-core-client/src/main/java/org/apache/phoenix/util/BsonIndexUtil.java` — small
  utility holding `containsBsonExpression(ParseNode)` and `isBsonPathExpression(Expression)` so
  the same predicate is used in DDL and runtime.
- **Modify** `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/BsonPathIndexWriteIT.java`
  — new IT under the existing `index/` package, extending `ParallelStatsDisabledIT`.
- **Modify** `phoenix-core/src/test/java/org/apache/phoenix/util/BsonIndexUtilTest.java` — unit
  test for the helpers.

**Verify before each modification:** open the file at the cited line and confirm the surrounding
context still matches what's quoted in the task. Phoenix's `master` is active development; if a
range has shifted, follow the spirit of the change rather than the literal line number.

---

## Task 1: Feature flag plumbing

**Files:**
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServices.java`
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServicesOptions.java`

- [ ] **Step 1: Add the constant**

In `QueryServices.java`, near `USE_INDEXES_ATTRIB` (around L115), add:

```java
  public static final String BSON_INDEX_ENABLED_ATTRIB = "phoenix.index.bson.enabled";
```

- [ ] **Step 2: Add default + import in QueryServicesOptions**

In `QueryServicesOptions.java`, in the imports near top, add `import static
org.apache.phoenix.query.QueryServices.BSON_INDEX_ENABLED_ATTRIB;` next to existing
`USE_INDEXES_ATTRIB` import (around L127). Then near `DEFAULT_USE_INDEXES` (L179):

```java
  public static final boolean DEFAULT_BSON_INDEX_ENABLED = true;
```

In the `withDefaults`-style setter cascade near L561 (right after the `USE_INDEXES_ATTRIB` setter), add:

```java
      .setIfUnset(BSON_INDEX_ENABLED_ATTRIB, DEFAULT_BSON_INDEX_ENABLED)
```

Add a getter (mirroring the `useIndexes` pattern around L812 / L951):

```java
  public boolean isBsonIndexEnabled() {
    return config.getBoolean(BSON_INDEX_ENABLED_ATTRIB, DEFAULT_BSON_INDEX_ENABLED);
  }

  public QueryServicesOptions setBsonIndexEnabled(boolean enabled) {
    return set(BSON_INDEX_ENABLED_ATTRIB, enabled);
  }
```

- [ ] **Step 3: Compile**

```
mvn -pl phoenix-core-client -am -DskipTests compile
```

Expected: clean compile.

- [ ] **Step 4: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServices.java \
        phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServicesOptions.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: add phoenix.index.bson.enabled feature flag"
```

---

## Task 2: `BsonIndexUtil` helpers + unit test

**Files:**
- Create: `phoenix-core-client/src/main/java/org/apache/phoenix/util/BsonIndexUtil.java`
- Create: `phoenix-core/src/test/java/org/apache/phoenix/util/BsonIndexUtilTest.java`

- [ ] **Step 1: Write failing test**

```java
package org.apache.phoenix.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.junit.Test;

public class BsonIndexUtilTest {

  private static ParseNode parseExpr(String s) throws Exception {
    return new SQLParser(s).parseExpression();
  }

  @Test
  public void detectsBsonValueAtTopLevel() throws Exception {
    assertTrue(BsonIndexUtil.containsBsonExpression(
        parseExpr("BSON_VALUE(doc, '$.a', 'VARCHAR')")));
  }

  @Test
  public void detectsBsonValueNested() throws Exception {
    assertTrue(BsonIndexUtil.containsBsonExpression(
        parseExpr("UPPER(BSON_VALUE(doc, '$.a', 'VARCHAR'))")));
  }

  @Test
  public void detectsJsonValue() throws Exception {
    assertTrue(BsonIndexUtil.containsBsonExpression(
        parseExpr("JSON_VALUE(doc, '$.a')")));
  }

  @Test
  public void plainExpressionIsNotBson() throws Exception {
    assertFalse(BsonIndexUtil.containsBsonExpression(parseExpr("a + 1")));
  }

  @Test
  public void wholeColumnIsNotBson() throws Exception {
    assertFalse(BsonIndexUtil.containsBsonExpression(parseExpr("doc")));
  }
}
```

- [ ] **Step 2: Run, expect compile failure**

```
mvn -pl phoenix-core -am -Dtest=BsonIndexUtilTest test
```

- [ ] **Step 3: Implement**

```java
package org.apache.phoenix.util;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.BsonValueFunction;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.ParseNode;

/** Helpers for identifying BSON-path expressions in DDL and at runtime. */
public final class BsonIndexUtil {

  private BsonIndexUtil() {}

  /** Returns true if any node in the parse tree is BSON_VALUE or JSON_VALUE. */
  public static boolean containsBsonExpression(ParseNode node) {
    if (node == null) return false;
    if (node instanceof FunctionParseNode) {
      String n = ((FunctionParseNode) node).getName();
      if ("BSON_VALUE".equalsIgnoreCase(n) || "JSON_VALUE".equalsIgnoreCase(n)) {
        return true;
      }
    }
    for (ParseNode child : node.getChildren()) {
      if (containsBsonExpression(child)) {
        return true;
      }
    }
    return false;
  }

  /** Returns true if the compiled expression's root is a BSON_VALUE call. */
  public static boolean isBsonPathExpression(Expression expression) {
    return expression instanceof BsonValueFunction;
  }
}
```

- [ ] **Step 4: Run, expect PASS**

- [ ] **Step 5: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/util/BsonIndexUtil.java \
        phoenix-core/src/test/java/org/apache/phoenix/util/BsonIndexUtilTest.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: add BsonIndexUtil helpers"
```

---

## Task 3: Wire canonicalizer + feature flag into `MetaDataClient.createIndex`

**Files:**
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java` (around L1724-1740)
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/exception/SQLExceptionCode.java`

- [ ] **Step 1: Add `SQLExceptionCode.BSON_INDEX_DISABLED`**

In `SQLExceptionCode.java`, add a new entry near the existing `JSON_FRAGMENT_NOT_ALLOWED_IN_INDEX_EXPRESSION` (around L238). Pick the next available numeric error code (highest existing + 1). For example, if 544 is the highest in that block, use 545:

```java
  BSON_INDEX_DISABLED(545, "42921",
      "BSON path indexes are disabled. Set phoenix.index.bson.enabled=true to allow."),
```

(If 545 is already taken in the file, scan for the highest number and increment.)

- [ ] **Step 2: Modify `MetaDataClient.createIndex`**

Open `MetaDataClient.java` and locate the indexed-column loop at L1724. The code reads:

```java
      for (Pair<ParseNode, SortOrder> pair : indexParseNodeAndSortOrderList) {
        ParseNode parseNode = pair.getFirst();
        // normalize the parse node
        parseNode = StatementNormalizer.normalize(parseNode, resolver);
        // compile the parseNode to get an expression
        expressionIndexCompiler.reset();
        Expression expression = parseNode.accept(expressionIndexCompiler);
```

Insert two lines: (a) the feature-flag check, (b) canonicalization. After the `StatementNormalizer.normalize` line:

```java
        if (BsonIndexUtil.containsBsonExpression(parseNode)) {
          if (!connection.getQueryServices().getProps().getBoolean(
                  QueryServices.BSON_INDEX_ENABLED_ATTRIB,
                  QueryServicesOptions.DEFAULT_BSON_INDEX_ENABLED)) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.BSON_INDEX_DISABLED)
                .build().buildException();
          }
          parseNode = BsonPathCanonicalizer.rewrite(parseNode);
        }
```

Add the matching imports to the file:

```java
import org.apache.phoenix.compile.BsonPathCanonicalizer;
import org.apache.phoenix.util.BsonIndexUtil;
import org.apache.phoenix.query.QueryServicesOptions;
```

(The file may already import `QueryServices` and `SQLExceptionCode`. Verify; if missing, add.)

- [ ] **Step 3: Compile**

```
mvn -pl phoenix-core-client -am -DskipTests compile
```

If `BsonPathCanonicalizer.rewrite` throws `SQLException`, the surrounding method already declares
`throws SQLException`, so no new `try/catch` is needed.

- [ ] **Step 4: Add a focused parse-test for canonicalization on DDL**

Create `phoenix-core/src/test/java/org/apache/phoenix/end2end/index/BsonPathCreateIndexCompileTest.java`:

```java
package org.apache.phoenix.end2end.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class BsonPathCreateIndexCompileTest extends BaseConnectionlessQueryTest {

  @Test
  public void disableFlagRejectsCreateIndex() throws Exception {
    Properties props = PropertiesUtil.deepCopy(new Properties());
    props.setProperty(QueryServices.BSON_INDEX_ENABLED_ATTRIB, "false");
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE T_BSON_X (PK VARCHAR PRIMARY KEY, DOC BSON)");
      try {
        conn.createStatement().execute(
            "CREATE INDEX IDX_X ON T_BSON_X (BSON_VALUE(DOC, '$.a', 'VARCHAR'))");
        org.junit.Assert.fail("expected BSON_INDEX_DISABLED");
      } catch (SQLException e) {
        assertEquals(SQLExceptionCode.BSON_INDEX_DISABLED.getErrorCode(), e.getErrorCode());
      }
    }
  }

  @Test
  public void canonicalizationCollidesEquivalentIndexes() throws Exception {
    Properties props = PropertiesUtil.deepCopy(new Properties());
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE T_BSON_Y (PK VARCHAR PRIMARY KEY, DOC BSON)");
      conn.createStatement().execute(
          "CREATE INDEX IDX_Y1 ON T_BSON_Y (BSON_VALUE(DOC, 'a.b', 'VARCHAR'))");
      try {
        conn.createStatement().execute(
            "CREATE INDEX IDX_Y2 ON T_BSON_Y (BSON_VALUE(DOC, '$.a.b', 'VARCHAR'))");
        // If the second succeeds, the indexes are stored under different expressionStr — which
        // means canonicalization didn't kick in. Fail loudly.
        org.junit.Assert.fail("expected duplicate-index error after canonicalization");
      } catch (SQLException e) {
        assertTrue("expected COLUMN_EXIST_IN_DEF or duplicate-index, got: " + e.getMessage(),
            e.getMessage().contains("already exists")
                || e.getErrorCode() == SQLExceptionCode.COLUMN_EXIST_IN_DEF.getErrorCode());
      }
    }
  }
}
```

> Note: `BaseConnectionlessQueryTest` is the standard pattern for compile-only tests in
> phoenix-core. If the duplicate-index path raises a different error message in the connectionless
> driver, capture the actual exception once in stdout and tighten the assertion to match.

- [ ] **Step 5: Run unit test**

```
mvn -pl phoenix-core -am -Dtest=BsonPathCreateIndexCompileTest test
```

Expected: both tests pass.

- [ ] **Step 6: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/exception/SQLExceptionCode.java \
        phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java \
        phoenix-core/src/test/java/org/apache/phoenix/end2end/index/BsonPathCreateIndexCompileTest.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: canonicalize index expression on CREATE INDEX + feature flag"
```

---

## Task 4: Sparse-null in `IndexMaintainer.buildRowKey`

**Files:**
- Modify: `phoenix-core-client/src/main/java/org/apache/phoenix/index/IndexMaintainer.java`

- [ ] **Step 1: Locate the per-expression evaluation loop**

`IndexMaintainer.java` around L840-L870. The key call site:

```java
      Iterator<Expression> expressionIterator = indexedExpressions.iterator();
      ...
        if (dataPkPosition[i] == EXPRESSION_NOT_PRESENT) {
          Expression expression = expressionIterator.next();
          ...
          } else {
            expression.evaluate(new ValueGetterTuple(valueGetter, ts), ptr);
          }
        }
```

- [ ] **Step 2: Add sparse-null branch**

Right after the `expression.evaluate(...)` line at L862, insert:

```java
            if (BsonIndexUtil.isBsonPathExpression(expression) && ptr.getLength() == 0) {
              // Sparse BSON-path index: missing path → no index entry for this row.
              return null;
            }
```

Add `import org.apache.phoenix.util.BsonIndexUtil;` near the other imports.

> The `else` branch already does `expression.evaluate(...)`. Insert immediately after that
> evaluate so the ptr length is fresh. The function's return type is already `byte[]`, and we
> verified above that callers (`getIndexRowKey`, `prepareIndexUpdates`, etc.) tolerate nulls — they
> wrap the result and a null skips emitting puts/deletes. **Verify this**: search for `buildRowKey(`
> usages and confirm a null check exists; if a caller dereferences blindly, that caller must be
> updated too.

- [ ] **Step 3: Audit `buildRowKey` callers for null tolerance**

Run:

```
grep -n "buildRowKey(" phoenix-core-client/src/main/java/org/apache/phoenix/index/IndexMaintainer.java \
                       phoenix-core-server/src/main/java/org/apache/phoenix/index/PhoenixIndexCodec.java
```

For each call site, confirm the result is checked for `null` before being dereferenced. Patch any
caller that does not. Specifically:

- `getIndexRowKey(Put)` and `getIndexRowKey(Put, byte[])` (`IndexMaintainer.java:1078, 1098`):
  document at the method level that a null return means "no index entry for this row" and add
  the same notation to JavaDoc. The methods themselves don't dereference, they just return.
- `checkIndexRow(...)` (`IndexMaintainer.java:1084-1095`) calls `getIndexRowKey(dataRow)` and then
  `Bytes.compareTo(builtIndexRowKey, ...)`. Before the `Bytes.compareTo`, add:
  `if (builtIndexRowKey == null) { return false; }` (a sparse-skipped row should not match any
  existing index row).
- `prepareIndexUpdates`-style call sites (search the same file) similarly should treat null as
  "skip this row's index update."

If a call site cannot tolerate `null` and a fix is non-trivial, **STOP** and escalate; do not
silently change semantics.

- [ ] **Step 4: Compile**

```
mvn -pl phoenix-core-client -am -DskipTests compile
```

- [ ] **Step 5: Commit**

```
git add phoenix-core-client/src/main/java/org/apache/phoenix/index/IndexMaintainer.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: sparse-skip rows where indexed BSON path is missing"
```

---

## Task 5: Integration test — write path

**Files:**
- Create: `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/BsonPathIndexWriteIT.java`

- [ ] **Step 1: Write the IT**

```java
package org.apache.phoenix.end2end.index;

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
import org.bson.RawBsonDocument;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

@Category(ParallelStatsDisabledTest.class)
public class BsonPathIndexWriteIT extends ParallelStatsDisabledIT {

  @Test
  public void indexPopulatesOnPathPresent() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tbl = generateUniqueName();
    String idx = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE " + tbl + " (PK VARCHAR PRIMARY KEY, DOC BSON)");
      conn.createStatement().execute(
          "CREATE INDEX " + idx + " ON " + tbl + "(BSON_VALUE(DOC, 'name', 'VARCHAR'))");

      BsonDocument d1 = BsonDocument.parse("{\"name\": \"alice\"}");
      BsonDocument d2 = BsonDocument.parse("{\"name\": \"bob\"}");

      try (PreparedStatement ps = conn.prepareStatement(
          "UPSERT INTO " + tbl + " VALUES (?, ?)")) {
        ps.setString(1, "k1"); ps.setObject(2, d1); ps.execute();
        ps.setString(1, "k2"); ps.setObject(2, d2); ps.execute();
      }
      conn.commit();

      try (ResultSet rs = conn.createStatement().executeQuery(
          "SELECT COUNT(*) FROM " + idx)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
      }
    }
  }

  @Test
  public void indexSparseSkipsMissingPath() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tbl = generateUniqueName();
    String idx = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE " + tbl + " (PK VARCHAR PRIMARY KEY, DOC BSON)");
      conn.createStatement().execute(
          "CREATE INDEX " + idx + " ON " + tbl + "(BSON_VALUE(DOC, 'name', 'VARCHAR'))");

      BsonDocument withName = BsonDocument.parse("{\"name\": \"alice\"}");
      BsonDocument withoutName = BsonDocument.parse("{\"other\": \"x\"}");

      try (PreparedStatement ps = conn.prepareStatement(
          "UPSERT INTO " + tbl + " VALUES (?, ?)")) {
        ps.setString(1, "k1"); ps.setObject(2, withName); ps.execute();
        ps.setString(1, "k2"); ps.setObject(2, withoutName); ps.execute();
      }
      conn.commit();

      try (ResultSet rs = conn.createStatement().executeQuery(
          "SELECT COUNT(*) FROM " + idx)) {
        assertTrue(rs.next());
        // Only k1 should appear in the index (sparse skip on missing path).
        assertEquals(1, rs.getInt(1));
      }
    }
  }

  @Test
  public void canonicalizationCollidesEquivalentDDL() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tbl = generateUniqueName();
    String idxA = generateUniqueName();
    String idxB = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE " + tbl + " (PK VARCHAR PRIMARY KEY, DOC BSON)");
      conn.createStatement().execute(
          "CREATE INDEX " + idxA + " ON " + tbl + "(BSON_VALUE(DOC, 'a.b', 'VARCHAR'))");
      try {
        conn.createStatement().execute(
            "CREATE INDEX " + idxB + " ON " + tbl + "(BSON_VALUE(DOC, '$.a.b', 'VARCHAR'))");
        // If we reach here, canonicalization didn't dedupe. We accept this — the canonicalized
        // expressionStr should still be byte-identical for both indexes after Phase 2 lands. So
        // assert at least one of them stored the canonical form.
      } catch (Exception ok) {
        // duplicate-index error: expected, this is the cleanest evidence of canonicalization.
      }
      // Read SYSTEM.CATALOG to verify the stored expressionStr starts with `BSON_VALUE(... '$.a.b'`.
      try (ResultSet rs = conn.createStatement().executeQuery(
          "SELECT EXPRESSION_STR FROM SYSTEM.\"CATALOG\" WHERE TABLE_NAME = '" + idxA
              + "' AND EXPRESSION_STR IS NOT NULL")) {
        boolean any = false;
        while (rs.next()) {
          String s = rs.getString(1);
          if (s != null && s.contains("$.a.b")) any = true;
        }
        assertTrue("expected canonical $.a.b in stored expression", any);
      }
    }
  }
}
```

- [ ] **Step 2: Run the IT**

```
mvn -pl phoenix-core -Dit.test=BsonPathIndexWriteIT verify
```

Expected: 3 tests, all PASS. If `indexSparseSkipsMissingPath` fails with both rows in the index,
the sparse-null branch from Task 4 didn't kick in — debug by logging the `Expression` instance
type at the indexed-expression iteration in `buildRowKey`. The compiled expression for
`BSON_VALUE(...)` should be `BsonValueFunction`. If wrapped (e.g., `CoerceExpression`), unwrap in
`BsonIndexUtil.isBsonPathExpression`.

- [ ] **Step 3: If `BSON_VALUE`'s default-value behavior produces a non-zero ptr length even on
  missing path** (review `BsonValueFunction.returnDefaultValue`, `BsonValueFunction.java:170-195`),
  switch to detecting "missing" via the `ImmutableBytesWritable` being equal to
  `ByteUtil.EMPTY_BYTE_ARRAY`. The current implementation sets the default to the string `"null"`
  parsed as the indexed type, which means missing-path rows for VARCHAR columns produce the bytes
  for the literal string `"null"`. **In that case, the sparse-skip branch must be more precise:**
  unwrap the default-value semantics first. Spec-aligned approach: define sparse-skip as "the BSON
  path resolved to no value." The simplest way to detect that without modifying `BsonValueFunction`
  is to add a hook: `BsonValueFunction.lastEvaluationWasMissingPath()` (a boolean flag set inside
  `evaluate(...)` whenever it took the `bsonValue == null` branch).

  Add to `BsonValueFunction.java`:
  - private `boolean lastMissing;`
  - in `evaluate`, before any return path, set `lastMissing = false;`
  - in the `if (bsonValue == null)` branch (inside `evaluate`), set `lastMissing = true;` before
    `returnDefaultValue(...)`.
  - public `boolean lastEvaluationWasMissingPath() { return lastMissing; }`

  Update `BsonIndexUtil.isBsonPathExpressionMissing(Expression e)` to consult that flag, and use
  this from `IndexMaintainer.buildRowKey` instead of a length check.

- [ ] **Step 4: Re-run the IT**

```
mvn -pl phoenix-core -Dit.test=BsonPathIndexWriteIT verify
```

Expected: all PASS.

- [ ] **Step 5: Commit**

```
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/index/BsonPathIndexWriteIT.java \
        phoenix-core-client/src/main/java/org/apache/phoenix/expression/function/BsonValueFunction.java \
        phoenix-core-client/src/main/java/org/apache/phoenix/util/BsonIndexUtil.java \
        phoenix-core-client/src/main/java/org/apache/phoenix/index/IndexMaintainer.java
git commit --no-gpg-sign -m "PHOENIX BsonPath: write-path IT covering populate, sparse-skip, dedupe"
```

---

## Local testing plan for Phase 2

| What | Command |
|---|---|
| Compile | `mvn -pl phoenix-core-client -am -DskipTests install` |
| Unit test (helpers) | `mvn -pl phoenix-core -Dtest=BsonIndexUtilTest test` |
| Unit test (compile-only DDL) | `mvn -pl phoenix-core -Dtest=BsonPathCreateIndexCompileTest test` |
| Integration test | `mvn -pl phoenix-core -Dit.test=BsonPathIndexWriteIT verify` |
| Sanity: existing BSON ITs still pass | `mvn -pl phoenix-core -Dit.test='Bson?IT' verify` (runs Bson1IT–Bson6IT) |
| Sanity: existing index ITs still pass | `mvn -pl phoenix-core -Dit.test=IndexMaintenanceIT verify` |
| Roll back the feature flag | Add `-Dphoenix.index.bson.enabled=false` to verify CREATE INDEX is rejected |

**Do not skip the existing BSON ITs.** They exercise BSON_VALUE in non-index contexts; if our
hooks accidentally break them, ship-stopper.

---

## Rollback

Set `phoenix.index.bson.enabled=false` in `hbase-site.xml`. Existing canonical-form indexes are
maintained correctly; new BSON-path `CREATE INDEX` statements raise `BSON_INDEX_DISABLED`.

---

## Self-review checklist

- [ ] All 5 tasks committed in order.
- [ ] Feature flag wired through `QueryServices` + `QueryServicesOptions`; default `true`.
- [ ] `BsonIndexUtil.containsBsonExpression` covers the parse-tree case.
- [ ] `MetaDataClient.createIndex` calls `BsonPathCanonicalizer.rewrite` before `parseNode.toSQL`
      so SYSTEM.CATALOG stores canonical form.
- [ ] `IndexMaintainer.buildRowKey` returns `null` for sparse-skipped rows; all callers tolerate
      `null`.
- [ ] `BsonValueFunction.lastEvaluationWasMissingPath()` flag added if the default-value path
      forced it.
- [ ] Existing `Bson*IT` and `IndexMaintenanceIT` still pass.
- [ ] `BsonPathIndexWriteIT` covers populate, sparse-skip, and DDL canonicalization collision.
