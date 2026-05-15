# Phase 2 — Write Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Track progress on the terminal via the TaskCreate / TaskUpdate task tool.

**Goal:** UPSERTs that supply the promoted virtual column maintain the index correctly. UPSERTs that omit the column produce no index entry (sparse). UPSERTs that supply an inline `ColumnDef` whose type conflicts with the registered virtual column are rejected at compile time with `PHOENIX_DYNAMIC_TYPE_CONFLICT`. Side-by-side correctness: query results with index match results without index across a randomized 200-row fixture.

**Architecture:** A single guard in `UpsertCompiler` rejects type conflicts; index maintenance reuses the existing `IndexMaintainer.buildRowKey` machinery (no new code on the maintainer); sparse-skip falls out of the existing null-result-skip branch added in Feature A+B Phase 2 (commit `b801ef2b59`).

**Tech Stack:** Java 8, Apache Phoenix 5.x, JUnit 4, Mockito, HBase 2.5/2.6.

**Spec:** `docs/superpowers/specs/2026-05-15-dynamic-column-indexes-design.md` § Phase 2.

**Predecessor:** Phase 1 — `CREATE INDEX` promotes virtual columns; queries resolve them; index maintenance does **not** yet validate UPSERT-time type conflicts.

---

## Files

- **Modify:** `phoenix-core-client/src/main/java/org/apache/phoenix/compile/UpsertCompiler.java` — type-conflict guard.
- **Create:** `phoenix-core-client/src/test/java/org/apache/phoenix/compile/UpsertCompilerVirtualConflictTest.java`.
- **Create:** `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/DynamicColumnIndexWriteIT.java`.
- **Create:** `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/DynamicColumnIndexConsistencyIT.java`.

---

## Task 0 — Confirm prerequisites

- [ ] **Step 0.1 — Verify Phase 1 merged**

```bash
git log --oneline | grep -E 'dyncol-index.*Phase 1|DDL grammar|promot' | head -5
```

Expected: ≥ 3 matching commits.

- [ ] **Step 0.2 — Build baseline**

```bash
mvn -q -pl phoenix-core-client -am -DskipTests install
```

Expected: BUILD SUCCESS.

---

## Task 1 — `UpsertCompiler` type-conflict guard

**Files:** `phoenix-core-client/src/main/java/org/apache/phoenix/compile/UpsertCompiler.java`

- [ ] **Step 1.1 — Locate the column-resolution loop**

In `UpsertCompiler.compile()` (line 366+), find the section that iterates the user-supplied `ColumnRef`/`ColumnName` list. If the user wrote `UPSERT INTO t(pk, extra VARCHAR) VALUES (...)`, the inline `(extra VARCHAR)` is parsed as a `ColumnDef` attached to the `UpsertStatement`. Search for `getColumnDefs` or the dynamic-column resolution path:

```bash
grep -n 'ColumnDef\|isDynamic\|getDynColumns' phoenix-core-client/src/main/java/org/apache/phoenix/compile/UpsertCompiler.java | head -20
```

- [ ] **Step 1.2 — Write failing unit test**

Create `phoenix-core-client/src/test/java/org/apache/phoenix/compile/UpsertCompilerVirtualConflictTest.java`:

```java
package org.apache.phoenix.compile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class UpsertCompilerVirtualConflictTest extends BaseConnectionlessQueryTest {

  @Test
  public void rejectsConflictingType() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection c = DriverManager.getConnection(getUrl(), props)) {
      c.createStatement().execute(
          "CREATE TABLE t (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      VirtualColumnTestUtil.injectVirtualColumn(c, "T", "EXTRA", "VARCHAR");

      try {
        c.prepareStatement(
            "UPSERT INTO t (pk, extra INTEGER) VALUES ('a', 42)").executeUpdate();
        fail("expected SQLException");
      } catch (SQLException ex) {
        assertEquals(
            SQLExceptionCode.PHOENIX_DYNAMIC_TYPE_CONFLICT.getErrorCode(),
            ex.getErrorCode());
      }
    }
  }

  @Test
  public void acceptsMatchingType() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection c = DriverManager.getConnection(getUrl(), props)) {
      c.createStatement().execute(
          "CREATE TABLE t2 (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      VirtualColumnTestUtil.injectVirtualColumn(c, "T2", "EXTRA", "VARCHAR");

      // Compiles cleanly — connectionless driver doesn't actually execute against HBase.
      c.prepareStatement(
          "UPSERT INTO t2 (pk, extra VARCHAR) VALUES ('a', 'hi')");
    }
  }

  @Test
  public void acceptsOmittedColumn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection c = DriverManager.getConnection(getUrl(), props)) {
      c.createStatement().execute(
          "CREATE TABLE t3 (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      VirtualColumnTestUtil.injectVirtualColumn(c, "T3", "EXTRA", "VARCHAR");

      // No reference to extra at all — should compile.
      c.prepareStatement(
          "UPSERT INTO t3 (pk, regular) VALUES ('a', 'hi')");
    }
  }

  @Test
  public void acceptsImplicitTypeFromCatalog() throws Exception {
    // UPSERT without inline ColumnDef — type is read from catalog (virtual VARCHAR).
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection c = DriverManager.getConnection(getUrl(), props)) {
      c.createStatement().execute(
          "CREATE TABLE t4 (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      VirtualColumnTestUtil.injectVirtualColumn(c, "T4", "EXTRA", "VARCHAR");

      c.prepareStatement("UPSERT INTO t4 (pk, extra) VALUES ('a', 'hi')");
    }
  }
}
```

- [ ] **Step 1.3 — Run to verify it fails**

```bash
mvn -q -pl phoenix-core-client -Dtest=UpsertCompilerVirtualConflictTest test
```

Expected: `rejectsConflictingType` FAILS — no exception thrown (or wrong error code).

- [ ] **Step 1.4 — Add the guard**

In `UpsertCompiler.compile()`, after the `UpsertStatement upsert = ...` resolution and *before* the column-projection iteration, walk the inline `ColumnDef` list:

```java
// Guard: reject inline ColumnDef whose type conflicts with a registered
// virtual (dynamic-index-promoted) column. Type stability across UPSERTs
// is the contract of dynamic-column indexes.
List<ColumnDef> upsertColumnDefs = upsert.getDynamicColumns();
if (upsertColumnDefs != null && !upsertColumnDefs.isEmpty()) {
  for (ColumnDef cd : upsertColumnDefs) {
    String name = cd.getColumnDefName().getColumnName();
    PColumn registered = null;
    try {
      registered = table.getColumnForColumnName(name);
    } catch (ColumnNotFoundException e) {
      // Not registered — pure dynamic column, allowed.
      continue;
    }
    if (!registered.isVirtual()) {
      // Regular column — existing semantics apply (covered by other code).
      continue;
    }
    if (!registered.getDataType().equals(cd.getDataType())) {
      throw new SQLExceptionInfo.Builder(
          SQLExceptionCode.PHOENIX_DYNAMIC_TYPE_CONFLICT)
          .setColumnName(name)
          .setMessage("Registered type "
              + registered.getDataType().getSqlTypeName()
              + " does not match supplied " + cd.getDataType().getSqlTypeName())
          .build().buildException();
    }
  }
}
```

The exact accessor for the dynamic-column-defs list on `UpsertStatement` may differ — verify by:

```bash
grep -n 'class UpsertStatement\|getDyn\|getColumnDefs' \
    phoenix-core-client/src/main/java/org/apache/phoenix/parse/UpsertStatement.java
```

Adapt the call.

- [ ] **Step 1.5 — Run to verify it passes**

```bash
mvn -q -pl phoenix-core-client -Dtest=UpsertCompilerVirtualConflictTest test
```

Expected: 4 tests, 0 failures.

- [ ] **Step 1.6 — Run wider compile-side regression**

```bash
mvn -q -pl phoenix-core-client -Dtest='UpsertCompiler*Test,*UpsertTest' test
```

Expected: BUILD SUCCESS.

- [ ] **Step 1.7 — Commit**

```bash
git add phoenix-core-client/src/main/java/org/apache/phoenix/compile/UpsertCompiler.java \
        phoenix-core-client/src/test/java/org/apache/phoenix/compile/UpsertCompilerVirtualConflictTest.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: UpsertCompiler rejects type conflicts on virtual columns"
```

---

## Task 2 — `DynamicColumnIndexWriteIT`

**Files:** `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/DynamicColumnIndexWriteIT.java`

- [ ] **Step 2.1 — Write the IT**

```java
package org.apache.phoenix.end2end.index.dyncol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class DynamicColumnIndexWriteIT extends ParallelStatsDisabledIT {

  private Connection conn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props);
  }

  private long countIndexRows(Connection c, String index) throws SQLException {
    ResultSet rs = c.createStatement().executeQuery(
        "SELECT /*+ NO_INDEX */ COUNT(*) FROM " + index);
    rs.next();
    return rs.getLong(1);
  }

  @Test
  public void upsertWithColumnPopulatesIndex() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('row1', 'hello')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('row2', 'world')").executeUpdate();
      c.commit();

      assertEquals(2L, countIndexRows(c, index));
    }
  }

  @Test
  public void upsertWithoutColumnSkipsIndex() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, regular) VALUES ('row1', 'r1')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, regular) VALUES ('row2', 'r2')").executeUpdate();
      c.commit();

      assertEquals("rows omitting the indexed column must be sparse-skipped",
          0L, countIndexRows(c, index));
    }
  }

  @Test
  public void mixedSparseAndPopulatedRows() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      // 3 with extra, 2 without.
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('a', '1')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('b', '2')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, regular) VALUES ('c', 'r')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('d', '3')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, regular) VALUES ('e', 'r')").executeUpdate();
      c.commit();

      assertEquals(3L, countIndexRows(c, index));
    }
  }

  @Test
  public void rejectsConflictingTypeAtUpsert() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      try {
        c.prepareStatement(
            "UPSERT INTO " + table + " (pk, extra INTEGER) VALUES ('row1', 42)").executeUpdate();
        fail("expected PHOENIX_DYNAMIC_TYPE_CONFLICT");
      } catch (SQLException ex) {
        assertEquals(
            SQLExceptionCode.PHOENIX_DYNAMIC_TYPE_CONFLICT.getErrorCode(),
            ex.getErrorCode());
      }
    }
  }

  @Test
  public void deleteRowRemovesIndexEntry() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('a', '1')").executeUpdate();
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('b', '2')").executeUpdate();
      c.commit();
      assertEquals(2L, countIndexRows(c, index));

      c.createStatement().execute("DELETE FROM " + table + " WHERE pk='a'");
      c.commit();
      assertEquals(1L, countIndexRows(c, index));
    }
  }

  @Test
  public void updateOfExtraReplacesIndexEntry() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");
      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('a', 'v1')").executeUpdate();
      c.commit();

      c.prepareStatement(
          "UPSERT INTO " + table + " (pk, extra) VALUES ('a', 'v2')").executeUpdate();
      c.commit();

      ResultSet rs = c.createStatement().executeQuery(
          "SELECT pk FROM " + table + " WHERE extra='v2'");
      assertTrue(rs.next());
      assertEquals("a", rs.getString(1));
      assertFalse(rs.next());

      rs = c.createStatement().executeQuery(
          "SELECT pk FROM " + table + " WHERE extra='v1'");
      assertFalse("old index entry must be gone after update", rs.next());
    }
  }
}
```

- [ ] **Step 2.2 — Run the IT**

```bash
./run-it-tests-local.sh DynamicColumnIndexWriteIT 2>&1 | tee /tmp/dyncol-write.log | tail -40
```

Expected: 6 tests, 0 failures.

- [ ] **Step 2.3 — Iterate**

If a test fails:
1. Read `/tmp/dyncol-write.log` and `phoenix-core/target/failsafe-reports/`.
2. The most likely failure modes:
   - **Index has no entries** — `IndexMaintainer` is not seeing the virtual column on UPSERT. Check that `IndexMaintainer.toProto`/`fromProto` carries `isVirtual` (it should, via the underlying `PColumn` serialization). If not, the regression is in Phase 0 plumbing.
   - **Type conflict not raised at UPSERT time** — `UpsertCompiler` wasn't re-loading the cached PTable. Verify `LastDDLTimestamp` bump from Phase 1.
   - **Stale index entries after UPDATE** — `IndexMaintainer` is treating the virtual column as immutable. Verify the index was created without `IMMUTABLE_ROWS=true`.
3. Patch and re-run.

- [ ] **Step 2.4 — Commit**

```bash
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/DynamicColumnIndexWriteIT.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: DynamicColumnIndexWriteIT — populate, sparse-skip, conflict, update, delete"
```

---

## Task 3 — `DynamicColumnIndexConsistencyIT` (randomized side-by-side)

**Files:** `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/DynamicColumnIndexConsistencyIT.java`

- [ ] **Step 3.1 — Write the IT**

```java
package org.apache.phoenix.end2end.index.dyncol;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Side-by-side correctness: across a 200-row randomized fixture with
 * sparse virtual-column population, the same SELECT must return identical
 * results with the index ENABLED vs. DISABLED.
 */
@Category(ParallelStatsDisabledTest.class)
public class DynamicColumnIndexConsistencyIT extends ParallelStatsDisabledIT {

  private static final int N_ROWS = 200;
  private static final int N_QUERIES = 50;
  private static final long SEED = 0xC0FFEEL;

  private Connection conn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props);
  }

  @Test
  public void resultsMatchWithAndWithoutIndex() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    Random rng = new Random(SEED);

    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      // Populate: 60% have extra, 40% don't.
      List<String> values = new ArrayList<>();
      for (int i = 0; i < N_ROWS; i++) {
        String pk = String.format("p%05d", i);
        if (rng.nextInt(100) < 60) {
          String v = "v" + rng.nextInt(20); // 20 distinct values
          values.add(v);
          c.prepareStatement(
              "UPSERT INTO " + table + " (pk, extra) VALUES (?, ?)")
              .setString(1, pk).setString(2, v);
          PreparedStatement ps = c.prepareStatement(
              "UPSERT INTO " + table + " (pk, extra) VALUES (?, ?)");
          ps.setString(1, pk);
          ps.setString(2, v);
          ps.executeUpdate();
        } else {
          PreparedStatement ps = c.prepareStatement(
              "UPSERT INTO " + table + " (pk, regular) VALUES (?, ?)");
          ps.setString(1, pk);
          ps.setString(2, "r" + i);
          ps.executeUpdate();
        }
      }
      c.commit();

      // For each of N_QUERIES randomized predicates, compare hinted vs non-hinted.
      for (int q = 0; q < N_QUERIES; q++) {
        String v = "v" + rng.nextInt(20);
        String withIndex = "SELECT pk FROM " + table + " WHERE extra = ?";
        String noIndex = "SELECT /*+ NO_INDEX */ pk FROM " + table + " WHERE extra = ?";

        List<String> a = collect(c, withIndex, v);
        List<String> b = collect(c, noIndex, v);
        assertEquals("query " + q + " (extra='" + v + "')", b, a);
      }
    }
  }

  private static List<String> collect(Connection c, String sql, String v) throws Exception {
    PreparedStatement ps = c.prepareStatement(sql);
    ps.setString(1, v);
    ResultSet rs = ps.executeQuery();
    List<String> out = new ArrayList<>();
    while (rs.next()) out.add(rs.getString(1));
    java.util.Collections.sort(out);
    return out;
  }
}
```

- [ ] **Step 3.2 — Run the IT**

```bash
./run-it-tests-local.sh DynamicColumnIndexConsistencyIT 2>&1 | tee /tmp/dyncol-consistency.log | tail -40
```

Expected: 1 test, 0 failures. Runtime ~15–30s.

- [ ] **Step 3.3 — Iterate until green**

If results differ, the index is missing rows or returning extras. Inspect:

```bash
grep -E 'expected|but was' /tmp/dyncol-consistency.log
```

Then run a single seed manually with extra logging in a one-off debug branch.

- [ ] **Step 3.4 — Commit**

```bash
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/DynamicColumnIndexConsistencyIT.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: DynamicColumnIndexConsistencyIT — 200-row randomized side-by-side"
```

---

## Task 4 — Wider regression sweep

- [ ] **Step 4.1 — Phase-1 + Phase-2 + Feature A+B IT smoke**

```bash
./run-it-tests-local.sh \
    DynamicColumnIT \
    DynamicColumnIndexCreationIT \
    DynamicColumnIndexWriteIT \
    DynamicColumnIndexConsistencyIT \
    BsonFlatIndexIT \
    JsonFlatIndexIT
```

Expected: all green.

- [ ] **Step 4.2 — Update PROGRESS.md**

```bash
git add docs/superpowers/PROGRESS.md
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: PROGRESS.md — Phase 2 done"
```

---

## Exit Criteria for Phase 2

- All 4 cases in `UpsertCompilerVirtualConflictTest` green.
- All 6 cases in `DynamicColumnIndexWriteIT` green.
- `DynamicColumnIndexConsistencyIT` green (side-by-side correctness across 50 randomized queries on a 200-row fixture).
- `BsonFlatIndexIT` and `JsonFlatIndexIT` still green — Feature A+B not regressed.
- Phase 2 commits visible in `git log --oneline -15`.
