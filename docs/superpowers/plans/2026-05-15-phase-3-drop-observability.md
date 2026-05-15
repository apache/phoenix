# Phase 3 — DROP INDEX Un-promotion + Observability + Docs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Track progress on the terminal via the TaskCreate / TaskUpdate task tool.

**Goal:** When `DROP INDEX` removes the last index referencing a virtual column, that virtual column is removed from `SYSTEM.CATALOG` (un-promotion). Add lightweight client-side counters (`promotions`, `unpromotions`, `type_conflict_rejects`) following the `BsonPathMetrics` pattern. Ship a short user-facing doc.

**Architecture:** Two new branches in `MetaDataClient.dropIndex` (post-drop scan and conditional column-drop mutation). One new metrics class. One markdown doc.

**Tech Stack:** Java 8, Apache Phoenix 5.x, JUnit 4, HBase 2.5/2.6.

**Spec:** `docs/superpowers/specs/2026-05-15-dynamic-column-indexes-design.md` § Phase 3.

**Predecessor:** Phase 2 — index maintenance + type-conflict guard work end-to-end.

---

## Files

- **Modify:** `phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java` — un-promotion logic in `dropIndex` + counter wiring in `createIndex`.
- **Modify:** `phoenix-core-client/src/main/java/org/apache/phoenix/compile/UpsertCompiler.java` — counter increment on `PHOENIX_DYNAMIC_TYPE_CONFLICT`.
- **Create:** `phoenix-core-client/src/main/java/org/apache/phoenix/monitoring/DynamicColumnIndexMetrics.java`.
- **Create:** `phoenix-core-client/src/test/java/org/apache/phoenix/monitoring/DynamicColumnIndexMetricsTest.java`.
- **Create:** `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/DynamicColumnIndexDropIT.java`.
- **Create:** `docs/superpowers/specs/2026-05-15-dynamic-column-indexes-user-guide.md`.

---

## Task 0 — Confirm prerequisites

- [ ] **Step 0.1 — Verify Phase 2 merged**

```bash
git log --oneline | grep -E 'dyncol-index.*Phase 2|UpsertCompiler.*virtual|consistency' | head -5
```

Expected: ≥ 3 matching commits.

- [ ] **Step 0.2 — Build baseline**

```bash
mvn -q -pl phoenix-core-client -am -DskipTests install
```

Expected: BUILD SUCCESS.

---

## Task 1 — `DynamicColumnIndexMetrics`

**Files:**
- `phoenix-core-client/src/main/java/org/apache/phoenix/monitoring/DynamicColumnIndexMetrics.java`
- `phoenix-core-client/src/test/java/org/apache/phoenix/monitoring/DynamicColumnIndexMetricsTest.java`

- [ ] **Step 1.1 — Write failing unit test**

```java
package org.apache.phoenix.monitoring;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class DynamicColumnIndexMetricsTest {

  @Before
  public void resetCounters() {
    DynamicColumnIndexMetrics.resetForTesting();
  }

  @Test
  public void countersStartAtZero() {
    assertEquals(0, DynamicColumnIndexMetrics.getPromotions());
    assertEquals(0, DynamicColumnIndexMetrics.getUnpromotions());
    assertEquals(0, DynamicColumnIndexMetrics.getTypeConflictRejects());
  }

  @Test
  public void incrementsArePerCounter() {
    DynamicColumnIndexMetrics.incrementPromotions();
    DynamicColumnIndexMetrics.incrementPromotions();
    DynamicColumnIndexMetrics.incrementUnpromotions();
    DynamicColumnIndexMetrics.incrementTypeConflictRejects();

    assertEquals(2, DynamicColumnIndexMetrics.getPromotions());
    assertEquals(1, DynamicColumnIndexMetrics.getUnpromotions());
    assertEquals(1, DynamicColumnIndexMetrics.getTypeConflictRejects());
  }
}
```

- [ ] **Step 1.2 — Run test to verify it fails**

```bash
mvn -q -pl phoenix-core-client -Dtest=DynamicColumnIndexMetricsTest test
```

Expected: COMPILE FAILURE.

- [ ] **Step 1.3 — Write the metrics class**

```java
package org.apache.phoenix.monitoring;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Lightweight client-side counters for dynamic-column secondary indexes.
 * Mirrors BsonPathMetrics. Counters are best-effort, JVM-local, and
 * intended for ad-hoc diagnostics — not aggregated metric reporting.
 */
public final class DynamicColumnIndexMetrics {

  private static final AtomicLong PROMOTIONS = new AtomicLong();
  private static final AtomicLong UNPROMOTIONS = new AtomicLong();
  private static final AtomicLong TYPE_CONFLICT_REJECTS = new AtomicLong();

  private DynamicColumnIndexMetrics() {}

  public static void incrementPromotions() { PROMOTIONS.incrementAndGet(); }
  public static void incrementUnpromotions() { UNPROMOTIONS.incrementAndGet(); }
  public static void incrementTypeConflictRejects() { TYPE_CONFLICT_REJECTS.incrementAndGet(); }

  public static long getPromotions() { return PROMOTIONS.get(); }
  public static long getUnpromotions() { return UNPROMOTIONS.get(); }
  public static long getTypeConflictRejects() { return TYPE_CONFLICT_REJECTS.get(); }

  /** Test-only. Not part of the public API. */
  public static void resetForTesting() {
    PROMOTIONS.set(0);
    UNPROMOTIONS.set(0);
    TYPE_CONFLICT_REJECTS.set(0);
  }
}
```

- [ ] **Step 1.4 — Run test to verify it passes**

```bash
mvn -q -pl phoenix-core-client -Dtest=DynamicColumnIndexMetricsTest test
```

Expected: BUILD SUCCESS, 2 tests pass.

- [ ] **Step 1.5 — Commit**

```bash
git add phoenix-core-client/src/main/java/org/apache/phoenix/monitoring/DynamicColumnIndexMetrics.java \
        phoenix-core-client/src/test/java/org/apache/phoenix/monitoring/DynamicColumnIndexMetricsTest.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: DynamicColumnIndexMetrics counters"
```

---

## Task 2 — Wire counters into `createIndex` and `UpsertCompiler`

**Files:**
- `phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java`
- `phoenix-core-client/src/main/java/org/apache/phoenix/compile/UpsertCompiler.java`

- [ ] **Step 2.1 — Hook `incrementPromotions` in `MetaDataClient.promoteDynamicColumnsForIndex`**

In the loop that creates virtual columns, after each successful `addColumnMutation` call, add:

```java
DynamicColumnIndexMetrics.incrementPromotions();
```

- [ ] **Step 2.2 — Hook `incrementTypeConflictRejects` in `UpsertCompiler`**

In the throw path added in Phase 2 — immediately before `throw new SQLExceptionInfo.Builder(...PHOENIX_DYNAMIC_TYPE_CONFLICT)...`, add:

```java
DynamicColumnIndexMetrics.incrementTypeConflictRejects();
```

- [ ] **Step 2.3 — Compile**

```bash
mvn -q -pl phoenix-core-client -am -DskipTests compile
```

Expected: BUILD SUCCESS.

- [ ] **Step 2.4 — Commit**

```bash
git add phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java \
        phoenix-core-client/src/main/java/org/apache/phoenix/compile/UpsertCompiler.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: increment counters on promote + type-conflict reject"
```

---

## Task 3 — `dropIndex` un-promotion

**Files:** `phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java`

- [ ] **Step 3.1 — Locate `dropIndex`**

`dropIndex` is at line 4249. The current body delegates to `dropTable(...)`. We need to wrap the delegation: identify the indexed virtual columns *before* the drop, then *after* the drop check whether any remaining indexes still reference them.

- [ ] **Step 3.2 — Add the helper**

```java
/**
 * After an index drop, remove from SYSTEM.CATALOG any virtual PColumn
 * that was indexed by the dropped index and is no longer referenced
 * by any remaining index on the base table.
 *
 * Cells in HBase are NOT deleted — only the catalog row is removed,
 * making the column unresolvable from SQL going forward.
 */
private void unpromoteOrphanedVirtualColumns(
        PhoenixConnection connection,
        PTable parentTable,
        PTable droppedIndex) throws SQLException {

  // 1. Collect names of base-table virtual columns referenced as keys of the dropped index.
  Set<String> droppedIndexKeyNames = new HashSet<>();
  for (PColumn idxCol : droppedIndex.getPKColumns()) {
    String dataColName = IndexUtil.getDataColumnName(idxCol.getName().getString());
    droppedIndexKeyNames.add(dataColName);
  }

  // 2. Of those, keep only the ones that are virtual on the base table.
  Set<String> candidateForDrop = new HashSet<>();
  for (String name : droppedIndexKeyNames) {
    try {
      PColumn col = parentTable.getColumnForColumnName(name);
      if (col.isVirtual()) {
        candidateForDrop.add(name);
      }
    } catch (ColumnNotFoundException e) {
      // Already gone — skip.
    }
  }
  if (candidateForDrop.isEmpty()) {
    return;
  }

  // 3. Re-resolve the (now updated) PTable and discard any name still referenced
  // by another remaining index.
  PhoenixConnection refreshed = connection;
  PTable freshParent = refreshed.getTable(
      parentTable.getName().getString());
  for (PTable remaining : freshParent.getIndexes()) {
    for (PColumn ic : remaining.getPKColumns()) {
      String dn = IndexUtil.getDataColumnName(ic.getName().getString());
      candidateForDrop.remove(dn);
    }
  }

  if (candidateForDrop.isEmpty()) {
    return;
  }

  // 4. Drop the orphaned virtual columns via the existing ALTER TABLE DROP COLUMN path.
  for (String orphan : candidateForDrop) {
    String drop = "ALTER TABLE " + freshParent.getName().getString()
        + " DROP COLUMN " + orphan;
    refreshed.createStatement().execute(drop);
    DynamicColumnIndexMetrics.incrementUnpromotions();
  }
}
```

`IndexUtil.getDataColumnName` is the canonical helper that strips the index-column-name prefix (e.g., `:EXTRA` → `EXTRA`). Verify by:

```bash
grep -n 'getDataColumnName' phoenix-core-client/src/main/java/org/apache/phoenix/util/IndexUtil.java
```

- [ ] **Step 3.3 — Wire into `dropIndex`**

Modify `dropIndex` to fetch the index PTable *before* the drop, then call the helper after `dropTable` succeeds:

```java
public MutationState dropIndex(DropIndexStatement statement) throws SQLException {
  String schemaName = statement.getTableName().getSchemaName();
  String indexName = statement.getIndexName().getName();
  String parentTableName = statement.getTableName().getTableName();

  // Snapshot the parent and dropped index PTables before mutation.
  PhoenixConnection conn = (PhoenixConnection) connection;
  PTable parent = conn.getTable(SchemaUtil.getTableName(schemaName, parentTableName));
  PTable indexBeforeDrop = null;
  for (PTable idx : parent.getIndexes()) {
    if (idx.getTableName().getString().equalsIgnoreCase(indexName)) {
      indexBeforeDrop = idx;
      break;
    }
  }

  MutationState result = dropTable(schemaName, indexName, parentTableName,
      PTableType.INDEX, statement.ifExists(), false, false);

  if (indexBeforeDrop != null) {
    unpromoteOrphanedVirtualColumns(conn, parent, indexBeforeDrop);
  }
  return result;
}
```

The exact `connection` field name on `MetaDataClient` may be `this.connection` — verify and adapt.

- [ ] **Step 3.4 — Compile**

```bash
mvn -q -pl phoenix-core-client -am -DskipTests install
```

Expected: BUILD SUCCESS.

- [ ] **Step 3.5 — Commit**

```bash
git add phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: dropIndex un-promotes orphaned virtual columns"
```

---

## Task 4 — `DynamicColumnIndexDropIT`

**Files:** `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/DynamicColumnIndexDropIT.java`

- [ ] **Step 4.1 — Write the IT**

```java
package org.apache.phoenix.end2end.index.dyncol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.DynamicColumnIndexMetrics;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class DynamicColumnIndexDropIT extends ParallelStatsDisabledIT {

  @Before
  public void resetMetrics() {
    DynamicColumnIndexMetrics.resetForTesting();
  }

  private Connection conn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props);
  }

  private boolean hasColumn(Connection c, String table, String col) throws Exception {
    PhoenixConnection pc = c.unwrap(PhoenixConnection.class);
    PTable t = pc.getTable(SchemaUtil.normalizeIdentifier(table));
    try {
      PColumn pc2 = t.getColumnForColumnName(SchemaUtil.normalizeIdentifier(col));
      return pc2 != null;
    } catch (ColumnNotFoundException e) {
      return false;
    }
  }

  @Test
  public void singleIndexDropUnpromotesColumn() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");
      assertTrue(hasColumn(c, table, "extra"));
      assertEquals(1, DynamicColumnIndexMetrics.getPromotions());

      c.createStatement().execute("DROP INDEX " + index + " ON " + table);
      assertFalse("virtual column must be removed", hasColumn(c, table, "extra"));
      assertEquals(1, DynamicColumnIndexMetrics.getUnpromotions());
    }
  }

  @Test
  public void twoIndexesDropOnePersists() throws Exception {
    String table = generateUniqueName();
    String i1 = generateUniqueName();
    String i2 = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      c.createStatement().execute(
          "CREATE INDEX " + i1 + " ON " + table + " (extra VARCHAR DYNAMIC)");
      c.createStatement().execute(
          "CREATE INDEX " + i2 + " ON " + table + " (extra VARCHAR DYNAMIC, regular)");
      // Phase 1's name-conflict guard rejects re-promotion if extra exists,
      // so the second index reuses the existing virtual column.
      assertEquals("only one promotion should have happened", 1,
          DynamicColumnIndexMetrics.getPromotions());

      c.createStatement().execute("DROP INDEX " + i1 + " ON " + table);
      assertTrue("extra still referenced by " + i2 + ", must persist",
          hasColumn(c, table, "extra"));
      assertEquals(0, DynamicColumnIndexMetrics.getUnpromotions());

      c.createStatement().execute("DROP INDEX " + i2 + " ON " + table);
      assertFalse("now orphaned, must be unpromoted",
          hasColumn(c, table, "extra"));
      assertEquals(1, DynamicColumnIndexMetrics.getUnpromotions());
    }
  }

  @Test
  public void droppingNonDynamicIndexDoesNotTouchVirtual() throws Exception {
    String table = generateUniqueName();
    String i1 = generateUniqueName();
    String i2 = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      c.createStatement().execute(
          "CREATE INDEX " + i1 + " ON " + table + " (extra VARCHAR DYNAMIC)");
      c.createStatement().execute(
          "CREATE INDEX " + i2 + " ON " + table + " (regular)");
      c.createStatement().execute("DROP INDEX " + i2 + " ON " + table);
      assertTrue(hasColumn(c, table, "extra"));
      assertEquals(0, DynamicColumnIndexMetrics.getUnpromotions());
    }
  }
}
```

- [ ] **Step 4.2 — Run the IT**

```bash
./run-it-tests-local.sh DynamicColumnIndexDropIT 2>&1 | tee /tmp/dyncol-drop.log | tail -40
```

Expected: 3 tests, 0 failures.

- [ ] **Step 4.3 — Iterate until green**

Likely failure modes:
- Test 2 fails because Phase 1's promotion logic re-throws `DYNAMIC_INDEX_NAME_CONFLICTS_WITH_REGULAR_COLUMN` for the second index — it shouldn't, because the column is already promoted (so it exists, but it's virtual). Adjust `promoteDynamicColumnsForIndex` in Phase 1 (you'd retroactively patch it): if the existing column is `isVirtual()` and the type matches, skip re-promotion silently. If the type **differs**, throw `PHOENIX_DYNAMIC_TYPE_CONFLICT`. Update the helper from Phase 1, Task 4.2 accordingly.
- Test 3 fails because `unpromoteOrphanedVirtualColumns` is being called on non-DYNAMIC drops. Verify the helper short-circuits if `candidateForDrop` is empty (which it should since the dropped index doesn't have any virtual key columns).

Patch and re-run until all 3 pass.

- [ ] **Step 4.4 — Commit**

```bash
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/DynamicColumnIndexDropIT.java \
        phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: DynamicColumnIndexDropIT — un-promotion + ref-counted virtual columns"
```

---

## Task 5 — User-facing docs

**Files:** `docs/superpowers/specs/2026-05-15-dynamic-column-indexes-user-guide.md`

- [ ] **Step 5.1 — Write the doc**

```markdown
# Indexing Dynamic Columns in Apache Phoenix

**Audience:** Phoenix users wanting indexes on tenant-defined / sparse columns
that aren't present in the base table schema.
**Available from:** Apache Phoenix 5.x (commit hash to be filled in at release).

## TL;DR

```sql
CREATE INDEX idx_extra ON my_table (extra VARCHAR DYNAMIC);

-- Sparse: only rows that supply `extra` produce an index entry.
UPSERT INTO my_table (pk, extra) VALUES ('row1', 'hello');
UPSERT INTO my_table (pk, regular) VALUES ('row2', 'no extra');

-- Hits the index.
SELECT pk FROM my_table WHERE extra = 'hello';

-- Removes both the index AND the virtual column registration
-- (when no other index still references `extra`).
DROP INDEX idx_extra ON my_table;
```

## When to use

- Tenant-defined or schema-on-read columns where most rows omit the column.
- You want index lookups on the column without widening the base schema.
- The column has a stable, declared type — Phoenix rejects UPSERTs that
  declare a conflicting type.

## When NOT to use

- The column will be present on most rows: use `ALTER TABLE ADD COLUMN` +
  `CREATE INDEX`. You give up no flexibility and gain `SELECT *` ergonomics.
- You need different types per row: this is a category mismatch with
  Phoenix's typed index model. Consider a BSON column with path indexes
  (Feature A+B in this codebase).

## Semantics

| Behavior | Detail |
|---|---|
| Visibility in `SELECT *` | Excluded. Reference by explicit name. |
| Sparse rows | UPSERTs that omit the column produce no index entry. |
| Type conflict | `UPSERT INTO t(extra INTEGER)` against a `VARCHAR DYNAMIC` index → `PHOENIX_DYNAMIC_TYPE_CONFLICT`. |
| Encoded tables | Not supported. Use `COLUMN_ENCODED_BYTES=0`. |
| `DROP INDEX` | When the last index referencing the virtual column is dropped, the column is also removed from `SYSTEM.CATALOG`. Cells in HBase remain. |

## Restrictions

- Type must be explicit. `CREATE INDEX idx ON t(extra DYNAMIC)` is rejected.
- Names cannot collide with existing regular columns.
- Sync global indexes only in v1. Local, async, and eventually-consistent
  variants are not yet supported on virtual columns.

## Observability

Three JVM-local counters live in
`org.apache.phoenix.monitoring.DynamicColumnIndexMetrics`:

- `getPromotions()` — virtual columns added to `SYSTEM.CATALOG`.
- `getUnpromotions()` — virtual columns removed via DROP INDEX.
- `getTypeConflictRejects()` — UPSERTs rejected with `PHOENIX_DYNAMIC_TYPE_CONFLICT`.

These are best-effort, JVM-local AtomicLongs. Use for ad-hoc diagnostics
during development and migration; not aggregated reporting.

## Configuration

| Property | Default | Effect |
|---|---|---|
| `phoenix.index.dynamic.enabled` | `true` | Master switch. When false, `CREATE INDEX … DYNAMIC` is rejected. Already-promoted columns remain readable. |
```

- [ ] **Step 5.2 — Commit**

```bash
git add docs/superpowers/specs/2026-05-15-dynamic-column-indexes-user-guide.md
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: user guide v1"
```

---

## Task 6 — Final regression sweep + PROGRESS

- [ ] **Step 6.1 — Run all dyncol ITs**

```bash
./run-it-tests-local.sh \
    IsVirtualBackfillIT \
    DynamicColumnIndexCreationIT \
    DynamicColumnIndexWriteIT \
    DynamicColumnIndexConsistencyIT \
    DynamicColumnIndexDropIT
```

Expected: all green.

- [ ] **Step 6.2 — Run Feature A+B regression**

```bash
./run-it-tests-local.sh \
    DynamicColumnIT \
    BsonFlatIndexIT \
    BsonNestedIndexIT \
    JsonFlatIndexIT \
    JsonNestedIndexIT
```

Expected: all green.

- [ ] **Step 6.3 — Run client unit tests**

```bash
mvn -q -pl phoenix-core-client test 2>&1 | tail -10
```

Expected: BUILD SUCCESS.

- [ ] **Step 6.4 — Update PROGRESS.md**

Mark Phase 3 done; mark Feature C v1 complete.

```bash
git add docs/superpowers/PROGRESS.md
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: PROGRESS.md — Feature C v1 complete"
```

---

## Exit Criteria for Phase 3

- All 2 cases in `DynamicColumnIndexMetricsTest` green.
- All 3 cases in `DynamicColumnIndexDropIT` green.
- Feature A+B suites (BSON + JSON) and the legacy `DynamicColumnIT` still green.
- `mvn -q -pl phoenix-core-client test` clean.
- User guide committed.
- `PROGRESS.md` shows all 4 phases of Feature C complete.

## Open follow-on work (out of scope for v1)

- Multi-tenant view interaction with virtual columns.
- CDC inclusion of virtual columns in change images.
- Local / async / EC index variants on virtual columns.
- BSON-typed virtual columns + path indexes (composes with Feature A+B; requires a separate spec).
