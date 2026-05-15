# Phase 0 — `isVirtual` Plumbing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Track progress on the terminal via the TaskCreate / TaskUpdate task tool.

**Goal:** Introduce a new `isVirtual` boolean on `PColumn`, plumb it through protobuf serialization and projection, and backfill it during catalog upgrade. **No user-visible behavior change yet** — Phase 1 wires it to DDL.

**Architecture:** Strictly additive. New protobuf field at tag 18; new `boolean` field on `PColumnImpl`; one filter line in `ProjectionCompiler.projectAllTableColumns()`; one upgrade UPSERT.

**Tech Stack:** Java 8, Protobuf 2.5, ANTLR 3.5, Apache Phoenix 5.x, JUnit 4, Mockito, HBase 2.5/2.6.

**Spec:** `docs/superpowers/specs/2026-05-15-dynamic-column-indexes-design.md` § Phase 0.

---

## Files

- **Modify:** `phoenix-core-client/src/main/protobuf/PTable.proto` — add `isVirtual` field at tag 18.
- **Modify:** `phoenix-core-client/src/main/java/org/apache/phoenix/schema/PColumn.java` — add `isVirtual()` interface method.
- **Modify:** `phoenix-core-client/src/main/java/org/apache/phoenix/schema/PColumnImpl.java` — new field, init, ctor, toProto, createFromProto, all overloaded ctors that copy from another `PColumn`.
- **Modify:** `phoenix-core-client/src/main/java/org/apache/phoenix/compile/ProjectionCompiler.java` — `projectAllTableColumns()` skips virtual columns.
- **Modify:** `phoenix-core-client/src/main/java/org/apache/phoenix/util/UpgradeUtil.java` — add column to `SYSTEM.CATALOG` and backfill defaults to false.
- **Modify:** `phoenix-core-client/src/main/java/org/apache/phoenix/coprocessor/MetaDataProtocol.java` (or version constant location) — bump catalog schema version constant if one exists.
- **Create:** `phoenix-core-client/src/test/java/org/apache/phoenix/schema/PColumnImplVirtualFlagTest.java` — unit test for serialization round-trip.
- **Create:** `phoenix-core-client/src/test/java/org/apache/phoenix/compile/ProjectionCompilerVirtualFilterTest.java` — unit test for `*` expansion.
- **Create:** `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/IsVirtualBackfillIT.java` — IT that asserts upgrade backfills cleanly.

---

## Task 0 — Branch + clean workspace

- [ ] **Step 0.1 — Confirm branch**

```bash
git branch --show-current
```
Expected: `feature/json-indexes`. If different, switch.

- [ ] **Step 0.2 — Build baseline**

```bash
mvn -q -pl phoenix-core-client -am -DskipTests install
```
Expected: BUILD SUCCESS. If not, do not start Phase 0 — fix the build first.

---

## Task 1 — Protobuf field

**Files:** `phoenix-core-client/src/main/protobuf/PTable.proto`

- [ ] **Step 1.1 — Add the field**

Open `phoenix-core-client/src/main/protobuf/PTable.proto`. Inside `message PColumn`, after the `derived` field (tag 17), add:

```protobuf
  optional bool isVirtual = 18 [default = false];
```

Resulting block:

```protobuf
message PColumn {
  required bytes columnNameBytes = 1;
  ...
  optional bool derived = 17 [default = false];
  optional bool isVirtual = 18 [default = false];
}
```

- [ ] **Step 1.2 — Regenerate Java protobuf**

```bash
mvn -q -pl phoenix-core-client -am -DskipTests -Pgenerate-protobuf install
```

If the project doesn't have a `generate-protobuf` profile, regenerate via the standard build:

```bash
mvn -q -pl phoenix-core-client clean compile
```

Expected: build succeeds; `target/generated-sources/.../PTableProtos.java` exists and contains `getIsVirtual()` and `hasIsVirtual()`.

- [ ] **Step 1.3 — Verify generated code**

```bash
grep -c 'getIsVirtual\|hasIsVirtual' phoenix-core-client/target/generated-sources/protobuf/java/org/apache/phoenix/coprocessor/generated/PTableProtos.java
```

Expected: `>= 2`.

- [ ] **Step 1.4 — Commit**

```bash
git add phoenix-core-client/src/main/protobuf/PTable.proto
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: add isVirtual proto field at tag 18"
```

---

## Task 2 — `PColumn` interface method

**Files:** `phoenix-core-client/src/main/java/org/apache/phoenix/schema/PColumn.java`

- [ ] **Step 2.1 — Write failing unit test**

Create `phoenix-core-client/src/test/java/org/apache/phoenix/schema/PColumnImplVirtualFlagTest.java`:

```java
package org.apache.phoenix.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.phoenix.coprocessor.generated.PTableProtos;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.schema.SortOrder;
import org.junit.Test;

public class PColumnImplVirtualFlagTest {

  private PColumnImpl makeCol(boolean isVirtual) {
    return new PColumnImpl(
        PNameFactory.newName("EXTRA"),
        PNameFactory.newName("0"),
        PVarchar.INSTANCE,
        null, null, true, 1,
        SortOrder.getDefault(), null, null,
        false, null, false, false,
        new byte[]{0, 0, 0, 0}, 0L, false, isVirtual);
  }

  @Test
  public void defaultIsFalse() {
    PColumnImpl col = makeCol(false);
    assertFalse(col.isVirtual());
  }

  @Test
  public void roundTripsTrueThroughProto() throws Exception {
    PColumnImpl orig = makeCol(true);
    PTableProtos.PColumn proto = PColumnImpl.toProto(orig);
    assertTrue(proto.getIsVirtual());
    PColumnImpl copy = PColumnImpl.createFromProto(proto);
    assertTrue(copy.isVirtual());
  }

  @Test
  public void roundTripsFalseThroughProto() throws Exception {
    PColumnImpl orig = makeCol(false);
    PTableProtos.PColumn proto = PColumnImpl.toProto(orig);
    assertFalse(proto.getIsVirtual());
    PColumnImpl copy = PColumnImpl.createFromProto(proto);
    assertFalse(copy.isVirtual());
  }

  @Test
  public void copyConstructorPreservesVirtual() {
    PColumnImpl orig = makeCol(true);
    PColumnImpl copy = new PColumnImpl(orig, orig.getPosition());
    assertTrue(copy.isVirtual());
  }
}
```

- [ ] **Step 2.2 — Run test to verify it fails**

```bash
mvn -q -pl phoenix-core-client -Dtest=PColumnImplVirtualFlagTest test
```

Expected: COMPILE FAILURE — `cannot find symbol method isVirtual()`. This is the failing-test signal.

- [ ] **Step 2.3 — Add interface method**

Open `phoenix-core-client/src/main/java/org/apache/phoenix/schema/PColumn.java`. After the existing `boolean isDynamic();` (around line 57), add:

```java
    /**
     * @return true if this column was registered in SYSTEM.CATALOG solely
     * to support a secondary index on a previously dynamic column.
     * Virtual columns are excluded from {@code SELECT *} expansion but are
     * indexable and resolvable by explicit name.
     */
    boolean isVirtual();
```

- [ ] **Step 2.4 — Stub on PColumnImpl**

In `PColumnImpl.java`, near `isDynamic()` (around line 200), add a stub:

```java
    @Override
    public boolean isVirtual() {
        return false;
    }
```

This lets the project compile while we build out the rest in Task 3.

- [ ] **Step 2.5 — Compile**

```bash
mvn -q -pl phoenix-core-client -am -DskipTests compile
```

Expected: BUILD SUCCESS.

- [ ] **Step 2.6 — Commit**

```bash
git add phoenix-core-client/src/main/java/org/apache/phoenix/schema/PColumn.java \
        phoenix-core-client/src/main/java/org/apache/phoenix/schema/PColumnImpl.java \
        phoenix-core-client/src/test/java/org/apache/phoenix/schema/PColumnImplVirtualFlagTest.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: add PColumn.isVirtual() interface + failing test"
```

---

## Task 3 — `PColumnImpl` field + canonical constructor

**Files:** `phoenix-core-client/src/main/java/org/apache/phoenix/schema/PColumnImpl.java`

- [ ] **Step 3.1 — Add the field**

In the field block (around lines 31–47), after `private boolean isDynamic;`, add:

```java
    private boolean isVirtual;
```

- [ ] **Step 3.2 — Add the canonical constructor parameter**

The constructor at line 76 is:

```java
public PColumnImpl(PName name, PName familyName, PDataType dataType, Integer maxLength,
    Integer scale, boolean nullable, int position, SortOrder sortOrder, Integer arrSize,
    byte[] viewConstant, boolean isViewReferenced, String expressionStr, boolean isRowTimestamp,
    boolean isDynamic, byte[] columnQualifierBytes, long timestamp, boolean derived)
```

Add a *new* constructor that takes all the existing args plus `isVirtual`. **Do not modify the existing 17-arg constructor** — too many call sites. Place the new constructor immediately below:

```java
public PColumnImpl(PName name, PName familyName, PDataType dataType, Integer maxLength,
    Integer scale, boolean nullable, int position, SortOrder sortOrder, Integer arrSize,
    byte[] viewConstant, boolean isViewReferenced, String expressionStr, boolean isRowTimestamp,
    boolean isDynamic, byte[] columnQualifierBytes, long timestamp, boolean derived,
    boolean isVirtual) {
    init(name, familyName, dataType, maxLength, scale, nullable, position, sortOrder, arrSize,
        viewConstant, isViewReferenced, expressionStr, isRowTimestamp, isDynamic,
        columnQualifierBytes, timestamp, derived, isVirtual);
}
```

Have the *existing* 17-arg constructor delegate to the new one with `false`:

```java
public PColumnImpl(PName name, PName familyName, PDataType dataType, Integer maxLength,
    Integer scale, boolean nullable, int position, SortOrder sortOrder, Integer arrSize,
    byte[] viewConstant, boolean isViewReferenced, String expressionStr, boolean isRowTimestamp,
    boolean isDynamic, byte[] columnQualifierBytes, long timestamp, boolean derived) {
    this(name, familyName, dataType, maxLength, scale, nullable, position, sortOrder, arrSize,
        viewConstant, isViewReferenced, expressionStr, isRowTimestamp, isDynamic,
        columnQualifierBytes, timestamp, derived, false);
}
```

- [ ] **Step 3.3 — Update `init()`**

Change the `init()` signature to accept `boolean isVirtualVal` as the last parameter, and inside the method body assign `this.isVirtual = isVirtualVal;` immediately after the existing `this.derived = derivedVal;` assignment.

- [ ] **Step 3.4 — Update copy-constructor overloads**

The constructors at lines 52, 56, 64 (taking another `PColumn`) all call the canonical ctor with column fields. For each, append `column.isVirtual()` as the new last arg. Concretely:

- Line 52 — `public PColumnImpl(PColumn column, int position)` — already creates a new col by copying fields. Add `column.isVirtual()` to its delegation.
- Line 56 — `public PColumnImpl(PColumn column, byte[] viewConstant, boolean isViewReferenced)` — same.
- Line 64 — `public PColumnImpl(PColumn column, boolean derivedColumn, int position, byte[] viewConstant)` — same.

If any of those delegate to the 17-arg constructor (not the new 18-arg one), the default `false` propagates correctly *only* when the source column has `isVirtual = false`. We need them to preserve the flag, so explicitly switch them to call the 18-arg constructor. Example for line 52:

```java
public PColumnImpl(PColumn column, int position) {
    this(column.getName(), column.getFamilyName(), column.getDataType(),
        column.getMaxLength(), column.getScale(), column.isNullable(), position,
        column.getSortOrder(), column.getArraySize(), column.getViewConstant(),
        column.isViewReferenced(), column.getExpressionStr(), column.isRowTimestamp(),
        column.isDynamic(), column.getColumnQualifierBytes(), column.getTimestamp(),
        column.isDerived(), column.isVirtual());
}
```

- [ ] **Step 3.5 — Replace the stub `isVirtual()`**

Replace the temporary stub from Task 2.4 with:

```java
    @Override
    public boolean isVirtual() {
        return isVirtual;
    }
```

- [ ] **Step 3.6 — Update `toProto()`**

In `toProto()` (line 331+), after `builder.setDerived(column.isDerived());` (line 369), add:

```java
    builder.setIsVirtual(column.isVirtual());
```

- [ ] **Step 3.7 — Update `createFromProto()`**

In `createFromProto()` (line 273+), after the existing `isDynamic` extraction (line 310-313), add:

```java
    boolean isVirtual = false;
    if (column.hasIsVirtual()) {
      isVirtual = column.getIsVirtual();
    }
```

Find the place where `createFromProto` constructs the new `PColumnImpl` (it ends with a `new PColumnImpl(...)` call) and append `isVirtual` as the new last argument; switch it to the 18-arg constructor.

- [ ] **Step 3.8 — Run unit test**

```bash
mvn -q -pl phoenix-core-client -Dtest=PColumnImplVirtualFlagTest test
```

Expected: BUILD SUCCESS, all 4 tests pass.

- [ ] **Step 3.9 — Run full PColumn-adjacent unit tests**

```bash
mvn -q -pl phoenix-core-client -Dtest='PColumn*Test,PTable*Test,*SchemaUtil*' test
```

Expected: BUILD SUCCESS.

- [ ] **Step 3.10 — Commit**

```bash
git add phoenix-core-client/src/main/java/org/apache/phoenix/schema/PColumnImpl.java \
        phoenix-core-client/src/test/java/org/apache/phoenix/schema/PColumnImplVirtualFlagTest.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: PColumnImpl carries isVirtual through ctors + protobuf"
```

---

## Task 4 — `ProjectionCompiler` virtual filter

**Files:** `phoenix-core-client/src/main/java/org/apache/phoenix/compile/ProjectionCompiler.java`

- [ ] **Step 4.1 — Write failing unit test**

Create `phoenix-core-client/src/test/java/org/apache/phoenix/compile/ProjectionCompilerVirtualFilterTest.java`:

```java
package org.apache.phoenix.compile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

/**
 * Unit-style test using the connectionless driver. Verifies that a column
 * with isVirtual=true does NOT appear in SELECT * but DOES resolve via
 * explicit reference. Phase 0 cannot create virtual columns via DDL yet,
 * so this test injects one via PTableImpl.
 */
public class ProjectionCompilerVirtualFilterTest extends BaseConnectionlessQueryTest {

  @Test
  public void wildcardSkipsVirtualColumn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE t (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      // Inject a virtual column via direct PTable manipulation.
      // (Helper: VirtualColumnTestUtil.makeVirtual(conn, "t", "extra", PVarchar.INSTANCE))
      VirtualColumnTestUtil.injectVirtualColumn(conn, "T", "EXTRA", "VARCHAR");

      ResultSet rs = conn.createStatement().executeQuery(
          "SELECT * FROM t");
      ResultSetMetaData md = rs.getMetaData();
      // Expect 2 columns: PK + REGULAR. EXTRA must be skipped.
      assertEquals(2, md.getColumnCount());
      assertEquals("PK", md.getColumnName(1));
      assertEquals("REGULAR", md.getColumnName(2));
    }
  }

  @Test
  public void explicitReferenceResolvesVirtualColumn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.createStatement().execute(
          "CREATE TABLE t2 (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
      VirtualColumnTestUtil.injectVirtualColumn(conn, "T2", "EXTRA", "VARCHAR");

      // Should compile without error.
      conn.prepareStatement("SELECT extra FROM t2").executeQuery().close();
    }
  }
}
```

Also create `phoenix-core-client/src/test/java/org/apache/phoenix/compile/VirtualColumnTestUtil.java`:

```java
package org.apache.phoenix.compile;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SchemaUtil;

/** Test helper that inserts a virtual PColumn into a PTable cached on the connection. */
public final class VirtualColumnTestUtil {
  private VirtualColumnTestUtil() {}

  public static void injectVirtualColumn(Connection conn, String tableName,
      String columnName, String typeName) throws Exception {
    PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
    PTable table = pconn.getTable(SchemaUtil.normalizeIdentifier(tableName));
    PName name = PNameFactory.newName(SchemaUtil.normalizeIdentifier(columnName));
    PName fam = table.getDefaultFamilyName() != null
        ? table.getDefaultFamilyName() : PNameFactory.newName("0");
    PDataType<?> type = PDataType.fromSqlTypeName(typeName);
    int position = table.getColumns().size();
    PColumnImpl virtual = new PColumnImpl(name, fam, type, null, null, true,
        position, SortOrder.getDefault(), null, null, false, null, false, false,
        name.getBytes(), 0L, false, /* isVirtual = */ true);

    List<PColumn> cols = new ArrayList<>(table.getColumns());
    cols.add(virtual);
    PTable updated = PTableImpl.builderWithColumns(table, cols).build();
    pconn.addTable(updated, System.currentTimeMillis());
  }
}
```

If `PTableImpl.builderWithColumns` does not exist, use `PTableImpl.Builder` from `PTableImpl` (find current builder API and adapt). The test asserts behavior; the helper can use whatever existing API mutates the cached PTable.

- [ ] **Step 4.2 — Run test to verify it fails**

```bash
mvn -q -pl phoenix-core-client -Dtest=ProjectionCompilerVirtualFilterTest test
```

Expected: `wildcardSkipsVirtualColumn` FAILS — column count is 3, not 2 (virtual still appears).

- [ ] **Step 4.3 — Add the filter**

In `phoenix-core-client/src/main/java/org/apache/phoenix/compile/ProjectionCompiler.java`, in `projectAllTableColumns()` around line 145, inside the loop:

```java
for (int i = posOffset, j = posOffset; i < table.getColumns().size(); i++) {
    PColumn column = table.getColumns().get(i);
    if (column.isVirtual()) {
        continue;  // virtual columns are excluded from SELECT *
    }
    if (SchemaUtil.isPKColumn(column) && j++ < minPKOffset) {
        posOffset++;
        continue;
    }
    // ... rest unchanged
}
```

Make the same change in `projectAllIndexColumns()` if it exists in the same file (search for `getColumns()` loops in the file and add the filter wherever `*` expansion happens). For Phase 0 we filter only in the base-table wildcard path; index `*` expansion is irrelevant since there's no index-on-virtual-column yet.

- [ ] **Step 4.4 — Run test to verify it passes**

```bash
mvn -q -pl phoenix-core-client -Dtest=ProjectionCompilerVirtualFilterTest test
```

Expected: BUILD SUCCESS, both tests pass.

- [ ] **Step 4.5 — Run all compile-side tests for regression**

```bash
mvn -q -pl phoenix-core-client -Dtest='*Compiler*Test' test
```

Expected: BUILD SUCCESS.

- [ ] **Step 4.6 — Commit**

```bash
git add phoenix-core-client/src/main/java/org/apache/phoenix/compile/ProjectionCompiler.java \
        phoenix-core-client/src/test/java/org/apache/phoenix/compile/ProjectionCompilerVirtualFilterTest.java \
        phoenix-core-client/src/test/java/org/apache/phoenix/compile/VirtualColumnTestUtil.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: ProjectionCompiler skips virtual columns in SELECT *"
```

---

## Task 5 — `UpgradeUtil` backfill

**Files:** `phoenix-core-client/src/main/java/org/apache/phoenix/util/UpgradeUtil.java`

- [ ] **Step 5.1 — Locate add-column pattern**

Open `UpgradeUtil.java` and find the `addColumn`/`upgradeTo*` method that adds a column to `SYSTEM.CATALOG` during a version transition. Existing patterns appear around line 1198 (`UPSERT INTO SYSTEM.CATALOG(...)`).

- [ ] **Step 5.2 — Add the backfill helper**

Add a new method:

```java
/**
 * Adds the IS_VIRTUAL column to SYSTEM.CATALOG and backfills the default
 * value (false) for all existing PColumn rows.
 */
public static void addIsVirtualColumnIfMissing(PhoenixConnection conn) throws SQLException {
  String addCol = "ALTER TABLE " + SYSTEM_CATALOG_NAME +
      " ADD IF NOT EXISTS " + IS_VIRTUAL + " BOOLEAN";
  conn.createStatement().execute(addCol);
  // No backfill UPSERT needed — Phoenix BOOLEAN columns default to NULL,
  // and PColumnImpl.createFromProto treats absent isVirtual as false.
  // We only need to ensure the catalog SCHEMA carries the column for
  // downstream tooling that introspects SYSTEM.CATALOG.
}
```

Add the constant at the top of the file:

```java
private static final String IS_VIRTUAL = "IS_VIRTUAL";
```

- [ ] **Step 5.3 — Hook into upgrade entry point**

Find the canonical upgrade dispatcher (commonly `upgradeSystemTables` or similar). Add a call to `addIsVirtualColumnIfMissing(conn)` at the appropriate version transition. If the project uses a `MIN_PHOENIX_VERSION` style guard, gate it: `if (currentServerSideTableTimestamp < MIN_PHOENIX_VERSION_FOR_VIRTUAL_COLUMN) { addIsVirtualColumnIfMissing(conn); }`.

- [ ] **Step 5.4 — Write IT**

Create `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/IsVirtualBackfillIT.java`:

```java
package org.apache.phoenix.end2end.index.dyncol;

import static org.apache.phoenix.util.PhoenixRuntime.PHOENIX_TEST_DRIVER_URL_PARAM;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class IsVirtualBackfillIT extends ParallelStatsDisabledIT {

  @Test
  public void systemCatalogHasIsVirtualColumn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      DatabaseMetaData md = conn.getMetaData();
      ResultSet rs = md.getColumns(null, "SYSTEM", "CATALOG", "IS_VIRTUAL");
      assertTrue("IS_VIRTUAL column should exist on SYSTEM.CATALOG", rs.next());
    }
  }
}
```

- [ ] **Step 5.5 — Run IT**

```bash
./run-it-tests-local.sh IsVirtualBackfillIT
```

Expected: 1 test, 0 failures. If the runner doesn't accept a class arg, fall back to:

```bash
mvn -q -pl phoenix-core verify -DfailIfNoTests=false -Dit.test=IsVirtualBackfillIT
```

- [ ] **Step 5.6 — Commit**

```bash
git add phoenix-core-client/src/main/java/org/apache/phoenix/util/UpgradeUtil.java \
        phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/IsVirtualBackfillIT.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: UpgradeUtil adds IS_VIRTUAL column to SYSTEM.CATALOG"
```

---

## Task 6 — Full module test pass

- [ ] **Step 6.1 — Run client unit tests**

```bash
mvn -q -pl phoenix-core-client test 2>&1 | tee /tmp/phase0-client-test.log | tail -40
```

Expected: BUILD SUCCESS. Investigate any new failures; do not proceed until clean.

- [ ] **Step 6.2 — Run a small IT smoke**

```bash
./run-it-tests-local.sh IsVirtualBackfillIT DynamicColumnIT
```

Expected: both pass.

- [ ] **Step 6.3 — Final commit (if any test fixups were needed)**

If 6.1 or 6.2 surfaced regressions and you fixed them, commit:

```bash
git add -A
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: Phase 0 stabilization"
```

---

## Task 7 — Update PROGRESS.md

**Files:** `docs/superpowers/PROGRESS.md`

- [ ] **Step 7.1 — Append Phase 0 status block**

Add to `docs/superpowers/PROGRESS.md`:

```
## Feature C (dynamic-column indexing)
- [x] Phase 0 — isVirtual plumbing (commit <SHA>)
- [ ] Phase 1 — DDL grammar + promotion
- [ ] Phase 2 — write path
- [ ] Phase 3 — drop + observability
```

- [ ] **Step 7.2 — Commit**

```bash
git add docs/superpowers/PROGRESS.md
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: PROGRESS.md — Phase 0 done"
```

---

## Exit Criteria for Phase 0

- All unit tests in Tasks 2, 3, 4 green.
- `IsVirtualBackfillIT` green via `./run-it-tests-local.sh`.
- `mvn -q -pl phoenix-core-client test` clean.
- `git log --oneline -10` shows the 5–6 Phase-0 commits.
- No user-visible behavior change: existing `DynamicColumnIT` unchanged.
