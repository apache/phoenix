# Phase 1 — DDL Grammar + Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Track progress on the terminal via the TaskCreate / TaskUpdate task tool.

**Goal:** `CREATE INDEX idx ON t(extra VARCHAR DYNAMIC)` parses, promotes `extra` to a virtual `PColumn` in `SYSTEM.CATALOG`, and creates the secondary index. Querying `SELECT extra FROM t` resolves it; `SELECT *` does not. Reject invalid combos with new error codes. **Index maintenance on UPSERT is Phase 2 — Phase 1 ends with the catalog state correct.**

**Architecture:** New soft keyword `DYNAMIC` on the `expression_with_sort_order` rule; `IndexKeyConstraint` carries an extra per-column `dynamic` flag and `ColumnDef`; `MetaDataClient.createIndex` resolves missing-but-dynamic columns by emitting an `addColumnMutation` for a virtual `PColumn` *atomically* with the index-create RPC.

**Tech Stack:** Java 8, ANTLR 3.5, Protobuf 2.5, Apache Phoenix 5.x, JUnit 4, Mockito.

**Spec:** `docs/superpowers/specs/2026-05-15-dynamic-column-indexes-design.md` § Phase 1.

**Predecessor:** Phase 0 (`isVirtual` flag plumbed through `PColumn`/`PColumnImpl`/protobuf/`ProjectionCompiler`/`UpgradeUtil`).

---

## Files

- **Modify:** `phoenix-core-client/src/main/antlr3/PhoenixSQL.g` — add `DYNAMIC` soft keyword and a new alternative on the index-column rule.
- **Modify:** `phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexKeyConstraint.java` — carry per-column `dynamic` flag + optional `ColumnDef`.
- **Modify:** `phoenix-core-client/src/main/java/org/apache/phoenix/parse/ParseNodeFactory.java` — new factory method overload for index-key with dynamic columns.
- **Modify:** `phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java` — promotion branch in `createIndex`.
- **Modify:** `phoenix-core-client/src/main/java/org/apache/phoenix/exception/SQLExceptionCode.java` — three new error codes.
- **Modify:** `phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServices.java` and `QueryServicesOptions.java` — feature flag `phoenix.index.dynamic.enabled`.
- **Create:** `phoenix-core-client/src/test/java/org/apache/phoenix/parse/DynamicIndexGrammarTest.java`.
- **Create:** `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/DynamicColumnIndexCreationIT.java`.

---

## Task 0 — Confirm prerequisites

- [ ] **Step 0.1 — Verify Phase 0 merged**

```bash
git log --oneline | grep -E 'dyncol-index.*Phase 0|isVirtual' | head -5
```

Expected: at least 4 commits matching. If not, do not start Phase 1.

- [ ] **Step 0.2 — Build baseline**

```bash
mvn -q -pl phoenix-core-client -am -DskipTests install
```

Expected: BUILD SUCCESS.

---

## Task 1 — `SQLExceptionCode` entries

**Files:** `phoenix-core-client/src/main/java/org/apache/phoenix/exception/SQLExceptionCode.java`

- [ ] **Step 1.1 — Add entries**

Find an unused 3-digit code in the 13xx range (search `grep -E '^\s+[A-Z_]+\(13' SQLExceptionCode.java | sort`). Pick the next available — e.g., 1330. Add three contiguous entries:

```java
    DYNAMIC_INDEX_REQUIRES_TYPE(1330, "44A30",
        "Dynamic-column index requires an explicit type: CREATE INDEX ... (col TYPE DYNAMIC)."),
    DYNAMIC_INDEX_NAME_CONFLICTS_WITH_REGULAR_COLUMN(1331, "44A31",
        "Cannot use DYNAMIC for a column name that already exists as a regular column."),
    DYNAMIC_INDEX_NOT_ALLOWED_ON_ENCODED_TABLE(1332, "44A32",
        "Dynamic-column indexes are not supported on tables with encoded column qualifiers."),
    PHOENIX_DYNAMIC_TYPE_CONFLICT(1333, "44A33",
        "UPSERT specifies a different type for a registered dynamic-column index than its declared type."),
```

`PHOENIX_DYNAMIC_TYPE_CONFLICT` will be used in Phase 2 — declaring it now keeps error-code numbering stable.

- [ ] **Step 1.2 — Compile**

```bash
mvn -q -pl phoenix-core-client -am -DskipTests compile
```

Expected: BUILD SUCCESS.

- [ ] **Step 1.3 — Commit**

```bash
git add phoenix-core-client/src/main/java/org/apache/phoenix/exception/SQLExceptionCode.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: SQLExceptionCode entries for dynamic-index errors"
```

---

## Task 2 — Feature flag

**Files:** `phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServices.java`, `QueryServicesOptions.java`

- [ ] **Step 2.1 — Add the flag constant**

In `QueryServices.java`, near the existing `phoenix.index.bson.rewrite.enabled` constant, add:

```java
    String PHOENIX_INDEX_DYNAMIC_ENABLED = "phoenix.index.dynamic.enabled";
```

In `QueryServicesOptions.java`, near the BSON default, add:

```java
    public static final boolean DEFAULT_PHOENIX_INDEX_DYNAMIC_ENABLED = true;
```

And in the constructor / `withDefault` chain, add the corresponding `setIfUnset` call (mirror the pattern used for `phoenix.index.bson.rewrite.enabled`).

- [ ] **Step 2.2 — Commit**

```bash
git add phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServices.java \
        phoenix-core-client/src/main/java/org/apache/phoenix/query/QueryServicesOptions.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: feature flag phoenix.index.dynamic.enabled (default true)"
```

---

## Task 3 — Grammar: `DYNAMIC` keyword

**Files:** `phoenix-core-client/src/main/antlr3/PhoenixSQL.g`

- [ ] **Step 3.1 — Write failing parser test**

Create `phoenix-core-client/src/test/java/org/apache/phoenix/parse/DynamicIndexGrammarTest.java`:

```java
package org.apache.phoenix.parse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.junit.Test;

public class DynamicIndexGrammarTest {

  private SQLParser parser(String sql) {
    return new SQLParser(new StringReader(sql), new ParseNodeFactory());
  }

  @Test
  public void parsesDynamicTokenInIndexColumn() throws Exception {
    CreateIndexStatement stmt = (CreateIndexStatement) parser(
        "CREATE INDEX idx ON t(extra VARCHAR DYNAMIC)").parseStatement();
    IndexKeyConstraint ik = stmt.getIndexConstraint();
    assertEquals(1, ik.getParseNodeAndSortOrderList().size());
    assertTrue(ik.isDynamic(0));
    assertEquals("VARCHAR", ik.getColumnDef(0).getDataType().getSqlTypeName());
  }

  @Test
  public void backwardCompatRegularIndexParses() throws Exception {
    CreateIndexStatement stmt = (CreateIndexStatement) parser(
        "CREATE INDEX idx ON t(regular_col)").parseStatement();
    IndexKeyConstraint ik = stmt.getIndexConstraint();
    assertEquals(1, ik.getParseNodeAndSortOrderList().size());
    assertTrue("non-DYNAMIC entries must report dynamic=false", !ik.isDynamic(0));
  }

  @Test
  public void parsesMixedIndexColumns() throws Exception {
    CreateIndexStatement stmt = (CreateIndexStatement) parser(
        "CREATE INDEX idx ON t(regular_col, extra VARCHAR DYNAMIC)").parseStatement();
    IndexKeyConstraint ik = stmt.getIndexConstraint();
    assertEquals(2, ik.getParseNodeAndSortOrderList().size());
    assertTrue(!ik.isDynamic(0));
    assertTrue(ik.isDynamic(1));
  }

  @Test
  public void parsesDynamicWithSortOrder() throws Exception {
    CreateIndexStatement stmt = (CreateIndexStatement) parser(
        "CREATE INDEX idx ON t(extra BIGINT DYNAMIC DESC)").parseStatement();
    IndexKeyConstraint ik = stmt.getIndexConstraint();
    assertTrue(ik.isDynamic(0));
    assertEquals(SortOrder.DESC, ik.getParseNodeAndSortOrderList().get(0).getSecond());
  }

  @Test(expected = Exception.class)
  public void rejectsDynamicWithoutType() throws Exception {
    // "extra DYNAMIC" without an explicit type — ambiguous, must fail at parse time.
    parser("CREATE INDEX idx ON t(extra DYNAMIC)").parseStatement();
  }

  @Test
  public void dynamicAsRegularIdentifierStillWorks() throws Exception {
    // Soft keyword: DYNAMIC must still be usable as a regular column name elsewhere.
    parser("SELECT dynamic FROM t").parseStatement();
    parser("CREATE TABLE t (dynamic VARCHAR PRIMARY KEY)").parseStatement();
  }
}
```

- [ ] **Step 3.2 — Run test to verify it fails**

```bash
mvn -q -pl phoenix-core-client -Dtest=DynamicIndexGrammarTest test
```

Expected: COMPILE FAILURE — `IndexKeyConstraint.isDynamic(int)` and `getColumnDef(int)` don't exist.

- [ ] **Step 3.3 — Add `DYNAMIC` token**

In `PhoenixSQL.g`, find the soft-keyword block (search for `INCLUDE='include'`). Add:

```antlr
DYNAMIC='dynamic';
```

- [ ] **Step 3.4 — Modify `expression_with_sort_order` rule**

Locate the rule (around line 678):

```antlr
expression_with_sort_order returns [Pair<ParseNode, SortOrder> ret]
    :   (x=expression) (order=ASC|order=DESC)?
        {$ret = Pair.newPair(x, order == null ? SortOrder.getDefault() : SortOrder.fromDDLValue(order.getText()));}
;
```

Replace with the `ik_constraint`-context version. To keep parsing unambiguous, introduce a **separate** rule used only by `ik_constraint`:

```antlr
ik_constraint returns [IndexKeyConstraint ret]
@init{ List<IndexKeyConstraint.Entry> entries = new ArrayList<IndexKeyConstraint.Entry>(); }
    :   x=ik_index_column { entries.add(x); }
        (COMMA y=ik_index_column { entries.add(y); })*
        {$ret = factory.indexKey(entries); }
;

ik_index_column returns [IndexKeyConstraint.Entry ret]
    :   // Dynamic form: <name> <TYPE> DYNAMIC [ASC|DESC]
        ( name=column_name td=identifier dyn=DYNAMIC (order=ASC|order=DESC)?
          { $ret = IndexKeyConstraint.Entry.dynamic(
                factory.column(null, name.getText(), name.getText()),
                factory.columnDef(name, td.getText(), true, null, true, null, null, true, SortOrder.getDefault(), null, null, false),
                order == null ? SortOrder.getDefault() : SortOrder.fromDDLValue(order.getText())); }
        )
        |
        // Regular form: <expression> [ASC|DESC]  (existing behavior)
        ( x=expression (order=ASC|order=DESC)?
          { $ret = IndexKeyConstraint.Entry.regular(
                x,
                order == null ? SortOrder.getDefault() : SortOrder.fromDDLValue(order.getText())); }
        )
;
```

The exact `factory.columnDef(...)` call must match the project's existing `ParseNodeFactory.columnDef` signature — verify it exists by:

```bash
grep -n 'public ColumnDef columnDef' phoenix-core-client/src/main/java/org/apache/phoenix/parse/ParseNodeFactory.java
```

Adapt the arguments to match.

- [ ] **Step 3.5 — Add `Entry` to `IndexKeyConstraint`**

Open `IndexKeyConstraint.java`. Replace its body with:

```java
package org.apache.phoenix.parse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.schema.SortOrder;

import com.google.common.collect.ImmutableList;

public class IndexKeyConstraint {
  public static final IndexKeyConstraint EMPTY =
      new IndexKeyConstraint(Collections.<Entry>emptyList());

  /** One column entry in an index-key constraint. */
  public static class Entry {
    private final ParseNode parseNode;
    private final SortOrder sortOrder;
    private final boolean dynamic;
    private final ColumnDef columnDef; // non-null iff dynamic

    private Entry(ParseNode parseNode, ColumnDef columnDef, SortOrder sortOrder, boolean dynamic) {
      this.parseNode = parseNode;
      this.columnDef = columnDef;
      this.sortOrder = sortOrder;
      this.dynamic = dynamic;
    }

    public static Entry regular(ParseNode parseNode, SortOrder sortOrder) {
      return new Entry(parseNode, null, sortOrder, false);
    }

    public static Entry dynamic(ParseNode parseNode, ColumnDef columnDef, SortOrder sortOrder) {
      if (columnDef == null) {
        throw new IllegalArgumentException("dynamic Entry requires a ColumnDef");
      }
      return new Entry(parseNode, columnDef, sortOrder, true);
    }

    public ParseNode getParseNode() { return parseNode; }
    public SortOrder getSortOrder() { return sortOrder; }
    public boolean isDynamic() { return dynamic; }
    public ColumnDef getColumnDef() { return columnDef; }
  }

  private final List<Entry> entries;

  IndexKeyConstraint(List<Entry> entries) {
    this.entries = ImmutableList.copyOf(entries);
  }

  public List<Entry> getEntries() {
    return entries;
  }

  /** Backwards-compatible accessor used by older callers. */
  public List<Pair<ParseNode, SortOrder>> getParseNodeAndSortOrderList() {
    List<Pair<ParseNode, SortOrder>> out = new ArrayList<>(entries.size());
    for (Entry e : entries) {
      out.add(Pair.newPair(e.getParseNode(), e.getSortOrder()));
    }
    return out;
  }

  public boolean isDynamic(int index) { return entries.get(index).isDynamic(); }
  public ColumnDef getColumnDef(int index) { return entries.get(index).getColumnDef(); }
}
```

- [ ] **Step 3.6 — Update `ParseNodeFactory`**

In `ParseNodeFactory.java`, replace the existing `indexKey` (line 352):

```java
public IndexKeyConstraint indexKey(List<IndexKeyConstraint.Entry> entries) {
  return new IndexKeyConstraint(entries);
}

/** Backwards-compatible factory used by tests / older call sites. */
public IndexKeyConstraint indexKeyLegacy(List<Pair<ParseNode, SortOrder>> pairs) {
  List<IndexKeyConstraint.Entry> entries = new ArrayList<>(pairs.size());
  for (Pair<ParseNode, SortOrder> p : pairs) {
    entries.add(IndexKeyConstraint.Entry.regular(p.getFirst(), p.getSecond()));
  }
  return new IndexKeyConstraint(entries);
}
```

- [ ] **Step 3.7 — Build + regenerate ANTLR**

```bash
mvn -q -pl phoenix-core-client -am -DskipTests install
```

Expected: BUILD SUCCESS. If ANTLR generation fails, the rule is ambiguous — use ANTLR's `-report` flag locally to surface the conflict, then refactor `ik_index_column` (e.g., add syntactic predicates `=>` to disambiguate).

- [ ] **Step 3.8 — Run grammar test**

```bash
mvn -q -pl phoenix-core-client -Dtest=DynamicIndexGrammarTest test
```

Expected: BUILD SUCCESS, all 6 tests pass.

- [ ] **Step 3.9 — Run wider parser regression**

```bash
mvn -q -pl phoenix-core-client -Dtest='*ParserTest,*GrammarTest,QueryParserTest' test
```

Expected: BUILD SUCCESS — no regression in existing parsers.

- [ ] **Step 3.10 — Commit**

```bash
git add phoenix-core-client/src/main/antlr3/PhoenixSQL.g \
        phoenix-core-client/src/main/java/org/apache/phoenix/parse/IndexKeyConstraint.java \
        phoenix-core-client/src/main/java/org/apache/phoenix/parse/ParseNodeFactory.java \
        phoenix-core-client/src/test/java/org/apache/phoenix/parse/DynamicIndexGrammarTest.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: grammar accepts DYNAMIC token in CREATE INDEX"
```

---

## Task 4 — `MetaDataClient.createIndex` promotion branch

**Files:** `phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java`

- [ ] **Step 4.1 — Locate insertion points**

`createIndex` starts at line 1595. After `dataTable = tableRef.getTable();` (around line 1640), we need to:

1. Reject `DYNAMIC` on encoded tables.
2. For each `IndexKeyConstraint.Entry` with `dynamic=true`:
   - Validate the column name doesn't already exist as a regular `PColumn`.
   - Validate type is non-null.
   - Build a `PColumnImpl` with `isVirtual=true`, declared type, default `SortOrder`, default CF, qualifier = column name bytes.
   - Add it to the in-memory `PTable` *and* emit an `addColumnMutation`.

- [ ] **Step 4.2 — Add the helper method**

In `MetaDataClient.java`, add:

```java
/**
 * For each DYNAMIC entry in the index-key constraint, promote the column
 * to a virtual PColumn in SYSTEM.CATALOG. Returns the updated PTable
 * with the virtual columns appended (so the index-build phase resolves them).
 */
private PTable promoteDynamicColumnsForIndex(
        PhoenixConnection connection,
        PTable dataTable,
        IndexKeyConstraint ik,
        List<Mutation> tableMetaData) throws SQLException {
  if (!connection.getQueryServices().getProps().getBoolean(
        QueryServices.PHOENIX_INDEX_DYNAMIC_ENABLED,
        QueryServicesOptions.DEFAULT_PHOENIX_INDEX_DYNAMIC_ENABLED)) {
    for (IndexKeyConstraint.Entry e : ik.getEntries()) {
      if (e.isDynamic()) {
        throw new SQLExceptionInfo.Builder(SQLExceptionCode.FEATURE_NOT_SUPPORTED)
            .setMessage("phoenix.index.dynamic.enabled is false")
            .build().buildException();
      }
    }
    return dataTable;
  }

  // Reject encoded tables.
  boolean hasDynamic = false;
  for (IndexKeyConstraint.Entry e : ik.getEntries()) {
    if (e.isDynamic()) { hasDynamic = true; break; }
  }
  if (!hasDynamic) {
    return dataTable;
  }
  if (dataTable.getEncodingScheme() != null
      && dataTable.getEncodingScheme()
          != PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS) {
    throw new SQLExceptionInfo.Builder(
        SQLExceptionCode.DYNAMIC_INDEX_NOT_ALLOWED_ON_ENCODED_TABLE)
        .setTableName(dataTable.getName().getString())
        .build().buildException();
  }

  List<PColumn> newCols = new ArrayList<>(dataTable.getColumns());
  int position = newCols.size();
  Set<String> existing = new HashSet<>();
  for (PColumn c : dataTable.getColumns()) {
    existing.add(c.getName().getString());
  }

  PName defaultFam = dataTable.getDefaultFamilyName() != null
      ? dataTable.getDefaultFamilyName()
      : PNameFactory.newName(QueryConstants.DEFAULT_COLUMN_FAMILY);

  for (IndexKeyConstraint.Entry e : ik.getEntries()) {
    if (!e.isDynamic()) continue;

    ColumnDef cd = e.getColumnDef();
    if (cd == null || cd.getDataType() == null) {
      throw new SQLExceptionInfo.Builder(
          SQLExceptionCode.DYNAMIC_INDEX_REQUIRES_TYPE).build().buildException();
    }
    String name = cd.getColumnDefName().getColumnName();
    if (existing.contains(name)) {
      throw new SQLExceptionInfo.Builder(
          SQLExceptionCode.DYNAMIC_INDEX_NAME_CONFLICTS_WITH_REGULAR_COLUMN)
          .setColumnName(name).build().buildException();
    }

    PColumnImpl virtualCol = new PColumnImpl(
        PNameFactory.newName(name), defaultFam, cd.getDataType(),
        cd.getMaxLength(), cd.getScale(), true /* nullable */, position++,
        e.getSortOrder(), null, null, false, null, false, false,
        Bytes.toBytes(name), EnvironmentEdgeManager.currentTimeMillis(),
        false, /* isVirtual = */ true);
    newCols.add(virtualCol);

    // Emit catalog mutation — same path that ALTER TABLE ADD COLUMN uses.
    ColumnMetaDataOps.addColumnMutation(
        connection,
        dataTable.getSchemaName().getString(),
        dataTable.getTableName().getString(),
        virtualCol,
        false /* not a PK column */);
  }

  // Return an updated PTable for downstream index-creation logic.
  return PTableImpl.builderWithColumns(dataTable, newCols).build();
}
```

If `PTableImpl.builderWithColumns` doesn't exist, use the canonical `PTableImpl.builder().setColumns(newCols).build()` style — verify the current builder by:

```bash
grep -n 'public.*Builder\|builder\s*(' phoenix-core-client/src/main/java/org/apache/phoenix/schema/PTableImpl.java | head
```

If `addColumnMutation` is package-private with a different signature, mirror the call site at `MetaDataClient.addColumn` (around line 5088) — use the same form.

- [ ] **Step 4.3 — Wire the helper into `createIndex`**

After `dataTable = tableRef.getTable();`, add:

```java
List<Mutation> dynamicColumnMutations = new ArrayList<>();
dataTable = promoteDynamicColumnsForIndex(connection, dataTable,
    ik, dynamicColumnMutations);
```

Then locate the place where `createIndex` builds the final mutation list (typically wraps `tableMetaData`) and prepend `dynamicColumnMutations`:

```java
tableMetaData.addAll(0, dynamicColumnMutations);
```

The single RPC that posts `tableMetaData` to the server now atomically (a) promotes the virtual column and (b) registers the index — single `LastDDLTimestamp` bump because both mutations target the same base table.

- [ ] **Step 4.4 — Run targeted unit tests**

```bash
mvn -q -pl phoenix-core-client -Dtest='MetaDataClientTest,DynamicIndexGrammarTest' test
```

Expected: BUILD SUCCESS.

- [ ] **Step 4.5 — Commit**

```bash
git add phoenix-core-client/src/main/java/org/apache/phoenix/schema/MetaDataClient.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: createIndex promotes DYNAMIC columns to virtual PColumns"
```

---

## Task 5 — `DynamicColumnIndexCreationIT`

**Files:** `phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/DynamicColumnIndexCreationIT.java`

- [ ] **Step 5.1 — Write the IT**

```java
package org.apache.phoenix.end2end.index.dyncol;

import static org.apache.phoenix.util.PhoenixRuntime.getTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class DynamicColumnIndexCreationIT extends ParallelStatsDisabledIT {

  private Connection conn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    return DriverManager.getConnection(getUrl(), props);
  }

  private String createBaseTable(Connection conn, String table) throws SQLException {
    conn.createStatement().execute(
        "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR)");
    return table;
  }

  @Test
  public void createsIndexAndPromotesColumn() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");

      PhoenixConnection pc = c.unwrap(PhoenixConnection.class);
      PTable t = pc.getTable(SchemaUtil.normalizeIdentifier(table));

      PColumn extra = null;
      for (PColumn col : t.getColumns()) {
        if (col.getName().getString().equals("EXTRA")) { extra = col; break; }
      }
      assertNotNull("EXTRA must be promoted into base table catalog", extra);
      assertTrue("promoted column must be virtual", extra.isVirtual());
      assertFalse("promoted column must NOT be marked dynamic-runtime", extra.isDynamic());
    }
  }

  @Test
  public void selectStarSkipsPromotedColumn() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");
      ResultSet rs = c.createStatement().executeQuery("SELECT * FROM " + table);
      ResultSetMetaData md = rs.getMetaData();
      assertEquals(2, md.getColumnCount());
      assertEquals("PK", md.getColumnName(1));
      assertEquals("REGULAR", md.getColumnName(2));
    }
  }

  @Test
  public void explicitReferenceResolvesPromotedColumn() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (extra VARCHAR DYNAMIC)");
      // No rows yet — but must compile without "column not found".
      c.prepareStatement("SELECT extra FROM " + table).executeQuery().close();
      c.prepareStatement("SELECT extra FROM " + table + " WHERE extra='x'")
          .executeQuery().close();
    }
  }

  @Test
  public void rejectsDynamicWithoutType() throws Exception {
    // Grammar level — should fail at parse, mapped to a SQLException from the parser.
    String table = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      try {
        c.createStatement().execute(
            "CREATE INDEX " + generateUniqueName() + " ON " + table + " (extra DYNAMIC)");
        fail("expected SQLException");
      } catch (SQLException ex) {
        // Either the grammar threw, or our DYNAMIC_INDEX_REQUIRES_TYPE fires.
        // Either way we just want a SQLException, not a successful CREATE.
      }
    }
  }

  @Test
  public void rejectsConflictWithRegularColumn() throws Exception {
    String table = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, extra VARCHAR)");
      try {
        c.createStatement().execute(
            "CREATE INDEX " + generateUniqueName() + " ON " + table + " (extra VARCHAR DYNAMIC)");
        fail("expected SQLException");
      } catch (SQLException ex) {
        assertEquals(
            SQLExceptionCode.DYNAMIC_INDEX_NAME_CONFLICTS_WITH_REGULAR_COLUMN.getErrorCode(),
            ex.getErrorCode());
      }
    }
  }

  @Test
  public void rejectsOnEncodedTable() throws Exception {
    String table = generateUniqueName();
    try (Connection c = conn()) {
      c.createStatement().execute(
          "CREATE TABLE " + table + " (pk VARCHAR PRIMARY KEY, regular VARCHAR) "
              + "COLUMN_ENCODED_BYTES=2");
      try {
        c.createStatement().execute(
            "CREATE INDEX " + generateUniqueName() + " ON " + table + " (extra VARCHAR DYNAMIC)");
        fail("expected SQLException");
      } catch (SQLException ex) {
        assertEquals(
            SQLExceptionCode.DYNAMIC_INDEX_NOT_ALLOWED_ON_ENCODED_TABLE.getErrorCode(),
            ex.getErrorCode());
      }
    }
  }

  @Test
  public void multipleDynamicColumnsInOneIndex() throws Exception {
    String table = generateUniqueName();
    String index = generateUniqueName();
    try (Connection c = conn()) {
      createBaseTable(c, table);
      c.createStatement().execute(
          "CREATE INDEX " + index + " ON " + table + " (a VARCHAR DYNAMIC, b BIGINT DYNAMIC)");
      PhoenixConnection pc = c.unwrap(PhoenixConnection.class);
      PTable t = pc.getTable(SchemaUtil.normalizeIdentifier(table));
      int virtualCount = 0;
      for (PColumn col : t.getColumns()) {
        if (col.isVirtual()) virtualCount++;
      }
      assertEquals(2, virtualCount);
    }
  }
}
```

- [ ] **Step 5.2 — Run the IT**

```bash
./run-it-tests-local.sh DynamicColumnIndexCreationIT 2>&1 | tee /tmp/dyncol-creation.log | tail -40
```

Expected: 7 tests, 0 failures, 0 errors. If the runner doesn't accept a class arg, fall back:

```bash
mvn -q -pl phoenix-core verify -DfailIfNoTests=false -Dit.test=DynamicColumnIndexCreationIT
```

- [ ] **Step 5.3 — Iterate until green**

If a test fails:
1. Read `/tmp/dyncol-creation.log` for the full stack trace.
2. Check `phoenix-core/target/failsafe-reports/*.txt` for the specific failure.
3. Patch and re-run **the same test only** (not the full suite) until it's green.
4. Then re-run the full Phase 1 IT class once more.

Do not skip a failing test. Do not move to Step 5.4 until all 7 pass.

- [ ] **Step 5.4 — Commit**

```bash
git add phoenix-core/src/it/java/org/apache/phoenix/end2end/index/dyncol/DynamicColumnIndexCreationIT.java
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: DynamicColumnIndexCreationIT — DDL + promotion + 5 negative cases"
```

---

## Task 6 — Wider regression sweep

- [ ] **Step 6.1 — Run all parser + grammar tests**

```bash
mvn -q -pl phoenix-core-client -Dtest='*Parser*Test,*Grammar*Test' test 2>&1 | tail -30
```

Expected: BUILD SUCCESS.

- [ ] **Step 6.2 — Run a representative IT subset**

```bash
./run-it-tests-local.sh \
    DynamicColumnIT \
    DynamicColumnIndexCreationIT \
    BsonFlatIndexIT
```

Expected: all green. The first proves the legacy dynamic-column API is unchanged; the second is new; the third proves Phase 0/1 didn't break the BSON-path index work.

- [ ] **Step 6.3 — Update PROGRESS.md**

Update `docs/superpowers/PROGRESS.md` Feature C section: tick Phase 1.

```bash
git add docs/superpowers/PROGRESS.md
git -c commit.gpgsign=false commit --no-gpg-sign -m "PHOENIX dyncol-index: PROGRESS.md — Phase 1 done"
```

---

## Exit Criteria for Phase 1

- All 7 cases in `DynamicColumnIndexCreationIT` green.
- All 6 cases in `DynamicIndexGrammarTest` green.
- `DynamicColumnIT` (legacy) still green — no regression.
- `BsonFlatIndexIT` (Feature A+B) still green — no regression.
- `mvn -q -pl phoenix-core-client test` clean.
- Phase 1 commits visible in `git log --oneline -15`.
- **Phase 2 prerequisite:** virtual columns are queryable but UPSERTs do **not** yet maintain the index for the virtual column. Sparse-skip works incidentally (the UPSERT just doesn't write the indexed cell), but type-conflict guard isn't installed yet.
