/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Verifies that internal server-side index-maintenance current-row reads honor Phoenix TTL exactly
 * like a client read, and that index-referenced data columns are re-persisted so the data table and
 * a global index stay aligned under compaction.
 * <p>
 * The scenario is the divergence being fixed: a covered column ({@code covcol}) is written once,
 * then never re-written by later partial "touch" upserts. On the index side {@code covcol} is
 * rebuilt at every touch's {@code batchTimestamp}; on the data side it would otherwise keep its
 * original timestamp, and a later major compaction opens a {@code >ttl} gap on only the data side,
 * dropping {@code covcol} there while the index keeps it.
 * <p>
 * The column re-sync re-injects {@code covcol} (and any indexed column) into the touch's data Put at
 * {@code HConstants.LATEST_TIMESTAMP}, so {@code setTimestamps} re-stamps it to {@code batchTimestamp}
 * and it stays aligned with the index cell. Under a {@link ManualEnvironmentEdge} the server's
 * {@code batchTimestamp} equals the touch's wall-clock, so the load-bearing, timing-independent signal
 * is: <b>after a covcol-omitting touch, a raw scan's maximum {@code covcol} timestamp on the data side
 * advances to the touch time</b>. Pre-fix (or with the resync flag off) it stays at the original write
 * time. This class runs with the resync flag defaulting on; {@link IndexDataTableTTLSyncFlagOffIT}
 * asserts the same probe stays at the original timestamp when the flag is off.
 * <p>
 * {@link #testNoIndexAtomicUpsertMasksExpiredRow} covers the no-index masking path: an atomic
 * {@code ON DUPLICATE KEY UPDATE} on a TTL table with no secondary index still triggers an internal
 * current-row read, but there is no {@code IndexMaintainer} to supply the empty-column CF/CQ. That
 * scan is masked using the CF/CQ the client threads on the mutation, so an expired row is treated as
 * absent rather than resurrected.
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class IndexDataTableTTLSyncIT extends BaseTest {
  static final int MAX_LOOKBACK_AGE = 10;
  static final int TTL = 60;
  static final String COVCOL_VALUE = "cov-original";
  static final String IDXCOL_VALUE = "idx-original";

  private final boolean columnEncoded;
  private ManualEnvironmentEdge injectEdge;

  public IndexDataTableTTLSyncIT(boolean columnEncoded) {
    this.columnEncoded = columnEncoded;
  }

  @Parameterized.Parameters(name = "columnEncoded={0}")
  public static synchronized Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { false }, { true } });
  }

  @BeforeClass
  public static synchronized void doSetup() throws Exception {
    setUpTestDriver(new ReadOnlyProps(baseServerProps().entrySet().iterator()));
  }

  /**
   * Common server props for the TTL-sync ITs. Kept as a static helper so
   * {@link IndexDataTableTTLSyncFlagOffIT} reuses the exact same cluster configuration and only adds
   * the resync-disabled flag.
   */
  static Map<String, String> baseServerProps() {
    Map<String, String> props = Maps.newHashMapWithExpectedSize(4);
    props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
    props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
      Integer.toString(MAX_LOOKBACK_AGE));
    props.put("hbase.procedure.remote.dispatcher.delay.msec", "0");
    // The view case threads the view's literal TTL as the per-mutation _TTL attribute.
    props.put(QueryServices.PHOENIX_VIEW_TTL_ENABLED, Boolean.toString(true));
    return props;
  }

  @Before
  public void beforeTest() {
    EnvironmentEdgeManager.reset();
    injectEdge = new ManualEnvironmentEdge();
    injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
  }

  @After
  public synchronized void afterTest() throws Exception {
    boolean refCountLeaked = isAnyStoreRefCountLeaked();
    EnvironmentEdgeManager.reset();
    Assert.assertFalse("refCount leaked", refCountLeaked);
  }

  /**
   * Builds the comma-separated table-property clause. When {@code ttlSeconds >= 0} a {@code TTL} is
   * emitted as the first property; {@code COLUMN_ENCODED_BYTES} always follows, and
   * {@code IS_STRICT_TTL=false} is appended for a non-strict table. Properties are comma-separated
   * (a bare space between properties is a Phoenix parser error).
   */
  private String withClause(int ttlSeconds, boolean strict) {
    StringBuilder sb = new StringBuilder(" ");
    if (ttlSeconds >= 0) {
      sb.append("TTL=").append(ttlSeconds).append(", ");
    }
    sb.append("COLUMN_ENCODED_BYTES=").append(columnEncoded ? 2 : 0);
    if (!strict) {
      sb.append(", IS_STRICT_TTL=false");
    }
    return sb.toString();
  }

  /**
   * Base table with a literal TTL and a covered global index. After a partial touch that never
   * writes covcol, the fix re-stamps covcol on the data side to the touch's batchTimestamp; a full
   * expiry after that shows the data row and the index expiring as a unit.
   */
  @Test
  public void testBaseTableCoveredColumnResync() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (id VARCHAR NOT NULL PRIMARY KEY, "
          + "idxcol VARCHAR, covcol VARCHAR, touchcol VARCHAR)" + withClause(TTL, true));
      conn.createStatement().execute(
        "CREATE INDEX " + indexName + " ON " + tableName + " (idxcol) INCLUDE (covcol)");
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);

      // Full initial upsert: covcol is written exactly once here and never again.
      conn.createStatement().execute("UPSERT INTO " + tableName
        + " (id, idxcol, covcol, touchcol) VALUES ('r1', 'k1', '" + COVCOL_VALUE + "', 'x0')");
      conn.commit();

      // Partial touch that does not write covcol, well within the TTL so the row is alive.
      injectEdge.incrementValue(1000);
      long touchTime = injectEdge.currentTime();
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x1')");
      conn.commit();

      // Load-bearing, deterministic signal: covcol was re-injected into the touch's data Put and
      // re-stamped to batchTimestamp (== touchTime under the manual edge). Pre-fix it stays at the
      // original write time.
      long dataCovTs =
        maxTimestampForValue(conn, TableName.valueOf(tableName), Bytes.toBytes("r1"), COVCOL_VALUE);
      assertTrue("covcol should be re-stamped forward on the data side; touchTime=" + touchTime
        + " dataCovTs=" + dataCovTs, dataCovTs >= touchTime);

      // Data and index agree on covcol: the masked internal scan and the column re-sync keep them
      // aligned.
      assertCovColConsistent(conn, tableName, "r1", "k1", COVCOL_VALUE);

      // Advancing past TTL + max-lookback and compacting expires the row on BOTH sides together,
      // because post-fix covcol sits at batchTimestamp next to empty@batchTimestamp on each side.
      injectEdge.incrementValue((TTL + MAX_LOOKBACK_AGE + 1) * 1000L);
      flushAndMajorCompact(conn, tableName);
      flushAndMajorCompact(conn, indexName);
      assertCovColAbsent(conn, tableName, "r1", "k1");
    }
  }

  /**
   * Uncovered global index on a base table with a literal TTL. An uncovered index stores its
   * indexed column ({@code idxcol}) positionally in the index row key, rebuilt at every touch's
   * {@code batchTimestamp}, while a partial touch that omits {@code idxcol} would otherwise leave
   * the data-side {@code idxcol} cell at its original timestamp. A later major compaction can then
   * trim the stale data-side {@code idxcol} while the row stays alive, so the index key encodes a
   * value the data row no longer has — and because the uncovered read path re-verifies the rebuilt
   * key against the data row, the live row silently drops out of an {@code idxcol} predicate rather
   * than surfacing the stale value. The re-sync must therefore cover uncovered indexes too: it
   * re-injects {@code idxcol} into the touch's data Put at {@code LATEST_TIMESTAMP} so
   * {@code setTimestamps} advances it to {@code batchTimestamp} alongside the index key.
   * <p>
   * Load-bearing, timing-independent signal (mirrors {@link #testBaseTableCoveredColumnResync}):
   * after an {@code idxcol}-omitting touch of a still-alive row, a raw scan's maximum {@code idxcol}
   * timestamp on the data side advances to the touch time. Pre-fix (uncovered skipped) it stays at
   * the original write time.
   */
  @Test
  public void testUncoveredIndexIndexedColumnResync() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (id VARCHAR NOT NULL PRIMARY KEY, "
          + "idxcol VARCHAR, touchcol VARCHAR)" + withClause(TTL, true));
      conn.createStatement()
        .execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + " (idxcol)");
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);

      // Full initial upsert: idxcol is written exactly once here and never again.
      conn.createStatement().execute("UPSERT INTO " + tableName
        + " (id, idxcol, touchcol) VALUES ('r1', '" + IDXCOL_VALUE + "', 'x0')");
      conn.commit();

      // Partial touch that does not write idxcol, well within the TTL so the row is alive and the
      // masked internal scan still returns idxcol for the re-sync to re-persist.
      injectEdge.incrementValue(1000);
      long touchTime = injectEdge.currentTime();
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x1')");
      conn.commit();

      // The uncovered index's indexed column was re-injected into the touch's data Put and
      // re-stamped to batchTimestamp (== touchTime under the manual edge). Pre-fix it stays at the
      // original write time.
      long dataIdxTs =
        maxTimestampForValue(conn, TableName.valueOf(tableName), Bytes.toBytes("r1"), IDXCOL_VALUE);
      assertTrue("uncovered idxcol should be re-stamped forward on the data side; touchTime="
        + touchTime + " dataIdxTs=" + dataIdxTs, dataIdxTs >= touchTime);

      // The row is retrievable through the uncovered index (join-back + key re-verification passes
      // because the data-side idxcol still encodes the same key) and touchcol reflects the touch.
      assertEquals("touchcol via uncovered index", "x1",
        touchColViaUncoveredIndex(conn, tableName, indexName, IDXCOL_VALUE));

      // Advancing past TTL + max-lookback and compacting expires the row on BOTH sides together.
      injectEdge.incrementValue((TTL + MAX_LOOKBACK_AGE + 1) * 1000L);
      flushAndMajorCompact(conn, tableName);
      flushAndMajorCompact(conn, indexName);
      try (ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ touchcol FROM "
        + tableName + " WHERE id = 'r1'")) {
        assertFalse("data-table row should be expired", rs.next());
      }
      assertNull("row should be expired via the uncovered index too",
        touchColViaUncoveredIndex(conn, tableName, indexName, IDXCOL_VALUE));
    }
  }

  /**
   * View with a view-level literal TTL (not on the shared CF descriptor) and a covered index on the
   * view. Exercises the literal-_TTL per-mutation threading: the internal scan masks with the view's
   * TTL, and covcol is re-stamped just like the base-table case.
   */
  @Test
  public void testViewCoveredColumnResync() throws Exception {
    String baseTableName = generateUniqueName();
    String viewName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      // Base table carries NO TTL; the TTL lives only on the view.
      conn.createStatement()
        .execute("CREATE TABLE " + baseTableName + " (id VARCHAR NOT NULL PRIMARY KEY, "
          + "idxcol VARCHAR, covcol VARCHAR, touchcol VARCHAR)" + withClause(-1, true));
      conn.createStatement().execute(
        "CREATE VIEW " + viewName + " AS SELECT * FROM " + baseTableName + " TTL=" + TTL);
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + viewName + " (idxcol) INCLUDE (covcol)");
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);

      conn.createStatement().execute("UPSERT INTO " + viewName
        + " (id, idxcol, covcol, touchcol) VALUES ('r1', 'k1', '" + COVCOL_VALUE + "', 'x0')");
      conn.commit();

      injectEdge.incrementValue(1000);
      long touchTime = injectEdge.currentTime();
      conn.createStatement()
        .execute("UPSERT INTO " + viewName + " (id, touchcol) VALUES ('r1', 'x1')");
      conn.commit();

      long dataCovTs = maxTimestampForValue(conn, TableName.valueOf(baseTableName),
        Bytes.toBytes("r1"), COVCOL_VALUE);
      assertTrue("covcol should be re-stamped forward on the view's data side; touchTime="
        + touchTime + " dataCovTs=" + dataCovTs, dataCovTs >= touchTime);

      assertCovColConsistent(conn, viewName, "r1", "k1", COVCOL_VALUE);
    }
  }

  /**
   * A non-strict table must not be over-masked: the strictness flag is threaded to the internal
   * scan, so a value that a non-strict client read still returns is retained and re-stamped rather
   * than dropped. If strictness were NOT ported, the internal scan would default to strict and mask
   * covcol away past the TTL, so the rewrite would find nothing to re-persist and the probe would
   * stay at the original timestamp.
   */
  @Test
  public void testNonStrictTableNotOverMasked() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (id VARCHAR NOT NULL PRIMARY KEY, "
          + "idxcol VARCHAR, covcol VARCHAR, touchcol VARCHAR)" + withClause(TTL, false));
      conn.createStatement().execute(
        "CREATE INDEX " + indexName + " ON " + tableName + " (idxcol) INCLUDE (covcol)");
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);

      conn.createStatement().execute("UPSERT INTO " + tableName
        + " (id, idxcol, covcol, touchcol) VALUES ('r1', 'k1', '" + COVCOL_VALUE + "', 'x0')");
      conn.commit();

      // Advance beyond TTL, then touch. A strict table would mask covcol at the internal scan;
      // a non-strict table must not.
      injectEdge.incrementValue((TTL + 5) * 1000L);
      long touchTime = injectEdge.currentTime();
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x1')");
      conn.commit();

      long dataCovTs =
        maxTimestampForValue(conn, TableName.valueOf(tableName), Bytes.toBytes("r1"), COVCOL_VALUE);
      assertTrue("non-strict covcol should be retained and re-stamped, not masked away; touchTime="
        + touchTime + " dataCovTs=" + dataCovTs, dataCovTs >= touchTime);

      assertCovColConsistent(conn, tableName, "r1", "k1", COVCOL_VALUE);
    }
  }

  /**
   * The no-index masking path: an atomic {@code ON DUPLICATE KEY UPDATE} on a base table with a
   * literal TTL and NO secondary index. The atomic path still opens an internal current-row read
   * ({@code getCurrentRowStates} fires for {@code hasAtomic}), but there is no
   * {@code IndexMaintainer} to supply the empty-column CF/CQ that let {@code TTLRegionScanner} mask
   * the scan. Those are instead threaded on the mutation by the client
   * ({@code ScanUtil.annotateMutationWithLiteralTTL}) and captured server-side, so the internal scan
   * masks an expired row exactly like a client read — treating it as absent rather than resurrecting
   * it.
   * <p>
   * {@code counter = counter + 1} reads the current {@code counter} from the masked current-row
   * state, so it is the sharpest probe of masking. After the row TTL-expires, a masked scan returns
   * no current row, the {@code ON DUPLICATE KEY} clause is skipped, and the {@code UPSERT VALUES}
   * ({@code counter = 0}) are inserted fresh. Pre-fix the unmasked scan resurrects the expired row
   * and increments {@code counter} to 1. No flush/compaction is involved: the expired cells are
   * still physically present, so read masking alone — not compaction — governs the outcome.
   */
  @Test
  public void testNoIndexAtomicUpsertMasksExpiredRow() throws Exception {
    String tableName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, counter INTEGER)" + withClause(TTL, true));
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);

      // First atomic upsert: the row does not exist, so the ON DUPLICATE clause is skipped and the
      // UPSERT VALUES insert counter = 0.
      conn.createStatement().execute("UPSERT INTO " + tableName
        + " (id, counter) VALUES ('r1', 0) ON DUPLICATE KEY UPDATE counter = counter + 1");
      conn.commit();
      assertEquals("fresh insert of a new row", 0, atomicCounter(conn, tableName));

      // Advance past the TTL. No flush/compact: the row's cells are still physically present, so
      // only read masking - not compaction - can hide the now-expired row from the internal scan.
      injectEdge.incrementValue((TTL + 1) * 1000L);

      // Second atomic upsert on the now-expired row. Post-fix the masked internal scan returns no
      // current row, so this inserts counter = 0 again; pre-fix the unmasked scan resurrects the
      // row and the ON DUPLICATE clause increments counter to 1.
      conn.createStatement().execute("UPSERT INTO " + tableName
        + " (id, counter) VALUES ('r1', 0) ON DUPLICATE KEY UPDATE counter = counter + 1");
      conn.commit();

      assertEquals("expired row must be treated as absent, not resurrected and incremented", 0,
        atomicCounter(conn, tableName));
    }
  }

  // ---- helpers ----

  /**
   * Reads the single {@code counter} value for row {@code r1}, asserting exactly one live row. Used
   * by {@link #testNoIndexAtomicUpsertMasksExpiredRow} to observe the atomic upsert's result.
   */
  private static int atomicCounter(Connection conn, String tableName) throws SQLException {
    try (ResultSet rs = conn.createStatement()
      .executeQuery("SELECT counter FROM " + tableName + " WHERE id = 'r1'")) {
      assertTrue("row should be present", rs.next());
      int value = rs.getInt(1);
      assertFalse("expected exactly one row", rs.next());
      return value;
    }
  }

  private void assertCovColConsistent(Connection conn, String queryTableName, String id,
    String idxColValue, String expectedCovVal) throws SQLException {
    // Force the data-table read.
    try (ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ covcol FROM "
      + queryTableName + " WHERE id = '" + id + "'")) {
      assertTrue("data-table row should exist", rs.next());
      assertEquals("data-table covcol", expectedCovVal, rs.getString(1));
      assertFalse(rs.next());
    }
    // Read via the index (idxcol is the leading index column).
    try (ResultSet rs = conn.createStatement().executeQuery(
      "SELECT covcol FROM " + queryTableName + " WHERE idxcol = '" + idxColValue + "'")) {
      assertTrue("index-visible row should exist", rs.next());
      assertEquals("index covcol", expectedCovVal, rs.getString(1));
      assertFalse(rs.next());
    }
  }

  private void assertCovColAbsent(Connection conn, String queryTableName, String id,
    String idxColValue) throws SQLException {
    try (ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ covcol FROM "
      + queryTableName + " WHERE id = '" + id + "'")) {
      assertFalse("data-table row should be expired", rs.next());
    }
    try (ResultSet rs = conn.createStatement().executeQuery(
      "SELECT covcol FROM " + queryTableName + " WHERE idxcol = '" + idxColValue + "'")) {
      assertFalse("index-visible row should be expired", rs.next());
    }
  }

  /**
   * Raw-scans a single data-table row and returns the maximum timestamp among cells whose value
   * equals {@code value}. Matching by value bytes is column-encoding independent (encoding rewrites
   * qualifiers, not VARCHAR values), so this works for both COLUMN_ENCODED_BYTES=0 and 2.
   */
  static long maxTimestampForValue(Connection conn, TableName tableName, byte[] rowKey, String value)
    throws SQLException, IOException {
    byte[] valueBytes = Bytes.toBytes(value);
    ConnectionQueryServices cqs = conn.unwrap(PhoenixConnection.class).getQueryServices();
    long maxTs = -1L;
    try (Table table = cqs.getTable(tableName.getName())) {
      Scan scan = new Scan();
      scan.withStartRow(rowKey, true);
      scan.withStopRow(rowKey, true);
      scan.setRaw(true);
      scan.readAllVersions();
      try (ResultScanner scanner = table.getScanner(scan)) {
        Result result;
        while ((result = scanner.next()) != null) {
          CellScanner cellScanner = result.cellScanner();
          while (cellScanner.advance()) {
            Cell cell = cellScanner.current();
            if (
              Bytes.equals(valueBytes, 0, valueBytes.length, cell.getValueArray(),
                cell.getValueOffset(), cell.getValueLength())
            ) {
              maxTs = Math.max(maxTs, cell.getTimestamp());
            }
          }
        }
      }
    }
    return maxTs;
  }

  static void flushAndMajorCompact(Connection conn, String tableName) throws Exception {
    TableName tn = TableName.valueOf(tableName);
    try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
      admin.flush(tn);
    }
    TestUtil.majorCompact(getUtility(), tn);
  }

  /**
   * Reads {@code touchcol} for the given indexed value through the uncovered index, forcing the
   * join-back-and-verify read path ({@code touchcol} is neither an index key nor a covered column,
   * so the query must join back to the data table and the uncovered scanner re-verifies the index
   * key against the data row). Returns the {@code touchcol} value, or {@code null} if the uncovered
   * read excludes the row — which happens when the data-side indexed column was trimmed by
   * compaction so the rebuilt index key no longer matches the stored one.
   */
  static String touchColViaUncoveredIndex(Connection conn, String tableName, String indexName,
    String idxColValue) throws SQLException {
    String sql = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ touchcol FROM "
      + tableName + " WHERE idxcol = '" + idxColValue + "'";
    try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
      if (!rs.next()) {
        return null;
      }
      String value = rs.getString(1);
      assertFalse("expected at most one row via the uncovered index", rs.next());
      return value;
    }
  }
}
