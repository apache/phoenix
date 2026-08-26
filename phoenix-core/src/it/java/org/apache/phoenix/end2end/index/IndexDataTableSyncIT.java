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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
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
 * like a client read, and that the read is anchored at the mutation timestamp so the data table and
 * a global index stay aligned under compaction/TTL masking.
 * <p>
 * The scenario is the divergence being fixed: a covered column (covcol) is written once, then never
 * re-written by later partial "touch" upserts. On the index side covcol is rebuilt at every touch's
 * mutationTimestamp from the masked current-row read; on the data side covcol keeps its original
 * timestamp. The fix anchors that internal read's masking clock at mutationTimestamp (the same
 * instant the index is built at) via scan.setTimeRange(0, mutationTimestamp), so both sides always
 * agree on whether covcol is still alive: whatever the read masks the index rebuild omits, and
 * whatever the read keeps a later major compaction keeps on the data side too. The row and its
 * index then expire as a unit rather than covcol being dropped on only one side.
 * {@link #testNoIndexAtomicUpsertMasksExpiredRow} covers the no-index masking path: an atomic ON
 * DUPLICATE KEY UPDATE on a TTL table with no secondary index still triggers an internal
 * current-row read. That scan is masked using the empty-column CF/CQ the client threads on the
 * mutation — the same single source used for the secondary-index case — so an expired row is
 * treated as absent rather than resurrected.
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class IndexDataTableSyncIT extends BaseTest {
  static final int MAX_LOOKBACK_AGE = 10;
  static final int TTL = 60;
  static final String COVCOL_VALUE = "cov-original";
  static final String IDXCOL_VALUE = "idx-original";

  private final boolean columnEncoded;
  private ManualEnvironmentEdge injectEdge;

  public IndexDataTableSyncIT(boolean columnEncoded) {
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
   * Common server props for the TTL-sync IT: max-lookback and immediate global-index row aging so a
   * major compaction deterministically exercises the covered-column timestamp-skew divergence.
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
   * Builds the comma-separated table-property clause. When ttlSeconds >= 0 a TTL is emitted as the
   * first property; COLUMN_ENCODED_BYTES always follows, and IS_STRICT_TTL=false is appended for a
   * non-strict table.
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
   * writes covcol, the data-side covcol keeps its original timestamp; the masked internal read
   * anchored at mutationTimestamp keeps the data and index agreeing on covcol while the row is
   * alive, and a full expiry after that shows the data row and the index expiring as a unit.
   */
  @Test
  public void testBaseTableCoveredColumnResync() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (id VARCHAR NOT NULL PRIMARY KEY, "
          + "idxcol VARCHAR, covcol VARCHAR, touchcol VARCHAR)" + withClause(TTL, true));
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + " (idxcol) INCLUDE (covcol)");
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);

      // Full initial upsert: covcol is written exactly once here and never again.
      conn.createStatement().execute("UPSERT INTO " + tableName
        + " (id, idxcol, covcol, touchcol) VALUES ('r1', 'k1', '" + COVCOL_VALUE + "', 'x0')");
      conn.commit();

      // Partial touch that does not write covcol, well within the TTL so the row is alive.
      injectEdge.incrementValue(1000);
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x1')");
      conn.commit();
      assertTouchColConsistent(conn, tableName, indexName, "r1", "x1");
      // Data and index agree on covcol
      assertCovColConsistent(conn, tableName, indexName, "r1", "k1", COVCOL_VALUE);

      // Advancing past TTL expires the row in data table for internal current row read.
      injectEdge.incrementValue((TTL + 1) * 1000L);
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x2')");
      conn.commit();
      assertTouchColConsistent(conn, tableName, indexName, "r1", "x2");
      // Data and index agree on covcol
      assertCovColAbsent(conn, tableName, indexName, "r1");
    }
  }

  /**
   * Uncovered global index on a base table with a literal TTL. An uncovered index stores its
   * indexed column (idxcol) in the index row key, rebuilt at every touch's mutationTimestamp, while
   * a partial touch that omits idxcol would otherwise leave the data-side idxcol cell at its
   * original timestamp.
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
      // internal scan anchored at batchTimestamp still returns idxcol for the index rebuild.
      injectEdge.incrementValue(1000);
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x1')");
      conn.commit();

      // An uncovered index carries no data columns, so touchcol is only readable data-side; the
      // index side agrees on idxcol below. The row is alive well within the TTL.
      assertDataTouchColAlive(conn, tableName, "r1", "x1");
      assertUncoveredIdxColConsistent(conn, tableName, indexName, "r1", IDXCOL_VALUE);

      // Advancing past TTL expires the row in data table for internal current row read.
      injectEdge.incrementValue((TTL + 1) * 1000L);
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x2')");
      conn.commit();
      // Data side: row still alive by PK (touchcol keeps it); the masked idxcol@T0 is trimmed, so
      // the uncovered index no longer carries a usable entry keyed by idxcol (asserted below).
      assertDataTouchColAlive(conn, tableName, "r1", "x2");
      assertUncoveredIdxColAbsent(conn, tableName, indexName, IDXCOL_VALUE);
    }
  }

  /**
   * View with a view-level literal TTL (not on the shared CF descriptor) and a covered index on the
   * view. Exercises the literal-_TTL per-mutation threading: the internal scan masks with the
   * view's TTL, anchored at mutationTimestamp, and covcol keeps its original data-side timestamp
   * just like the base-table case while the data and index reads stay consistent. A full expiry
   * after that shows the view's data row and its index expiring as a unit.
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
      conn.createStatement()
        .execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + baseTableName + " TTL=" + TTL);
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + viewName + " (idxcol) INCLUDE (covcol)");
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);

      conn.createStatement().execute("UPSERT INTO " + viewName
        + " (id, idxcol, covcol, touchcol) VALUES ('r1', 'k1', '" + COVCOL_VALUE + "', 'x0')");
      conn.commit();

      injectEdge.incrementValue(1000);
      conn.createStatement()
        .execute("UPSERT INTO " + viewName + " (id, touchcol) VALUES ('r1', 'x1')");
      conn.commit();
      assertTouchColConsistent(conn, viewName, indexName, "r1", "x1");
      // View and index agree on covcol
      assertCovColConsistent(conn, viewName, indexName, "r1", "k1", COVCOL_VALUE);

      // Advancing past the view TTL expires the row in data table for internal current row read.
      injectEdge.incrementValue((TTL + 1) * 1000L);
      conn.createStatement()
        .execute("UPSERT INTO " + viewName + " (id, touchcol) VALUES ('r1', 'x2')");
      conn.commit();
      assertTouchColConsistent(conn, viewName, indexName, "r1", "x2");
      assertCovColAbsent(conn, viewName, indexName, "r1");
    }
  }

  /**
   * A table with IS_STRICT_TTL=false must not be masked away: the internal scan anchored at
   * mutationTimestamp must not mask covcol away.
   */
  @Test
  public void testNonStrictTableNotOverMasked() throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (id VARCHAR NOT NULL PRIMARY KEY, "
          + "idxcol VARCHAR, covcol VARCHAR, touchcol VARCHAR)" + withClause(TTL, false));
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + " (idxcol) INCLUDE (covcol)");
      conn.commit();

      EnvironmentEdgeManager.injectEdge(injectEdge);

      conn.createStatement().execute("UPSERT INTO " + tableName
        + " (id, idxcol, covcol, touchcol) VALUES ('r1', 'k1', '" + COVCOL_VALUE + "', 'x0')");
      conn.commit();

      // Advance beyond TTL, then touch. A strict table would mask covcol at the internal scan;
      // a non-strict table must not.
      injectEdge.incrementValue((TTL + 5) * 1000L);
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x1')");
      conn.commit();
      assertTouchColConsistent(conn, tableName, indexName, "r1", "x1");
      // The decisive check: a non-strict internal read is not masked away, so the index rebuilds
      // with covcol and both the data read and the index read still return it.
      assertCovColConsistent(conn, tableName, indexName, "r1", "k1", COVCOL_VALUE);

      injectEdge.incrementValue(MAX_LOOKBACK_AGE * 1000L);
      flushAndMajorCompact(conn, tableName);
      flushAndMajorCompact(conn, indexName);

      // Data side: the row is still alive (touchcol keeps it) but the stale covcol was physically
      // collected by compaction, so a forced data-table read returns covcol = NULL.
      try (ResultSet rs = conn.createStatement()
        .executeQuery("SELECT /*+ NO_INDEX */ covcol FROM " + tableName + " WHERE id = 'r1'")) {
        assertTrue("data-table row should still be alive (touchcol keeps it)", rs.next());
        assertNull("stale data-side covcol should be collected by compaction", rs.getString(1));
        assertFalse("exactly one data-table row", rs.next());
      }

      // Index side: the covered index copy survives compaction, so the index read still returns it.
      try (ResultSet rs = conn.createStatement()
        .executeQuery("SELECT covcol FROM " + tableName + " WHERE idxcol = 'k1'")) {
        assertTrue("index-visible row should still exist", rs.next());
        assertEquals("index covcol should survive compaction", COVCOL_VALUE, rs.getString(1));
        assertFalse("exactly one index-visible row", rs.next());
      }
    }
  }

  /**
   * The no-index masking path: an atomic ON DUPLICATE KEY UPDATE on a base table with a literal TTL
   * and NO secondary index.
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

  @Test
  public void testUpsertSelectTouchNearTtlBoundaryKeepsDataAndIndexConsistent() throws Exception {
    runTtlBoundaryScenario(true);
  }

  @Test
  public void testUpsertValuesTouchNearTtlBoundaryKeepsDataAndIndexConsistent() throws Exception {
    runTtlBoundaryScenario(false);
  }

  @Test
  public void testImmutableDifferentStorageSchemeIndexKeepsDataAndIndexConsistent()
    throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      // The scheme mismatch (independent of the class columnEncoded parameter, which this test does
      // not use) is the point: it forces server-side index maintenance and the masked current-row
      // read. Direction ONE_CELL data -> SINGLE_CELL index is the only allowed one.
      conn.createStatement().execute("CREATE IMMUTABLE TABLE " + tableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, idxcol VARCHAR, covcol VARCHAR, touchcol VARCHAR) "
        + "TTL=" + TTL + ", IMMUTABLE_STORAGE_SCHEME=ONE_CELL_PER_COLUMN, COLUMN_ENCODED_BYTES=0");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + " (idxcol) INCLUDE (covcol) "
          + "IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=2");
      conn.commit();

      // Confirm the two sides really do use different storage schemes.
      assertMetadata(conn, PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN,
        PTable.QualifierEncodingScheme.NON_ENCODED_QUALIFIERS, tableName);
      assertMetadata(conn, PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS,
        PTable.QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, indexName);

      // Freeze the clock at t0; every cell of the full initial row lands at t0.
      long t0 = EnvironmentEdgeManager.currentTimeMillis();
      injectEdge.setValue(t0);
      EnvironmentEdgeManager.injectEdge(injectEdge);

      conn.createStatement().execute("UPSERT INTO " + tableName
        + " (id, idxcol, covcol, touchcol) VALUES ('r1', 'k1', '" + COVCOL_VALUE + "', 'x0')");
      conn.commit();

      // Push the t0 cells onto an HFile so the later major compaction merges two files.
      flushTable(conn, tableName);
      flushTable(conn, indexName);

      // Phase 1: a partial touch that omits covcol, committed WITHIN the TTL so the row is alive.
      // The masked current-row read still returns covcol@t0, so the index is rebuilt WITH covcol
      // and
      // both sides agree covcol is present (non-null).
      injectEdge.incrementValue(1000);
      conn.createStatement()
        .executeUpdate("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x1')");
      conn.commit();
      assertCovColConsistent(conn, tableName, indexName, "r1", "k1", COVCOL_VALUE);

      // Phase 2: a second partial touch that also omits covcol, committed past covcol@t0's TTL
      // boundary. With autoCommit off the UPSERT VALUES only buffers on the client; the server-side
      // masked current-row read and the mutationTimestamp are both established at commit, by which
      // point the clock is past covcol@t0's expiry. So the masked read drops covcol@t0 and the
      // index is rebuilt WITHOUT covcol.
      injectEdge.incrementValue(TTL * 1000L + 1);
      conn.createStatement()
        .executeUpdate("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x2')");
      conn.commit();

      assertDataTouchColAlive(conn, tableName, "r1", "x2");
      // Past the TTL boundary covcol reads NULL on both the data table and the index - agreement
      // AND
      // the specific expired value: the index was rebuilt without the stale covcol.
      assertCovColAbsent(conn, tableName, indexName, "r1");
    }
  }

  @Test
  public void testConcurrentMajorCompactionDuringIndexWriteKeepsDataAndIndexConsistent()
    throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (id VARCHAR NOT NULL PRIMARY KEY, "
          + "idxcol VARCHAR, covcol VARCHAR, touchcol VARCHAR)" + withClause(TTL, true));
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + " (idxcol) INCLUDE (covcol)");
      conn.commit();

      // Freeze the clock at t0; every cell of the full initial row lands at t0.
      long t0 = EnvironmentEdgeManager.currentTimeMillis();
      injectEdge.setValue(t0);
      EnvironmentEdgeManager.injectEdge(injectEdge);

      // Full initial row: covcol is written once here and never again. Flush so covcol@t0 is on an
      // HFile and the later major compaction has a file to rewrite.
      conn.createStatement().execute("UPSERT INTO " + tableName
        + " (id, idxcol, covcol, touchcol) VALUES ('r1', 'k1', '" + COVCOL_VALUE + "', 'x0')");
      conn.commit();
      flushTable(conn, tableName);
      flushTable(conn, indexName);

      // Arm the index-table observer so the NEXT index write (the touch's doPre) parks mid-flight,
      // then advance to a time still comfortably within the TTL: the touch's current-row read sees
      // covcol@t0 as alive and rebuilds the index with covcol.
      CountDownLatch indexWriteReached = new CountDownLatch(1);
      CountDownLatch indexWriteProceed = new CountDownLatch(1);
      BlockingIndexWriteObserver.arm(indexName, indexWriteReached, indexWriteProceed);
      TestUtil.addCoprocessor(conn, indexName, BlockingIndexWriteObserver.class);
      injectEdge.setValue(t0 + 50_000L);

      // Run the touch on its own thread/connection: it parks inside the index region's
      // preBatchMutate (i.e. inside doPre) after the data-table read and after the data locks were
      // released, letting us drive the compaction while it is parked.
      AtomicReference<Exception> touchError = new AtomicReference<>();
      Thread toucher = new Thread(() -> {
        try (Connection tc = DriverManager.getConnection(getUrl())) {
          tc.setAutoCommit(false);
          tc.createStatement()
            .executeUpdate("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x1')");
          tc.commit();
        } catch (Exception e) {
          touchError.set(e);
        }
      }, "ttl-sync-toucher");
      toucher.start();

      // Wait until the touch's index write has parked (read done, doPre not yet complete).
      assertTrue("index write did not reach the observer in time",
        indexWriteReached.await(60, TimeUnit.SECONDS));

      // The row is now past the TTL (61s) but not past TTL + max-lookback (70s), so the concurrent
      // major compaction on the data table must NOT physically collect covcol@t0.
      injectEdge.setValue(t0 + 61_000L);
      TestUtil.majorCompact(getUtility(), TableName.valueOf(tableName));

      // Release the parked index write and let the touch finish its data-side persist.
      indexWriteProceed.countDown();
      toucher.join(TimeUnit.SECONDS.toMillis(60));
      assertFalse("toucher thread did not finish", toucher.isAlive());
      if (touchError.get() != null) {
        throw touchError.get();
      }

      // The touch's fresh heartbeat (@t0+50s) keeps the row alive at the current clock (t0+61s, gap
      // 11s < TTL). covcol@t0 survived the concurrent compaction, so the data table and the index
      // still agree covcol is present.
      assertCovColConsistent(conn, tableName, indexName, "r1", "k1", COVCOL_VALUE);
    }
  }

  private void runTtlBoundaryScenario(boolean useUpsertSelect) throws Exception {
    String tableName = generateUniqueName();
    String indexName = generateUniqueName();
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      conn.createStatement()
        .execute("CREATE TABLE " + tableName + " (id VARCHAR NOT NULL PRIMARY KEY, "
          + "idxcol VARCHAR, covcol VARCHAR, touchcol VARCHAR)" + withClause(TTL, true));
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + " (idxcol) INCLUDE (covcol)");
      conn.commit();

      // Freeze the clock at t0; every cell of the full initial row lands at t0.
      long t0 = EnvironmentEdgeManager.currentTimeMillis();
      injectEdge.setValue(t0);
      EnvironmentEdgeManager.injectEdge(injectEdge);

      conn.createStatement().execute("UPSERT INTO " + tableName
        + " (id, idxcol, covcol, touchcol) VALUES ('r1', 'k1', '" + COVCOL_VALUE + "', 'x0')");
      conn.commit();

      // Push the t0 cells onto an HFile so the later major compaction merges two files.
      flushTable(conn, tableName);
      flushTable(conn, indexName);

      // Read the partial touch while the row is still alive (one tick before the boundary) but do
      // NOT commit yet. For the UPSERT SELECT variant the inner SELECT reads the live row here.
      injectEdge.incrementValue(TTL * 1000L - 1);
      String expectedTouch;
      if (useUpsertSelect) {
        int affected = conn.createStatement().executeUpdate("UPSERT INTO " + tableName
          + " (id, touchcol) SELECT id, touchcol FROM " + tableName + " WHERE id = 'r1'");
        assertEquals("UPSERT SELECT must read the still-alive row and buffer one touch", 1,
          affected);
        expectedTouch = "x0";
      } else {
        conn.createStatement()
          .executeUpdate("UPSERT INTO " + tableName + " (id, touchcol) VALUES ('r1', 'x1')");
        expectedTouch = "x1";
      }

      // Advance two ticks past the boundary and commit: the touch's mutationTimestamp is now
      // t0 + TTL + 1 (gap > TTL). idxcol@t0 / covcol@t0 are masked at the internal read anchored at
      // mutationTimestamp, so the index is rebuilt without them - matching the data side.
      injectEdge.incrementValue(2);
      conn.commit();

      // Settle each side's physical state; the major compaction merges the t0 HFile with this one.
      flushAndMajorCompact(conn, tableName);
      flushAndMajorCompact(conn, indexName);

      // Data path (forced data-table read): the row is alive by PK, covcol read past the boundary.
      String dataCov;
      String dataTouch;
      try (ResultSet rs = conn.createStatement().executeQuery(
        "SELECT /*+ NO_INDEX */ covcol, touchcol FROM " + tableName + " WHERE id = 'r1'")) {
        assertTrue("data-table row must still exist (touchcol keeps it alive)", rs.next());
        dataCov = rs.getString(1);
        dataTouch = rs.getString(2);
        assertFalse("exactly one data-table row", rs.next());
      }

      // Index path (forced index read by PK): idxcol@t0 is masked past the boundary, so the row is
      // reachable through the covered index only by its PK suffix, not by idxcol='k1'.
      String indexCov;
      try (ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ INDEX(" + tableName + " "
        + indexName + ") */ covcol FROM " + tableName + " WHERE id = 'r1'")) {
        assertTrue("index-visible row must exist", rs.next());
        indexCov = rs.getString(1);
        assertFalse("exactly one index-visible row", rs.next());
      }

      assertEquals("touchcol must survive (row alive)", expectedTouch, dataTouch);
      assertEquals("covcol must agree between data and index", dataCov, indexCov);
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

  private void assertCovColConsistent(Connection conn, String queryTableName, String indexName,
    String id, String idxColValue, String expectedCovVal) throws SQLException {
    // Force the data-table read.
    try (ResultSet rs = conn.createStatement().executeQuery(
      "SELECT /*+ NO_INDEX */ covcol FROM " + queryTableName + " WHERE id = '" + id + "'")) {
      assertTrue("data-table row should exist", rs.next());
      assertEquals("data-table covcol", expectedCovVal, rs.getString(1));
      assertFalse(rs.next());
    }
    // Read via the index (idxcol is the leading index column).
    try (ResultSet rs =
      conn.createStatement().executeQuery("SELECT /*+ INDEX(" + queryTableName + " " + indexName
        + ") */ id, covcol FROM " + queryTableName + " WHERE idxcol = '" + idxColValue + "'")) {
      assertTrue("index-visible row should exist", rs.next());
      assertEquals("index id", id, rs.getString(1));
      assertEquals("index covcol", expectedCovVal, rs.getString(2));
      assertFalse("exactly one index-visible row", rs.next());
    }
  }

  private void assertCovColAbsent(Connection conn, String queryTableName, String indexName,
    String id) throws SQLException {
    try (ResultSet rs = conn.createStatement().executeQuery(
      "SELECT /*+ NO_INDEX */ covcol FROM " + queryTableName + " WHERE id = '" + id + "'")) {
      assertTrue("data-table row should exist", rs.next());
      assertNull("data-table row should not have covcol", rs.getString(1));
      assertFalse("exactly one data-table row", rs.next());
    }
    try (ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ INDEX(" + queryTableName
      + " " + indexName + ") */ covcol FROM " + queryTableName + " WHERE id = '" + id + "'")) {
      assertTrue("index-visible row should exist", rs.next());
      assertNull("index-visible row should not have covcol", rs.getString(1));
      assertFalse("exactly one index-visible row", rs.next());
    }
  }

  private void assertTouchColConsistent(Connection conn, String queryTableName, String indexName,
    String id, String expectedTouchVal) throws SQLException {
    try (ResultSet rs = conn.createStatement().executeQuery(
      "SELECT /*+ NO_INDEX */ touchcol FROM " + queryTableName + " WHERE id = '" + id + "'")) {
      assertTrue("data-table row should exist", rs.next());
      assertEquals("data-table touchcol", expectedTouchVal, rs.getString(1));
      assertFalse(rs.next());
    }
    try (ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ INDEX(" + queryTableName
      + " " + indexName + ") */ touchcol FROM " + queryTableName + " WHERE id = '" + id + "'")) {
      assertTrue("index-visible row should exist", rs.next());
      assertEquals("index touchcol", expectedTouchVal, rs.getString(1));
      assertFalse("exactly one index-visible row", rs.next());
    }
  }

  /**
   * Data-side-only liveness check for the uncovered-index case, where touchcol cannot be read
   * through the index (an uncovered index stores no data columns and is reachable only by its
   * indexed column). Asserts the data row is alive and carries the expected touchcol.
   */
  private void assertDataTouchColAlive(Connection conn, String queryTableName, String id,
    String expectedTouchVal) throws SQLException {
    try (ResultSet rs = conn.createStatement().executeQuery(
      "SELECT /*+ NO_INDEX */ touchcol FROM " + queryTableName + " WHERE id = '" + id + "'")) {
      assertTrue("data-table row should exist", rs.next());
      assertEquals("data-table touchcol", expectedTouchVal, rs.getString(1));
      assertFalse(rs.next());
    }
  }

  private void assertUncoveredIdxColConsistent(Connection conn, String queryTableName,
    String indexName, String id, String idxColValue) throws SQLException {
    try (ResultSet rs =
      conn.createStatement().executeQuery("SELECT /*+ INDEX(" + queryTableName + " " + indexName
        + ") */ id, idxcol FROM " + queryTableName + " WHERE idxcol = '" + idxColValue + "'")) {
      assertTrue("index-visible row should exist", rs.next());
      assertEquals("index id", id, rs.getString(1));
      assertEquals("index idxcol", idxColValue, rs.getString(2));
      assertFalse("exactly one index-visible row", rs.next());
    }
  }

  private void assertUncoveredIdxColAbsent(Connection conn, String queryTableName, String indexName,
    String idxColValue) throws SQLException {
    try (ResultSet rs =
      conn.createStatement().executeQuery("SELECT /*+ INDEX(" + queryTableName + " " + indexName
        + ") */ idxcol FROM " + queryTableName + " WHERE idxcol = '" + idxColValue + "'")) {
      assertFalse("no index-visible row should exist", rs.next());
    }
  }

  static void flushTable(Connection conn, String tableName) throws Exception {
    try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
      admin.flush(TableName.valueOf(tableName));
    }
  }

  static void flushAndMajorCompact(Connection conn, String tableName) throws Exception {
    TableName tn = TableName.valueOf(tableName);
    try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
      admin.flush(tn);
    }
    TestUtil.majorCompact(getUtility(), tn);
  }

  /**
   * A region observer installed on the index table that parks the first write to a named index in
   * {@code preBatchMutate}, letting a test run a concurrent major compaction on the data table
   * while the secondary-index write is in flight. This is the "slow index write" seam: the index
   * write is driven by {@code IndexRegionObserver.doPre -> table.batch(...)}, which reaches the
   * index region's {@code preBatchMutate} strictly after the data-table current-row read and
   * strictly before doPre returns. It is a one-shot per arming so only the touch's write blocks,
   * not later maintenance.
   */
  public static class BlockingIndexWriteObserver extends SimpleRegionObserver {
    private static volatile String targetIndexName;
    private static volatile CountDownLatch reached;
    private static volatile CountDownLatch proceed;
    private static final AtomicBoolean fired = new AtomicBoolean(false);

    static void arm(String indexName, CountDownLatch reachedLatch, CountDownLatch proceedLatch) {
      targetIndexName = indexName;
      reached = reachedLatch;
      proceed = proceedLatch;
      fired.set(false);
    }

    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      String tableName = c.getEnvironment().getRegionInfo().getTable().getNameAsString();
      if (tableName.equals(targetIndexName) && fired.compareAndSet(false, true)) {
        reached.countDown();
        try {
          proceed.await(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("interrupted while parking index write", e);
        }
      }
    }
  }
}
