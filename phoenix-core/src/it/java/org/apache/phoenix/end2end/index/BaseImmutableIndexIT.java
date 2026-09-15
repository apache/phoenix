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

import static org.apache.phoenix.end2end.IndexToolIT.assertExplainPlan;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.IMMUTABLE_STORAGE_SCHEME;
import static org.apache.phoenix.schema.PTable.ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.getRowCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.transaction.PhoenixTransactionProvider;
import org.apache.phoenix.transaction.PhoenixTransactionProvider.Feature;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

public abstract class BaseImmutableIndexIT extends BaseTest {

  private final boolean localIndex;
  private final boolean serverSideIndex;
  private final PhoenixTransactionProvider transactionProvider;
  private final String tableDDLOptions;

  private volatile boolean stopThreads = false;

  private static String TABLE_NAME;
  private static String INDEX_DDL;
  public static final AtomicInteger NUM_ROWS = new AtomicInteger(0);

  public BaseImmutableIndexIT(boolean localIndex, boolean transactional, String transactionProvider,
    boolean columnEncoded, boolean serverSideIndex) {
    StringBuilder optionBuilder = new StringBuilder("IMMUTABLE_ROWS=true");
    this.localIndex = localIndex;
    this.serverSideIndex = serverSideIndex;
    if (!columnEncoded) {
      optionBuilder.append(",COLUMN_ENCODED_BYTES=0,IMMUTABLE_STORAGE_SCHEME="
        + PTableImpl.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
    }
    if (transactional) {
      optionBuilder
        .append(",TRANSACTIONAL=true, TRANSACTION_PROVIDER='" + transactionProvider + "'");
      this.transactionProvider =
        TransactionFactory.Provider.valueOf(transactionProvider).getTransactionProvider();
    } else {
      this.transactionProvider = null;
    }
    this.tableDDLOptions = optionBuilder.toString();

  }

  protected static Map<String, String> createServerProps() {
    Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
    serverProps.put("hbase.coprocessor.region.classes", CreateIndexRegionObserver.class.getName());
    return serverProps;
  }

  protected static Map<String, String> createClientProps() {
    Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(5);
    clientProps.put(QueryServices.TRANSACTIONS_ENABLED, "true");
    clientProps.put(QueryServices.INDEX_POPULATION_SLEEP_TIME, "15000");
    clientProps.put(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB, "true");
    clientProps.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "1");
    clientProps.put(HConstants.HBASE_CLIENT_PAUSE, "1");
    return clientProps;
  }

  @Test
  public void testClientVsServerSideIndexMutations() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = "TBL_" + generateUniqueName();
    String indexName = "IND_" + generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
    String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(false);
      String ddl = "CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
      Statement stmt = conn.createStatement();
      stmt.execute(ddl);
      ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName
        + " (long_col1)";
      stmt.execute(ddl);
      upsertRows(conn, fullTableName, 3);
      assertClientVsServerSideIndexMutations(conn);
      conn.commit();
      ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullTableName);
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      String dml = "DELETE from " + fullTableName + " WHERE varchar_pk='varchar1'";
      assertEquals(1, conn.createStatement().executeUpdate(dml));
      assertClientVsServerSideIndexMutations(conn);
      conn.commit();
    }
  }

  @Test
  public void testDropIfImmutableKeyValueColumn() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = "TBL_" + generateUniqueName();
    String indexName = "IND_" + generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
    String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(false);
      String ddl = "CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
      Statement stmt = conn.createStatement();
      stmt.execute(ddl);
      populateTestTable(fullTableName);
      ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName
        + " (long_col1)";
      stmt.execute(ddl);

      ResultSet rs;

      rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullTableName);
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));

      conn.setAutoCommit(true);
      String dml = "DELETE from " + fullTableName + " WHERE long_col2 = 4";
      assertEquals(1, conn.createStatement().executeUpdate(dml));

      rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullTableName);
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));

      conn.createStatement().execute("DROP TABLE " + fullTableName);
    }
  }

  @Test
  public void testDeleteFromPartialPK() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = "TBL_" + generateUniqueName();
    String indexName = "IND_" + generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
    String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(false);
      String ddl = "CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
      Statement stmt = conn.createStatement();
      stmt.execute(ddl);
      populateTestTable(fullTableName);
      ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName
        + " (char_pk, varchar_pk)";
      stmt.execute(ddl);

      ResultSet rs;

      rs =
        conn.createStatement().executeQuery("SELECT /*+ NO_INDEX*/ COUNT(*) FROM " + fullTableName);
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));

      String dml = "DELETE from " + fullTableName + " WHERE varchar_pk='varchar1'";
      assertEquals(1, conn.createStatement().executeUpdate(dml));
      assertIndexMutations(conn);
      conn.commit();

      rs =
        conn.createStatement().executeQuery("SELECT /*+ NO_INDEX*/ COUNT(*) FROM " + fullTableName);
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
    }
  }

  @Test
  public void testDeleteFromNonPK() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = "TBL_" + generateUniqueName();
    String indexName = "IND_" + generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
    String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(false);
      String ddl = "CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
      Statement stmt = conn.createStatement();
      stmt.execute(ddl);
      populateTestTable(fullTableName);
      ddl = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName
        + " (varchar_col1, varchar_pk)";
      stmt.execute(ddl);

      ResultSet rs;

      rs =
        conn.createStatement().executeQuery("SELECT /*+ NO_INDEX*/ COUNT(*) FROM " + fullTableName);
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));
      rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
      assertTrue(rs.next());
      assertEquals(3, rs.getInt(1));

      String dml = "DELETE from " + fullTableName
        + " WHERE varchar_col1='varchar_a' AND varchar_pk='varchar1'";
      assertEquals(1, conn.createStatement().executeUpdate(dml));
      assertIndexMutations(conn);
      conn.commit();

      TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices()
        .getTable(Bytes.toBytes(fullTableName)));

      rs =
        conn.createStatement().executeQuery("SELECT /*+ NO_INDEX*/ COUNT(*) FROM " + fullTableName);
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
    }
  }

  private boolean isServerSideIndex() {
    // An index is a server-side index if its mutations are generated and applied on the server
    // side, otherwise
    // it is a client-side index
    // 1. Non-transactional local indexes are server-side indexes
    // 2. Transactional global indexes are client-side indexes
    // 3. Transactional local indexes are server-side indexes unless the transaction provider does
    // not support it
    // 4. Non-transactional global mutable indexes are server-side indexes
    // 5. If configured using QueryServices.SERVER_SIDE_IMMUTABLE_INDEXES_ENABLED_ATTRIB (that is,
    // when serverSideIndex = true), non-transactional immutable indexes are also server-side
    // indexes, otherwise they
    // are client-side indexes
    if (
      (localIndex && transactionProvider != null
        && transactionProvider.isUnsupported(Feature.MAINTAIN_LOCAL_INDEX_ON_SERVER))
        || (!localIndex && transactionProvider != null) || (!localIndex && !serverSideIndex)
    ) {
      return false;
    }
    return serverSideIndex;
  }

  private void assertClientVsServerSideIndexMutations(Connection conn) throws SQLException {
    boolean serverSideMutations = isServerSideIndex();
    Iterator<Pair<byte[], List<Cell>>> iterator = PhoenixRuntime.getUncommittedDataIterator(conn);
    while (iterator.hasNext()) {
      byte[] tableName = iterator.next().getFirst();
      PTable table = conn.unwrap(PhoenixConnection.class).getTable(Bytes.toString(tableName));
      if (table.getType() == PTableType.INDEX) {
        assertFalse(serverSideMutations);
      }
    }
  }

  private void assertIndexMutations(Connection conn) throws SQLException {
    if (isServerSideIndex()) {
      return;
    }
    Iterator<Pair<byte[], List<Cell>>> iterator = PhoenixRuntime.getUncommittedDataIterator(conn);
    assertTrue(iterator.hasNext());
    iterator.next();
    assertEquals(
      !localIndex || (transactionProvider != null
        && transactionProvider.isUnsupported(Feature.MAINTAIN_LOCAL_INDEX_ON_SERVER)),
      iterator.hasNext());
  }

  private void createAndPopulateTableAndIndexForConsistentIndex(Connection conn, String tableName,
    String indexName, int numOfRowsToInsert, String storageProps) throws Exception {
    String tableOptions = tableDDLOptions;
    if (storageProps != null) {
      tableOptions += " ,IMMUTABLE_STORAGE_SCHEME=" + storageProps;
    }
    String ddl = "CREATE TABLE " + tableName + TestUtil.TEST_TABLE_SCHEMA + tableOptions;
    INDEX_DDL = "CREATE " + " INDEX IF NOT EXISTS " + SchemaUtil.getTableNameFromFullName(indexName)
      + " ON " + tableName + " (long_pk, varchar_pk)" + " INCLUDE (long_col1, long_col2) ";

    conn.createStatement().execute(ddl);
    conn.createStatement().execute(INDEX_DDL);
    upsertRows(conn, tableName, numOfRowsToInsert);
    conn.commit();

    TestUtil.waitForIndexState(conn, indexName, PIndexState.ACTIVE);
  }

  @Test
  public void testGlobalImmutableIndexCreate() throws Exception {
    if (localIndex || transactionProvider != null) {
      return;
    }
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

    ArrayList<String> immutableStorageProps = new ArrayList<String>();
    immutableStorageProps.add(null);
    if (!tableDDLOptions.contains(IMMUTABLE_STORAGE_SCHEME)) {
      immutableStorageProps.add(SINGLE_CELL_ARRAY_WITH_OFFSETS.toString());
    }
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(true);
      for (String storageProp : immutableStorageProps) {
        String tableName = "TBL_" + generateUniqueName();
        String indexName = "IND_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
        String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
        TABLE_NAME = fullTableName;
        int numRows = 1;
        createAndPopulateTableAndIndexForConsistentIndex(conn, fullTableName, fullIndexName,
          numRows, storageProp);

        ResultSet rs;
        rs =
          conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + TABLE_NAME);
        assertTrue(rs.next());
        assertEquals(numRows, rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
        assertTrue(rs.next());
        assertEquals(numRows, rs.getInt(1));
        IndexTestUtil.assertRowsForEmptyColValue(conn, fullIndexName,
          QueryConstants.VERIFIED_BYTES);
        rs = conn.createStatement().executeQuery("SELECT * FROM " + fullIndexName);
        assertTrue(rs.next());
        assertEquals("1", rs.getString(1));

        // Now try to fail Phase1 and observe that index state is not DISABLED
        try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();) {
          admin.disableTable(TableName.valueOf(fullIndexName));
          boolean isWriteOnDisabledIndexFailed = false;
          try {
            upsertRows(conn, fullTableName, numRows);
          } catch (SQLException ex) {
            isWriteOnDisabledIndexFailed = true;
          }
          assertEquals(true, isWriteOnDisabledIndexFailed);
          PIndexState indexState = TestUtil.getIndexState(conn, fullIndexName);
          assertEquals(PIndexState.ACTIVE, indexState);

        }
      }
    }
  }

  @Test
  public void testGlobalImmutableIndexDelete() throws Exception {
    if (localIndex || transactionProvider != null) {
      return;
    }
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = "TBL_" + generateUniqueName();
    String indexName = "IND_" + generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
    String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
    TABLE_NAME = fullTableName;
    try (Connection conn = DriverManager.getConnection(getUrl(), props);
      Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();) {
      conn.setAutoCommit(true);
      int numRows = 2;
      createAndPopulateTableAndIndexForConsistentIndex(conn, fullTableName, fullIndexName, numRows,
        null);

      String dml = "DELETE from " + fullTableName + " WHERE varchar_pk='varchar1'";
      conn.createStatement().execute(dml);
      conn.commit();
      ResultSet rs;
      rs =
        conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + TABLE_NAME);
      assertTrue(rs.next());
      assertEquals(numRows - 1, rs.getInt(1));
      rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
      assertTrue(rs.next());
      assertEquals(numRows - 1, rs.getInt(1));
      IndexTestUtil.assertRowsForEmptyColValue(conn, fullIndexName, QueryConstants.VERIFIED_BYTES);

      // Force delete to fail (data removed but operation failed) on data table and check index
      // table row remains as unverified
      TestUtil.addCoprocessor(conn, fullTableName, DeleteFailingRegionObserver.class);
      dml = "DELETE from " + fullTableName + " WHERE varchar_pk='varchar2'";
      boolean isDeleteFailed = false;
      try {
        conn.createStatement().execute(dml);
      } catch (Exception ex) {
        isDeleteFailed = true;
      }
      assertEquals(true, isDeleteFailed);
      TestUtil.removeCoprocessor(conn, fullTableName, DeleteFailingRegionObserver.class);
      assertEquals(numRows - 1, getRowCount(conn.unwrap(PhoenixConnection.class).getQueryServices()
        .getTable(Bytes.toBytes(fullIndexName)), false));
      IndexTestUtil.assertRowsForEmptyColValue(conn, fullIndexName,
        QueryConstants.UNVERIFIED_BYTES);

      // Now delete via hbase, read from unverified index and see that we don't get any data
      admin.disableTable(TableName.valueOf(fullTableName));
      admin.truncateTable(TableName.valueOf(fullTableName), true);
      String selectFromIndex = "SELECT long_pk, varchar_pk, long_col1 FROM " + TABLE_NAME
        + " WHERE varchar_pk='varchar2' AND long_pk=2";
      rs = conn.createStatement().executeQuery("EXPLAIN " + selectFromIndex);
      String actualExplainPlan = QueryUtil.getExplainPlan(rs);
      assertExplainPlan(false, actualExplainPlan, fullTableName, fullIndexName);

      rs = conn.createStatement().executeQuery(selectFromIndex);
      assertFalse(rs.next());
    }
  }

  public static class DeleteFailingRegionObserver extends SimpleRegionObserver {
    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      throw new DoNotRetryIOException();
    }
  }

  public static class UpsertFailingRegionObserver extends SimpleRegionObserver {
    @Override
    public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c,
      MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      throw new DoNotRetryIOException();
    }
  }

  // This test is know to flap. We need PHOENIX-2582 to be fixed before enabling this back.
  @Ignore
  @Test
  public void testCreateIndexDuringUpsertSelect() throws Exception {
    // This test times out at the UPSERT SELECT call for local index
    if (localIndex) { // TODO: remove after PHOENIX-3314 is fixed
      return;
    }
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = "TBL_" + generateUniqueName();
    String indexName = "IND_" + generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
    TABLE_NAME = fullTableName;
    String ddl = "CREATE TABLE " + TABLE_NAME + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
    INDEX_DDL = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX IF NOT EXISTS " + indexName
      + " ON " + TABLE_NAME + " (long_pk, varchar_pk)" + " INCLUDE (long_col1, long_col2)";

    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(false);
      Statement stmt = conn.createStatement();
      stmt.execute(ddl);

      upsertRows(conn, TABLE_NAME, 220);
      conn.commit();

      // run the upsert select and also create an index
      conn.setAutoCommit(true);
      String upsertSelect = "UPSERT INTO " + TABLE_NAME
        + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) "
        + "SELECT varchar_pk||'_upsert_select', char_pk, int_pk, long_pk, decimal_pk, date_pk FROM "
        + TABLE_NAME;
      conn.createStatement().execute(upsertSelect);
      TestUtil.waitForIndexRebuild(conn, indexName, PIndexState.ACTIVE);
      ResultSet rs;
      rs =
        conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + TABLE_NAME);
      assertTrue(rs.next());
      assertEquals(440, rs.getInt(1));
      rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexName);
      assertTrue(rs.next());
      assertEquals(440, rs.getInt(1));
    }
  }

  // used to create an index while a batch of rows are being written
  public static class CreateIndexRegionObserver extends SimpleRegionObserver {
    @Override
    public void postPut(
      org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c, Put put,
      org.apache.hadoop.hbase.wal.WALEdit edit, Durability durability) throws java.io.IOException {
      String tableName =
        c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
      if (
        tableName.equalsIgnoreCase(TABLE_NAME)
          // create the index after the second batch
          && Bytes.startsWith(put.getRow(), Bytes.toBytes("varchar200_upsert_select"))
      ) {
        Runnable r = new Runnable() {

          @Override
          public void run() {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
              // Run CREATE INDEX call in separate thread as otherwise we block
              // this thread (not a realistic scenario) and prevent our catchup
              // query from adding the missing rows.
              conn.createStatement().execute(INDEX_DDL);
            } catch (SQLException e) {
            }
          }

        };
        new Thread(r).start();
      }
    }
  }

  private class UpsertRunnable implements Runnable {
    private static final int NUM_ROWS_IN_BATCH = 10;
    private final String fullTableName;

    public UpsertRunnable(String fullTableName) {
      this.fullTableName = fullTableName;
    }

    @Override
    public void run() {
      Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
      try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
        while (!stopThreads) {
          // write a large batch of rows
          boolean fistRowInBatch = true;
          for (int i = 0; i < NUM_ROWS_IN_BATCH && !stopThreads; ++i) {
            BaseTest.upsertRow(conn, fullTableName, NUM_ROWS.incrementAndGet(), fistRowInBatch);
            fistRowInBatch = false;
          }
          conn.commit();
          Thread.sleep(10);
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
      }
    }
  }

  // This test is know to flap. We need PHOENIX-2582 to be fixed before enabling this back.
  @Ignore
  @Test
  public void testCreateIndexWhileUpsertingData() throws Exception {
    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
    String tableName = "TBL_" + generateUniqueName();
    String indexName = "IND_" + generateUniqueName();
    String fullTableName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, tableName);
    String fullIndexName = SchemaUtil.getTableName(TestUtil.DEFAULT_SCHEMA_NAME, indexName);
    String ddl = "CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
    String indexDDL = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON "
      + fullTableName + " (long_pk, varchar_pk)" + " INCLUDE (long_col1, long_col2)";
    int numThreads = 2;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = Executors.defaultThreadFactory().newThread(r);
        t.setDaemon(true);
        t.setPriority(Thread.MIN_PRIORITY);
        return t;
      }
    });
    try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
      conn.setAutoCommit(true);
      Statement stmt = conn.createStatement();
      stmt.execute(ddl);

      ResultSet rs;
      rs = conn.createStatement()
        .executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + fullTableName);
      assertTrue(rs.next());
      int dataTableRowCount = rs.getInt(1);
      assertEquals(0, dataTableRowCount);

      List<Future<?>> futureList = Lists.newArrayListWithExpectedSize(numThreads);
      for (int i = 0; i < numThreads; ++i) {
        futureList.add(executorService.submit(new UpsertRunnable(fullTableName)));
      }
      // upsert some rows before creating the index
      Thread.sleep(100);

      // create the index
      try (Connection conn2 = DriverManager.getConnection(getUrl(), props)) {
        conn2.createStatement().execute(indexDDL);
      }

      // upsert some rows after creating the index
      Thread.sleep(50);
      // cancel the running threads
      stopThreads = true;
      executorService.shutdown();
      assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));

      rs = conn.createStatement()
        .executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + fullTableName);
      assertTrue(rs.next());
      dataTableRowCount = rs.getInt(1);
      rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
      assertTrue(rs.next());
      int indexTableRowCount = rs.getInt(1);
      assertEquals("Data and Index table should have the same number of rows ", dataTableRowCount,
        indexTableRowCount);
    } finally {
      executorService.shutdownNow();
    }
  }

  private void setupForDeleteCount(Connection conn, String schemaName, String dataTableName,
    String indexTableName1, String indexTableName2) throws SQLException {

    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

    conn.createStatement().execute("CREATE TABLE " + dataTableFullName
      + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) " + this.tableDDLOptions);

    if (indexTableName1 != null) {
      conn.createStatement().execute(String.format("CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)",
        indexTableName1, dataTableFullName));
    }

    if (indexTableName2 != null) {
      conn.createStatement().execute(String.format("CREATE INDEX %s ON %s (VAL2) INCLUDE (VAL1)",
        indexTableName2, dataTableFullName));
    }

    PreparedStatement dataPreparedStatement =
      conn.prepareStatement("UPSERT INTO " + dataTableFullName + " VALUES(?,?,?)");
    for (int i = 1; i <= 10; i++) {
      dataPreparedStatement.setInt(1, i);
      dataPreparedStatement.setInt(2, i + 1);
      dataPreparedStatement.setInt(3, i * 2);
      dataPreparedStatement.execute();
    }
    conn.commit();
  }

  @Test
  public void testDeleteCount_PK() throws Exception {
    String schemaName = generateUniqueName();
    String dataTableName = "TBL_" + generateUniqueName();
    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
    String indexTableName = "IND_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupForDeleteCount(conn, schemaName, dataTableName, indexTableName, null);

      PreparedStatement deleteStmt =
        conn.prepareStatement("DELETE FROM " + dataTableFullName + " WHERE ID > 5");
      assertEquals(5, deleteStmt.executeUpdate());
      conn.commit();
    }
  }

  @Test
  public void testDeleteCount_nonPK() throws Exception {
    String schemaName = generateUniqueName();
    String dataTableName = "TBL_" + generateUniqueName();
    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
    String indexTableName1 = "IND_" + generateUniqueName();
    String indexTableName2 = "IND_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupForDeleteCount(conn, schemaName, dataTableName, indexTableName1, indexTableName2);

      PreparedStatement deleteStmt =
        conn.prepareStatement("DELETE FROM " + dataTableFullName + " WHERE VAL1 > 6");
      assertEquals(5, deleteStmt.executeUpdate());
      conn.commit();
    }
  }

  @Test
  public void testDeleteCount_limit() throws Exception {
    String schemaName = generateUniqueName();
    String dataTableName = "TBL_" + generateUniqueName();
    String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
    String indexTableName1 = "IND_" + generateUniqueName();
    String indexTableName2 = "IND_" + generateUniqueName();

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupForDeleteCount(conn, schemaName, dataTableName, indexTableName1, indexTableName2);

      PreparedStatement deleteStmt =
        conn.prepareStatement("DELETE FROM " + dataTableFullName + " WHERE VAL1 > 6 LIMIT 3");
      assertEquals(3, deleteStmt.executeUpdate());
      conn.commit();
    }
  }

  @Test
  public void testDeleteCount_index() throws Exception {
    String schemaName = generateUniqueName();
    String dataTableName = "TBL_" + generateUniqueName();
    String indexTableName = "IND_" + generateUniqueName();
    String indexTableFullName = SchemaUtil.getTableName(schemaName, indexTableName);

    try (Connection conn = DriverManager.getConnection(getUrl())) {
      setupForDeleteCount(conn, schemaName, dataTableName, indexTableName, null);

      PreparedStatement deleteStmt =
        conn.prepareStatement("DELETE FROM " + indexTableFullName + " WHERE \"0:VAL1\" > 6");
      assertEquals(5, deleteStmt.executeUpdate());
      conn.commit();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Partial-upsert and delete coverage for server-side immutable index maintenance
  // (SERVER_SIDE_IMMUTABLE_INDEXES_ENABLED_ATTRIB). These run under both storage schemes (the
  // columnEncoded parameter selects ONE_CELL_PER_COLUMN vs SINGLE_CELL_ARRAY_WITH_OFFSETS) and
  // under both flag states -- ServerSideImmutableIndexIT forces the flag on,
  // ClientSideImmutableIndexIT
  // forces it off -- so every assertion below must hold whether the index is maintained on the
  // server or the client. Only non-transactional global indexes are affected by the flag, so each
  // test bails out for local and transactional parameterizations.
  // ---------------------------------------------------------------------------------------------

  /**
   * Reads every row of a query into "col1|col2|..." strings (a null column renders as the literal
   * "null"), so an index-served result can be compared for exact agreement with the data table
   * under either storage scheme.
   */
  private static List<String> readRows(Connection conn, String sql) throws SQLException {
    List<String> rows = new ArrayList<>();
    try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
      int cols = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= cols; i++) {
          if (i > 1) {
            sb.append('|');
          }
          sb.append(rs.getString(i));
        }
        rows.add(sb.toString());
      }
    }
    return rows;
  }

  /**
   * A partial upsert on an immutable table with a covered global index must leave the index
   * consistent with the data table under BOTH storage schemes. Under ONE_CELL_PER_COLUMN the
   * omitted covered column keeps its earlier value; under SINGLE_CELL_ARRAY_WITH_OFFSETS the whole
   * row is a single cell so the omitted column is overwritten to null. Either way the index the
   * server maintains must read back exactly what the data table returns. The SINGLE_CELL branch
   * (columnEncoded=true) is the load-bearing case the ONE_CELL-only GlobalIndexCheckerIT test
   * skips.
   */
  @Test
  public void testPartialUpsertForImmutableCoveredIndex() throws Exception {
    if (localIndex || transactionProvider != null) {
      return;
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      String tableName = "TBL_" + generateUniqueName();
      String indexName = "IND_" + generateUniqueName();
      conn.createStatement()
        .execute("CREATE TABLE " + tableName
          + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR, val3 VARCHAR) "
          + tableDDLOptions);
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " VALUES ('a', 'ab', 'abc', 'abcd')");
      conn.commit();
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + " (val1) INCLUDE (val2, val3)");

      String idxSql = "SELECT /*+ INDEX(" + tableName + " " + indexName
        + ") */ id, val1, val2, val3 " + "FROM " + tableName + " WHERE val1 = 'ab'";
      String dataSql =
        "SELECT /*+ NO_INDEX */ id, val1, val2, val3 FROM " + tableName + " WHERE val1 = 'ab'";
      // Full upsert: the index is actually used (guards against a vacuous data-scan agreement) and
      // returns the complete row.
      assertExplainPlan(false,
        QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + idxSql)),
        tableName, indexName);
      assertEquals("[a|ab|abc|abcd]", readRows(conn, idxSql).toString());
      assertEquals(readRows(conn, dataSql), readRows(conn, idxSql));

      // Partial upsert re-supplies val1 (the index key) and val2 but omits the covered column val3.
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, val1, val2) VALUES ('a', 'ab', 'abcc')");
      conn.commit();
      // The server-maintained index must still agree with the data table, whatever the storage
      // scheme did to the omitted column.
      assertEquals("index must agree with data after partial upsert", readRows(conn, dataSql),
        readRows(conn, idxSql));
    }
  }

  /**
   * A partial upsert that omits the indexed column of an UNCOVERED global index must leave the
   * index path resolving exactly the rows the data table does. Under ONE_CELL_PER_COLUMN the
   * indexed column keeps its earlier value so both find the row under 'ab'; under
   * SINGLE_CELL_ARRAY_WITH_OFFSETS the single-cell overwrite nulls the indexed column so both find
   * nothing. The scan path agrees under both maintenance modes because it self-heals unverified
   * index rows at read time. The COUNT (aggregate) path does not self-heal, so it only agrees when
   * the index is maintained on the server: server-side maintenance reads the current row back and
   * rewrites a verified entry for the retained key, whereas client-side maintenance cannot rebuild
   * the omitted index key and leaves the COUNT path undercounting -- the pre-existing gap this PR's
   * server-side path closes.
   */
  @Test
  public void testPartialUpsertForImmutableUncoveredIndex() throws Exception {
    if (localIndex || transactionProvider != null) {
      return;
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      String tableName = "TBL_" + generateUniqueName();
      String indexName = "IND_" + generateUniqueName();
      conn.createStatement().execute("CREATE TABLE " + tableName
        + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR) " + tableDDLOptions);
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, val1, val2) VALUES ('a', 'ab', 'abc')");
      conn.commit();
      conn.createStatement()
        .execute("CREATE UNCOVERED INDEX " + indexName + " ON " + tableName + " (val1)");

      String idxSql = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ id FROM "
        + tableName + " WHERE val1 = 'ab'";
      String dataSql = "SELECT /*+ NO_INDEX */ id FROM " + tableName + " WHERE val1 = 'ab'";
      String idxCount = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ COUNT(*) FROM "
        + tableName + " WHERE val1 = 'ab'";
      String dataCount = "SELECT /*+ NO_INDEX */ COUNT(*) FROM " + tableName + " WHERE val1 = 'ab'";
      // Full upsert: the uncovered index is used and resolves the row.
      assertExplainPlan(false,
        QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + idxSql)),
        tableName, indexName);
      assertEquals("[a]", readRows(conn, idxSql).toString());

      // Partial upsert omits the indexed column val1.
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, val2) VALUES ('a', 'xyz')");
      conn.commit();
      // The scan path self-heals unverified index rows against the data table, so it agrees under
      // both maintenance modes.
      assertEquals("uncovered index scan must agree with data after partial upsert",
        readRows(conn, dataSql), readRows(conn, idxSql));
      if (serverSideIndex) {
        // The COUNT (aggregate) path does not self-heal. Only server-side maintenance rebuilds a
        // verified uncovered-index entry from the read-back row, so only then does the COUNT path
        // agree with the data table; the client-side path is the pre-existing gap this PR closes.
        assertEquals("uncovered index COUNT must agree with data after partial upsert",
          readRows(conn, dataCount), readRows(conn, idxCount));
      }
    }
  }

  /**
   * A partial upsert on a table with more than one covered global index must leave every maintained
   * index in agreement with the data table for the covered column it reads back.
   */
  @Test
  public void testPartialUpsertForImmutableMultipleIndexes() throws Exception {
    if (localIndex || transactionProvider != null) {
      return;
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      String tableName = "TBL_" + generateUniqueName();
      String indexName1 = "IND_" + generateUniqueName();
      String indexName2 = "IND_" + generateUniqueName();
      conn.createStatement()
        .execute("CREATE TABLE " + tableName
          + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR, val3 VARCHAR) "
          + tableDDLOptions);
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " VALUES ('a', 'ab', 'abc', 'abcd')");
      conn.commit();
      conn.createStatement()
        .execute("CREATE INDEX " + indexName1 + " ON " + tableName + " (val1) INCLUDE (val3)");
      conn.createStatement()
        .execute("CREATE INDEX " + indexName2 + " ON " + tableName + " (val2) INCLUDE (val3)");
      // Partial upsert re-supplies val1 and val2 (both index keys) and omits the covered column
      // val3.
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, val1, val2) VALUES ('a', 'ab', 'abc')");
      conn.commit();

      String viaIdx1 = "SELECT /*+ INDEX(" + tableName + " " + indexName1 + ") */ val3 FROM "
        + tableName + " WHERE val1 = 'ab'";
      String viaIdx2 = "SELECT /*+ INDEX(" + tableName + " " + indexName2 + ") */ val3 FROM "
        + tableName + " WHERE val2 = 'abc'";
      String data = "SELECT /*+ NO_INDEX */ val3 FROM " + tableName + " WHERE id = 'a'";
      assertExplainPlan(false,
        QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + viaIdx1)),
        tableName, indexName1);
      assertExplainPlan(false,
        QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + viaIdx2)),
        tableName, indexName2);
      assertEquals("index1 must agree with data", readRows(conn, data), readRows(conn, viaIdx1));
      assertEquals("index2 must agree with data", readRows(conn, data), readRows(conn, viaIdx2));
    }
  }

  /**
   * A partial upsert that touches only one column family of a multi-column-family immutable table
   * must read back a covered column that lives in an untouched family. Family b is never written by
   * the partial upsert, so its cell survives under BOTH storage schemes and val3 must remain 'abcd'
   * on the data table -- and the server-maintained index must read back the same value.
   */
  @Test
  public void testPartialUpsertForImmutableMultipleColumnFamilies() throws Exception {
    if (localIndex || transactionProvider != null) {
      return;
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      String tableName = "TBL_" + generateUniqueName();
      String indexName = "IND_" + generateUniqueName();
      conn.createStatement()
        .execute("CREATE TABLE " + tableName
          + " (id VARCHAR NOT NULL PRIMARY KEY, a.val1 VARCHAR, a.val2 VARCHAR, b.val3 VARCHAR) "
          + tableDDLOptions);
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " VALUES ('a', 'ab', 'abc', 'abcd')");
      conn.commit();
      conn.createStatement()
        .execute("CREATE INDEX " + indexName + " ON " + tableName + " (val1) INCLUDE (val2, val3)");
      // Partial upsert touches only family a (val1, val2); family b's val3 is untouched.
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, val1, val2) VALUES ('a', 'ab', 'abcc')");
      conn.commit();

      String idxSql = "SELECT /*+ INDEX(" + tableName + " " + indexName
        + ") */ val1, val2, val3 FROM " + tableName + " WHERE val1 = 'ab'";
      String dataSql =
        "SELECT /*+ NO_INDEX */ val1, val2, val3 FROM " + tableName + " WHERE id = 'a'";
      assertExplainPlan(false,
        QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + idxSql)),
        tableName, indexName);
      List<String> dataRows = readRows(conn, dataSql);
      List<String> idxRows = readRows(conn, idxSql);
      if (tableDDLOptions.contains("ONE_CELL_PER_COLUMN")) {
        // ONE_CELL_PER_COLUMN keeps the untouched family's cell, so val3 must survive as 'abcd'.
        // This
        // anchors the cross-family read-back so an all-null agreement cannot pass vacuously.
        // (SINGLE_CELL_ARRAY_WITH_OFFSETS does not retain the omitted column on a partial upsert;
        // the
        // agreement check below still verifies the index tracks the data table under that scheme.)
        assertEquals("[ab|abcc|abcd]", dataRows.toString());
      }
      // The server-maintained index must read back identically to the data table across families.
      assertEquals("index must agree with data across column families", dataRows, idxRows);
    }
  }

  /**
   * A partial upsert that omits the WHERE-clause column of a partial index (CREATE INDEX ... WHERE)
   * must leave the partial index selecting exactly the rows the data-table predicate does. Under
   * ONE_CELL_PER_COLUMN the WHERE column keeps its earlier value so the row stays indexed; under
   * SINGLE_CELL_ARRAY_WITH_OFFSETS the single-cell overwrite nulls it so neither the index nor the
   * data path matches val2 = 'keep'. Either way the index path and the data path must agree.
   */
  @Test
  public void testPartialUpsertForImmutablePartialIndex() throws Exception {
    if (localIndex || transactionProvider != null) {
      return;
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      String tableName = "TBL_" + generateUniqueName();
      String indexName = "IND_" + generateUniqueName();
      conn.createStatement()
        .execute("CREATE TABLE " + tableName
          + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR, val3 VARCHAR) "
          + tableDDLOptions);
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " VALUES ('a', 'ab', 'keep', 'abcd')");
      conn.commit();
      // Only rows with val2 = 'keep' are indexed; val2 is the WHERE-clause column.
      conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName
        + " (val1) INCLUDE (val3) WHERE val2 = 'keep'");
      // Partial upsert omits val2 (the partial-index predicate column) and updates val3.
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, val1, val3) VALUES ('a', 'ab', 'updated')");
      conn.commit();

      // The query predicate implies the partial-index predicate, so the index is applicable; force
      // it and compare against the data table for the same predicate.
      String idxSql = "SELECT /*+ INDEX(" + tableName + " " + indexName + ") */ val3 FROM "
        + tableName + " WHERE val1 = 'ab' AND val2 = 'keep'";
      String dataSql =
        "SELECT /*+ NO_INDEX */ val3 FROM " + tableName + " WHERE val1 = 'ab' AND val2 = 'keep'";
      assertEquals("partial index must agree with data after partial upsert",
        readRows(conn, dataSql), readRows(conn, idxSql));
    }
  }

  /**
   * A partial upsert on an immutable table carrying BOTH a matching-storage-scheme index (inherits
   * the base scheme) and a mismatched-storage-scheme index (explicit SINGLE_CELL_ARRAY_WITH_OFFSETS
   * on a ONE_CELL_PER_COLUMN base) must leave every index in agreement with the data table. The
   * mismatched index is always maintained on the server because its storage scheme differs from the
   * data table's (IndexMaintainer), independent of the server-side-immutable-index flag; the
   * matching index is maintained on the server only when the flag is on. So with the flag off the
   * same upsert batch carries one server-maintained and one client-maintained index, exercising the
   * single per-batch immutableRows classification -- both indexes must still read back the retained
   * covered column. The mismatched pairing is only legal in the upgrade direction (a SINGLE_CELL
   * base rejects a ONE_CELL index with INVALID_IMMUTABLE_STORAGE_SCHEME_CHANGE), so the mismatched
   * index is created only under the ONE_CELL_PER_COLUMN base; under the SINGLE_CELL base only the
   * matching index runs.
   */
  @Test
  public void testPartialUpsertForImmutableMixedStorageSchemeIndexes() throws Exception {
    if (localIndex || transactionProvider != null) {
      return;
    }
    boolean oneCellBase = tableDDLOptions.contains("ONE_CELL_PER_COLUMN");
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(false);
      String tableName = "TBL_" + generateUniqueName();
      String matchingIndex = "IND_" + generateUniqueName();
      String mismatchedIndex = "IND_" + generateUniqueName();
      conn.createStatement()
        .execute("CREATE TABLE " + tableName
          + " (id VARCHAR NOT NULL PRIMARY KEY, val1 VARCHAR, val2 VARCHAR, val3 VARCHAR) "
          + tableDDLOptions);
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " VALUES ('a', 'ab', 'abc', 'abcd')");
      conn.commit();
      // Matching-scheme index: inherits the base table's storage scheme.
      conn.createStatement()
        .execute("CREATE INDEX " + matchingIndex + " ON " + tableName + " (val1) INCLUDE (val3)");
      // Mismatched-scheme index: explicit SINGLE_CELL on a ONE_CELL base (the only legal mismatch
      // direction). Skipped under a SINGLE_CELL base, where a ONE_CELL index would be rejected.
      if (oneCellBase) {
        conn.createStatement()
          .execute("CREATE INDEX " + mismatchedIndex + " ON " + tableName
            + " (val2) INCLUDE (val3) IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, "
            + "COLUMN_ENCODED_BYTES=2");
      }
      // Partial upsert re-supplies both index keys (val1, val2) and omits the covered column val3.
      conn.createStatement()
        .execute("UPSERT INTO " + tableName + " (id, val1, val2) VALUES ('a', 'ab', 'abc')");
      conn.commit();

      String viaMatching = "SELECT /*+ INDEX(" + tableName + " " + matchingIndex
        + ") */ id, val3 FROM " + tableName + " WHERE val1 = 'ab'";
      String data = "SELECT /*+ NO_INDEX */ id, val3 FROM " + tableName + " WHERE id = 'a'";
      assertExplainPlan(false,
        QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + viaMatching)),
        tableName, matchingIndex);
      List<String> dataRows = readRows(conn, data);
      if (oneCellBase) {
        // ONE_CELL base keeps the omitted covered column, so val3 must survive as 'abcd' (id
        // anchors
        // the row so an all-null agreement cannot pass vacuously).
        assertEquals("[a|abcd]", dataRows.toString());
      }
      assertEquals("matching-scheme index must agree with data", dataRows,
        readRows(conn, viaMatching));
      if (oneCellBase) {
        String viaMismatched = "SELECT /*+ INDEX(" + tableName + " " + mismatchedIndex + ") */ id, "
          + "val3 FROM " + tableName + " WHERE val2 = 'abc'";
        assertExplainPlan(false,
          QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + viaMismatched)),
          tableName, mismatchedIndex);
        // The mismatched index is always server-maintained (schemes differ); it must read back the
        // same retained covered column as the data table, even when the matching index in the same
        // batch is maintained on the client (flag off).
        assertEquals("mismatched-scheme index must agree with data", dataRows,
          readRows(conn, viaMismatched));

        // Aggregate (COUNT) path does not self-heal against the data table the way the point scan
        // above does, so it is a strong guard that the SINGLE_CELL index actually retained the
        // omitted covered column. The single-cell family is rewritten wholesale on the partial
        // upsert, so without the always-on server read-back val3 would be lost and this COUNT would
        // drop to 0. It must equal the data-table COUNT in BOTH flag modes because a mismatched
        // storage scheme keeps the index server-maintained regardless of the flag.
        String countByCovered = "COUNT(*) FROM " + tableName + " WHERE val3 = 'abcd'";
        String dataCount = "SELECT /*+ NO_INDEX */ " + countByCovered;
        String mismatchedCount =
          "SELECT /*+ INDEX(" + tableName + " " + mismatchedIndex + ") */ " + countByCovered;
        // Filtering on a covered (non-key) column is a full index scan, not a range scan, so assert
        // the index is used by name rather than via the range-scan-only assertExplainPlan helper.
        String mismatchedCountPlan = QueryUtil
          .getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + mismatchedCount));
        assertTrue(
          "COUNT must be served from the mismatched-scheme index; plan was: " + mismatchedCountPlan,
          mismatchedCountPlan.contains(mismatchedIndex));
        assertEquals("mismatched-scheme index COUNT must agree with data",
          readRows(conn, dataCount), readRows(conn, mismatchedCount));
      }
    }
  }

  /**
   * Deleting from an immutable table that has a ROW_TIMESTAMP column and a secondary index. The
   * ROW_TIMESTAMP carve-out must keep index maintenance on the client even when the server-side
   * flag is on, so the region server does not re-stamp the cells with a server clock; otherwise a
   * ROW_TIMESTAMP range scan would silently drop the surviving rows.
   */
  @Test
  public void testDeleteFromImmutableRowTimestampTableWithIndex() throws Exception {
    if (localIndex || transactionProvider != null) {
      return;
    }
    try (Connection conn = DriverManager.getConnection(getUrl())) {
      conn.setAutoCommit(true);
      String tableName = "TBL_" + generateUniqueName();
      String indexName = "IND_" + generateUniqueName();
      conn.createStatement().execute(
        "CREATE TABLE " + tableName + " (k1 BIGINT NOT NULL, k2 VARCHAR NOT NULL, val VARCHAR "
          + "CONSTRAINT pk PRIMARY KEY (k1 ROW_TIMESTAMP, k2)) " + tableDDLOptions);
      conn.createStatement().execute("CREATE INDEX " + indexName + " ON " + tableName + " (val)");
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (100, 'a', 'v1')");
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (200, 'b', 'v2')");
      conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES (300, 'c', 'v3')");

      conn.createStatement().execute("DELETE FROM " + tableName + " WHERE k1 = 200 AND k2 = 'b'");

      // Data and index counts must agree after the delete.
      try (ResultSet rs =
        conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + tableName)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
      }
      String countViaIndex = "SELECT COUNT(*) FROM " + tableName + " WHERE val IS NOT NULL";
      assertExplainPlan(false,
        QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + countViaIndex)),
        tableName, indexName);
      try (ResultSet rs = conn.createStatement().executeQuery(countViaIndex)) {
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
      }
      // Each surviving row keeps its ROW_TIMESTAMP-derived cell timestamp: a scan bounded to a
      // surviving row's exact ROW_TIMESTAMP band must still return it. This is the ROW_TIMESTAMP
      // carve-out under test -- maintenance stays on the client so cells are not re-stamped with
      // the
      // server clock; a server-clock re-stamp would move the cell outside its ROW_TIMESTAMP band
      // and
      // this bounded scan would return 0. The band deliberately excludes the deleted k1=200: a
      // DELETE
      // marker is stamped at wall-clock time, outside any ROW_TIMESTAMP band, so a bounded scan
      // cannot observe the delete -- an orthogonal ROW_TIMESTAMP semantics unrelated to this flag.
      for (long k1 : new long[] { 100L, 300L }) {
        try (
          ResultSet rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM "
            + tableName + " WHERE k1 >= " + k1 + " AND k1 <= " + k1)) {
          assertTrue(rs.next());
          assertEquals("surviving ROW_TIMESTAMP row " + k1 + " must remain reachable in its band",
            1, rs.getInt(1));
        }
      }
      // Index read resolves a surviving row to its original ROW_TIMESTAMP key.
      String viaIndex = "SELECT k1 FROM " + tableName + " WHERE val = 'v3'";
      assertExplainPlan(false,
        QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + viaIndex)),
        tableName, indexName);
      try (ResultSet rs = conn.createStatement().executeQuery(viaIndex)) {
        assertTrue(rs.next());
        assertEquals(300L, rs.getLong(1));
        assertFalse(rs.next());
      }
    }
  }
}
