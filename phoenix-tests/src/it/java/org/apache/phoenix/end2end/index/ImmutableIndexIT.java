/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.transaction.PhoenixTransactionProvider;
import org.apache.phoenix.transaction.PhoenixTransactionProvider.Feature;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class ImmutableIndexIT extends BaseTest {

    private final boolean localIndex;
    private final PhoenixTransactionProvider transactionProvider;
    private final String tableDDLOptions;

    private volatile boolean stopThreads = false;

    private static String TABLE_NAME;
    private static String INDEX_DDL;
    public static final AtomicInteger NUM_ROWS = new AtomicInteger(0);

    public ImmutableIndexIT(boolean localIndex, boolean transactional, String transactionProvider, boolean columnEncoded) {
        StringBuilder optionBuilder = new StringBuilder("IMMUTABLE_ROWS=true");
        this.localIndex = localIndex;
        if (!columnEncoded) {
            optionBuilder.append(",COLUMN_ENCODED_BYTES=0,IMMUTABLE_STORAGE_SCHEME="+PTableImpl.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
        }
        if (transactional) {
            optionBuilder.append(",TRANSACTIONAL=true, TRANSACTION_PROVIDER='" + transactionProvider + "'");
            this.transactionProvider = TransactionFactory.Provider.valueOf(transactionProvider).getTransactionProvider();
        } else {
            this.transactionProvider = null;
        }
        this.tableDDLOptions = optionBuilder.toString();

    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put("hbase.coprocessor.region.classes", CreateIndexRegionObserver.class.getName());
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(5);
        clientProps.put(QueryServices.TRANSACTIONS_ENABLED, "true");
        clientProps.put(QueryServices.INDEX_POPULATION_SLEEP_TIME, "15000");
        clientProps.put(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB, "true");
        clientProps.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "1");
        clientProps.put(HConstants.HBASE_CLIENT_PAUSE, "1");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    // name is used by failsafe as file name in reports
    @Parameters(name="ImmutableIndexIT_localIndex={0},transactional={1},transactionProvider={2},columnEncoded={3}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            { false, false, null, false }, { false, false, null, true },
            // OMID does not support local indexes or column encoding
            { false, true, "OMID", false },
            { true, false, null, false }, { true, false, null, true },
        });
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
            String ddl =
                    "CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            populateTestTable(fullTableName);
            ddl =
                    "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON "
                            + fullTableName + " (long_col1)";
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
            String ddl =
                    "CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            populateTestTable(fullTableName);
            ddl =
                    "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON "
                            + fullTableName + " (char_pk, varchar_pk)";
            stmt.execute(ddl);

            ResultSet rs;

            rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX*/ COUNT(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));

            String dml = "DELETE from " + fullTableName + " WHERE varchar_pk='varchar1'";
            assertEquals(1, conn.createStatement().executeUpdate(dml));
            assertIndexMutations(conn);
            conn.commit();
            
            rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX*/ COUNT(*) FROM " + fullTableName);
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
            String ddl =
                    "CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            populateTestTable(fullTableName);
            ddl =
                    "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON "
                            + fullTableName + " (varchar_col1, varchar_pk)";
            stmt.execute(ddl);

            ResultSet rs;

            rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX*/ COUNT(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));

            String dml = "DELETE from " + fullTableName + " WHERE varchar_col1='varchar_a' AND varchar_pk='varchar1'";
            assertEquals(1, conn.createStatement().executeUpdate(dml));
            assertIndexMutations(conn);
            conn.commit();
            
            TestUtil.dumpTable(conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes(fullTableName)));
            
            rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX*/ COUNT(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    private void assertIndexMutations(Connection conn) throws SQLException {
        Iterator<Pair<byte[], List<Cell>>> iterator = PhoenixRuntime.getUncommittedDataIterator(conn);
        assertTrue(iterator.hasNext());
        iterator.next();
        assertEquals(!localIndex || 
                (transactionProvider != null && 
                 transactionProvider.isUnsupported(Feature.MAINTAIN_LOCAL_INDEX_ON_SERVER)), iterator.hasNext());
    }

    private void createAndPopulateTableAndIndexForConsistentIndex(Connection conn, String tableName, String indexName,
            int numOfRowsToInsert, String storageProps)
            throws Exception {
        String tableOptions = tableDDLOptions;
        if (storageProps != null) {
            tableOptions += " ,IMMUTABLE_STORAGE_SCHEME=" + storageProps;
        }
        String ddl = "CREATE TABLE " + tableName + TestUtil.TEST_TABLE_SCHEMA + tableOptions;
        INDEX_DDL =
                "CREATE " + " INDEX IF NOT EXISTS " + SchemaUtil.getTableNameFromFullName(indexName)
                        + " ON " + tableName + " (long_pk, varchar_pk)"
                        + " INCLUDE (long_col1, long_col2) ";

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
                rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + TABLE_NAME);
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
            createAndPopulateTableAndIndexForConsistentIndex(conn, fullTableName, fullIndexName, numRows, null);

            String dml = "DELETE from " + fullTableName + " WHERE varchar_pk='varchar1'";
            conn.createStatement().execute(dml);
            conn.commit();
            ResultSet rs;
            rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + TABLE_NAME);
            assertTrue(rs.next());
            assertEquals(numRows - 1, rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
            assertTrue(rs.next());
            assertEquals(numRows - 1, rs.getInt(1));
            IndexTestUtil.assertRowsForEmptyColValue(conn, fullIndexName,
                QueryConstants.VERIFIED_BYTES);

            // Force delete to fail (data removed but operation failed) on data table and check index table row remains as unverified
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
            String selectFromIndex = "SELECT long_pk, varchar_pk, long_col1 FROM " + TABLE_NAME + " WHERE varchar_pk='varchar2' AND long_pk=2";
            rs =
                    conn.createStatement().executeQuery(
                            "EXPLAIN " + selectFromIndex);
            String actualExplainPlan = QueryUtil.getExplainPlan(rs);
            assertExplainPlan(false, actualExplainPlan, fullTableName, fullIndexName);

            rs = conn.createStatement().executeQuery(selectFromIndex);
            assertFalse(rs.next());
        }
    }

    public static class DeleteFailingRegionObserver extends SimpleRegionObserver {
        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws
                IOException {
            throw new DoNotRetryIOException();
        }
    }

    public static class UpsertFailingRegionObserver extends SimpleRegionObserver {
        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws
                IOException {
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
        String ddl ="CREATE TABLE " + TABLE_NAME + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        INDEX_DDL = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX IF NOT EXISTS " + indexName + " ON " + TABLE_NAME
                + " (long_pk, varchar_pk)"
                + " INCLUDE (long_col1, long_col2)";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);

            upsertRows(conn, TABLE_NAME, 220);
            conn.commit();

            // run the upsert select and also create an index
            conn.setAutoCommit(true);
            String upsertSelect = "UPSERT INTO " + TABLE_NAME + "(varchar_pk, char_pk, int_pk, long_pk, decimal_pk, date_pk) " +
                    "SELECT varchar_pk||'_upsert_select', char_pk, int_pk, long_pk, decimal_pk, date_pk FROM "+ TABLE_NAME;
            conn.createStatement().execute(upsertSelect);
            TestUtil.waitForIndexRebuild(conn, indexName, PIndexState.ACTIVE);
            ResultSet rs;
            rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + TABLE_NAME);
            assertTrue(rs.next());
            assertEquals(440,rs.getInt(1));
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexName);
            assertTrue(rs.next());
            assertEquals(440,rs.getInt(1));
        }
    }

    // used to create an index while a batch of rows are being written
    public static class CreateIndexRegionObserver extends SimpleRegionObserver {
        @Override
        public void postPut(org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c,
                Put put, org.apache.hadoop.hbase.wal.WALEdit edit, Durability durability) throws java.io.IOException {
            String tableName = c.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
            if (tableName.equalsIgnoreCase(TABLE_NAME)
                    // create the index after the second batch  
                    && Bytes.startsWith(put.getRow(), Bytes.toBytes("varchar200_upsert_select"))) {
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
                    for (int i=0; i<NUM_ROWS_IN_BATCH && !stopThreads; ++i) {
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
        String ddl ="CREATE TABLE " + fullTableName + TestUtil.TEST_TABLE_SCHEMA + tableDDLOptions;
        String indexDDL = "CREATE " + (localIndex ? "LOCAL" : "") + " INDEX " + indexName + " ON " + fullTableName
                + " (long_pk, varchar_pk)"
                + " INCLUDE (long_col1, long_col2)";
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
            rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + fullTableName);
            assertTrue(rs.next());
            int dataTableRowCount = rs.getInt(1);
            assertEquals(0,dataTableRowCount);

            List<Future<?>> futureList = Lists.newArrayListWithExpectedSize(numThreads);
            for (int i =0; i<numThreads; ++i) {
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

            rs = conn.createStatement().executeQuery("SELECT /*+ NO_INDEX */ COUNT(*) FROM " + fullTableName);
            assertTrue(rs.next());
            dataTableRowCount = rs.getInt(1);
            rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM " + fullIndexName);
            assertTrue(rs.next());
            int indexTableRowCount = rs.getInt(1);
            assertEquals("Data and Index table should have the same number of rows ", dataTableRowCount, indexTableRowCount);
        } finally {
            executorService.shutdownNow();
        }
    }

    private void setupForDeleteCount(Connection conn, String schemaName, String dataTableName,
            String indexTableName1, String indexTableName2) throws SQLException {

        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);

        conn.createStatement().execute("CREATE TABLE " + dataTableFullName
            + " (ID INTEGER NOT NULL PRIMARY KEY, VAL1 INTEGER, VAL2 INTEGER) "
            + this.tableDDLOptions);

        if (indexTableName1 != null) {
            conn.createStatement().execute(String.format(
                "CREATE INDEX %s ON %s (VAL1) INCLUDE (VAL2)", indexTableName1, dataTableFullName));
        }

        if (indexTableName2 != null) {
            conn.createStatement().execute(String.format(
                "CREATE INDEX %s ON %s (VAL2) INCLUDE (VAL1)", indexTableName2, dataTableFullName));
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
}
