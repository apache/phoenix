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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.coprocessor.MetaDataRegionObserver;
import org.apache.phoenix.coprocessor.MetaDataRegionObserver.BuildIndexScheduleTask;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.execute.CommitException;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.index.PhoenixIndexFailurePolicy;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.transaction.PhoenixTransactionProvider;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.IndexScrutiny;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
/**
 * 
 * Test for failure of region server to write to index table.
 * For some reason dropping tables after running this test
 * fails unless it runs its own mini cluster. 
 * 
 */

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class MutableIndexFailureIT extends BaseTest {
    public static volatile boolean FAIL_WRITE = false;
    public static volatile String fullTableName;
    
    private String tableName;
    private String indexName;
    private String fullIndexName;

    private final boolean transactional;
    private final PhoenixTransactionProvider transactionProvider;
    private final boolean localIndex;
    private final String tableDDLOptions;
    private final boolean isNamespaceMapped;
    private final boolean leaveIndexActiveOnFailure;
    private final boolean failRebuildTask;
    private final boolean throwIndexWriteFailure;
    private String schema = generateUniqueName();
    private List<CommitException> exceptions = Lists.newArrayList();
    protected static RegionCoprocessorEnvironment indexRebuildTaskRegionEnvironment;
    protected static final int forwardOverlapMs = 1000;
    protected static final int disableTimestampThresholdMs = 10000;
    protected static final int numRpcRetries = 2;

    public MutableIndexFailureIT(String transactionProvider, boolean localIndex, boolean isNamespaceMapped, Boolean disableIndexOnWriteFailure, boolean failRebuildTask, Boolean throwIndexWriteFailure) {
        this.transactional = transactionProvider != null;
        this.transactionProvider = transactionProvider == null ? null :
            TransactionFactory.getTransactionProvider(TransactionFactory.Provider.valueOf(transactionProvider));
        this.localIndex = localIndex;
        this.tableDDLOptions = " SALT_BUCKETS=2, COLUMN_ENCODED_BYTES=NONE" + (transactional ? (",TRANSACTIONAL=true,TRANSACTION_PROVIDER='"+transactionProvider+"'") : "") 
                + (disableIndexOnWriteFailure == null ? "" : (", " + PhoenixIndexFailurePolicy.DISABLE_INDEX_ON_WRITE_FAILURE + "=" + disableIndexOnWriteFailure))
                + (throwIndexWriteFailure == null ? "" : (", " + PhoenixIndexFailurePolicy.THROW_INDEX_WRITE_FAILURE + "=" + throwIndexWriteFailure));
        this.tableName = FailingRegionObserver.FAIL_TABLE_NAME;
        this.indexName = "A_" + FailingRegionObserver.FAIL_INDEX_NAME;
        fullTableName = SchemaUtil.getTableName(schema, tableName);
        this.fullIndexName = SchemaUtil.getTableName(schema, indexName);
        this.isNamespaceMapped = isNamespaceMapped;
        this.leaveIndexActiveOnFailure = ! (disableIndexOnWriteFailure == null ? QueryServicesOptions.DEFAULT_INDEX_FAILURE_DISABLE_INDEX : disableIndexOnWriteFailure);
        this.failRebuildTask = failRebuildTask;
        this.throwIndexWriteFailure = ! Boolean.FALSE.equals(throwIndexWriteFailure);
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> serverProps = getServerProps();
        Map<String, String> clientProps = Maps.newHashMapWithExpectedSize(2);
        clientProps.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "2");
        clientProps.put(QueryServices.INDEX_REGION_OBSERVER_ENABLED_ATTRIB, Boolean.FALSE.toString());
        NUM_SLAVES_BASE = 4;
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
        indexRebuildTaskRegionEnvironment = getUtility()
                .getRSForFirstRegionInTable(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME)
                .getRegions(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME).get(0).getCoprocessorHost()
                .findCoprocessorEnvironment(MetaDataRegionObserver.class.getName());
        MetaDataRegionObserver.initRebuildIndexConnectionProps(
            indexRebuildTaskRegionEnvironment.getConfiguration());
    }
    
    protected static Map<String,String> getServerProps(){
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(10);
        serverProps.put("hbase.coprocessor.region.classes", FailingRegionObserver.class.getName());
        serverProps.put(HConstants.HBASE_RPC_TIMEOUT_KEY, "10000");
        serverProps.put(IndexWriterUtils.INDEX_WRITER_RPC_PAUSE, "5000");
        serverProps.put("data.tx.snapshot.dir", "/tmp");
        serverProps.put("hbase.balancer.period", String.valueOf(Integer.MAX_VALUE));
        // need to override rpc retries otherwise test doesn't pass
        serverProps.put(QueryServices.INDEX_REBUILD_RPC_RETRIES_COUNTER, Long.toString(numRpcRetries));
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_FORWARD_TIME_ATTRIB, Long.toString(forwardOverlapMs));
        serverProps.put(QueryServices.INDEX_REBUILD_DISABLE_TIMESTAMP_THRESHOLD, Long.toString(disableTimestampThresholdMs));
        /*
         * Effectively disable running the index rebuild task by having an infinite delay
         * because we want to control it's execution ourselves
         */
        serverProps.put(QueryServices.INDEX_REBUILD_TASK_INITIAL_DELAY, Long.toString(Long.MAX_VALUE));
        return serverProps;
    }

    @Parameters(name = "MutableIndexFailureIT_transactionProvider={0},localIndex={1},isNamespaceMapped={2},disableIndexOnWriteFailure={3},failRebuildTask={4},throwIndexWriteFailure={5}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Object[]> data() {
        return TestUtil.filterTxParamData(
                Arrays.asList(new Object[][] { 
                    // note - can't disableIndexOnWriteFailure without throwIndexWriteFailure, PHOENIX-4130
                    { null, false, false, false, false, false},
                    { null, false, false, true, false, null},
                    { "TEPHRA", false, false, true, false, null},
                    { "OMID", false, false, true, false, null},
                    { null, true, false, null, false, null},
                    { "TEPHRA", true, false, true, false, null},
                    { null, false, false, false, false, null},
                    { null, true, false, false, false, null},
                    { null, false, false, false, false, null},
                    { null, false, false, true, false, null},
                    { null, true, false, true, false, null},
                    { null, true, false, true, false, null},
                    { null, false, false, true, true, null},
                    { null, false, false, false, true, false},
                    }), 0);
        
    }

    private void runRebuildTask(Connection conn) throws InterruptedException, SQLException {
        BuildIndexScheduleTask task =
                new MetaDataRegionObserver.BuildIndexScheduleTask(
                        indexRebuildTaskRegionEnvironment);
        dumpStateOfIndexes(conn, fullTableName, true);
        task.run();
        dumpStateOfIndexes(conn, fullTableName, false);
        Thread.sleep(forwardOverlapMs + 100);
        if (failRebuildTask) {
            Thread.sleep(disableTimestampThresholdMs + 100);
        }
        dumpStateOfIndexes(conn, fullTableName, true);
        task.run();
        dumpStateOfIndexes(conn, fullTableName, false);
    }

    private static final void dumpStateOfIndexes(Connection conn, String tableName,
            boolean beforeRebuildTaskRun) throws SQLException {
        PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
        PTable table = phxConn.getTable(new PTableKey(phxConn.getTenantId(), tableName));
        List<PTable> indexes = table.getIndexes();
        String s = beforeRebuildTaskRun ? "before rebuild run" : "after rebuild run";
        System.out.println("************Index state in connection " + s + "******************");
        for (PTable idx : indexes) {
            System.out.println(
                "Index Name: " + idx.getName().getString() + " State: " + idx.getIndexState()
                        + " Disable timestamp: " + idx.getIndexDisableTimestamp());
        }
        System.out.println("************Index state from server  " + s + "******************");
        table = PhoenixRuntime.getTableNoCache(phxConn, fullTableName);
        for (PTable idx : table.getIndexes()) {
            System.out.println(
                "Index Name: " + idx.getName().getString() + " State: " + idx.getIndexState()
                        + " Disable timestamp: " + idx.getIndexDisableTimestamp());
        }
    }

    @Test
    public void testIndexWriteFailure() throws Exception {
        String secondIndexName = "B_" + FailingRegionObserver.FAIL_INDEX_NAME;
        String thirdIndexName = "C_IDX";
        String thirdFullIndexName = SchemaUtil.getTableName(schema, thirdIndexName);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, String.valueOf(isNamespaceMapped));
        try (Connection conn = driver.connect(url, props)) {
            String query;
            ResultSet rs;
            conn.setAutoCommit(false);
            if (isNamespaceMapped) {
                conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schema);
            }
            conn.createStatement().execute("CREATE TABLE " + fullTableName
                    + " (k VARCHAR NOT NULL PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) " + tableDDLOptions);
            query = "SELECT * FROM " + fullTableName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            FailingRegionObserver.FAIL_WRITE = false;
            conn.createStatement().execute(
                    "CREATE " + (localIndex ? "LOCAL " : "") + " INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE (v2)");
            // Create other index which should be local/global if the other index is global/local to
            // check the drop index.
            conn.createStatement().execute(
                    "CREATE "  + ((!localIndex && 
                            (transactionProvider == null 
                            || !transactionProvider.isUnsupported(PhoenixTransactionProvider.Feature.ALLOW_LOCAL_INDEX))) 
                            ? "LOCAL " : "") + " INDEX " + secondIndexName + " ON " + fullTableName + " (v2) INCLUDE (v1)");
            conn.createStatement().execute(
                    "CREATE " + (localIndex ? "LOCAL " : "") + " INDEX " + thirdIndexName + " ON " + fullTableName + " (v1) INCLUDE (v2)");

            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            initializeTable(conn, fullTableName);
            addRowsInTableDuringRetry(fullTableName);

            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(schema), null,
                    new String[] { PTableType.INDEX.toString() });
            assertTrue(rs.next());
            assertEquals(indexName, rs.getString(3));
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertTrue(rs.next());
            assertEquals(secondIndexName, rs.getString(3));
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertTrue(rs.next());
            assertEquals(thirdIndexName, rs.getString(3));
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            // we should be able to write to ACTIVE index even in case of disable index on failure policy
            addRowToTable(conn, fullTableName);

            query = "SELECT /*+ NO_INDEX */ k,v1 FROM " + fullTableName;
            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 2-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("FULL SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(
                SchemaUtil.getPhysicalTableName(fullTableName.getBytes(),
                    isNamespaceMapped).toString(),
                explainPlanAttributes.getTableName());
            assertEquals("CLIENT MERGE SORT",
                explainPlanAttributes.getClientSortAlgo());

            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("x", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertEquals("z", rs.getString(2));
            assertFalse(rs.next());

            updateTable(conn, true);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(schema), StringUtil.escapeLike(indexName),
                    new String[] { PTableType.INDEX.toString() });
            assertTrue(rs.next());
            assertEquals(indexName, rs.getString(3));
            // the index is only disabled for non-txn tables upon index table write failure
            String indexState = rs.getString("INDEX_STATE");
            if (transactional || leaveIndexActiveOnFailure || localIndex) {
                assertTrue(PIndexState.ACTIVE.toString().equalsIgnoreCase(indexState) || PIndexState.PENDING_ACTIVE.toString().equalsIgnoreCase(indexState));
            } else {
                assertTrue(PIndexState.DISABLE.toString().equals(indexState) || PIndexState.INACTIVE.toString().equals(indexState));
                // non-failing index should remain active
                ResultSet thirdRs = conn.createStatement().executeQuery(getSysCatQuery(thirdIndexName));
                assertTrue(thirdRs.next());
                assertEquals(PIndexState.ACTIVE.getSerializedValue(), thirdRs.getString(1));
            }
            assertFalse(rs.next());

            // If the table is transactional the write to both the data and index table will fail 
            // in an all or none manner. If the table is not transactional, then the data writes
            // would have succeeded while the index writes would have failed.
            if (!transactional) {
                updateTableAgain(conn, false);
                // Verify previous writes succeeded to data table
                query = "SELECT /*+ NO_INDEX */ k,v1 FROM " + fullTableName;
                plan = conn.prepareStatement(query)
                    .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                    .getExplainPlan();
                explainPlanAttributes = plan.getPlanStepsAsAttributes();
                assertEquals("PARALLEL 2-WAY",
                    explainPlanAttributes.getIteratorTypeAndScanSize());
                assertEquals("FULL SCAN ",
                    explainPlanAttributes.getExplainScanType());
                assertEquals(
                    SchemaUtil.getPhysicalTableName(fullTableName.getBytes(),
                        isNamespaceMapped).toString(),
                    explainPlanAttributes.getTableName());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());

                rs = conn.createStatement().executeQuery(query);
                assertTrue(rs.next());
                assertEquals("a", rs.getString(1));
                assertEquals("x2", rs.getString(2));
                assertTrue(rs.next());
                assertEquals("a3", rs.getString(1));
                assertEquals("x3", rs.getString(2));
                assertTrue(rs.next());
                assertEquals("c", rs.getString(1));
                assertEquals("z", rs.getString(2));
                assertTrue(rs.next());
                assertEquals("d", rs.getString(1));
                assertEquals("d", rs.getString(2));
                assertFalse(rs.next());
            }
            IndexScrutiny.scrutinizeIndex(conn, fullTableName, thirdFullIndexName);

            if (!failRebuildTask) {
                // re-enable index table
                FailingRegionObserver.FAIL_WRITE = false;
                runRebuildTask(conn);
                // wait for index to be rebuilt automatically
                checkStateAfterRebuild(conn, fullIndexName, PIndexState.ACTIVE);
                // Verify UPSERT on data table still works after index table is caught up
                PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
                stmt.setString(1, "a3");
                stmt.setString(2, "x4");
                stmt.setString(3, "4");
                stmt.execute();
                conn.commit();

                // verify index table has correct data (note that second index has been dropped)
                validateDataWithIndex(conn, fullTableName, fullIndexName, localIndex);
            } else {
                // Wait for index to be rebuilt automatically. This should fail because
                // we haven't flipped the FAIL_WRITE flag to false and as a result this
                // should cause index rebuild to fail too.
                runRebuildTask(conn);
                checkStateAfterRebuild(conn, fullIndexName, PIndexState.DISABLE);
                // verify that the index was marked as disabled and the index disable
                // timestamp set to 0
                String q = getSysCatQuery(indexName);
                try (ResultSet r = conn.createStatement().executeQuery(q)) {
                    assertTrue(r.next());
                    assertEquals(PIndexState.DISABLE.getSerializedValue(), r.getString(1));
                    assertEquals(0, r.getLong(2));
                    assertFalse(r.next());
                }

            }
        } finally {
            FAIL_WRITE = false;
        }
    }

    private String getSysCatQuery(String iName) {
        String q =
                "SELECT INDEX_STATE, INDEX_DISABLE_TIMESTAMP FROM SYSTEM.CATALOG WHERE TABLE_SCHEM = '"
                        + schema + "' AND TABLE_NAME = '" + iName + "'"
                        + " AND COLUMN_NAME IS NULL AND COLUMN_FAMILY IS NULL";
        return q;
    }


    private void checkStateAfterRebuild(Connection conn, String fullIndexName, PIndexState expectedIndexState) throws InterruptedException, SQLException {
        if (!transactional) {
            assertTrue(TestUtil.checkIndexState(conn,fullIndexName, expectedIndexState, 0l));
        }
    }
    
    private void initializeTable(Connection conn, String tableName) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        conn.commit();
    }

    private void addRowToTable(Connection conn, String tableName) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
        stmt.setString(1, "c");
        stmt.setString(2, "z");
        stmt.setString(3, "3");
        stmt.execute();
        conn.commit();
    }

    private void addRowsInTableDuringRetry(final String tableName)
            throws SQLException, InterruptedException, ExecutionException {
        int threads=10;
        boolean wasFailWrite = FailingRegionObserver.FAIL_WRITE;
        boolean wasToggleFailWriteForRetry = FailingRegionObserver.TOGGLE_FAIL_WRITE_FOR_RETRY;
        try {
            Callable callable = new Callable() {

                @Override
                public Boolean call() {
                    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
                    props.put(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, String.valueOf(isNamespaceMapped));
                    try (Connection conn = driver.connect(url, props)) {
                        // In case of disable index on failure policy, INDEX will be in PENDING_DISABLE on first retry
                        // but will
                        // become active if retry is successfull
                        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
                        stmt.setString(1, "b");
                        stmt.setString(2, "y");
                        stmt.setString(3, "2");
                        stmt.execute();
                        if (!leaveIndexActiveOnFailure && !transactional) {
                            FailingRegionObserver.FAIL_WRITE = true;
                            FailingRegionObserver.TOGGLE_FAIL_WRITE_FOR_RETRY = true;
                        }
                        conn.commit();
                    } catch (SQLException e) {
                        return false;
                    }
                    return true;
                }
            };
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
            for (int i = 0; i < threads; i++) {
                futures.add(executor.submit(callable));
            }
            for (Future<Boolean> future : futures) {
                Boolean isSuccess = future.get();
                // transactions can have conflict so ignoring the check for them
                if (!transactional) {
                    assertTrue(isSuccess);
                }
            }
            executor.shutdown();
        } finally {
            FailingRegionObserver.FAIL_WRITE = wasFailWrite;
            FailingRegionObserver.TOGGLE_FAIL_WRITE_FOR_RETRY = wasToggleFailWriteForRetry;
        }
    }

    private void validateDataWithIndex(Connection conn, String fullTableName, String fullIndexName, boolean localIndex) throws Exception {
        String query = "SELECT /*+ INDEX(" + fullTableName + " " + SchemaUtil.getTableNameFromFullName(fullIndexName) + ")  */ k,v1 FROM " + fullTableName;
        ResultSet rs = conn.createStatement().executeQuery(query);
        String expectedPlan = " OVER "
                + (localIndex
                        ? Bytes.toString(
                                SchemaUtil.getPhysicalTableName(fullTableName.getBytes(), isNamespaceMapped).getName())
                        : SchemaUtil.getPhysicalTableName(fullIndexName.getBytes(), isNamespaceMapped).getNameAsString());
        String explainPlan = QueryUtil.getExplainPlan(conn.createStatement().executeQuery("EXPLAIN " + query));
        assertTrue(explainPlan, explainPlan.contains(expectedPlan));
        if (transactional) { // failed commit does not get retried
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("x", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("a3", rs.getString(1));
            assertEquals("x4", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertEquals("y", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertEquals("z", rs.getString(2));
            assertFalse(rs.next());
        } else { // failed commit eventually succeeds
            assertTrue(rs.next());
            assertEquals("d", rs.getString(1));
            assertEquals("d", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("x2", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("a3", rs.getString(1));
            assertEquals("x4", rs.getString(2));
            assertTrue(rs.next());
            assertEquals("c", rs.getString(1));
            assertEquals("z", rs.getString(2));
            assertFalse(rs.next());
        }
    }

    private void updateTable(Connection conn, boolean commitShouldFail) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
        // Insert new row
        stmt.setString(1, "d");
        stmt.setString(2, "d");
        stmt.setString(3, "4");
        stmt.execute();
        // Update existing row
        stmt.setString(1, "a");
        stmt.setString(2, "x2");
        stmt.setString(3, "2");
        stmt.execute();
        // Delete existing row
        stmt = conn.prepareStatement("DELETE FROM " + fullTableName + " WHERE k=?");
        stmt.setString(1, "b");
        stmt.execute();
        // Set to fail after the DELETE, since transactional tables will write
        // uncommitted data when the DELETE is executed.
        FailingRegionObserver.FAIL_WRITE = true;
        try {
            FailingRegionObserver.FAIL_NEXT_WRITE = localIndex && transactional;
            conn.commit();
            if (commitShouldFail && (!localIndex || transactional) && this.throwIndexWriteFailure) {
                fail();
            }
        } catch (CommitException e) {
            if (!commitShouldFail || !this.throwIndexWriteFailure) {
                throw e;
            }
            exceptions.add(e);
        }

    }

    private void updateTableAgain(Connection conn, boolean commitShouldFail) throws SQLException {
        // Verify UPSERT on data table still work after index is disabled
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
        stmt.setString(1, "a3");
        stmt.setString(2, "x3");
        stmt.setString(3, "3");
        stmt.execute();
        try {
            conn.commit();
            if (commitShouldFail && !localIndex && this.throwIndexWriteFailure) {
                fail();
            }
        } catch (CommitException e) {
            if (!commitShouldFail || !this.throwIndexWriteFailure) {
                throw e;
            }
            exceptions.add(e);
        }
    }

    public static class FailingRegionObserver extends SimpleRegionObserver {
        public static boolean TOGGLE_FAIL_WRITE_FOR_RETRY = false;
        public static volatile boolean FAIL_WRITE = false;
        public static volatile boolean FAIL_NEXT_WRITE = false;
        public static final String FAIL_INDEX_NAME = "FAIL_IDX";
        public static final String FAIL_TABLE_NAME = "FAIL_TABLE";

        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
            boolean throwException = false;
            if (FAIL_NEXT_WRITE) {
                throwException = true;
                FAIL_NEXT_WRITE = false;
            } else if (c.getEnvironment().getRegionInfo().getTable().getNameAsString().endsWith("A_" + FAIL_INDEX_NAME)
                    && FAIL_WRITE) {
                throwException = true;
                if (TOGGLE_FAIL_WRITE_FOR_RETRY) {
                    FAIL_WRITE = !FAIL_WRITE;
                }
            } else {
                // When local index updates are atomic with data updates, testing a write failure to a local
                // index won't make sense.
                Mutation operation = miniBatchOp.getOperation(0);
                if (FAIL_WRITE) {
                    Map<byte[],List<Cell>>cellMap = operation.getFamilyCellMap();
                    for (Map.Entry<byte[],List<Cell>> entry : cellMap.entrySet()) {
                        byte[] family = entry.getKey();
                        if (Bytes.toString(family).startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX)) {
                            int regionStartKeyLen = c.getEnvironment().getRegionInfo().getStartKey().length;
                            Cell firstCell = entry.getValue().get(0);
                            long indexId = MetaDataUtil.getViewIndexIdDataType().getCodec().decodeLong(firstCell.getRowArray(), firstCell.getRowOffset() + regionStartKeyLen, SortOrder.getDefault());
                            // Only throw for first local index as the test may have multiple local indexes
                            if (indexId == Short.MIN_VALUE) {
                                throwException = true;
                                break;
                            }
                        }
                    }
                }
            }
            if (throwException) {
                if (!TOGGLE_FAIL_WRITE_FOR_RETRY) {
                    dropIndex(c);
                }
                throw new DoNotRetryIOException();
            }
        }

         private void dropIndex(ObserverContext<RegionCoprocessorEnvironment> c) {
             try {
                 Connection connection =
                         QueryUtil.getConnection(c.getEnvironment().getConfiguration());
                 connection.createStatement().execute(
                        "DROP INDEX IF EXISTS " + "B_" + FAIL_INDEX_NAME + " ON "
                             + fullTableName);
             } catch (SQLException e) {
             }
         }
    }

}
