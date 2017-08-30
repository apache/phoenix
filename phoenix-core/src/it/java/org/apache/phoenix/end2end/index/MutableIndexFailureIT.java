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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.execute.CommitException;
import org.apache.phoenix.hbase.index.write.IndexWriter;
import org.apache.phoenix.hbase.index.write.IndexWriterUtils;
import org.apache.phoenix.hbase.index.write.TrackingParallelWriterIndexCommitter;
import org.apache.phoenix.index.PhoenixIndexFailurePolicy;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.SortOrder;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
    private final boolean localIndex;
    private final String tableDDLOptions;
    private final boolean isNamespaceMapped;
    private final boolean leaveIndexActiveOnFailure;
    private final boolean rebuildIndexOnWriteFailure;
    private final boolean failRebuildTask;
    private final boolean throwIndexWriteFailure;
    private String schema = generateUniqueName();
    private List<CommitException> exceptions = Lists.newArrayList();

    public MutableIndexFailureIT(boolean transactional, boolean localIndex, boolean isNamespaceMapped, Boolean disableIndexOnWriteFailure, Boolean rebuildIndexOnWriteFailure, boolean failRebuildTask, Boolean throwIndexWriteFailure) {
        this.transactional = transactional;
        this.localIndex = localIndex;
        this.tableDDLOptions = " SALT_BUCKETS=2 " + (transactional ? ", TRANSACTIONAL=true " : "") 
                + (disableIndexOnWriteFailure == null ? "" : (", " + PhoenixIndexFailurePolicy.DISABLE_INDEX_ON_WRITE_FAILURE + "=" + disableIndexOnWriteFailure))
                + (rebuildIndexOnWriteFailure == null ? "" : (", " + PhoenixIndexFailurePolicy.REBUILD_INDEX_ON_WRITE_FAILURE + "=" + rebuildIndexOnWriteFailure))
                + (throwIndexWriteFailure == null ? "" : (", " + PhoenixIndexFailurePolicy.THROW_INDEX_WRITE_FAILURE + "=" + throwIndexWriteFailure));
        this.tableName = FailingRegionObserver.FAIL_TABLE_NAME;
        this.indexName = "A_" + FailingRegionObserver.FAIL_INDEX_NAME;
        fullTableName = SchemaUtil.getTableName(schema, tableName);
        this.fullIndexName = SchemaUtil.getTableName(schema, indexName);
        this.isNamespaceMapped = isNamespaceMapped;
        this.leaveIndexActiveOnFailure = ! (disableIndexOnWriteFailure == null ? QueryServicesOptions.DEFAULT_INDEX_FAILURE_DISABLE_INDEX : disableIndexOnWriteFailure);
        this.rebuildIndexOnWriteFailure = ! Boolean.FALSE.equals(rebuildIndexOnWriteFailure);
        this.failRebuildTask = failRebuildTask;
        this.throwIndexWriteFailure = ! Boolean.FALSE.equals(throwIndexWriteFailure);
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(10);
        serverProps.put("hbase.coprocessor.region.classes", FailingRegionObserver.class.getName());
        serverProps.put(IndexWriterUtils.INDEX_WRITER_RPC_RETRIES_NUMBER, "2");
        serverProps.put(HConstants.HBASE_RPC_TIMEOUT_KEY, "10000");
        serverProps.put(IndexWriterUtils.INDEX_WRITER_RPC_PAUSE, "5000");
        serverProps.put("data.tx.snapshot.dir", "/tmp");
        serverProps.put("hbase.balancer.period", String.valueOf(Integer.MAX_VALUE));
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_ATTRIB, Boolean.TRUE.toString());
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_INTERVAL_ATTRIB, "4000");
        serverProps.put(QueryServices.INDEX_REBUILD_DISABLE_TIMESTAMP_THRESHOLD, "30000"); // give up rebuilding after 30 seconds
        // need to override rpc retries otherwise test doesn't pass
        serverProps.put(QueryServices.INDEX_REBUILD_RPC_RETRIES_COUNTER, Long.toString(1));
        serverProps.put(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_OVERLAP_FORWARD_TIME_ATTRIB, Long.toString(1000));
        Map<String, String> clientProps = Collections.singletonMap(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        NUM_SLAVES_BASE = 4;
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    @Parameters(name = "MutableIndexFailureIT_transactional={0},localIndex={1},isNamespaceMapped={2},disableIndexOnWriteFailure={3},rebuildIndexOnWriteFailure={4},failRebuildTask={5},throwIndexWriteFailure={6}") // name is used by failsafe as file name in reports
    public static List<Object[]> data() {
        return Arrays.asList(new Object[][] { 
                { false, false, false, true, true, false, false},
                { false, false, true, true, true, false, null},
                { false, false, true, true, true, false, true},
                { false, false, false, true, true, false, null},
                { true, false, false, true, true, false, null},
                { true, false, true, true, true, false, null},
                { false, true, true, true, true, false, null},
                { false, true, false, null, null, false, null},
                { true, true, false, true, null, false, null},
                { true, true, true, null, true, false, null},

                { false, false, false, false, true, false, null},
                { false, true, false, false, null, false, null},
                { false, false, false, false, false, false, null},
                { false, false, false, true, true, false, null},
                { false, false, false, true, true, false, null},
                { false, true, false, true, true, false, null},
                { false, true, false, true, true, false, null},
                { false, false, false, true, true, true, null},
                { false, false, true, true, true, true, null},
                { false, false, false, true, true, true, false},
                { false, false, true, true, true, true, false},
                } 
        );
    }

    @Test
    public void testIndexWriteFailure() throws Exception {
        String secondIndexName = "B_" + FailingRegionObserver.FAIL_INDEX_NAME;
        String thirdIndexName = "C_" + "IDX";
//        String thirdFullIndexName = SchemaUtil.getTableName(schema, thirdIndexName);
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
                    "CREATE "  + (!localIndex ? "LOCAL " : "") + " INDEX " + secondIndexName + " ON " + fullTableName + " (v2) INCLUDE (v1)");
            conn.createStatement().execute(
                    "CREATE " + (localIndex ? "LOCAL " : "") + " INDEX " + thirdIndexName + " ON " + fullTableName + " (v1) INCLUDE (v2)");

            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(schema), null,
                    new String[] { PTableType.INDEX.toString() });
            assertTrue(rs.next());
            assertEquals(indexName, rs.getString(3));
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertTrue(rs.next());
            assertEquals(secondIndexName, rs.getString(3));
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
//            assertTrue(rs.next());
//            assertEquals(thirdIndexName, rs.getString(3));
//            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            initializeTable(conn, fullTableName);
            
            query = "SELECT /*+ NO_INDEX */ k,v1 FROM " + fullTableName;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String expectedPlan = "CLIENT PARALLEL 2-WAY FULL SCAN OVER "
                    + SchemaUtil.getPhysicalTableName(fullTableName.getBytes(), isNamespaceMapped)+"\nCLIENT MERGE SORT";
            assertEquals(expectedPlan, QueryUtil.getExplainPlan(rs));
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

            FailingRegionObserver.FAIL_WRITE = true;
            updateTable(conn, true);
            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(schema), StringUtil.escapeLike(indexName),
                    new String[] { PTableType.INDEX.toString() });
            assertTrue(rs.next());
            assertEquals(indexName, rs.getString(3));
            // the index is only disabled for non-txn tables upon index table write failure
            if (transactional || leaveIndexActiveOnFailure || localIndex) {
                assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            } else {
                String indexState = rs.getString("INDEX_STATE");
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
                updateTableAgain(conn, leaveIndexActiveOnFailure);
                // Verify previous writes succeeded to data table
                query = "SELECT /*+ NO_INDEX */ k,v1 FROM " + fullTableName;
                rs = conn.createStatement().executeQuery("EXPLAIN " + query);
                expectedPlan = "CLIENT PARALLEL 2-WAY FULL SCAN OVER "
                        + SchemaUtil.getPhysicalTableName(fullTableName.getBytes(), isNamespaceMapped)+"\nCLIENT MERGE SORT";
                assertEquals(expectedPlan, QueryUtil.getExplainPlan(rs));
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
            // Comment back in when PHOENIX-3815 is fixed
//            validateDataWithIndex(conn, fullTableName, thirdFullIndexName, false);

            if (!failRebuildTask) {
                // re-enable index table
                FailingRegionObserver.FAIL_WRITE = false;
                if (rebuildIndexOnWriteFailure) {
                    // wait for index to be rebuilt automatically
                    waitForIndexRebuild(conn,fullIndexName, PIndexState.ACTIVE);
                } else {
                    // simulate replaying failed mutation
                    replayMutations();
                }

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
                waitForIndexRebuild(conn, fullIndexName, PIndexState.DISABLE);
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

    private void waitForIndexRebuild(Connection conn, String fullIndexName, PIndexState expectedIndexState) throws InterruptedException, SQLException {
        if (!transactional) {
            TestUtil.waitForIndexRebuild(conn, fullIndexName, expectedIndexState);
        }
    }
    
    private void initializeTable(Connection conn, String tableName) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName + " VALUES(?,?,?)");
        stmt.setString(1, "a");
        stmt.setString(2, "x");
        stmt.setString(3, "1");
        stmt.execute();
        stmt.setString(1, "b");
        stmt.setString(2, "y");
        stmt.setString(3, "2");
        stmt.execute();
        stmt.setString(1, "c");
        stmt.setString(2, "z");
        stmt.setString(3, "3");
        stmt.execute();
        conn.commit();

    }

    private void validateDataWithIndex(Connection conn, String fullTableName, String fullIndexName, boolean localIndex) throws SQLException {
        String query = "SELECT /*+ INDEX(" + fullTableName + " " + SchemaUtil.getTableNameFromFullName(fullIndexName) + ")  */ k,v1 FROM " + fullTableName;
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        String expectedPlan = " OVER "
                + (localIndex
                        ? Bytes.toString(
                                SchemaUtil.getPhysicalTableName(fullTableName.getBytes(), isNamespaceMapped).getName())
                        : SchemaUtil.getPhysicalTableName(fullIndexName.getBytes(), isNamespaceMapped).getNameAsString());
        String explainPlan = QueryUtil.getExplainPlan(rs);
        assertTrue(explainPlan, explainPlan.contains(expectedPlan));
        rs = conn.createStatement().executeQuery(query);
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
    
    private void replayMutations() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        for (int i = 0; i < exceptions.size(); i++) {
            CommitException e = exceptions.get(i);
            long ts = e.getServerTimestamp();
            props.setProperty(PhoenixRuntime.REPLAY_AT_ATTRIB, Long.toString(ts));
            try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
                if (i == 0) {
                    updateTable(conn, false);
                } else if (i == 1) {
                    updateTableAgain(conn, false);
                } else {
                    fail();
                }
            }
        }
    }
    
    private void updateTable(Connection conn, boolean commitShouldFail) throws SQLException {
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
        public static volatile boolean FAIL_WRITE = false;
        public static final String FAIL_INDEX_NAME = "FAIL_IDX";
        public static final String FAIL_TABLE_NAME = "FAIL_TABLE";

        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws HBaseIOException {
            boolean throwException = false;
            if (c.getEnvironment().getRegionInfo().getTable().getNameAsString().endsWith("A_" + FAIL_INDEX_NAME)
                    && FAIL_WRITE) {
                throwException = true;
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
                            short indexId = MetaDataUtil.getViewIndexIdDataType().getCodec().decodeShort(firstCell.getRowArray(), firstCell.getRowOffset() + regionStartKeyLen, SortOrder.getDefault());
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
                dropIndex(c);
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
             } catch (ClassNotFoundException e) {
             } catch (SQLException e) {
             }
         }
    }

}
