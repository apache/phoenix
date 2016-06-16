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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.BaseOwnClusterHBaseManagedTimeIT;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.MetaDataUtil;
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
public class MutableIndexFailureIT extends BaseOwnClusterHBaseManagedTimeIT {
    public static volatile boolean FAIL_WRITE = false;
    public static final String INDEX_NAME = "IDX";
    
    private String tableName;
    private String indexName;
    private String fullTableName;
    private String fullIndexName;

    private final boolean transactional;
    private final boolean localIndex;
    private final String tableDDLOptions;
    private final boolean isNamespaceMapped;
    private String schema = "TEST";

    public MutableIndexFailureIT(boolean transactional, boolean localIndex, boolean isNamespaceMapped) {
        this.transactional = transactional;
        this.localIndex = localIndex;
        this.tableDDLOptions = transactional ? " TRANSACTIONAL=true " : "";
        this.tableName = (localIndex ? "L_" : "") + TestUtil.DEFAULT_DATA_TABLE_NAME + (transactional ? "_TXN" : "")
                + (isNamespaceMapped ? "_NM" : "");
        this.indexName = INDEX_NAME;
        this.fullTableName = SchemaUtil.getTableName(schema, tableName);
        this.fullIndexName = SchemaUtil.getTableName(schema, indexName);
        this.isNamespaceMapped = isNamespaceMapped;
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(10);
        serverProps.put("hbase.coprocessor.region.classes", FailingRegionObserver.class.getName());
        serverProps.put(HConstants.HBASE_CLIENT_RETRIES_NUMBER, "2");
        serverProps.put(HConstants.HBASE_RPC_TIMEOUT_KEY, "10000");
        serverProps.put("hbase.client.pause", "5000");
        serverProps.put("data.tx.snapshot.dir", "/tmp");
        serverProps.put("hbase.balancer.period", String.valueOf(Integer.MAX_VALUE));
        Map<String, String> clientProps = Collections.singletonMap(QueryServices.TRANSACTIONS_ENABLED, "true");
        NUM_SLAVES_BASE = 4;
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()), new ReadOnlyProps(clientProps.entrySet().iterator()));
    }

    @Parameters(name = "transactional = {0}, localIndex = {1}, isNamespaceMapped = {2}")
    public static Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] { { false, false, true }, { false, false, false }, { false, true, true },
                { false, true, false }, { true, false, true }, { true, true, true }, { true, false, false },
                { true, true, false } });
    }

    @Test
    public void testWriteFailureDisablesIndex() throws Exception {
        helpTestWriteFailureDisablesIndex();
    }

    public void helpTestWriteFailureDisablesIndex() throws Exception {
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

            FAIL_WRITE = false;
            conn.createStatement().execute(
                    "CREATE " + (localIndex ? "LOCAL " : "") + "INDEX " + indexName + " ON " + fullTableName + " (v1) INCLUDE (v2)");

            query = "SELECT * FROM " + fullIndexName;
            rs = conn.createStatement().executeQuery(query);
            assertFalse(rs.next());

            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(schema), indexName,
                    new String[] { PTableType.INDEX.toString() });
            assertTrue(rs.next());
            assertEquals(indexName, rs.getString(3));
            assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            assertFalse(rs.next());

            PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
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

            query = "SELECT /*+ NO_INDEX */ k,v1 FROM " + fullTableName;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            String expectedPlan = "CLIENT PARALLEL 1-WAY FULL SCAN OVER "
                    + SchemaUtil.getPhysicalTableName(fullTableName.getBytes(), isNamespaceMapped);
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

            FAIL_WRITE = true;

            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
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
                fail();
            } catch (SQLException e) {
                System.out.println();
            }  catch(Exception e) {
                System.out.println();
            }

            // Verify the metadata for index is correct.
            rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(schema), indexName,
                    new String[] { PTableType.INDEX.toString() });
            assertTrue(rs.next());
            assertEquals(indexName, rs.getString(3));
            // the index is only disabled for non-txn tables upon index table write failure
            if (transactional) {
                assertEquals(PIndexState.ACTIVE.toString(), rs.getString("INDEX_STATE"));
            } else {
                String indexState = rs.getString("INDEX_STATE");
                assertTrue(PIndexState.DISABLE.toString().equals(indexState) || PIndexState.INACTIVE.toString().equals(indexState));
            }
            assertFalse(rs.next());

            // If the table is transactional the write to both the data and index table will fail 
            // in an all or none manner. If the table is not transactional, then the data writes
            // would have succeeded while the index writes would have failed.
            if (!transactional) {
                // Verify UPSERT on data table still work after index is disabled
                stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
                stmt.setString(1, "a3");
                stmt.setString(2, "x3");
                stmt.setString(3, "3");
                stmt.execute();
                conn.commit();

                // Verify previous writes succeeded to data table
                query = "SELECT /*+ NO_INDEX */ k,v1 FROM " + fullTableName;
                rs = conn.createStatement().executeQuery("EXPLAIN " + query);
                expectedPlan = "CLIENT PARALLEL 1-WAY FULL SCAN OVER "
                        + SchemaUtil.getPhysicalTableName(fullTableName.getBytes(), isNamespaceMapped);
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

            // re-enable index table
            FAIL_WRITE = false;
            
            boolean isActive = false;
            if (!transactional) {
                int maxTries = 3, nTries = 0;
                do {
                    Thread.sleep(15 * 1000); // sleep 15 secs
                    rs = conn.getMetaData().getTables(null, StringUtil.escapeLike(schema), indexName,
                            new String[] { PTableType.INDEX.toString() });
                    assertTrue(rs.next());
                    if(PIndexState.ACTIVE.toString().equals(rs.getString("INDEX_STATE"))){
                        isActive = true;
                        break;
                    }
                } while(++nTries < maxTries);
                assertTrue(isActive);
            }

            // Verify UPSERT on data table still work after index table is recreated
            stmt = conn.prepareStatement("UPSERT INTO " + fullTableName + " VALUES(?,?,?)");
            stmt.setString(1, "a3");
            stmt.setString(2, "x4");
            stmt.setString(3, "4");
            stmt.execute();
            conn.commit();

            // verify index table has correct data
            query = "SELECT /*+ INDEX(" + indexName + ") */ k,v1 FROM " + fullTableName;
            rs = conn.createStatement().executeQuery("EXPLAIN " + query);
            expectedPlan = " OVER "
                    + (localIndex
                            ? Bytes.toString(SchemaUtil
                                    .getPhysicalTableName(fullTableName.getBytes(), isNamespaceMapped).getName())
                            : SchemaUtil.getPhysicalTableName(fullIndexName.getBytes(), isNamespaceMapped).getNameAsString());
            String explainPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(explainPlan.contains(expectedPlan));
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
    }
    
    public static class FailingRegionObserver extends SimpleRegionObserver {
        @Override
        public void preBatchMutate(ObserverContext<RegionCoprocessorEnvironment> c, MiniBatchOperationInProgress<Mutation> miniBatchOp) throws HBaseIOException {
            if (c.getEnvironment().getRegionInfo().getTable().getNameAsString().contains(INDEX_NAME) && FAIL_WRITE) {
                throw new DoNotRetryIOException();
            }
            Mutation operation = miniBatchOp.getOperation(0);
            Set<byte[]> keySet = operation.getFamilyMap().keySet();
            for(byte[] family: keySet) {
                if(Bytes.toString(family).startsWith(QueryConstants.LOCAL_INDEX_COLUMN_FAMILY_PREFIX) && FAIL_WRITE) {
                    throw new DoNotRetryIOException();
                }
            }
        }
    }

}
