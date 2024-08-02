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
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.end2end.index.BaseLocalIndexIT;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlappingLocalIndexIT extends BaseLocalIndexIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlappingLocalIndexIT.class);

    public FlappingLocalIndexIT(boolean isNamespaceMapped) {
        super(isNamespaceMapped);
    }

    @Test
    public void testScanWhenATableHasMultipleLocalIndexes() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();

        createBaseTable(tableName, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try {
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + "2 ON " + tableName + "(k3)");
            conn1.commit();
            conn1 = DriverManager.getConnection(getUrl());
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(4, rs.getInt(1));
        } finally {
            conn1.close();
        }
    }

    @Test
    public void testLocalIndexScanWithSmallChunks() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();

        createBaseTable(tableName, 3, null);
        Properties props = new Properties();
        props.setProperty(QueryServices.SCAN_RESULT_CHUNK_SIZE, "2");
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        try{
            String[] strings = {"a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"};
            for (int i = 0; i < 26; i++) {
               conn1.createStatement().execute(
                    "UPSERT INTO " + tableName + " values('"+strings[i]+"'," + i + ","
                            + (i + 1) + "," + (i + 2) + ",'" + strings[25 - i] + "')");
            }
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + "_2 ON " + tableName + "(k3)");

            ResultSet rs = conn1.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());

            String query = "SELECT t_id,k1,v1 FROM " + tableName;
            rs = conn1.createStatement().executeQuery(query);
            for (int j = 0; j < 26; j++) {
                assertTrue(rs.next());
                assertEquals(strings[25 - j], rs.getString("t_id"));
                assertEquals(25 - j, rs.getInt("k1"));
                assertEquals(strings[j], rs.getString("V1"));
            }
            query = "SELECT t_id,k1,k3 FROM " + tableName;
            rs = conn1.createStatement().executeQuery(query);
            Thread.sleep(1000);
            for (int j = 0; j < 26; j++) {
                assertTrue(rs.next());
                assertEquals(strings[j], rs.getString("t_id"));
                assertEquals(j, rs.getInt("k1"));
                assertEquals(j + 2, rs.getInt("k3"));
            }
       } finally {
            conn1.close();
        }
    }
    
    @Test
    public void testLocalIndexScan() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String indexTableName = schemaName + "." + indexName;
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        try{
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('a',1,2,5,'y')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('b',1,2,4,'z')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('f',1,2,3,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('e',1,2,3,'b')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('j',2,4,2,'a')");
            conn1.createStatement().execute("UPSERT INTO " + tableName + " values('q',3,1,1,'c')");
            conn1.commit();
            conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
            
            ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexTableName);
            assertTrue(rs.next());
            
            Admin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
            int numRegions = admin.getRegions(physicalTableName).size();
            int trimmedRegionLocations = admin.getConfiguration()
                .getInt(QueryServices.MAX_REGION_LOCATIONS_SIZE_EXPLAIN_PLAN, -1);
            
            String query = "SELECT * FROM " + tableName +" where v1 like 'a%'";

            String explainPlanOutput =
                    QueryUtil.getExplainPlan(conn1.createStatement().executeQuery("EXPLAIN WITH REGIONS " + query));
            LOGGER.info("Explain plan output: {}", explainPlanOutput);
            // MAX_REGION_LOCATIONS_SIZE_EXPLAIN_PLAN is set as 2
            assertTrue("Expected total " + numRegions + " regions",
                    explainPlanOutput.contains("...total size = " + numRegions));

            ExplainPlan plan = conn1.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL " + numRegions + "-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(indexTableName + "(" + indexPhysicalTableName + ")",
                    explainPlanAttributes.getTableName());
            assertEquals(" [1,'a'] - [1,'b']",
                explainPlanAttributes.getKeyRanges());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
            assertEquals("CLIENT MERGE SORT",
                explainPlanAttributes.getClientSortAlgo());
            assertEquals(trimmedRegionLocations,
                explainPlanAttributes.getRegionLocations().size());

            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals("a", rs.getString("v1"));
            assertEquals(3, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals("a", rs.getString("v1"));
            assertEquals(2, rs.getInt("k3"));
            assertFalse(rs.next());
            query = "SELECT t_id, k1, k2,V1 FROM " + tableName +" where v1='a'";

            plan = conn1.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL " + numRegions + "-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(indexTableName + "(" + indexPhysicalTableName + ")",
                    explainPlanAttributes.getTableName());
            assertEquals(" [1,'a']",
                explainPlanAttributes.getKeyRanges());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
            assertEquals("CLIENT MERGE SORT",
                explainPlanAttributes.getClientSortAlgo());

            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertFalse(rs.next());
            query = "SELECT t_id, k1, k2,V1, k3 FROM " + tableName +" where v1<='z' order by k3";

            plan = conn1.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL " + numRegions + "-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(indexTableName + "(" + indexPhysicalTableName + ")",
                    explainPlanAttributes.getTableName());
            assertEquals(" [1,*] - [1,'z']",
                explainPlanAttributes.getKeyRanges());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
            assertEquals("CLIENT MERGE SORT",
                explainPlanAttributes.getClientSortAlgo());
            assertEquals("[\"K3\"]", explainPlanAttributes.getServerSortedBy());

            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals(4, rs.getInt("k3"));
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("k3"));
            assertFalse(rs.next());
            
            query = "SELECT t_id, k1, k2,v1 from " + tableName + " order by V1,t_id";

            plan = conn1.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL " + numRegions + "-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals(indexTableName + "(" + indexPhysicalTableName + ")",
                    explainPlanAttributes.getTableName());
            assertEquals(" [1]", explainPlanAttributes.getKeyRanges());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
            assertEquals("CLIENT MERGE SORT",
                explainPlanAttributes.getClientSortAlgo());
            assertNull(explainPlanAttributes.getServerSortedBy());

            rs = conn1.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals("f", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("j", rs.getString("t_id"));
            assertEquals(2, rs.getInt("k1"));
            assertEquals(4, rs.getInt("k2"));
            assertEquals("a", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("e", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals("b", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("q", rs.getString("t_id"));
            assertEquals(3, rs.getInt("k1"));
            assertEquals(1, rs.getInt("k2"));
            assertEquals("c", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("a", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals("y", rs.getString("V1"));
            assertTrue(rs.next());
            assertEquals("b", rs.getString("t_id"));
            assertEquals(1, rs.getInt("k1"));
            assertEquals(2, rs.getInt("k2"));
            assertEquals("z", rs.getString("V1"));
        } finally {
            conn1.close();
        }
    }

    @Test
    public void testBuildIndexWhenUserTableAlreadyHasData() throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String indexTableName = schemaName + "." + indexName;
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);
        String indexPhysicalTableName = physicalTableName.getNameAsString();

        createBaseTable(tableName, null, "('e','i','o')");
        Connection conn1 = DriverManager.getConnection(getUrl());
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('b',1,2,4,'z')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('f',1,2,3,'z')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('j',2,4,2,'a')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('q',3,1,1,'c')");
        conn1.commit();
        conn1.createStatement().execute("CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(v1)");
        ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexTableName);
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        Admin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        org.apache.hadoop.hbase.client.Connection hbaseConn = admin.getConnection();
        Table indexTable = hbaseConn.getTable(TableName.valueOf(indexPhysicalTableName));
        Pair<byte[][], byte[][]> startEndKeys = hbaseConn.getRegionLocator(TableName.valueOf(indexPhysicalTableName)).getStartEndKeys();
        byte[][] startKeys = startEndKeys.getFirst();
        byte[][] endKeys = startEndKeys.getSecond();
        for (int i = 0; i < startKeys.length; i++) {
            Scan s = new Scan();
            s.addFamily(QueryConstants.DEFAULT_LOCAL_INDEX_COLUMN_FAMILY_BYTES);
            s.withStartRow(startKeys[i]);
            s.withStopRow(endKeys[i]);
            ResultScanner scanner = indexTable.getScanner(s);
            int count = 0;
            for(Result r:scanner){
                count++;
            }
            scanner.close();
            assertEquals(1, count);
        }
        indexTable.close();
    }

    @Test
    public void testBuildingLocalIndexShouldHandleNoSuchColumnFamilyException() throws Exception {
        testBuildingLocalIndexShouldHandleNoSuchColumnFamilyException(false);
    }

    @Test
    public void testBuildingLocalCoveredIndexShouldHandleNoSuchColumnFamilyException() throws Exception {
        testBuildingLocalIndexShouldHandleNoSuchColumnFamilyException(true);
    }

    private void testBuildingLocalIndexShouldHandleNoSuchColumnFamilyException(boolean coveredIndex) throws Exception {
        String tableName = schemaName + "." + generateUniqueName();
        String indexName = "IDX_" + generateUniqueName();
        String indexTableName = schemaName + "." + indexName;
        TableName physicalTableName = SchemaUtil.getPhysicalTableName(tableName.getBytes(), isNamespaceMapped);

        createBaseTable(tableName, null, null, coveredIndex ? "cf" : null);
        Connection conn1 = DriverManager.getConnection(getUrl());
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('b',1,2,4,'z')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('f',1,2,3,'z')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('j',2,4,2,'a')");
        conn1.createStatement().execute("UPSERT INTO "+tableName+" values('q',3,1,1,'c')");
        conn1.commit();
        Admin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        TableDescriptor tableDescriptor = admin.getDescriptor(physicalTableName);
        tableDescriptor = TableDescriptorBuilder.newBuilder(tableDescriptor).setCoprocessor(
                CoprocessorDescriptorBuilder
                        .newBuilder(DeleyOpenRegionObserver.class.getName())
                        .setPriority(QueryServicesOptions.DEFAULT_COPROCESSOR_PRIORITY - 1)
                        .setProperties(Collections.emptyMap())
                        .build()).build();
        admin.disableTable(physicalTableName);
        admin.modifyTable(tableDescriptor);
        admin.enableTable(physicalTableName);
        DeleyOpenRegionObserver.DELAY_OPEN = true;
        conn1.createStatement().execute(
            "CREATE LOCAL INDEX " + indexName + " ON " + tableName + "(k3)"
                    + (coveredIndex ? " include(cf.v1)" : ""));
        DeleyOpenRegionObserver.DELAY_OPEN = false;
        ResultSet rs = conn1.createStatement().executeQuery("SELECT COUNT(*) FROM " + indexTableName);
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
    }

    public static class DeleyOpenRegionObserver implements RegionObserver {
        public static volatile boolean DELAY_OPEN = false;
        private int retryCount = 0;
        private CountDownLatch latch = new CountDownLatch(1);
        @Override
        public void preClose(org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c,
                boolean abortRequested) throws IOException {
            if(DELAY_OPEN) {
                try {
                    latch.await();
                } catch (InterruptedException e1) {
                    throw new DoNotRetryIOException(e1);
                }
            }
        }

        @Override
        public void preScannerOpen(org.apache.hadoop.hbase.coprocessor.ObserverContext<RegionCoprocessorEnvironment> c,
                Scan scan) throws IOException {
            if (DELAY_OPEN && retryCount == 1) {
                latch.countDown();
            }
            retryCount++;
        }
    }
}