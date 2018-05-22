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

import static org.apache.phoenix.util.TestUtil.analyzeTable;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.curator.shaded.com.google.common.collect.Sets;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ScanUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public abstract class BaseViewIT extends BaseUniqueNamesOwnClusterIT {

    protected String tableDDLOptions;
    protected boolean transactional;

    public BaseViewIT(boolean transactional) {
        StringBuilder optionBuilder = new StringBuilder();
        this.transactional = transactional;
        if (transactional) {
            optionBuilder.append(" TRANSACTIONAL=true ");
        }
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Parameters(name = "transactional = {0}")
    public static Collection<Boolean> data() {
        return Arrays.asList(new Boolean[] { false, true });
    }

    protected void testUpdatableViewWithIndex(Integer saltBuckets, boolean localIndex) throws Exception {
        String schemaName = TestUtil.DEFAULT_SCHEMA_NAME + "_" + generateUniqueName();
        String tableName = "T_" + generateUniqueName();
        String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
        String viewName = "V_" + generateUniqueName();
        testUpdatableView(fullTableName, viewName, null, null, saltBuckets);
        Pair<String, Scan> pair = testUpdatableViewIndex(fullTableName, saltBuckets, localIndex, viewName);
        Scan scan = pair.getSecond();
        String physicalTableName = pair.getFirst();
        // Confirm that dropping the view also deletes the rows in the index
        if (saltBuckets == null) {
            try (Connection conn = DriverManager.getConnection(getUrl())) {
                HTableInterface htable = conn.unwrap(PhoenixConnection.class).getQueryServices()
                        .getTable(Bytes.toBytes(physicalTableName));
                if (ScanUtil.isLocalIndex(scan)) {
                    ScanUtil.setLocalIndexAttributes(scan, 0, HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
                            scan.getStartRow(), scan.getStopRow());
                }
                ResultScanner scanner = htable.getScanner(scan);
                Result result = scanner.next();
                // Confirm index has rows
                assertTrue(result != null && !result.isEmpty());

                conn.createStatement().execute("DROP VIEW " + viewName);

                // Confirm index has no rows after view is dropped
                scanner = htable.getScanner(scan);
                result = scanner.next();
                assertTrue(result == null || result.isEmpty());
            }
        }
    }
    
    /**
     *  Split SYSTEM.CATALOG at the given split point 
     */
    protected void splitRegion(byte[] splitPoint) throws SQLException, IOException, InterruptedException {
        HBaseAdmin admin =
                driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        MiniHBaseCluster cluster = getUtility().getHBaseCluster();
        int startNumRegions = cluster.getRegions(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME).size();
        admin.split(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME, splitPoint);
        List<HRegion> regions = cluster.getRegions(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME);
        // wait for the split to happen
        while (regions.size() != startNumRegions+1) {
          System.out.println("Waiting for region to split....");
          Thread.sleep(1000);
          regions = cluster.getRegions(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME);
        }
    }
    
    /**
     * Returns true if the region contains atleast one of the metadata rows we are interested in
     */
    protected boolean regionContainsMetadataRows(HRegionInfo regionInfo,
            List<byte[]> metadataRowKeys) {
        for (byte[] rowKey : metadataRowKeys) {
            if (regionInfo.containsRow(rowKey)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * @param fullTableOrViewNames  list of global tables or views 
     */
    protected void splitSystemCatalog(List<String> fullTableOrViewNames) throws Exception {
        Map<String, List<String>> tenantToTableAndViewMap = Maps.newHashMapWithExpectedSize(1);
        tenantToTableAndViewMap.put(null, fullTableOrViewNames);
        // move metadata to multiple regions
        splitSystemCatalog(tenantToTableAndViewMap);
    }

    /**
     * Splits SYSTEM.CATALOG into multiple regions based on the table or view names passed in.
     * Metadata for each table or view is moved to a separate region,
     * @param tenantToTableAndViewMap map from tenant to tables and views owned by the tenant
     */
    protected void splitSystemCatalog(Map<String, List<String>> tenantToTableAndViewMap) throws Exception  {
        List<byte[]> splitPoints = Lists.newArrayListWithExpectedSize(5);
        // add the rows keys of the table or view metadata rows
        Set<String> schemaNameSet=Sets.newHashSetWithExpectedSize(5);
        for (Entry<String, List<String>> entrySet : tenantToTableAndViewMap.entrySet()) {
            for (String fullName : entrySet.getValue()) {
                String schemaName = SchemaUtil.getSchemaNameFromFullName(fullName);
                // we don't allow SYSTEM.CATALOG to split within a schema, so to ensure each table
                // or view is on a separate region they need to have a unique schema name
                assertTrue("Schema names of tables/view must be unique ", schemaNameSet.add(schemaName));
                String tableName = SchemaUtil.getTableNameFromFullName(fullName);
                splitPoints.add(
                    SchemaUtil.getTableKey(entrySet.getKey(), "".equals(schemaName) ? null : schemaName, tableName));
            }
        }
        Collections.sort(splitPoints, Bytes.BYTES_COMPARATOR);
        
        HBaseAdmin admin =
                driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        assertTrue("Needs at least two split points ", splitPoints.size() > 1);
        assertTrue(
            "Number of split points should be less than or equal to the number of region servers ",
            splitPoints.size() <= NUM_SLAVES_BASE);
        HBaseTestingUtility util = getUtility();
        MiniHBaseCluster cluster = util.getHBaseCluster();
        HMaster master = cluster.getMaster();
        AssignmentManager am = master.getAssignmentManager();
        // No need to split on the first splitPoint since the end key of region boundaries are exclusive
        for (int i=1; i<splitPoints.size(); ++i) {
            splitRegion(splitPoints.get(i));
        }
        HashMap<ServerName, List<HRegionInfo>> serverToRegionsList = Maps.newHashMapWithExpectedSize(NUM_SLAVES_BASE);
        Deque<ServerName> availableRegionServers = new ArrayDeque<ServerName>(NUM_SLAVES_BASE);
        for (int i=0; i<NUM_SLAVES_BASE; ++i) {
            availableRegionServers.push(util.getHBaseCluster().getRegionServer(i).getServerName());
        }
        List<HRegionInfo> tableRegions =
                admin.getTableRegions(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME);
        for (HRegionInfo hRegionInfo : tableRegions) {
            // filter on regions we are interested in
            if (regionContainsMetadataRows(hRegionInfo, splitPoints)) {
                ServerName serverName = am.getRegionStates().getRegionServerOfRegion(hRegionInfo);
                if (!serverToRegionsList.containsKey(serverName)) {
                    serverToRegionsList.put(serverName, new ArrayList<HRegionInfo>());
                }
                serverToRegionsList.get(serverName).add(hRegionInfo);
                availableRegionServers.remove(serverName);
                // Scan scan = new Scan();
                // scan.setStartRow(hRegionInfo.getStartKey());
                // scan.setStopRow(hRegionInfo.getEndKey());
                // HTable primaryTable = new HTable(getUtility().getConfiguration(),
                // PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME);
                // ResultScanner resultScanner = primaryTable.getScanner(scan);
                // for (Result result : resultScanner) {
                // System.out.println(result);
                // }
            }
        }
        assertTrue("No region servers available to move regions on to ", !availableRegionServers.isEmpty());
        for (Entry<ServerName, List<HRegionInfo>> entry : serverToRegionsList.entrySet()) {
            List<HRegionInfo> regions = entry.getValue();
            if (regions.size()>1) {
                for (int i=1; i< regions.size(); ++i) {
                    moveRegion(regions.get(i), entry.getKey(), availableRegionServers.pop());
                }
            }
        }
        
        // verify each region of SYSTEM.CATALOG is on its own region server
        tableRegions =
                admin.getTableRegions(PhoenixDatabaseMetaData.SYSTEM_CATALOG_HBASE_TABLE_NAME);
        Set<ServerName> serverNames = Sets.newHashSet();
        for (HRegionInfo hRegionInfo : tableRegions) {
            // filter on regions we are interested in
            if (regionContainsMetadataRows(hRegionInfo, splitPoints)) {
                ServerName serverName = am.getRegionStates().getRegionServerOfRegion(hRegionInfo);
                if (!serverNames.contains(serverName)) {
                    serverNames.add(serverName);
                }
                else {
                    fail("Multiple regions on "+serverName.getServerName());
                }
            }
        }
    }
    
    /**
     * Ensures each region of SYSTEM.CATALOG is on a different region server
     */
    private void moveRegion(HRegionInfo regionInfo, ServerName srcServerName, ServerName dstServerName) throws Exception  {
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TestUtil.TEST_PROPERTIES).getAdmin();
        HBaseTestingUtility util = getUtility();
        MiniHBaseCluster cluster = util.getHBaseCluster();
        HMaster master = cluster.getMaster();
        AssignmentManager am = master.getAssignmentManager();
   
        HRegionServer dstServer = util.getHBaseCluster().getRegionServer(dstServerName);
        HRegionServer srcServer = util.getHBaseCluster().getRegionServer(srcServerName);
        byte[] encodedRegionNameInBytes = regionInfo.getEncodedNameAsBytes();
        admin.move(encodedRegionNameInBytes, Bytes.toBytes(dstServer.getServerName().getServerName()));
        while (dstServer.getOnlineRegion(regionInfo.getRegionName()) == null
                || dstServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameInBytes)
                || srcServer.getRegionsInTransitionInRS().containsKey(encodedRegionNameInBytes)
                || master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
            // wait for the move to be finished
            Thread.sleep(100);
        }
    }

    protected String testUpdatableView(String fullTableName, String fullViewName,
            String fullChildViewName, String childViewDDL, Integer saltBuckets) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        if (saltBuckets != null) {
            if (tableDDLOptions.length() != 0) tableDDLOptions += ",";
            tableDDLOptions += (" SALT_BUCKETS=" + saltBuckets);
        }
        String ddl = "CREATE TABLE " + fullTableName
                + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, k3 DECIMAL, s VARCHAR CONSTRAINT pk PRIMARY KEY (k1, k2, k3))"
                + tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName + " WHERE k1 = 1";
        conn.createStatement().execute(ddl);
        ArrayList<String> splitPoints = Lists.newArrayList(fullTableName, fullViewName);
        if (fullChildViewName!=null) {
            conn.createStatement().execute(childViewDDL);
            splitPoints.add(fullChildViewName);
        }
        
        // move metadata to multiple regions
        splitSystemCatalog(splitPoints);
        
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(" + (i % 4) + "," + (i + 100) + ","
                    + (i > 5 ? 2 : 1) + ")");
        }
        conn.commit();
        ResultSet rs;

        rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + fullTableName);
        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + fullViewName);
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM " + fullViewName);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(101, rs.getInt(2));
        assertEquals(1, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(105, rs.getInt(2));
        assertEquals(1, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(109, rs.getInt(2));
        assertEquals(2, rs.getInt(3));
        assertFalse(rs.next());

        conn.createStatement().execute("UPSERT INTO " + fullViewName + "(k2,S,k3) VALUES(120,'foo',50.0)");
        conn.createStatement().execute("UPSERT INTO " + fullViewName + "(k2,S,k3) VALUES(121,'bar',51.0)");
        conn.commit();
        rs = conn.createStatement().executeQuery("SELECT k1, k2 FROM " + fullViewName + " WHERE k2 >= 120");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(120, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(121, rs.getInt(2));
        assertFalse(rs.next());
        conn.close();
        return fullViewName;
    }

    protected Pair<String, Scan> testUpdatableViewIndex(Integer saltBuckets, String viewName) throws Exception {
        return testUpdatableViewIndex(null, saltBuckets, false, viewName);
    }

    protected Pair<String, Scan> testUpdatableViewIndex(String fullTableName, Integer saltBuckets, boolean localIndex, String viewName)
            throws Exception {
        ResultSet rs;
        Connection conn = DriverManager.getConnection(getUrl());
        String viewIndexName1 = "I_" + generateUniqueName();
        String viewIndexPhysicalName =
                MetaDataUtil.getViewIndexPhysicalName(fullTableName);
        if (localIndex) {
            conn.createStatement().execute("CREATE LOCAL INDEX " + viewIndexName1 + " on " + viewName + "(k3)");
        } else {
            conn.createStatement().execute("CREATE INDEX " + viewIndexName1 + " on " + viewName + "(k3) include (s)");
        }
        conn.createStatement().execute("UPSERT INTO " + viewName + "(k2,S,k3) VALUES(120,'foo',50.0)");
        conn.commit();

        analyzeTable(conn, viewName);
        List<KeyRange> splits = getAllSplits(conn, viewIndexName1);
        // More guideposts with salted, since it's already pre-split at salt buckets
        assertEquals(saltBuckets == null ? 6 : 8, splits.size());

        String query = "SELECT k1, k2, k3, s FROM " + viewName + " WHERE k3 = 51.0";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(121, rs.getInt(2));
        assertTrue(BigDecimal.valueOf(51.0).compareTo(rs.getBigDecimal(3)) == 0);
        assertEquals("bar", rs.getString(4));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        String queryPlan = QueryUtil.getExplainPlan(rs);
        if (localIndex) {
            assertEquals("CLIENT PARALLEL " + (saltBuckets == null ? 1 : saltBuckets) + "-WAY RANGE SCAN OVER "
                    + fullTableName + " [1,51]\n" + "    SERVER FILTER BY FIRST KEY ONLY\n" + "CLIENT MERGE SORT",
                    queryPlan);
        } else {
            assertEquals(saltBuckets == null
                    ? "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + viewIndexPhysicalName + " [" + Short.MIN_VALUE + ",51]"
                    : "CLIENT PARALLEL " + saltBuckets + "-WAY RANGE SCAN OVER " + viewIndexPhysicalName + " [0,"
                            + Short.MIN_VALUE + ",51] - [" + (saltBuckets.intValue() - 1) + "," + Short.MIN_VALUE
                            + ",51]\nCLIENT MERGE SORT",
                    queryPlan);
        }

        String viewIndexName2 = "I_" + generateUniqueName();
        if (localIndex) {
            conn.createStatement().execute("CREATE LOCAL INDEX " + viewIndexName2 + " on " + viewName + "(s)");
        } else {
            conn.createStatement().execute("CREATE INDEX " + viewIndexName2 + " on " + viewName + "(s)");
        }

        // new index hasn't been analyzed yet
        splits = getAllSplits(conn, viewIndexName2);
        assertEquals(saltBuckets == null ? 1 : 3, splits.size());

        // analyze table should analyze all view data
        analyzeTable(conn, fullTableName);
        splits = getAllSplits(conn, viewIndexName2);
        assertEquals(saltBuckets == null ? 6 : 8, splits.size());

        query = "SELECT k1, k2, s FROM " + viewName + " WHERE s = 'foo'";
        Statement statement = conn.createStatement();
        rs = statement.executeQuery(query);
        Scan scan = statement.unwrap(PhoenixStatement.class).getQueryPlan().getContext().getScan();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(120, rs.getInt(2));
        assertEquals("foo", rs.getString(3));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        String physicalTableName;
        if (localIndex) {
            physicalTableName = fullTableName;
            assertEquals("CLIENT PARALLEL " + (saltBuckets == null ? 1 : saltBuckets) + "-WAY RANGE SCAN OVER "
                    + fullTableName + " [" + (2) + ",'foo']\n" + "    SERVER FILTER BY FIRST KEY ONLY\n"
                    + "CLIENT MERGE SORT", QueryUtil.getExplainPlan(rs));
        } else {
            physicalTableName = viewIndexPhysicalName;
            assertEquals(
                    saltBuckets == null
                            ? "CLIENT PARALLEL 1-WAY RANGE SCAN OVER " + viewIndexPhysicalName + " ["
                                    + (Short.MIN_VALUE + 1) + ",'foo']\n" + "    SERVER FILTER BY FIRST KEY ONLY"
                            : "CLIENT PARALLEL " + saltBuckets + "-WAY RANGE SCAN OVER " + viewIndexPhysicalName
                                    + " [0," + (Short.MIN_VALUE + 1) + ",'foo'] - [" + (saltBuckets.intValue() - 1)
                                    + "," + (Short.MIN_VALUE + 1) + ",'foo']\n"
                                    + "    SERVER FILTER BY FIRST KEY ONLY\n" + "CLIENT MERGE SORT",
                    QueryUtil.getExplainPlan(rs));
        }
        conn.close();
        return new Pair<>(physicalTableName, scan);
    }
}
