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
package org.apache.phoenix.mapreduce;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.end2end.BaseClientManagedTimeIT.getDefaultProps;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class VerifyReplicationToolIT extends BaseUniqueNamesOwnClusterIT {
    private static final Logger LOG = LoggerFactory.getLogger(VerifyReplicationToolIT.class);
    private static final String CREATE_USER_TABLE = "CREATE TABLE IF NOT EXISTS %s ( " +
            " TENANT_ID VARCHAR NOT NULL, USER_ID VARCHAR NOT NULL, AGE INTEGER " +
            " CONSTRAINT pk PRIMARY KEY ( TENANT_ID, USER_ID ))";
    private static final String UPSERT_USER = "UPSERT INTO %s VALUES (?, ?, ?)";
    private static final String UPSERT_SELECT_USERS =
            "UPSERT INTO %s SELECT TENANT_ID, USER_ID, %d FROM %s WHERE TENANT_ID = ? LIMIT %d";
    private static final Random RANDOM = new Random();

    private static int tenantNum = 0;
    private static int userNum = 0;
    private static String sourceTableName;
    private static String targetTableName;
    private List<String> sourceTenants;
    private String sourceOnlyTenant;
    private String sourceAndTargetTenant;
    private String targetOnlyTenant;

    @BeforeClass
    public static void createTables() throws Exception {
        NUM_SLAVES_BASE = 2;
        Map<String,String> props = getDefaultProps();
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        Connection conn = DriverManager.getConnection(getUrl());
        sourceTableName = generateUniqueName();
        targetTableName = generateUniqueName();
        // tables will have the same schema, but a different number of regions
        conn.createStatement().execute(String.format(CREATE_USER_TABLE, sourceTableName));
        conn.createStatement().execute(String.format(CREATE_USER_TABLE, targetTableName));
        conn.commit();
    }

    @Before
    public void setupTenants() throws Exception {
        sourceTenants = new ArrayList<>(2);
        sourceTenants.add("tenant" + tenantNum++);
        sourceTenants.add("tenant" + tenantNum++);
        sourceOnlyTenant = sourceTenants.get(0);
        sourceAndTargetTenant = sourceTenants.get(1);
        targetOnlyTenant = "tenant" + tenantNum++;
        upsertData();
        split(sourceTableName, 4);
        split(targetTableName, 2);
        // ensure scans for each table touch multiple region servers
        ensureRegionsOnDifferentServers(sourceTableName);
        ensureRegionsOnDifferentServers(targetTableName);
    }

    @Test
    public void testVerifyRowsMatch() throws Exception {
        verify(String.format("TENANT_ID = '%s'", sourceAndTargetTenant), 10, 0, 0, 0, 0);
    }

    @Test
    public void testVerifySourceOnly() throws Exception {
        verify(String.format("TENANT_ID = '%s'", sourceOnlyTenant), 0, 10, 10, 0, 0);
    }

    @Test
    public void testVerifyTargetOnly() throws Exception {
        verify(String.format("TENANT_ID = '%s'", targetOnlyTenant), 0, 10, 0, 10, 0);
    }

    @Test
    public void testVerifyRowsDifferent() throws Exception {
        // change three rows on the source table so they no longer match on the target
        upsertSelectData(sourceTableName, sourceAndTargetTenant, -1, 3);
        verify(String.format("TENANT_ID = '%s'", sourceAndTargetTenant), 7, 3, 0, 0, 3);
    }

    private void verify(String sqlConditions, int good, int bad, int onlyInSource, int onlyInTarget,
            int contentDifferent) throws Exception {
        VerifyReplicationTool.Builder builder =
                VerifyReplicationTool.newBuilder(getUtility().getConfiguration());
        builder.tableName(sourceTableName);
        builder.targetTableName(targetTableName);
        builder.sqlConditions(sqlConditions);
        VerifyReplicationTool tool = builder.build();
        Job job = tool.createSubmittableJob();
        // use the local runner and cleanup previous output
        job.getConfiguration().set("mapreduce.framework.name", "local");
        cleanupPreviousJobOutput(job);
        Assert.assertTrue("Job should have completed", job.waitForCompletion(true));
        Counters counters = job.getCounters();
        LOG.info(counters.toString());
        assertEquals(good,
                counters.findCounter(VerifyReplicationTool.Verifier.Counter.GOODROWS).getValue());
        assertEquals(bad,
                counters.findCounter(VerifyReplicationTool.Verifier.Counter.BADROWS).getValue());
        assertEquals(onlyInSource, counters.findCounter(
                VerifyReplicationTool.Verifier.Counter.ONLY_IN_SOURCE_TABLE_ROWS).getValue());
        assertEquals(onlyInTarget, counters.findCounter(
                VerifyReplicationTool.Verifier.Counter.ONLY_IN_TARGET_TABLE_ROWS).getValue());
        assertEquals(contentDifferent, counters.findCounter(
                VerifyReplicationTool.Verifier.Counter.CONTENT_DIFFERENT_ROWS).getValue());
    }

    private void upsertData() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        PreparedStatement sourceStmt =
                conn.prepareStatement(String.format(UPSERT_USER, sourceTableName));
        PreparedStatement targetStmt =
                conn.prepareStatement(String.format(UPSERT_USER, targetTableName));
        // create 2 tenants with 10 users each in source table
        for (int t = 0; t < 2; t++) {
            for (int u = 1; u <= 10; u++) {
                String tenantId = sourceTenants.get(t);
                String userId = "user" + userNum++;
                int age = RANDOM.nextInt(99) + 1;
                upsertData(sourceStmt, tenantId, userId, age);
                // add matching users for the shared tenant to the target table
                if (tenantId.equals(sourceAndTargetTenant)) {
                    upsertData(targetStmt, tenantId, userId, age);
                }
            }
        }
        // add 1 tenant with 10 users to the target table
        for (int u = 1; u <= 10; u++) {
            upsertData(targetStmt, targetOnlyTenant);
        }
        conn.commit();
        assertEquals(10, countUsers(sourceTableName, sourceTenants.get(0)));
        assertEquals(10, countUsers(sourceTableName, sourceTenants.get(1)));
        assertEquals(0, countUsers(sourceTableName, targetOnlyTenant));
        assertEquals(10, countUsers(targetTableName, sourceAndTargetTenant));
        assertEquals(10, countUsers(targetTableName, targetOnlyTenant));
        assertEquals(0, countUsers(targetTableName, sourceOnlyTenant));
    }

    private void upsertData(PreparedStatement stmt, String tenantId) throws SQLException {
        String userId = "user" + userNum++;
        int age = RANDOM.nextInt(99) + 1;
        upsertData(stmt, tenantId, userId, age);
    }

    private void upsertData(PreparedStatement stmt, String tenantId, String userId, int age)
            throws SQLException {
        int i = 1;
        stmt.setString(i++, tenantId);
        stmt.setString(i++, userId);
        stmt.setInt(i++, age);
        stmt.execute();
    }

    private void upsertSelectData(String tableName, String tenantId, int age, int limit)
            throws SQLException {
        String sql = String.format(UPSERT_SELECT_USERS, tableName, age, tableName, limit);
        Connection conn = DriverManager.getConnection(getUrl());
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, tenantId);
        ps.execute();
        conn.commit();
        assertEquals(10, countUsers(tableName, tenantId));
    }

    private int countUsers(String tableName, String tenantId) throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        PreparedStatement ps =
                conn.prepareStatement(
                        String.format("SELECT COUNT(*) FROM %s WHERE TENANT_ID = ?", tableName));
        ps.setString(1, tenantId);
        ResultSet rs = ps.executeQuery();
        rs.next();
        return rs.getInt(1);
    }

    private void cleanupPreviousJobOutput(Job job) throws IOException {
        Path outputDir =
            new Path(job.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir"));
        FileSystem fs = FileSystem.get(getUtility().getConfiguration());
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }
    }

    private void split(String tableName, int targetRegions) throws Exception {
        TableName table = TableName.valueOf(tableName);
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES).getAdmin();
        MiniHBaseCluster cluster = getUtility().getHBaseCluster();
        HMaster master = cluster.getMaster();
        List<HRegionInfo> regions = admin.getTableRegions(table);
        int numRegions = regions.size();
        // split the last region in the table until we have the target number of regions
        while (numRegions < targetRegions) {
            HRegionInfo region = regions.get(numRegions - 1);
            ServerName serverName =
                    master.getAssignmentManager().getRegionStates()
                            .getRegionServerOfRegion(region);
            byte[] splitPoint =
                    Arrays.copyOf(region.getStartKey(), region.getStartKey().length + 1);
            admin.split(serverName, region, splitPoint);
            int retries = 0;
            int expectedRegions = numRegions + 1;
            int actualRegions = numRegions;
            while ((actualRegions != expectedRegions) && (retries < 5)) {
                Thread.sleep(1000L);
                regions = admin.getTableRegions(table);
                actualRegions = regions.size();
                retries++;
            }
            assertEquals("New region not created", actualRegions, expectedRegions);
            numRegions = actualRegions;
        }
    }

    private void ensureRegionsOnDifferentServers(String tableName) throws Exception {
        TableName table = TableName.valueOf(tableName);
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), TEST_PROPERTIES).getAdmin();
        MiniHBaseCluster cluster = getUtility().getHBaseCluster();
        HMaster master = cluster.getMaster();
        HRegionServer server1 = cluster.getRegionServer(0);
        HRegionServer server2 = cluster.getRegionServer(1);
        List<HRegionInfo> regions = admin.getTableRegions(table);
        HRegionInfo region1 = regions.get(0);
        HRegionInfo region2 = regions.get(1);
        ServerName serverName1 =
                master.getAssignmentManager().getRegionStates().getRegionServerOfRegion(region1);
        ServerName serverName2 =
                master.getAssignmentManager().getRegionStates().getRegionServerOfRegion(region2);

        // if regions are on same region server, move one region
        if (serverName1.equals(serverName2)) {
            HRegionServer dstServer = null;
            HRegionServer srcServer = null;
            if (server1.getServerName().equals(serverName2)) {
                dstServer = server2;
                srcServer = server1;
            } else {
                dstServer = server1;
                srcServer = server2;
            }
            byte[] encodedRegionNameInBytes = region2.getEncodedNameAsBytes();
            admin.move(encodedRegionNameInBytes,
                    Bytes.toBytes(dstServer.getServerName().getServerName()));
            while (dstServer.getOnlineRegion(region2.getRegionName()) == null || dstServer
                    .getRegionsInTransitionInRS().containsKey(encodedRegionNameInBytes) || srcServer
                    .getRegionsInTransitionInRS().containsKey(encodedRegionNameInBytes) || master
                    .getAssignmentManager().getRegionStates().isRegionsInTransition()) {
                // wait for the move to finish
                Thread.sleep(1);
            }
        }

        region1 = admin.getTableRegions(table).get(0);
        serverName1 =
                master.getAssignmentManager().getRegionStates().getRegionServerOfRegion(region1);
        region2 = admin.getTableRegions(table).get(1);
        serverName2 =
                master.getAssignmentManager().getRegionStates().getRegionServerOfRegion(region2);

        // verify regions are on different servers
        assertNotEquals(
                "Region " + region1 + " and " + region2 + " should be on different region servers",
                serverName1, serverName2);
    }
}
