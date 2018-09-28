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
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class VerifyReplicationToolIT extends BaseUniqueNamesOwnClusterIT {
    private static final Logger LOG = LoggerFactory.getLogger(VerifyReplicationToolIT.class);
    private static final String CREATE_USER_TABLE = "CREATE TABLE IF NOT EXISTS %s ( " +
            " TENANT_ID VARCHAR NOT NULL, USER_ID VARCHAR NOT NULL, AGE INTEGER " +
            " CONSTRAINT pk PRIMARY KEY ( TENANT_ID, USER_ID ))";
    private static final String UPSERT_USER = "UPSERT INTO %s VALUES (?, ?, ?)";
    private static final String SELECT_USER =
            "SELECT * FROM %s WHERE TENANT_ID = ? LIMIT %d";
    private static final Random RANDOM = new Random();
    private static final int NUM_USERS = 10;
    private static final int NUM_TENANTS = 2;

    private static int tenantNum = 0;
    private static int userNum = 0;
    /** source table with 4 regions*/
    private static String sourceTableName;
    /** target table with 2 regions*/
    private static String targetTableName;
    private List<String> sourceTenants;
    private String sourceOnlyTenant;
    private String sourceAndTargetTenant;
    private String targetOnlyTenant;
    private PreparedStatement sourceStmt;
    private PreparedStatement targetStmt;
    private List<byte[]> sourceSplitPoints;
    private List<byte[]> targetSplitPoints;

    @BeforeClass
    public static void createTables() throws Exception {
        NUM_SLAVES_BASE = 4;
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
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
        sourceTenants = new ArrayList<>(NUM_TENANTS);
        sourceTenants.add("tenant" + tenantNum++);
        sourceTenants.add("tenant" + tenantNum++);
        sourceOnlyTenant = sourceTenants.get(0);
        sourceAndTargetTenant = sourceTenants.get(1);
        targetOnlyTenant = "tenant" + tenantNum++;
        /* upsert data and spilt table into multiple regions
        with each relevant region based on splitpoints
        residing on a different RS*/
        upsertData();
        /* splitting source table into 4 regions */
        splitSource(sourceTableName);
        /* splitting target table into 2 regions */
        splitTarget(targetTableName);
    }

    @Test
    public void testVerifyRowsMatch() throws Exception {
        verify(String.format("TENANT_ID = '%s'", sourceAndTargetTenant), 0, 10, 0, 0, 0, 0);
    }

    @Test
    public void testVerifySourceOnly() throws Exception {
        verify(String.format("TENANT_ID = '%s'", sourceOnlyTenant), 0, 0, 10, 10, 0, 0);
    }

    @Test
    public void testVerifyRowsDifferent() throws Exception {
        // change three rows on the source table so they no longer match on the target
        upsertSelectData(sourceTableName, sourceAndTargetTenant, -1, 3);
        verify(String.format("TENANT_ID = '%s'", sourceAndTargetTenant), 0, 7, 3, 0, 0, 3);
    }

    @Test
    public void testVerifyRowsCountMismatch() throws Exception {
        deleteData(targetTableName, sourceAndTargetTenant, 1);
        verify(String.format("TENANT_ID = '%s'", sourceAndTargetTenant), 0, 9, 1, 1, 0, 0);

    }

    @Test
    public void testVerifyAllRows() throws Exception {
        verify(String.format("TENANT_ID = '%s' OR TENANT_ID = '%s' OR TENANT_ID = '%s'",
                sourceOnlyTenant,sourceAndTargetTenant,targetOnlyTenant), 0, 10, 20, 10, 10, 0);
    }

    @Test
    public void testVerifyRowsMatchWithScn() throws Exception {
        long timestamp = EnvironmentEdgeManager.currentTimeMillis();
        upsertSelectData(sourceTableName, sourceAndTargetTenant, -1, 3);
        verify(String.format("TENANT_ID = '%s'", sourceAndTargetTenant), timestamp, 10, 0, 0, 0, 0);

        upsertSelectData(targetTableName, sourceAndTargetTenant, -2, 3);
        verify(String.format("TENANT_ID = '%s'", sourceAndTargetTenant), timestamp, 10, 0, 0, 0, 0);

    }

    private void splitSource(String tableName) throws Exception {
        TableName table = TableName.valueOf(tableName);
        splitTable(table, sourceSplitPoints);
    }

    private void splitTarget(String tableName) throws Exception {
        TableName table = TableName.valueOf(tableName);
        splitTable(table, targetSplitPoints);
    }

    private void verify(String sqlConditions, long ts, int good, int bad, int onlyInSource, int onlyInTarget,
            int contentDifferent) throws Exception {
        VerifyReplicationTool.Builder builder =
                VerifyReplicationTool.newBuilder(getUtility().getConfiguration());
        builder.tableName(sourceTableName);
        builder.targetTableName(targetTableName);
        builder.sqlConditions(sqlConditions);
        if(ts != 0) {
            builder.timestamp(ts);
        } else {
            builder.timestamp(EnvironmentEdgeManager.currentTimeMillis());
        }
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
        sourceStmt = conn.prepareStatement(String.format(UPSERT_USER, sourceTableName));
        targetStmt = conn.prepareStatement(String.format(UPSERT_USER, targetTableName));
        sourceSplitPoints = Lists.newArrayListWithExpectedSize(4);
        targetSplitPoints = Lists.newArrayListWithExpectedSize(2);

        // 1. add 1 tenant with NUM_USERS to source table
        upsertData(sourceOnlyTenant);
        // 2. add 1 matching tenant with NUM_USERS to both source and target table
        upsertData(sourceAndTargetTenant);
        // 3. add 1 tenant with NUM_USERS users to the target table
        upsertData(targetOnlyTenant);

        conn.commit();
        assertEquals(NUM_USERS, countUsers(sourceTableName, sourceOnlyTenant));
        assertEquals(NUM_USERS, countUsers(sourceTableName, sourceAndTargetTenant));
        assertEquals(0, countUsers(sourceTableName, targetOnlyTenant));
        assertEquals(NUM_USERS, countUsers(targetTableName, sourceAndTargetTenant));
        assertEquals(NUM_USERS, countUsers(targetTableName, targetOnlyTenant));
        assertEquals(0, countUsers(targetTableName, sourceOnlyTenant));
    }

    private void upsertData(String tenantId)
            throws SQLException {
        for (int u = 1; u <= NUM_USERS; u++) {
            String userId = "user" + userNum++;
            int age = RANDOM.nextInt(99) + 1;
            if(tenantId.equals(sourceOnlyTenant) || tenantId.equals(sourceAndTargetTenant)) {
                upsertData(sourceStmt, tenantId, userId, age);
                if (u == 1 || u == NUM_USERS/2) {
                    sourceSplitPoints.add(ByteUtil.concat(Bytes.toBytes(tenantId), QueryConstants.SEPARATOR_BYTE_ARRAY,
                            Bytes.toBytes(userId)));
                }
            }
            if (tenantId.equals(sourceAndTargetTenant) || tenantId.equals(targetOnlyTenant)) {
                upsertData(targetStmt, tenantId, userId, age);
                if (u == 1) {
                    targetSplitPoints.add(ByteUtil.concat(Bytes.toBytes(tenantId), QueryConstants.SEPARATOR_BYTE_ARRAY,
                            Bytes.toBytes(userId)));
                }
            }
        }
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
        // UPSERT SELECT doesn't work after region splitting PHOENIX-4849,
        // therefore did a select upsert
        Connection conn = DriverManager.getConnection(getUrl());
        String sql1 = String.format(SELECT_USER, tableName, limit);
        PreparedStatement ps1 = conn.prepareStatement(sql1);
        ps1.setString(1, tenantId);
        ResultSet rs = ps1.executeQuery();
        String sql2 = String.format(UPSERT_USER, tableName);
        while(rs.next()) {
            PreparedStatement ps2 = conn.prepareStatement(sql2);
            upsertData(ps2, rs.getString(1), rs.getString(2), age);
        }
        conn.commit();
        assertEquals(10, countUsers(tableName, tenantId));
    }

    private void deleteData(String tableName, String tenantId, int limit) throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        // delete common rows from the target
        PreparedStatement deleteTargetStmt =
                conn.prepareStatement(String.format("DELETE FROM %s WHERE TENANT_ID = '%s' LIMIT %d",
                        tableName, tenantId, limit));
        deleteTargetStmt.execute();
        conn.commit();
        assertEquals(NUM_USERS - limit, countUsers(tableName, tenantId));
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
}
