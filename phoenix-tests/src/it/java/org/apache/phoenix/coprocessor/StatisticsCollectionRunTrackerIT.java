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
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.schema.stats.StatisticsCollectionRunTracker.COMPACTION_UPDATE_STATS_ROW_COUNT;
import static org.apache.phoenix.schema.stats.StatisticsCollectionRunTracker.CONCURRENT_UPDATE_STATS_ROW_COUNT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.ParallelStatsEnabledIT;
import org.apache.phoenix.end2end.ParallelStatsEnabledTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.stats.StatisticsCollectionRunTracker;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsEnabledTest.class)
public class StatisticsCollectionRunTrackerIT extends ParallelStatsEnabledIT {
    private static final StatisticsCollectionRunTracker tracker = StatisticsCollectionRunTracker
            .getInstance(new Configuration());

    private String fullTableName;

    @Before
    public void generateTableNames() {
        String schemaName = TestUtil.DEFAULT_SCHEMA_NAME;
        String tableName = "T_" + generateUniqueName();
        fullTableName = SchemaUtil.getTableName(schemaName, tableName);
    }

    @Test
    public void testStateBeforeAndAfterUpdateStatsCommand() throws Exception {
        String tableName = fullTableName;
        RegionInfo regionInfo = createTableAndGetRegion(tableName);
        StatisticsCollectionRunTracker tracker =
                StatisticsCollectionRunTracker.getInstance(new Configuration());
        // assert that the region wasn't added to the tracker
        assertTrue(tracker.addUpdateStatsCommandRegion(regionInfo, new HashSet<byte[]>(Arrays.asList(Bytes.toBytes("0")))));
        assertTrue(tracker.addUpdateStatsCommandRegion(regionInfo, new HashSet<byte[]>(Arrays.asList(Bytes.toBytes("L#0")))));
        // assert that removing the region from the tracker works
        assertTrue(tracker.removeUpdateStatsCommandRegion(regionInfo, new HashSet<byte[]>(Arrays.asList(Bytes.toBytes("0")))));
        assertTrue(tracker.removeUpdateStatsCommandRegion(regionInfo, new HashSet<byte[]>(Arrays.asList(Bytes.toBytes("L#0")))));
        runUpdateStats(tableName);
        // assert that after update stats is complete, tracker isn't tracking the region any more
        assertFalse(tracker.removeUpdateStatsCommandRegion(regionInfo, new HashSet<byte[]>(Arrays.asList(Bytes.toBytes("0")))));
        assertFalse(tracker.removeUpdateStatsCommandRegion(regionInfo, new HashSet<byte[]>(Arrays.asList(Bytes.toBytes("L#0")))));;
    }
    
    @Test
    public void testStateBeforeAndAfterMajorCompaction() throws Exception {
        String tableName = fullTableName;
        RegionInfo regionInfo = createTableAndGetRegion(tableName);
        StatisticsCollectionRunTracker tracker =
                StatisticsCollectionRunTracker.getInstance(new Configuration());
        // Upsert values in the table.
        String keyPrefix = "aaaaaaaaaaaaaaaaaaaa";
        String upsert = "UPSERT INTO " + tableName + " VALUES (?, ?)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            PreparedStatement stmt = conn.prepareStatement(upsert);
            for (int i = 0; i < 1000; i++) {
                stmt.setString(1, keyPrefix + i);
                stmt.setString(2, "KV" + i);
                stmt.executeUpdate();
            }
            conn.commit();
        }
        // assert that the region wasn't added to the tracker
        assertTrue(tracker.addCompactingRegion(regionInfo));
        // assert that removing the region from the tracker works
        assertTrue(tracker.removeCompactingRegion(regionInfo));
        runMajorCompaction(tableName);
        
        // assert that after major compaction is complete, tracker isn't tracking the region any more
        assertFalse(tracker.removeCompactingRegion(regionInfo));
    }
    
    @Test
    public void testMajorCompactionPreventsUpdateStatsFromRunning() throws Exception {
        String tableName = fullTableName;
        RegionInfo regionInfo = createTableAndGetRegion(tableName);
        // simulate stats collection via major compaction by marking the region as compacting in the tracker
        markRegionAsCompacting(regionInfo);
        //there will be no update for local index and a table , so checking 2 * COMPACTION_UPDATE_STATS_ROW_COUNT
        Assert.assertEquals("Row count didn't match", COMPACTION_UPDATE_STATS_ROW_COUNT * 2, runUpdateStats(tableName));
        StatisticsCollectionRunTracker tracker =
                StatisticsCollectionRunTracker.getInstance(new Configuration());
        // assert that the tracker state was cleared.
        HashSet<byte[]> familyMap = new HashSet<byte[]>(Arrays.asList(Bytes.toBytes("0")));
        assertFalse(tracker.removeUpdateStatsCommandRegion(regionInfo,familyMap));
    }
    
    @Test
    public void testUpdateStatsPreventsAnotherUpdateStatsFromRunning() throws Exception {
        String tableName = fullTableName;
        RegionInfo regionInfo = createTableAndGetRegion(tableName);
        HashSet<byte[]> familyMap = new HashSet<byte[]>(Arrays.asList(Bytes.toBytes("0")));
        markRunningUpdateStats(regionInfo,familyMap);
        //there will be no update for a table but local index should succeed, so checking 2 * COMPACTION_UPDATE_STATS_ROW_COUNT
        assertTrue("Local index stats are not updated!", CONCURRENT_UPDATE_STATS_ROW_COUNT < runUpdateStats(tableName));
        
        // assert that running the concurrent and race-losing update stats didn't clear the region
        // from the tracker. If the method returned true it means the tracker was still tracking
        // the region. Slightly counter-intuitive, yes.
        assertTrue(tracker.removeUpdateStatsCommandRegion(regionInfo,familyMap));
    }
    
    private void markRegionAsCompacting(RegionInfo regionInfo) {
        StatisticsCollectionRunTracker tracker =
                StatisticsCollectionRunTracker.getInstance(new Configuration());
        tracker.addCompactingRegion(regionInfo);
    }

    private void markRunningUpdateStats(RegionInfo regionInfo, HashSet<byte[]> familyMap) {
        StatisticsCollectionRunTracker tracker =
                StatisticsCollectionRunTracker.getInstance(new Configuration());
        tracker.addUpdateStatsCommandRegion(regionInfo,familyMap);
    }

    private RegionInfo createTableAndGetRegion(String tableName) throws Exception {
        TableName tn = TableName.valueOf(tableName);
        String ddl = "CREATE TABLE " + tableName + " (PK1 VARCHAR PRIMARY KEY, KV1 VARCHAR)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("CREATE LOCAL INDEX " + generateUniqueName() + " ON " + tableName + "(KV1)");
            PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
            try (Admin admin = phxConn.getQueryServices().getAdmin()) {
                List<RegionInfo> tableRegions = admin.getRegions(tn);
                return tableRegions.get(0);
            }
        }
    }
    
    private long runUpdateStats(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            return conn.createStatement().executeUpdate("UPDATE STATISTICS " + tableName);
        }
    }
    
    private void runMajorCompaction(String tableName) throws Exception {
        try (PhoenixConnection conn = DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class)) {
            try (Admin admin = conn.getQueryServices().getAdmin()) {
                TableName t = TableName.valueOf(tableName);
                admin.flush(t);
                admin.majorCompact(t);
                Thread.sleep(10000);
            }
        }
    }
}
