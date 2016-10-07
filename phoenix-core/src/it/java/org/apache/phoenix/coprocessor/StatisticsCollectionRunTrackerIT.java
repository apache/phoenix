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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.end2end.ParallelStatsEnabledIT;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.stats.StatisticsCollectionRunTracker;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        HRegionInfo regionInfo = createTableAndGetRegion(tableName);
        StatisticsCollectionRunTracker tracker =
                StatisticsCollectionRunTracker.getInstance(new Configuration());
        // assert that the region wasn't added to the tracker
        assertTrue(tracker.addUpdateStatsCommandRegion(regionInfo));
        // assert that removing the region from the tracker works
        assertTrue(tracker.removeUpdateStatsCommandRegion(regionInfo));
        runUpdateStats(tableName);
        // assert that after update stats is complete, tracker isn't tracking the region any more
        assertFalse(tracker.removeUpdateStatsCommandRegion(regionInfo));
    }
    
    @Test
    public void testStateBeforeAndAfterMajorCompaction() throws Exception {
        String tableName = fullTableName;
        HRegionInfo regionInfo = createTableAndGetRegion(tableName);
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
        HRegionInfo regionInfo = createTableAndGetRegion(tableName);
        // simulate stats collection via major compaction by marking the region as compacting in the tracker
        markRegionAsCompacting(regionInfo);
        Assert.assertEquals("Row count didn't match", COMPACTION_UPDATE_STATS_ROW_COUNT, runUpdateStats(tableName));
        StatisticsCollectionRunTracker tracker =
                StatisticsCollectionRunTracker.getInstance(new Configuration());
        // assert that the tracker state was cleared.
        assertFalse(tracker.removeUpdateStatsCommandRegion(regionInfo));
    }
    
    @Test
    public void testUpdateStatsPreventsAnotherUpdateStatsFromRunning() throws Exception {
        String tableName = fullTableName;
        HRegionInfo regionInfo = createTableAndGetRegion(tableName);
        markRunningUpdateStats(regionInfo);
        Assert.assertEquals("Row count didn't match", CONCURRENT_UPDATE_STATS_ROW_COUNT,
            runUpdateStats(tableName));
        
        // assert that running the concurrent and race-losing update stats didn't clear the region
        // from the tracker. If the method returned true it means the tracker was still tracking
        // the region. Slightly counter-intuitive, yes.
        assertTrue(tracker.removeUpdateStatsCommandRegion(regionInfo));
    }
    
    private void markRegionAsCompacting(HRegionInfo regionInfo) {
        StatisticsCollectionRunTracker tracker =
                StatisticsCollectionRunTracker.getInstance(new Configuration());
        tracker.addCompactingRegion(regionInfo);
    }

    private void markRunningUpdateStats(HRegionInfo regionInfo) {
        StatisticsCollectionRunTracker tracker =
                StatisticsCollectionRunTracker.getInstance(new Configuration());
        tracker.addUpdateStatsCommandRegion(regionInfo);
    }

    private HRegionInfo createTableAndGetRegion(String tableName) throws Exception {
        byte[] tableNameBytes = Bytes.toBytes(tableName);
        String ddl = "CREATE TABLE " + tableName + " (PK1 VARCHAR PRIMARY KEY, KV1 VARCHAR)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
            try (HBaseAdmin admin = phxConn.getQueryServices().getAdmin()) {
                List<HRegionInfo> tableRegions = admin.getTableRegions(tableNameBytes);
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
            try (HBaseAdmin admin = conn.getQueryServices().getAdmin()) {
                TableName t = TableName.valueOf(tableName);
                admin.flush(t);
                admin.majorCompact(t);
                Thread.sleep(10000);
            }
        }
    }
}
