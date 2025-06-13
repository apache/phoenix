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

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixCoprocessorSourceFactory;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixMasterSource;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;
import static org.apache.phoenix.util.CDCUtil.CDC_STREAM_NAME_FORMAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class CDCStream2IT extends CDCBaseIT {

    private static final MetricsPhoenixMasterSource METRICS_SOURCE =
            MetricsPhoenixCoprocessorSourceFactory.getInstance().getPhoenixMasterSource();

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY,
                Integer.toString(60 * 60)); // An hour
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
        props.put("hbase.coprocessor.master.classes", TestPhoenixMasterObserver.class.getName());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testPartitionMetadataWithRetries() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName);

        assertEquals("Post split partition update failures should be 0 initially",
                0, METRICS_SOURCE.getPostSplitPartitionUpdateFailureCount());
        assertEquals("Post merge partition update failures should be 0 initially",
                0, METRICS_SOURCE.getPostMergePartitionUpdateFailureCount());

        // Perform a split operation - this will fail 15 times before succeeding
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("m"));

        // Verify split metric is 15
        assertEquals("Post split partition update failures should be 15 after retries",
                15, METRICS_SOURCE.getPostSplitPartitionUpdateFailureCount());

        List<HRegionLocation> regions = TestUtil.getAllTableRegions(conn, tableName);

        // Perform a merge operation - this will fail 18 times before succeeding
        TestUtil.mergeTableRegions(conn, tableName, regions.stream()
                .map(HRegionLocation::getRegion)
                .map(RegionInfo::getEncodedName)
                .collect(Collectors.toList()));

        // Verify merge metric is 18
        assertEquals("Post merge partition update failures should be 15 after retries",
                18, METRICS_SOURCE.getPostMergePartitionUpdateFailureCount());

        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + tableName + "'");
        List<PartitionMetadata> mergedDaughter = new ArrayList<>();
        List<PartitionMetadata> splitParents = new ArrayList<>();
        while (rs.next()) {
            PartitionMetadata pm = new PartitionMetadata(rs);
            if (pm.startKey == null && pm.endKey == null && pm.endTime == 0) {
                mergedDaughter.add(pm);
            }
            if (pm.startKey != null || pm.endKey != null) {
                splitParents.add(pm);
            }
        }
        assertEquals(2, mergedDaughter.size());
        assertEquals(2, splitParents.size());
        assertEquals(mergedDaughter.get(0).startTime, mergedDaughter.get(1).startTime);
        assertEquals(mergedDaughter.get(0).endTime, mergedDaughter.get(1).endTime);
        assertEquals(mergedDaughter.get(0).partitionId, mergedDaughter.get(1).partitionId);
        assertTrue(mergedDaughter.stream().anyMatch(
                d -> Objects.equals(d.parentPartitionId, splitParents.get(0).partitionId)));
        assertTrue(mergedDaughter.stream().anyMatch(
                d -> Objects.equals(d.parentPartitionId, splitParents.get(1).partitionId)));
        for (PartitionMetadata splitDaughter : splitParents) {
            Assert.assertEquals(mergedDaughter.get(0).startTime, splitDaughter.endTime);
        }
    }

    private void createTableAndEnableCDC(Connection conn, String tableName) throws Exception {
        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k VARCHAR PRIMARY KEY," + " v1 INTEGER,"
                        + " v2 VARCHAR)");
        createCDC(conn, cdc_sql, null);
        String streamName = getStreamName(conn, tableName, cdcName);
        while (!CDCUtil.CdcStreamStatus.ENABLED.toString()
                .equals(getStreamStatus(conn, tableName, streamName))) {
            Thread.sleep(1000);
        }
    }

    private String getStreamName(Connection conn, String tableName, String cdcName)
            throws Exception {
        return String.format(CDC_STREAM_NAME_FORMAT, tableName, cdcName,
                CDCUtil.getCDCCreationTimestamp(
                        conn.unwrap(PhoenixConnection.class).getTableNoCache(tableName)));
    }

    private String getStreamStatus(Connection conn, String tableName, String streamName)
            throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("SELECT STREAM_STATUS FROM "
                + SYSTEM_CDC_STREAM_STATUS_NAME + " WHERE TABLE_NAME='" + tableName +
                "' AND STREAM_NAME='" + streamName + "'");
        assertTrue(rs.next());
        return rs.getString(1);
    }

    /**
     * Partition Metadata class.
     */
    private class PartitionMetadata {
        public String partitionId;
        public String parentPartitionId;
        public Long startTime;
        public Long endTime;
        public byte[] startKey;
        public byte[] endKey;

        public PartitionMetadata(ResultSet rs) throws Exception {
            partitionId = rs.getString(3);
            parentPartitionId = rs.getString(4);
            startTime = rs.getLong(5);
            endTime = rs.getLong(6);
            startKey = rs.getBytes(7);
            endKey = rs.getBytes(8);
        }
    }
}