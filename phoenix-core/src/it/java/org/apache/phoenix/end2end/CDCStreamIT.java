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
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;
import static org.apache.phoenix.util.CDCUtil.CDC_STREAM_NAME_FORMAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class CDCStreamIT extends CDCBaseIT {
    private static RegionCoprocessorEnvironment TaskRegionEnvironment;

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(60*60)); // An hour
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(false));
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
                Long.toString(Long.MAX_VALUE));
        props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
        TaskRegionEnvironment =
                getUtility()
                        .getRSForFirstRegionInTable(
                                PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                        .getRegions(PhoenixDatabaseMetaData.SYSTEM_TASK_HBASE_TABLE_NAME)
                        .get(0).getCoprocessorHost()
                        .findCoprocessorEnvironment(TaskRegionObserver.class.getName());
    }

    @Test
    public void testStreamPartitionMetadataBootstrap() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER,"
                        + " v2 DATE)");
        createCDC(conn, cdc_sql, null);
        String streamName = getStreamName(conn, tableName, cdcName);

        // stream should be in ENABLING state
        assertStreamStatus(conn, tableName, streamName, CDCUtil.CdcStreamStatus.ENABLING);

        // run task to populate partitions and enable stream
        TaskRegionObserver.SelfHealingTask task =
                new TaskRegionObserver.SelfHealingTask(
                        TaskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
        task.run();

        // stream should be in ENABLED state and metadata is populated for every table region
        assertPartitionMetadata(conn, tableName, cdcName);
        assertStreamStatus(conn, tableName, streamName, CDCUtil.CdcStreamStatus.ENABLED);
    }

    @Test
    public void testOnlyOneStreamAllowed() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        String cdcName = generateUniqueName();
        String cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
        conn.createStatement().execute(
                "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER,"
                        + " v2 DATE)");
        createCDC(conn, cdc_sql, null);
        String streamName = getStreamName(conn, tableName, cdcName);

        // stream exists in ENABLING status
        String cdcName2 = generateUniqueName();
        String cdc_sql2 = "CREATE CDC " + cdcName2 + " ON " + tableName;
        try {
            createCDC(conn, cdc_sql2, null);
            Assert.fail("Only one CDC entity is allowed per table");
        } catch (SQLException e) {
            // expected
            assertEquals(SQLExceptionCode.CDC_ALREADY_ENABLED.getErrorCode(), e.getErrorCode());
            assertTrue(e.getMessage().contains(streamName));
        }

        // run task to populate partitions and enable stream
        TaskRegionObserver.SelfHealingTask task =
                new TaskRegionObserver.SelfHealingTask(
                        TaskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
        task.run();

        // stream exists in ENABLED status
        try {
            createCDC(conn, cdc_sql2, null);
            Assert.fail("Only one CDC entity is allowed per table");
        } catch (SQLException e) {
            // expected
            assertEquals(SQLExceptionCode.CDC_ALREADY_ENABLED.getErrorCode(), e.getErrorCode());
            assertTrue(e.getMessage().contains(streamName));
        }

        //drop cdc
        dropCDC(conn, cdcName, tableName);
        assertStreamStatus(conn, tableName, streamName, CDCUtil.CdcStreamStatus.DISABLED);

        //create new CDC
        String cdcName3 = generateUniqueName();
        String cdc_sql3 = "CREATE CDC " + cdcName3 + " ON " + tableName;
        createCDC(conn, cdc_sql3, null);
        streamName = getStreamName(conn, tableName, cdcName3);
        assertStreamStatus(conn, tableName, streamName, CDCUtil.CdcStreamStatus.ENABLING);
    }

    /**
     * Split the only region of the table with empty start key and empty end key.
     */
    @Test
    public void testPartitionMetadataTableWithSingleRegionSplits() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName);

        //split the only region somewhere in the middle
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("m"));

        //check partition metadata - daughter regions are inserted and parent's end time is updated.
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + tableName + "'");
        PartitionMetadata parent = null;
        List<PartitionMetadata> daughters = new ArrayList<>();
        while (rs.next()) {
            PartitionMetadata pm = new PartitionMetadata(rs);
            // parent which was split
            if (pm.endTime > 0) {
                parent = pm;
            } else {
                daughters.add(pm);
            }
        }
        assertNotNull(parent);
        assertEquals(2, daughters.size());
        assertEquals(daughters.get(0).startTime, parent.endTime);
        assertEquals(daughters.get(1).startTime, parent.endTime);
        assertEquals(parent.partitionId, daughters.get(0).parentPartitionId);
        assertEquals(parent.partitionId, daughters.get(1).parentPartitionId);
        assertTrue(daughters.stream().anyMatch(d -> d.startKey == null && d.endKey != null && d.endKey[0] == 'm'));
        assertTrue(daughters.stream().anyMatch(d -> d.endKey == null && d.startKey != null && d.startKey[0] == 'm'));
    }

    /**
     * Split the first region of the table with empty start key.
     */
    @Test
    public void testPartitionMetadataFirstRegionSplits() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName);

        //split the only region [null, null]
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("l"));
        // we have 2 regions - [null, l], [l, null], split the first region
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("d"));
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + tableName + "'");
        PartitionMetadata grandparent = null, splitParent = null, unSplitParent = null;
        List<PartitionMetadata> daughters = new ArrayList<>();
        while (rs.next()) {
            PartitionMetadata pm = new PartitionMetadata(rs);
            if (pm.endTime > 0) {
                if (pm.startKey == null && pm.endKey == null) {
                    grandparent = pm;
                } else {
                    splitParent = pm;
                }
            } else if (pm.endKey == null) {
                unSplitParent = pm;
            } else {
                daughters.add(pm);
            }
        }
        assertNotNull(grandparent);
        assertNotNull(unSplitParent);
        assertNotNull(splitParent);
        assertEquals(2, daughters.size());
        assertEquals(daughters.get(0).startTime, splitParent.endTime);
        assertEquals(daughters.get(1).startTime, splitParent.endTime);
        assertEquals(splitParent.partitionId, daughters.get(0).parentPartitionId);
        assertEquals(splitParent.partitionId, daughters.get(1).parentPartitionId);
        assertTrue(daughters.stream().anyMatch(d -> d.startKey == null && d.endKey != null && d.endKey[0] == 'd'));
        assertTrue(daughters.stream().anyMatch(d -> d.startKey != null && d.startKey[0] == 'd' && d.endKey[0] == 'l'));
    }

    /**
     * Split the last region of the table with empty end key.
     */
    @Test
    public void testPartitionMetadataLastRegionSplits() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName);

        //split the only region [null, null]
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("l"));
        // we have 2 regions - [null, l], [l, null], split the second region
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("q"));
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + tableName + "'");
        PartitionMetadata grandparent = null, splitParent = null, unSplitParent = null;
        List<PartitionMetadata> daughters = new ArrayList<>();
        while (rs.next()) {
            PartitionMetadata pm = new PartitionMetadata(rs);
            if (pm.endTime > 0) {
                if (pm.startKey == null && pm.endKey == null) {
                    grandparent = pm;
                } else {
                    splitParent = pm;
                }
            } else if (pm.startKey == null) {
                unSplitParent = pm;
            } else {
                daughters.add(pm);
            }
        }
        assertNotNull(grandparent);
        assertNotNull(unSplitParent);
        assertNotNull(splitParent);
        assertEquals(2, daughters.size());
        assertEquals(daughters.get(0).startTime, splitParent.endTime);
        assertEquals(daughters.get(1).startTime, splitParent.endTime);
        assertEquals(splitParent.partitionId, daughters.get(0).parentPartitionId);
        assertEquals(splitParent.partitionId, daughters.get(1).parentPartitionId);
        assertTrue(daughters.stream().anyMatch(d -> d.startKey[0] == 'l' && d.endKey[0] == 'q'));
        assertTrue(daughters.stream().anyMatch(d -> d.endKey == null && d.startKey != null && d.startKey[0] == 'q'));
    }

    /**
     * Split a middle region of the table with non-empty start/end key.
     */
    @Test
    public void testPartitionMetadataMiddleRegionSplits() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName);

        //split the only region [null, null]
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("d"));
        // we have 2 regions - [null, d], [d, null], split the second region
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("q"));
        // we have 3 regions - [null, d], [d, q], [q, null], split the second region
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("j"));
        // [null, d], [d, j], [j, q], [q, null]
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + tableName + "'");
        PartitionMetadata parent = null;
        List<PartitionMetadata> daughters = new ArrayList<>();
        while (rs.next()) {
            PartitionMetadata pm = new PartitionMetadata(rs);
            if (pm.startKey != null && pm.endKey != null) {
                if (pm.endTime > 0) parent = pm;
                else daughters.add(pm);
            }
        }
        assertNotNull(parent);
        assertEquals(2, daughters.size());
        assertEquals(daughters.get(0).startTime, parent.endTime);
        assertEquals(daughters.get(1).startTime, parent.endTime);
        assertEquals(parent.partitionId, daughters.get(0).parentPartitionId);
        assertEquals(parent.partitionId, daughters.get(1).parentPartitionId);
        assertTrue(daughters.stream().anyMatch(d -> d.startKey[0] == 'd' && d.endKey[0] == 'j'));
        assertTrue(daughters.stream().anyMatch(d -> d.startKey[0] == 'j' && d.endKey[0] == 'q'));
    }

    /**
     * Test split of a region which came from a merge.
     */
    @Test
    public void testPartitionMetadataMergedRegionSplits() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName);

        //split the only region
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("d"));

        //merge the 2 regions
        List<HRegionLocation> regions = TestUtil.getAllTableRegions(conn, tableName);
        TestUtil.mergeTableRegions(conn, tableName, regions.stream()
                .map(HRegionLocation::getRegion)
                .map(RegionInfo::getEncodedName)
                .collect(Collectors.toList()));

        //split again
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("l"));

        //verify partition metadata
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + tableName + "'");
        List<PartitionMetadata> mergedParent = new ArrayList<>();
        List<PartitionMetadata> splitDaughters = new ArrayList<>();

        while (rs.next()) {
            PartitionMetadata pm = new PartitionMetadata(rs);
            if (pm.startKey == null && pm.endKey == null && pm.parentPartitionId != null) {
                mergedParent.add(pm);
            }
            if (pm.endTime == 0) {
                splitDaughters.add(pm);
            }
        }
        assertEquals(2, mergedParent.size());
        assertEquals(2, splitDaughters.size());
        assertEquals(mergedParent.get(0).partitionId, mergedParent.get(1).partitionId);
        assertEquals(mergedParent.get(0).partitionId, splitDaughters.get(0).parentPartitionId);
        assertEquals(mergedParent.get(0).partitionId, splitDaughters.get(1).parentPartitionId);
        assertEquals(splitDaughters.get(0).startTime, splitDaughters.get(1).startTime);
        assertEquals(splitDaughters.get(0).startTime, mergedParent.get(0).endTime);
        assertEquals(splitDaughters.get(0).startTime, mergedParent.get(1).endTime);
    }



    /**
     * Test merge of 2 regions which came from a split.
     */
    @Test
    public void testPartitionMetadataSplitRegionsMerge() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName);

        //split the only region
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("l"));

        //merge the 2 regions
        List<HRegionLocation> regions = TestUtil.getAllTableRegions(conn, tableName);
        TestUtil.mergeTableRegions(conn, tableName, regions.stream()
                                                    .map(HRegionLocation::getRegion)
                                                    .map(RegionInfo::getEncodedName)
                                                    .collect(Collectors.toList()));

        //verify partition metadata
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + tableName + "'");

        List<PartitionMetadata> splitParents = new ArrayList<>();
        List<PartitionMetadata> mergedDaughter = new ArrayList<>();
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
        assertTrue(mergedDaughter.stream().anyMatch(d -> Objects.equals(d.parentPartitionId, splitParents.get(0).partitionId)));
        assertTrue(mergedDaughter.stream().anyMatch(d -> Objects.equals(d.parentPartitionId, splitParents.get(1).partitionId)));
        for (PartitionMetadata splitDaughter : splitParents) {
            Assert.assertEquals(mergedDaughter.get(0).startTime, splitDaughter.endTime);
        }
    }

    /**
     * Test merge of 2 regions which came from different merges.
     */
    @Test
    public void testPartitionMetadataMergedRegionsMerge() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName);

        // split the only region
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("l"));
        // split both regions
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("d"));
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("q"));
        // merge first two and last two regions
        List<HRegionLocation> regions = TestUtil.getAllTableRegions(conn, tableName);
        TestUtil.mergeTableRegions(conn, tableName, regions.subList(0,2).stream()
                .map(HRegionLocation::getRegion)
                .map(RegionInfo::getEncodedName)
                .collect(Collectors.toList()));
        TestUtil.mergeTableRegions(conn, tableName, regions.subList(2,4).stream()
                .map(HRegionLocation::getRegion)
                .map(RegionInfo::getEncodedName)
                .collect(Collectors.toList()));
        // compact to remove merge qualifier from merged regions i.e. clear references to parents
        TestUtil.doMajorCompaction(conn, tableName);
        // merge the two regions
        regions = TestUtil.getAllTableRegions(conn, tableName);
        TestUtil.mergeTableRegions(conn, tableName, regions.stream()
                .map(HRegionLocation::getRegion)
                .map(RegionInfo::getEncodedName)
                .collect(Collectors.toList()));

        //verify partition metadata
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + tableName + "'");

        List<PartitionMetadata> mergedDaughter = new ArrayList<>();
        List<PartitionMetadata> mergedParents = new ArrayList<>();
        while (rs.next()) {
            PartitionMetadata pm = new PartitionMetadata(rs);
            if (pm.endTime == 0) {
                mergedDaughter.add(pm);
            }
            // this will add extra rows, we will prune later
            else if (pm.startKey == null || pm.endKey == null) {
                mergedParents.add(pm);
            }
        }
        assertEquals(2, mergedDaughter.size());
        assertEquals(9, mergedParents.size());
        assertEquals(mergedDaughter.get(0).startTime, mergedDaughter.get(1).startTime);
        Collections.sort(mergedParents, Comparator.comparing(o -> o.endTime));
        for (PartitionMetadata mergedParent : mergedParents.subList(mergedParents.size()-4, mergedParents.size())) {
            assertEquals(mergedDaughter.get(0).startTime, mergedParent.endTime);
        }
    }

    private String getStreamName(Connection conn, String tableName, String cdcName) throws SQLException {
        return String.format(CDC_STREAM_NAME_FORMAT, tableName, cdcName, CDCUtil.getCDCCreationTimestamp(
                conn.unwrap(PhoenixConnection.class).getTableNoCache(tableName)));
    }

    private void assertStreamStatus(Connection conn, String tableName, String streamName,
                                    CDCUtil.CdcStreamStatus status) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT STREAM_STATUS FROM "
                + SYSTEM_CDC_STREAM_STATUS_NAME + " WHERE TABLE_NAME='" + tableName +
                "' AND STREAM_NAME='" + streamName + "'");
        assertTrue(rs.next());
        assertEquals(status.getSerializedValue(), rs.getString(1));
    }

    private void assertPartitionMetadata(Connection conn, String tableName, String cdcName)
            throws SQLException {
        String streamName = String.format(CDC_STREAM_NAME_FORMAT, tableName, cdcName,
                CDCUtil.getCDCCreationTimestamp(conn.unwrap(PhoenixConnection.class).getTableNoCache(tableName)));
        List<HRegionLocation> tableRegions
                = conn.unwrap(PhoenixConnection.class).getQueryServices().getAllTableRegions(tableName.getBytes());
        for (HRegionLocation tableRegion : tableRegions) {
            RegionInfo ri = tableRegion.getRegionInfo();
            PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + SYSTEM_CDC_STREAM_NAME +
                    " WHERE TABLE_NAME = ? AND STREAM_NAME = ? AND PARTITION_ID= ?");
            ps.setString(1, tableName);
            ps.setString(2, streamName);
            ps.setString(3, ri.getEncodedName());
            ResultSet rs = ps.executeQuery();
            assertTrue(rs.next());
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
        TaskRegionObserver.SelfHealingTask task =
                new TaskRegionObserver.SelfHealingTask(
                        TaskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
        task.run();
        assertStreamStatus(conn, tableName, streamName, CDCUtil.CdcStreamStatus.ENABLED);

        //upsert sample data
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('a', 1, 'foo')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('b', 2, 'bar')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('e', 3, 'alice')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('j', 4, 'bob')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('m', 5, 'cat')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('p', 6, 'cat')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('t', 7, 'cat')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('z', 8, 'cat')");
    }

    /**
     * Inner class to represent partition metadata for a region i.e. single row from SYSTEM.CDC_STREAM
     */
    private class PartitionMetadata {
        public String partitionId;
        public String parentPartitionId;
        public Long startTime;
        public Long endTime;
        public byte[] startKey;
        public byte[] endKey;

        public PartitionMetadata(ResultSet rs) throws SQLException {
            partitionId = rs.getString(3);
            parentPartitionId = rs.getString(4);
            startTime = rs.getLong(5);
            endTime = rs.getLong(6);
            startKey = rs.getBytes(7);
            endKey = rs.getBytes(8);
        }
    }
}
