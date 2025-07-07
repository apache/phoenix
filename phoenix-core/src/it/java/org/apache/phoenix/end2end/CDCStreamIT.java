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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixMasterSource;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixCoprocessorSourceFactory;

import java.io.IOException;
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
import static org.apache.phoenix.query.QueryConstants.CDC_EVENT_TYPE;
import static org.apache.phoenix.query.QueryConstants.CDC_PRE_IMAGE;
import static org.apache.phoenix.query.QueryConstants.CDC_UPSERT_EVENT_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(NeedsOwnMiniClusterTest.class)
public class CDCStreamIT extends CDCBaseIT {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final MetricsPhoenixMasterSource METRICS_SOURCE =
            MetricsPhoenixCoprocessorSourceFactory.getInstance().getPhoenixMasterSource();

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
        taskRegionEnvironment =
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
                        taskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
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
                        taskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
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

    public void testStreamMetadataWhenTableIsDropped() throws SQLException {
        Connection conn = newConnection();
        MetaDataClient mdc = new MetaDataClient(conn.unwrap(PhoenixConnection.class));
        String schemaName = "\"" + generateUniqueName().toLowerCase() + "\"";
        String tableName = SchemaUtil.getTableName(schemaName, "\"" + generateUniqueName().toLowerCase() + "\"");
        String unescapedFullTableName = SchemaUtil.getUnEscapedFullName(tableName);
        String create_table_sql = "CREATE TABLE  " + tableName + " ( k INTEGER PRIMARY KEY," + " v1 INTEGER, v2 DATE)";
        conn.createStatement().execute(create_table_sql);
        String cdcName = "\"" + generateUniqueName().toLowerCase() + "\"";
        String cdc_sql = "CREATE CDC " + cdcName + " ON " + tableName;
        conn.createStatement().execute(cdc_sql);
        TaskRegionObserver.SelfHealingTask task =
                new TaskRegionObserver.SelfHealingTask(
                        taskRegionEnvironment,
                        QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
        task.run();
        String partitionQuery = "SELECT * FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + unescapedFullTableName + "'";
        ResultSet rs = conn.createStatement().executeQuery(partitionQuery);
        Assert.assertTrue(rs.next());
        String drop_table_sql = "DROP TABLE " + tableName;
        Assert.assertNotNull(mdc.getStreamNameIfCDCEnabled(unescapedFullTableName));
        // check if stream metadata is cleared when table is dropped
        conn.createStatement().execute(drop_table_sql);
        Assert.assertNull(mdc.getStreamNameIfCDCEnabled(unescapedFullTableName));
        rs = conn.createStatement().executeQuery(partitionQuery);
        Assert.assertFalse(rs.next());
        // should be able to re-create same table with same cdc name and metadata should be populated
        conn.createStatement().execute(create_table_sql);
        conn.createStatement().execute(cdc_sql);
        Assert.assertNotNull(mdc.getStreamNameIfCDCEnabled(unescapedFullTableName));
        task.run();
        rs = conn.createStatement().executeQuery(partitionQuery);
        Assert.assertTrue(rs.next());
    }

    @Test
    public void testCDCStreamTTL() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName, true);
        TestUtil.splitTable(conn, tableName, Bytes.toBytes("m"));
        String sql = "SELECT PARTITION_END_TIME FROM SYSTEM.CDC_STREAM WHERE TABLE_NAME='" + tableName + "'";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(3, count);
        ManualEnvironmentEdge injectEdge = new ManualEnvironmentEdge();
        long t = System.currentTimeMillis() + QueryServicesOptions.DEFAULT_PHOENIX_CDC_STREAM_PARTITION_EXPIRY_MIN_AGE_MS;
        t = (t / 1000) * 1000;
        EnvironmentEdgeManager.injectEdge(injectEdge);
        injectEdge.setValue(t);
        rs = conn.createStatement().executeQuery(sql);
        int newCount = 0;
        while (rs.next()) {
            // parent partition row with non-zero end time should have expired
            if (rs.getLong(1) > 0) {
                Assert.fail("Closed partition should have expired after TTL.");
            }
            newCount++;
        }
        Assert.assertEquals(2, newCount);
        EnvironmentEdgeManager.reset();
    }

    /**
     * Split the only region of the table with empty start key and empty end key.
     */
    @Test
    public void testPartitionMetadataTableWithSingleRegionSplits() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName, true);

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
        assertEquals(parent.startTime, daughters.get(0).parentStartTime);
        assertEquals(parent.startTime, daughters.get(1).parentStartTime);
    }

    /**
     * Split the first region of the table with empty start key.
     */
    @Test
    public void testPartitionMetadataFirstRegionSplits() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName, true);

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
        assertEquals(splitParent.startTime, daughters.get(0).parentStartTime);
        assertEquals(splitParent.startTime, daughters.get(1).parentStartTime);
    }

    /**
     * Split the last region of the table with empty end key.
     */
    @Test
    public void testPartitionMetadataLastRegionSplits() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName, true);

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
        assertEquals(splitParent.startTime, daughters.get(0).parentStartTime);
        assertEquals(splitParent.startTime, daughters.get(1).parentStartTime);
    }

    /**
     * Split a middle region of the table with non-empty start/end key.
     */
    @Test
    public void testPartitionMetadataMiddleRegionSplits() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName, true);

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
        assertEquals(parent.startTime, daughters.get(0).parentStartTime);
        assertEquals(parent.startTime, daughters.get(1).parentStartTime);
    }

    /**
     * Test split of a region which came from a merge.
     */
    @Test
    public void testPartitionMetadataMergedRegionSplits() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName, true);

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
        assertEquals(mergedParent.get(0).startTime, mergedParent.get(1).startTime);
        assertEquals(mergedParent.get(0).partitionId, splitDaughters.get(0).parentPartitionId);
        assertEquals(mergedParent.get(0).partitionId, splitDaughters.get(1).parentPartitionId);
        assertEquals(splitDaughters.get(0).startTime, splitDaughters.get(1).startTime);
        assertEquals(splitDaughters.get(0).startTime, mergedParent.get(0).endTime);
        assertEquals(splitDaughters.get(0).startTime, mergedParent.get(1).endTime);
        assertEquals(mergedParent.get(0).startTime, splitDaughters.get(0).parentStartTime);
        assertEquals(mergedParent.get(0).startTime, splitDaughters.get(1).parentStartTime);
    }



    /**
     * Test merge of 2 regions which came from a split.
     */
    @Test
    public void testPartitionMetadataSplitRegionsMerge() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName, true);

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
        List<Long> parentStartTimes = new ArrayList<>();
        splitParents.forEach(p -> parentStartTimes.add(p.startTime));
        assertTrue(parentStartTimes.contains(mergedDaughter.get(0).parentStartTime));
        assertTrue(parentStartTimes.contains(mergedDaughter.get(1).parentStartTime));
    }

    /**
     * Test merge of 2 regions which came from different merges.
     */
    @Test
    public void testPartitionMetadataMergedRegionsMerge() throws Exception {
        // create table, cdc and bootstrap stream metadata
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName, true);

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
        List<Long> parentStartTimes = new ArrayList<>();
        for (PartitionMetadata mergedParent : mergedParents.subList(mergedParents.size()-4, mergedParents.size())) {
            assertEquals(mergedDaughter.get(0).startTime, mergedParent.endTime);
            parentStartTimes.add(mergedParent.startTime);
        }
        assertTrue(parentStartTimes.contains(mergedDaughter.get(0).parentStartTime));
        assertTrue(parentStartTimes.contains(mergedDaughter.get(1).parentStartTime));
    }

    @Test
    public void testGetRecords() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName, true);

        // upsert data
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('a', 1, 'foo')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('b', 2, 'bar')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('e', 3, 'alice')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('j', 4, 'bob')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('m', 5, 'cat')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('p', 6, 'cat')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('t', 7, 'cat')");
        conn.createStatement().execute("UPSERT INTO " + tableName + " VALUES ('z', 8, 'cat')");
        conn.commit();

        // get stream name
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT STREAM_NAME FROM SYSTEM.CDC_STREAM_STATUS WHERE TABLE_NAME='" + tableName + "'");
        Assert.assertTrue(rs.next());
        String streamName = rs.getString(1);

        // get partitions
        rs = conn.createStatement().executeQuery("SELECT PARTITION_ID, PARTITION_START_TIME FROM SYSTEM.CDC_STREAM " +
                "WHERE TABLE_NAME='" + tableName + "' AND STREAM_NAME='" + streamName + "'");
        Assert.assertTrue(rs.next());
        String partitionId = rs.getString(1);
        long partitionStartTime = rs.getLong(2);

        //get records
        String cdcName = streamName.split("/")[4];
        String sql = "SELECT /*+ CDC_INCLUDE(PRE, POST) */ *  FROM %s WHERE PARTITION_ID() = ?  AND PHOENIX_ROW_TIMESTAMP() >= CAST(CAST(? AS BIGINT) AS TIMESTAMP) LIMIT ? ";
        PreparedStatement stmt = conn.prepareStatement(String.format(sql, cdcName));
        stmt.setString(1, partitionId);
        stmt.setLong(2, partitionStartTime);
        stmt.setInt(3, 5);
        rs = stmt.executeQuery();
        while (rs.next()) {
            String cdcJson = rs.getString(3);
            Map<String, Object> map = OBJECT_MAPPER.readValue(cdcJson, Map.class);
            Assert.assertTrue(((Map<String, Object>)map.get(CDC_PRE_IMAGE)).isEmpty());
            Assert.assertEquals(CDC_UPSERT_EVENT_TYPE, map.get(CDC_EVENT_TYPE));
        }
    }

    @Test
    public void testPartitionUpdateFailureMetrics() throws Exception {
        Connection conn = newConnection();
        String tableName = generateUniqueName();
        createTableAndEnableCDC(conn, tableName, true);

        assertEquals("Post split partition update failures should be 0 initially",
            0, METRICS_SOURCE.getPostSplitPartitionUpdateFailureCount());
        assertEquals("Post merge partition update failures should be 0 initially", 
            0, METRICS_SOURCE.getPostMergePartitionUpdateFailureCount());

        TestUtil.splitTable(conn, tableName, Bytes.toBytes("m"));

        // Verify split metric is still 0 (successful split)
        assertEquals("Post split partition update failures should be 0 after successful split",
            0, METRICS_SOURCE.getPostSplitPartitionUpdateFailureCount());

        List<HRegionLocation> regions = TestUtil.getAllTableRegions(conn, tableName);
        
        TestUtil.mergeTableRegions(conn, tableName, regions.stream()
                .map(HRegionLocation::getRegion)
                .map(RegionInfo::getEncodedName)
                .collect(Collectors.toList()));

        // Verify merge metric is still 0 (successful merge)
        assertEquals("Post merge partition update failures should be 0 after successful merge", 
            0, METRICS_SOURCE.getPostMergePartitionUpdateFailureCount());
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
        String streamName = getStreamName(conn, tableName, cdcName);
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

}
