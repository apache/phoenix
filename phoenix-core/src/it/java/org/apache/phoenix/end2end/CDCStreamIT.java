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
import org.apache.phoenix.coprocessor.TaskRegionObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;
import static org.apache.phoenix.util.CDCUtil.CDC_STREAM_NAME_FORMAT;

@Category(ParallelStatsDisabledTest.class)
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
        String streamName = String.format(CDC_STREAM_NAME_FORMAT, tableName,
                CDCUtil.getCDCCreationTimestamp(conn.unwrap(PhoenixConnection.class).getTableNoCache(tableName)));

        // stream should be in ENABLING state
        assertStreamStatus(conn, tableName, streamName, CDCUtil.CdcStreamStatus.ENABLING);

        // run task to populate partitions and enable stream
        TaskRegionObserver.SelfHealingTask task =
                new TaskRegionObserver.SelfHealingTask(
                        TaskRegionEnvironment, QueryServicesOptions.DEFAULT_TASK_HANDLING_MAX_INTERVAL_MS);
        task.run();

        // stream should be in ENABLED state and metadata is populated for every table region
        assertPartitionMetadata(conn, tableName, streamName);
        assertStreamStatus(conn, tableName, streamName, CDCUtil.CdcStreamStatus.ENABLED);
    }

    private void assertStreamStatus(Connection conn, String tableName, String streamName,
                                    CDCUtil.CdcStreamStatus status) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("SELECT STREAM_STATUS FROM "
                + SYSTEM_CDC_STREAM_STATUS_NAME + " WHERE TABLE_NAME='" + tableName +
                "' AND STREAM_NAME='" + streamName + "'");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(status.getSerializedValue(), rs.getString(1));
    }

    private void assertPartitionMetadata(Connection conn, String tableName, String streamName)
            throws SQLException {
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
            Assert.assertTrue(rs.next());
        }
    }
}
