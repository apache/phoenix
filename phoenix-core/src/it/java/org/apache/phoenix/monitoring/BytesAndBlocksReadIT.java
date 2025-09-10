/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.monitoring;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.CompactSplit;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

@Category(NeedsOwnMiniClusterTest.class)
public class BytesAndBlocksReadIT extends BaseTest {

    @BeforeClass
    public static void setup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(4);
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, "true");
        props.put(QueryServices.SCAN_CACHE_SIZE_ATTRIB, "10");
        props.put(HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL, "0");
        props.put(CompactSplit.HBASE_REGION_SERVER_ENABLE_COMPACTION, "false");
        setUpTestDriver(new ReadOnlyProps(props));
    }

    @Test
    public void testSimpleQuery() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
            "CREATE TABLE " + tableName + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(ddl);
            conn.commit();
            Statement stmt = conn.createStatement();
            stmt.execute("UPSERT INTO " + tableName + " (id, name) VALUES (3, 'Jim')");
            conn.commit();
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
            assertRowReadFromMemstore(tableName, getQueryReadMetrics(rs));
            TestUtil.flush(utility, TableName.valueOf(tableName));
            rs = stmt.executeQuery("SELECT * FROM " + tableName);
            assertRowReadFromFs(tableName, getQueryReadMetrics(rs));
            rs = stmt.executeQuery("SELECT * FROM " + tableName);
            assertRowReadFromBlockcache(tableName, getQueryReadMetrics(rs));
        }
    }

    @Test
    public void testBytesReadInClientUpsertSelect() throws Exception {
        String sourceTableName = generateUniqueName();
        String targetTableName = generateUniqueName();
        String ddl = "CREATE TABLE " + sourceTableName + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR)";
        String ddl2 = "CREATE TABLE " + targetTableName + " (id INTEGER NOT NULL PRIMARY KEY, name VARCHAR)";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            Statement stmt = conn.createStatement();
            stmt.execute(ddl);
            conn.commit();
            stmt.execute(ddl2);
            conn.commit();
            stmt.execute("UPSERT INTO " + sourceTableName + " (id, name) VALUES (1, 'Kim')");
            conn.commit();
            stmt.execute("UPSERT INTO " + sourceTableName + " (id, name) VALUES (2, 'Tim')");
            conn.commit();
            stmt.execute("UPSERT INTO " + sourceTableName + " (id, name) VALUES (3, 'Jim')");
            conn.commit();
            assertRowReadFromMemstore(sourceTableName, getMutationReadMetrics(conn, targetTableName, sourceTableName, 1));
            TestUtil.flush(utility, TableName.valueOf(sourceTableName));
            assertRowReadFromFs(sourceTableName, getMutationReadMetrics(conn, targetTableName, sourceTableName, 2));
            assertRowReadFromBlockcache(sourceTableName, getMutationReadMetrics(conn, targetTableName, sourceTableName, 3));
        }
    }
    
    private void assertRowReadFromMemstore(String tableName, Map<String, Map<MetricType, Long>> readMetrics) throws Exception {
        Assert.assertTrue(readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_MEMSTORE) > 0);
        Assert.assertEquals(0,
            (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_FS));
        Assert.assertEquals(0,
            (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_BLOCKCACHE));
    }

    private void assertRowReadFromFs(String tableName,
        Map<String, Map<MetricType, Long>> readMetrics) throws Exception {
        Assert.assertTrue(readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_FS) > 0);
        Assert.assertTrue(readMetrics.get(tableName).get(MetricType.BLOCK_READ_OPS_COUNT) > 0);
        Assert.assertEquals(0,
            (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_MEMSTORE));
        Assert.assertEquals(0,
            (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_BLOCKCACHE));
    }

    private void assertRowReadFromBlockcache(String tableName, Map<String, Map<MetricType, Long>> readMetrics) throws Exception {
        Assert
            .assertTrue(readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_BLOCKCACHE) > 0);
        Assert.assertEquals(0,
            (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_FS));
        Assert.assertEquals(0,
            (long) readMetrics.get(tableName).get(MetricType.BYTES_READ_FROM_MEMSTORE));
        Assert.assertEquals(0,
            (long) readMetrics.get(tableName).get(MetricType.BLOCK_READ_OPS_COUNT));
    }
    
    private Map<String, Map<MetricType, Long>> getQueryReadMetrics(ResultSet rs) throws Exception {
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
        }
        Assert.assertEquals(1, rowCount);
        Map<String, Map<MetricType, Long>> readMetrics =
            PhoenixRuntime.getRequestReadMetricInfo(rs);
        return readMetrics;
    }

    private Map<String, Map<MetricType, Long>> getMutationReadMetrics(Connection conn, String targetTableName, String sourceTableName, int rowId)
        throws Exception {
        Statement stmt = conn.createStatement();
        stmt.execute(
                "UPSERT INTO " + targetTableName + " (id, name) SELECT * FROM " + sourceTableName + " WHERE id = " + rowId);
        conn.commit();
        Map<String, Map<MetricType, Long>> readMetrics =
            PhoenixRuntime.getReadMetricInfoForMutationsSinceLastReset(conn);
        PhoenixRuntime.resetMetrics(conn);
        return readMetrics;
    }


}
