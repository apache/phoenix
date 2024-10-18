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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.phoenix.end2end.NeedsOwnMiniClusterTest;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(NeedsOwnMiniClusterTest.class)
public class BlockBytesScannedMetricIT extends BaseTest {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, "true");
        setUpTestDriver(new ReadOnlyProps(props));
    }

    @Test
    public void testBlockBytesScannedMetric() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String tableName = generateUniqueName();
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        stmt.execute("CREATE TABLE " + tableName
                + " (A UNSIGNED_LONG NOT NULL PRIMARY KEY, Z UNSIGNED_LONG)");
        for (int i = 1; i <= 200; i++) {
            String sql = String.format("UPSERT INTO %s VALUES (%d, %d)", tableName, i, i);
            stmt.execute(sql);
            if (i%50==0) {
                conn.commit();
                flush(tableName);
            }
        }
        clearBlockCache(tableName);

        long count1 = countBlockBytesScannedFromSql(stmt, "SELECT * FROM " + tableName + " WHERE A = 67");
        Assert.assertTrue(count1 > 0);

        long count2 = countBlockBytesScannedFromSql(stmt, "SELECT * FROM " + tableName);
        Assert.assertTrue(count2 > 0);

        long count3 = countBlockBytesScannedFromSql(stmt, "SELECT * FROM " + tableName
                + " WHERE A > 21  AND A < 67");
        Assert.assertTrue(count3 > 0);
    }

    private void clearBlockCache(String tableName) {
        HRegionServer regionServer = utility.getMiniHBaseCluster().getRegionServer(0);
        for (HRegion region : regionServer.getRegions(TableName.valueOf(tableName))) {
            regionServer.clearRegionBlockCache(region);
        }
    }

    private void flush(String tableName) throws IOException {
        HRegionServer regionServer = utility.getMiniHBaseCluster().getRegionServer(0);
        for (HRegion region : regionServer.getRegions(TableName.valueOf(tableName))) {
            region.flush(true);
        }
    }


    private long countBlockBytesScannedFromSql(Statement stmt, String sql) throws SQLException {
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            // loop to the end
        }
        return getBlockBytesScanned(rs);
    }

    private long getBlockBytesScanned(ResultSet rs) throws SQLException {
        if (!(rs instanceof PhoenixResultSet)) {
            return -1;
        }
        Map<String, Map<MetricType, Long>> metrics = PhoenixRuntime.getRequestReadMetricInfo(rs);

        long sum = 0;
        boolean valid = false;
        for (Map.Entry<String, Map<MetricType, Long>> entry : metrics.entrySet()) {
            Long val = entry.getValue().get(MetricType.COUNT_BLOCK_BYTES_SCANNED);
            if (val != null) {
                sum += val.longValue();
                valid = true;
            }
        }
        if (valid) {
            return sum;
        } else {
            return -1;
        }
    }
}