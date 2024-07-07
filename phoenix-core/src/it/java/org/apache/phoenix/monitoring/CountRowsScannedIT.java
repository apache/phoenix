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

package org.apache.phoenix.monitoring;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.end2end.ParallelStatsEnabledTest;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsEnabledTest.class)
public class CountRowsScannedIT extends BaseTest {

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props));
    }

    @Test
    public void testSinglePrimaryKey() throws Exception {
        Properties props = new Properties();
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, "true");
        // force many rpc calls
        props.put(QueryServices.SCAN_CACHE_SIZE_ATTRIB, "10");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        stmt.execute("CREATE TABLE " + tableName
                + " (A UNSIGNED_LONG NOT NULL PRIMARY KEY, Z UNSIGNED_LONG)");
        for (int i = 1; i <= 100; i++) {
            String sql = String.format("UPSERT INTO %s VALUES (%d, %d)", tableName, i, i);
            stmt.execute(sql);
        }
        conn.commit();

        // both columns, but primary key 3 to 100, 98 rows
        long count1 = countRowsScannedFromSql(stmt,
                "SELECT A,Z FROM " + tableName + " WHERE A >= 3 AND Z >= 7");
        assertEquals(98, count1);

        // primary key, 3 to 100, 98 rows
        long count2 = countRowsScannedFromSql(stmt,
                "SELECT A,Z FROM " + tableName + " WHERE A >= 3");
        assertEquals(98, count2);

        // non-primary key, all rows
        long count3 = countRowsScannedFromSql(stmt,
                "SELECT A,Z FROM " + tableName + " WHERE Z >= 7");
        assertEquals(100, count3);

        // primary key with limit, the first row
        long count4 = countRowsScannedFromSql(stmt,
                "SELECT A,Z FROM " + tableName + " WHERE A >= 3 limit 1");
        assertEquals(1, count4);

        // non-primary key with limit, find the first Z >= 7, 1 to 7, 7 rows
        long count5 = countRowsScannedFromSql(stmt,
                "SELECT A,Z FROM " + tableName + " WHERE Z >= 7 limit 1");
        assertEquals(7, count5);

        // primary key with order by primary and limit
        long count6 = countRowsScannedFromSql(stmt,
                "SELECT A,Z FROM " + tableName + " WHERE A >= 3 ORDER BY A limit 1");
        assertEquals(1, count6);

        // primary key with order by non-primary and limit
        long count7 = countRowsScannedFromSql(stmt,
                "SELECT A,Z FROM " + tableName + " WHERE A >= 3 ORDER BY Z limit 1");
        assertEquals(98, count7);

        // select non-primary key with order by primary limit
        long count8 = countRowsScannedFromSql(stmt,
                "SELECT A,Z FROM " + tableName + " WHERE Z >= 7 ORDER BY A limit 1");
        assertEquals(7, count8);

        // select non-primary key with order by primary limit desc
        // scan from the last, 1 row
        long count9 = countRowsScannedFromSql(stmt,
                "SELECT A,Z FROM " + tableName + " WHERE Z >= 7 ORDER BY A desc limit 1");
        assertEquals(1, count9);

        // select non-primary key with order by primary limit desc
        // scan from the last, 1 row
        long count10 = countRowsScannedFromSql(stmt, "SELECT A,Z FROM " + tableName
                + " WHERE Z >= 7 AND Z <= 60 ORDER BY A desc limit 1");
        assertEquals(41, count10);

        // select non-primary key with order by primary limit
        long count11 = countRowsScannedFromSql(stmt,
                "SELECT A,Z FROM " + tableName + " WHERE Z >= 7 ORDER BY Z limit 1");
        assertEquals(100, count11);
    }

    @Test
    public void testMultiPrimaryKeys() throws Exception {
        Properties props = new Properties();
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, "true");
        // force many rpc calls
        props.put(QueryServices.SCAN_CACHE_SIZE_ATTRIB, "10");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        PhoenixStatement stmt = conn.createStatement().unwrap(PhoenixStatement.class);
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS " + tableName
                + " (A UNSIGNED_LONG NOT NULL, B UNSIGNED_LONG NOT NULL, "
                + " Z UNSIGNED_LONG, CONSTRAINT pk PRIMARY KEY (A, B))");
        for (int i = 1; i <= 100; i++) {
            String sql = String
                    .format("UPSERT INTO %s VALUES (%d, %d, %d)", tableName, (i % 5) + 1, i, i);
            stmt.execute(sql);
        }
        conn.commit();

        // pk1 and pk2, only needed rows are scanned
        long count1 = countRowsScannedFromSql(stmt,
                "SELECT A,B,Z FROM " + tableName + " WHERE A >= 2 AND B >= 3");
        assertEquals(79, count1);

        // pk2, all rows
        long count2 = countRowsScannedFromSql(stmt,
                "SELECT A,B,Z FROM " + tableName + " WHERE B >= 3");
        assertEquals(100, count2);

        // non-pk,  all rows
        long count3 = countRowsScannedFromSql(stmt,
                "SELECT A,B,Z FROM " + tableName + " WHERE Z >= 7");
        assertEquals(100, count3);
    }
    private long countRowsScannedFromSql(Statement stmt, String sql) throws SQLException {
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            // loop to the end
        }
        return getRowsScanned(rs);
    }

    private long getRowsScanned(ResultSet rs) {
        if (!(rs instanceof PhoenixResultSet)) {
            return -1;
        }
        PhoenixResultSet phoenixResultSet = (PhoenixResultSet) rs;
        Map<String, Map<MetricType, Long>> metrics = phoenixResultSet.getReadMetrics();

        long sum = 0;
        boolean valid = false;
        for (Map.Entry<String, Map<MetricType, Long>> entry : metrics.entrySet()) {
            Long val = entry.getValue().get(MetricType.COUNT_ROWS_SCANNED);
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