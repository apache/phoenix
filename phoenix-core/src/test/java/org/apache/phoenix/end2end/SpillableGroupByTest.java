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

import static org.apache.phoenix.util.TestUtil.GROUPBYTEST_NAME;
import static org.apache.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;

public class SpillableGroupByTest extends BaseConnectedQueryTest {

    private static final int NUM_ROWS_INSERTED = 1000;
    
    // covers: COUNT, COUNT(DISTINCT) SUM, AVG, MIN, MAX 
    private static String GROUPBY1 = "select "
            + "count(*), count(distinct uri), sum(appcpu), avg(appcpu), uri, min(id), max(id) from "
            + GROUPBYTEST_NAME + " group by uri";
    
    private int id;

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        // Set a very small cache size to force plenty of spilling
        props.put(QueryServices.GROUPBY_MAX_CACHE_SIZE_ATTRIB,
                Integer.toString(1));
        props.put(QueryServices.GROUPBY_SPILLABLE_ATTRIB, String.valueOf(true));
        props.put(QueryServices.GROUPBY_SPILL_FILES_ATTRIB,
                Integer.toString(1));

        // Must update config before starting server
        startServer(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }

    private long createTable() throws Exception {
        long ts = nextTimestamp();
        ensureTableCreated(getUrl(), GROUPBYTEST_NAME, null, ts - 2);
        return ts;
    }

    private void loadData(long ts) throws SQLException {
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        int groupFactor = NUM_ROWS_INSERTED / 2;
        for (int i = 0; i < NUM_ROWS_INSERTED; i++) {
            insertRow(conn, Integer.toString(i % (groupFactor)), 10);

            if ((i % 1000) == 0) {
                conn.commit();
            }
        }
        conn.commit();
        conn.close();
    }

    private void insertRow(Connection conn, String uri, int appcpu)
            throws SQLException {
        PreparedStatement statement = conn.prepareStatement("UPSERT INTO "
                + GROUPBYTEST_NAME + "(id, uri, appcpu) values (?,?,?)");
        statement.setString(1, String.valueOf(id));
        statement.setString(2, uri);
        statement.setInt(3, appcpu);
        statement.executeUpdate();
        id++;
    }

    
    @Test
    public void testScanUri() throws Exception {
        SpillableGroupByTest spGpByT = new SpillableGroupByTest();
        long ts = spGpByT.createTable();
        spGpByT.loadData(ts);
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 1));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(GROUPBY1);

            int count = 0;
            while (rs.next()) {
                String uri = rs.getString(5);
                assertEquals(2, rs.getInt(1));
                assertEquals(1, rs.getInt(2));
                assertEquals(20, rs.getInt(3));
                assertEquals(10, rs.getInt(4));
                int a = Integer.valueOf(rs.getString(6)).intValue();
                int b = Integer.valueOf(rs.getString(7)).intValue();
                assertEquals(Integer.valueOf(uri).intValue(), Math.min(a, b));
                assertEquals(NUM_ROWS_INSERTED / 2 + Integer.valueOf(uri), Math.max(a, b));
                count++;
            }
            assertEquals(NUM_ROWS_INSERTED / 2, count);
            
        } finally {
            conn.close();
        }
    }

}
