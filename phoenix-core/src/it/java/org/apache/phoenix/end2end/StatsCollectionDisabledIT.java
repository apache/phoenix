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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

/**
 * Verifies that statistics are not collected if they are disabled via a setting
 */
public class StatsCollectionDisabledIT extends StatsCollectorAbstractIT {

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(5);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        props.put(QueryServices.STATS_ENABLED_ATTRIB, Boolean.toString(false));
        props.put(QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB, Boolean.TRUE.toString());
        props.put(QueryServices.EXPLAIN_ROW_COUNT_ATTRIB, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testStatisticsAreNotWritten() throws SQLException {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE T1 (ID INTEGER NOT NULL PRIMARY KEY, NAME VARCHAR)");
        stmt.execute("UPSERT INTO T1 VALUES (1, 'NAME1')");
        stmt.execute("UPSERT INTO T1 VALUES (2, 'NAME2')");
        stmt.execute("UPSERT INTO T1 VALUES (3, 'NAME3')");
        conn.commit();
        stmt.execute("UPDATE STATISTICS T1");
        ResultSet rs = stmt.executeQuery("SELECT * FROM SYSTEM.STATS");
        assertFalse(rs.next());
        rs.close();
        stmt.close();
        rs = conn.createStatement().executeQuery("EXPLAIN SELECT * FROM T1");
        String explainPlan = QueryUtil.getExplainPlan(rs);
        assertEquals(
                "CLIENT 1-CHUNK PARALLEL 1-WAY FULL SCAN OVER T1",
                explainPlan);
       conn.close();
    }
}
