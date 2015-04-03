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

import org.apache.hadoop.hbase.client.Consistency;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * Basic tests for Alter Session Statements
 *
 */
public class AlterSessionIT extends BaseHBaseManagedTimeIT {

    Connection testConn;

    @Before
    public void initTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        testConn = DriverManager.getConnection(getUrl(), props);
        assertEquals(Consistency.STRONG, ((PhoenixConnection)testConn).getConsistency());
        testConn.createStatement().execute("create table AlterSessionIT (col1 varchar primary key)");
        testConn.commit();
    }

    @Test
    public void testUpdateConsistency() throws Exception {
        try {
            Statement st = testConn.createStatement();
            st.execute("alter session set Consistency = 'timeline'");
            ResultSet rs = st.executeQuery("explain select * from AlterSessionIT");
            assertEquals(Consistency.TIMELINE, ((PhoenixConnection)testConn).getConsistency());
            String queryPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(queryPlan.indexOf("TIMELINE") > 0);

            // turn off timeline read consistency
            st.execute("alter session set Consistency = 'strong'");
            rs = st.executeQuery("explain select * from AlterSessionIT");
            queryPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(queryPlan.indexOf("TIMELINE") < 0);
        } finally {
            this.testConn.close();
        }
    }

    @Test
    public void testSetConsistencyInURL() throws Exception {
        try {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(getUrl() + PhoenixRuntime.JDBC_PROTOCOL_TERMINATOR +
                    "Consistency=TIMELINE", props);
            assertEquals(Consistency.TIMELINE, ((PhoenixConnection)conn).getConsistency());
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("explain select * from AlterSessionIT");
            String queryPlan = QueryUtil.getExplainPlan(rs);
            assertTrue(queryPlan.indexOf("TIMELINE") > 0);
            conn.close();
        } finally {
            this.testConn.close();
        }
    }
}