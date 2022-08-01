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

import org.apache.phoenix.log.LogLevel;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@Category(ParallelStatsDisabledTest.class)
public class AuditLoggingIT extends ParallelStatsDisabledIT {

    @Test
    public void testEmptyLogging() throws Exception {
        String createqQery = "create table test1 (mykey integer not null primary key," +
                " mycolumn varchar)";
        String upsertQuery = "upsert into test1 values (1,'Hello')";
        String selectQuery = "select * from test1";
        String getLogsQuery = "select * from SYSTEM.LOG WHERE TABLE_NAME='TEST1' order by start_time";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            Statement stmt = conn.createStatement();
            stmt.execute(createqQery);
            stmt.execute(upsertQuery);
            stmt.executeQuery(selectQuery);
            conn.commit();

            Thread.sleep(4000);
            ResultSet rs = stmt.executeQuery(getLogsQuery);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testLoggingSelect() throws Exception {
        String createqQery = "create table test2 (mykey integer not null primary key," +
                " mycolumn varchar)";
        String upsertQuery = "upsert into test2 values (1,'Hello')";
        String selectQuery = "select * from test2";
        String getLogsQuery = "select * from SYSTEM.LOG WHERE TABLE_NAME='TEST2' order by start_time";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.LOG_LEVEL, LogLevel.TRACE.name());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement();
        ){
            conn.setAutoCommit(true);
            stmt.execute(createqQery);
            stmt.execute(upsertQuery);
            ResultSet rs = stmt.executeQuery(selectQuery);
            assertTrue(rs.next());
            assertFalse(rs.next());
            rs.close();

            int retryCount=10;
            while (true) {
                try {
                    ResultSet rs2 = stmt.executeQuery(getLogsQuery);
                    assertTrue(rs2.next());
                    assertEquals(rs2.getString(7), selectQuery);
                    assertFalse(rs2.next());
                    break;
                } catch (AssertionError e) {
                    if(retryCount-- <= 0) {
                        throw e;
                    }
                    Thread.sleep(4000);
                }
            }
        }
    }

    @Test
    public void testLoggingDMLAndDDL() throws Exception {
        String createqQery = "create table test3 (mykey integer not null primary key," +
                " mycolumn varchar)";
        String upsertQuery = "upsert into test3 values (1,'Hello')";
        String selectQuery = "select * from test3";
        String getLogsQuery = "select * from SYSTEM.LOG WHERE TABLE_NAME='TEST3' order by start_time";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.AUDIT_LOG_LEVEL, LogLevel.INFO.name());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement();
        ){
            conn.setAutoCommit(true);

            stmt.execute(createqQery);
            stmt.execute(upsertQuery);
            ResultSet rs = conn.createStatement().executeQuery(selectQuery);
            assertTrue(rs.next());
            assertFalse(rs.next());
            rs.close();

            int retryCount=10;
            while (true) {
                try {
                    ResultSet rs2 = stmt.executeQuery(getLogsQuery);
                    assertTrue(rs2.next());
                    assertEquals(rs2.getString(7), createqQery);
                    assertTrue(rs2.next());
                    assertEquals(rs2.getString(7), upsertQuery);
                    assertFalse(rs2.next());
                    break;
                } catch (AssertionError e) {
                    if(retryCount-- <= 0) {
                        throw e;
                    }
                    Thread.sleep(4000);
                }
            }
        }
    }

    @Test
    public void testLoggingDMLAandDDLandSelect() throws Exception {
        String createqQuery = "create table test4 (mykey integer not null primary key," +
                " mycolumn varchar)";
        String upsertQuery = "upsert into test4 values (1,'Hello')";
        String selectQuery = "select * from test4";
        String getLogsQuery = "select * from SYSTEM.LOG WHERE TABLE_NAME='TEST4' order by start_time";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.AUDIT_LOG_LEVEL, LogLevel.INFO.name());
        props.setProperty(QueryServices.LOG_LEVEL, LogLevel.TRACE.name());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement();
        ){
            conn.setAutoCommit(true);
            stmt.execute(createqQuery);
            stmt.execute(upsertQuery);
            ResultSet rs = stmt.executeQuery(selectQuery);
            assertTrue(rs.next());
            assertFalse(rs.next());
            rs.close();

            int retryCount=10;
            while (true) {
                try {
                    ResultSet rs2 = conn.createStatement().executeQuery(getLogsQuery);
                    assertTrue(rs2.next());
                    assertEquals(rs2.getString(7), createqQuery);
                    assertTrue(rs2.next());
                    assertEquals(rs2.getString(7), upsertQuery);
                    assertTrue(rs2.next());
                    assertEquals(rs2.getString(7), selectQuery);

                    assertFalse(rs2.next());
                    break;
                } catch (AssertionError e) {
                    if(retryCount-- <= 0) {
                        throw e;
                    }
                    Thread.sleep(4000);
                }
            }
        }
    }

    @Test
    public void testLogginParameterizedUpsert() throws Exception {
        String createqQery = "create table test5 (mykey integer not null primary key," +
                " mycolumn varchar)";
        String upsertQuery = "upsert into test5 values (?, ?)";
        String selectQuery = "select * from test5";
        String getLogsQuery = "select * from SYSTEM.LOG WHERE TABLE_NAME='TEST5' order by start_time";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.AUDIT_LOG_LEVEL, LogLevel.INFO.name());
        props.setProperty(QueryServices.LOG_LEVEL, LogLevel.TRACE.name());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                Statement stmt = conn.createStatement();
                PreparedStatement p = conn.prepareStatement(upsertQuery);
        ){
            conn.setAutoCommit(true);
            stmt.execute(createqQery);

            p.setInt(1, 1);
            p.setString(2, "foo");
            p.execute();

            p.setInt(1, 2);
            p.setString(2, "bar");
            p.execute();

            ResultSet rs = stmt.executeQuery(selectQuery);
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertFalse(rs.next());
            rs.close();

            int retryCount=10;
            while (true) {
                try {
                    ResultSet rs2 = conn.createStatement().executeQuery(getLogsQuery);
                    assertTrue(rs2.next());
                    assertTrue(rs2.next());
                    assertEquals("1,foo", rs2.getString(13));
                    assertTrue(rs2.next());
                    assertEquals( "2,bar", rs2.getString(13));
                    assertTrue(rs2.next());
                    assertFalse(rs2.next());
                    break;
                } catch (AssertionError e) {
                    if(retryCount-- <= 0) {
                        throw e;
                    }
                    Thread.sleep(4000);
                }
            }
        }
    }

    @Test
    public void testlogSamplingRate() throws Exception {
        String createqQery = "create table test6 (mykey integer not null primary key," +
                " mycolumn varchar)";

        String selectQuery = "select * from test6";
        String getLogsQuery = "select * from SYSTEM.LOG WHERE TABLE_NAME='TEST6' order by start_time";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(QueryServices.AUDIT_LOG_LEVEL, LogLevel.INFO.name());
        props.setProperty(QueryServices.LOG_LEVEL, LogLevel.TRACE.name());
        props.setProperty(QueryServices.LOG_SAMPLE_RATE, "0.5");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(true);
        try {
            Statement stat = conn.createStatement();
            stat.execute(createqQery);
            String upsertQuery;
            for (int i = 0; i<100; i++) {
                upsertQuery  = "upsert into test6 values (" + i + ",'asd')";
                stat.execute(upsertQuery);
                ResultSet rs = stat.executeQuery(selectQuery);
                assertTrue(rs.next());
                rs.close();
            }

            ResultSet rs2 = conn.createStatement().executeQuery(getLogsQuery);
            int numOfUpserts = 0;
            int numOfSelects = 0;
            while (rs2.next()) {
                String query = rs2.getString(7);
                if (query.equals(selectQuery)) {
                    numOfSelects++;
                }
                else if (query.contains("upsert into test6 values (")) {
                    numOfUpserts++;
                }
            }
            assertEquals(numOfUpserts, 100);
            assertTrue(numOfSelects > 0 && numOfSelects < 100);
            System.out.println(numOfSelects);

        } finally {
            conn.close();
        }
    }

}
