/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;


@Category(NeedsOwnMiniClusterTest.class)
public class QueryTimeoutIT extends BaseTest {
    private String tableName;
    
    @Before
    public void generateTableName() throws SQLException {
        tableName = generateUniqueName();
    }
    
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(5);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(700));
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(10000));
        props.put(QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB, Boolean.TRUE.toString());
        props.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @After
    public void assertNoUnfreedMemory() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            long unfreedBytes = conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
            assertEquals(0, unfreedBytes);
        }
        assertFalse("refCount leaked", refCountLeaked);
    }
    
    @Test
    public void testSetRPCTimeOnConnection() throws Exception {
        Properties overriddenProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        overriddenProps.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        overriddenProps.setProperty("hbase.rpc.timeout", Long.toString(100));
        String url = QueryUtil.getConnectionUrl(overriddenProps, config, "longRunning");
        Connection conn1 = DriverManager.getConnection(url, overriddenProps);
        ConnectionQueryServices s1 = conn1.unwrap(PhoenixConnection.class).getQueryServices();
        ReadOnlyProps configProps = s1.getProps();
        assertEquals("100", configProps.get("hbase.rpc.timeout"));
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.put(QueryServices.EXTRA_JDBC_ARGUMENTS_ATTRIB, QueryServicesOptions.DEFAULT_EXTRA_JDBC_ARGUMENTS);
        Connection conn2 = DriverManager.getConnection(getUrl(), props);
        ConnectionQueryServices s2 = conn2.unwrap(PhoenixConnection.class).getQueryServices();
        assertFalse(s1 == s2);
        Connection conn3 = DriverManager.getConnection(getUrl(), props);
        ConnectionQueryServices s3 = conn3.unwrap(PhoenixConnection.class).getQueryServices();
        assertTrue(s2 == s3);
        
        Connection conn4 = DriverManager.getConnection(url, overriddenProps);
        ConnectionQueryServices s4 = conn4.unwrap(PhoenixConnection.class).getQueryServices();
        assertTrue(s1 == s4);
    }
    
    @Test
    public void testQueryTimeout() throws Exception {
        Connection conn;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(
                "CREATE TABLE " + tableName + "(k BIGINT PRIMARY KEY, v VARCHAR)");
        TestUtil.addCoprocessor(conn, tableName, QueryTimeoutIT.SleepingRegionObserver.class);

        PhoenixStatement pstmt = conn.createStatement().unwrap(PhoenixStatement.class);
        pstmt.setQueryTimeout(1);
        long startTime = System.currentTimeMillis();
        try {
            ResultSet rs = pstmt.executeQuery("SELECT count(*) FROM " + tableName);
            startTime = System.currentTimeMillis();
            rs.next();
            fail("Total time of query was " + (System.currentTimeMillis() - startTime) + " ms, but expected to be greater than 1000");
        } catch (SQLTimeoutException e) {
            long elapsedTimeMillis = System.currentTimeMillis() - startTime;
            assertEquals(SQLExceptionCode.OPERATION_TIMED_OUT.getErrorCode(), e.getErrorCode());
            assertTrue("Total time of query was " + elapsedTimeMillis + " ms, but expected to be greater or equal to 1000",
                    elapsedTimeMillis >= 1000);
        }
        conn.close();
    }

    public static class SleepingRegionObserver extends SimpleRegionObserver {
        @Override
        public void preScannerClose(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s)
                throws IOException {
            try {
                Thread.sleep(1200); // Wait long enough
            } catch (InterruptedException e) {
            }
        }
    }
}
