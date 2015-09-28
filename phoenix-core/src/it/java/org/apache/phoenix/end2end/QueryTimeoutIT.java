/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLTimeoutException;
import java.util.Map;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;


public class QueryTimeoutIT extends BaseOwnClusterHBaseManagedTimeIT {
    private static final String TEST_TABLE_NAME = "T";
    
    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        // Must update config before starting server
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(700));
        props.put(QueryServices.QUEUE_SIZE_ATTRIB, Integer.toString(10000));
        props.put(QueryServices.EXPLAIN_CHUNK_COUNT_ATTRIB, Boolean.TRUE.toString());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Test
    public void testQueryTimeout() throws Exception {
        int nRows = 30000;
        Connection conn;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(
                "CREATE TABLE " + TEST_TABLE_NAME + "(k BIGINT PRIMARY KEY, v VARCHAR)");
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + TEST_TABLE_NAME + " VALUES(?, 'AAAAAAAAAAAAAAAAAAAA')");
        for (int i = 1; i <= nRows; i++) {
            stmt.setLong(1, i);
            stmt.executeUpdate();
            if ((i % 2000) == 0) {
                conn.commit();
            }
        }
        conn.commit();
        conn.createStatement().execute("UPDATE STATISTICS " + TEST_TABLE_NAME);
        
        PhoenixStatement pstmt = conn.createStatement().unwrap(PhoenixStatement.class);
        pstmt.setQueryTimeout(1);
        long startTime = System.currentTimeMillis();
        try {
            ResultSet rs = pstmt.executeQuery("SELECT count(*) FROM " + TEST_TABLE_NAME);
            // Force lots of chunks so query is cancelled
            assertTrue(pstmt.getQueryPlan().getSplits().size() > 1000);
            rs.next();
            fail("Total time of query was " + (System.currentTimeMillis() - startTime) + " ms, but expected to be greater than 1000");
        } catch (SQLTimeoutException e) {
            long elapsedTimeMillis = System.currentTimeMillis() - startTime;
            assertEquals(SQLExceptionCode.OPERATION_TIMED_OUT.getErrorCode(), e.getErrorCode());
            assertTrue(elapsedTimeMillis > 1000);
        }
        conn.close();
    }
}
