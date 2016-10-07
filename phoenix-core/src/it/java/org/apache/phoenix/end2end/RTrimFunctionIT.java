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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;


public class RTrimFunctionIT extends ParallelStatsDisabledIT {
    
    @Test
    public void testWithFixedLengthAscPK() throws Exception {
        testWithFixedLengthPK(SortOrder.ASC, Arrays.<Object>asList("b", "b ", "b  "));
    }
    
    @Test
    public void testWithFixedLengthDescPK() throws Exception {
        testWithFixedLengthPK(SortOrder.DESC, Arrays.<Object>asList("b  ", "b ", "b"));
    }
    
    private void testWithFixedLengthPK(SortOrder sortOrder, List<Object> expectedResults) throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute(
            "CREATE TABLE " + tableName + " ( k VARCHAR PRIMARY KEY " + (sortOrder == SortOrder.DESC ? "DESC" : "") + ")");

        conn.createStatement().execute("upsert into " + tableName + " (k) values ('a')");
        conn.createStatement().execute("upsert into " + tableName + " (k) values ('b')");
        conn.createStatement().execute("upsert into " + tableName + " (k) values ('b ')");
        conn.createStatement().execute("upsert into " + tableName + " (k) values ('b  ')");
        conn.createStatement().execute("upsert into " + tableName + " (k) values ('b  a')");
        conn.createStatement().execute("upsert into " + tableName + " (k) values (' b  ')");
        conn.createStatement().execute("upsert into " + tableName + " (k) values ('c')");
        conn.commit();

        String query = "select k from " + tableName + " WHERE rtrim(k)='b'";
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertValueEqualsResultSet(rs, expectedResults);
        
        rs = conn.createStatement().executeQuery("explain " + query);
        assertTrue(QueryUtil.getExplainPlan(rs).contains("RANGE SCAN OVER " + tableName));
        
        conn.close();
    }
}
