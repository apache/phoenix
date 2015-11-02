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
package org.apache.phoenix.expression;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

public class NullValueTest extends BaseConnectionlessQueryTest {
    
    @Test
    public void testComparisonExpressionWithNullOperands() throws Exception {
        String[] query = {"SELECT 'a' >= ''", 
                          "SELECT '' < 'a'", 
                          "SELECT '' = ''"};
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            for (String q : query) {
                ResultSet rs = conn.createStatement().executeQuery(q);
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertEquals(false, rs.getBoolean(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }       
    }
    
    @Test
    public void testAndExpressionWithNullOperands() throws Exception {
        String[] query = {"SELECT 'b' >= 'a' and '' < 'b'", 
                          "SELECT 'b' >= '' and 'a' < 'b'",
                          "SELECT 'a' >= 'b' and 'a' < ''",
                          "SELECT '' >= 'a' and 'b' < 'a'",
                          "SELECT 'a' >= '' and '' < 'a'"};
        Boolean[] result = {null,
                            null,
                            Boolean.FALSE,
                            Boolean.FALSE,
                            null};
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            for (int i = 0; i < query.length; i++) {
                ResultSet rs = conn.createStatement().executeQuery(query[i]);
                assertTrue(rs.next());
                assertEquals(result[i], rs.getObject(1));
                assertEquals(false, rs.getBoolean(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }       
    }
    
    @Test
    public void testOrExpressionWithNullOperands() throws Exception {
        String[] query = {"SELECT 'b' >= 'a' or '' < 'b'", 
                          "SELECT 'b' >= '' or 'a' < 'b'",
                          "SELECT 'a' >= 'b' or 'a' < ''",
                          "SELECT '' >= 'a' or 'b' < 'a'",
                          "SELECT 'a' >= '' or '' < 'a'"};
        Boolean[] result = {Boolean.TRUE,
                            Boolean.TRUE,
                            null,
                            null,
                            null};
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            for (int i = 0; i < query.length; i++) {
                ResultSet rs = conn.createStatement().executeQuery(query[i]);
                assertTrue(rs.next());
                assertEquals(result[i], rs.getObject(1));
                assertEquals(Boolean.TRUE.equals(result[i]) ? true : false, rs.getBoolean(1));
                assertFalse(rs.next());
            }
        } finally {
            conn.close();
        }       
    }

}
