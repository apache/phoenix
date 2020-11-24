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

import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class CastAndCoerceIT extends BaseQueryIT {

    public CastAndCoerceIT(String indexDDL, boolean columnEncoded, boolean keepDeletedCells) {
        super(indexDDL, columnEncoded, keepDeletedCells);
    }
    
    @Parameters(name="CastAndCoerceIT_{index}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Object> data() {
        return BaseQueryIT.allIndexes();
    }
    
    @Test
    public void testCastOperatorInSelect() throws Exception {
        String query = "SELECT CAST(a_integer AS decimal)/2 FROM " + tableName + " WHERE ?=organization_id and 5=a_integer";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(BigDecimal.valueOf(2.5), rs.getBigDecimal(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCastOperatorInWhere() throws Exception {
        String query = "SELECT a_integer FROM " + tableName + " WHERE ?=organization_id and 2.5 = CAST(a_integer AS DECIMAL)/2 ";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(5, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCoerceIntegerToLong() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " WHERE organization_id=? AND x_long >= x_integer";
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCoerceLongToDecimal1() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " WHERE organization_id=? AND x_decimal > x_integer";
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCoerceLongToDecimal2() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " WHERE organization_id=? AND x_integer <= x_decimal";
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCoerceTinyIntToSmallInt() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " WHERE organization_id=? AND a_byte >= a_short";
        String url = getUrl();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    
    @Test
    public void testCoerceDateToBigInt() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        PreparedStatement statement;
        ResultSet rs;
        String query;
        long dateAsLong;
        BigDecimal dateAsDecimal;
        String url;
        Connection conn;
        url = getUrl();
        conn = DriverManager.getConnection(url, props);
        conn.setAutoCommit(true);
        conn.createStatement().execute("UPSERT INTO " + tableName + " (organization_id,entity_id,a_time,a_timestamp) SELECT organization_id,entity_id,a_date,a_date FROM " + tableName);

        conn = DriverManager.getConnection(url, props);
        try {
            query = "SELECT entity_id, CAST(a_date AS BIGINT) FROM " + tableName + " WHERE organization_id=? AND a_date IS NOT NULL LIMIT 1";
            statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(1));
            dateAsLong = rs.getLong(2);
            assertFalse(rs.next());
        
            query = "SELECT entity_id FROM " + tableName + " WHERE organization_id=? AND a_date = CAST(? AS DATE) LIMIT 1";
            statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setLong(2, dateAsLong);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(1));
            assertFalse(rs.next());

            query = "SELECT entity_id, CAST(a_time AS BIGINT) FROM " + tableName + " WHERE organization_id=? AND a_time IS NOT NULL LIMIT 1";
            statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(1));
            dateAsLong = rs.getLong(2);
            assertFalse(rs.next());
        
            query = "SELECT entity_id FROM " + tableName + " WHERE organization_id=? AND a_time = CAST(? AS TIME) LIMIT 1";
            statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setLong(2, dateAsLong);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(1));
            assertFalse(rs.next());

            query = "SELECT entity_id, CAST(a_timestamp AS DECIMAL) FROM " + tableName + " WHERE organization_id=? AND a_timestamp IS NOT NULL LIMIT 1";
            statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(1));
            dateAsDecimal = rs.getBigDecimal(2);
            assertFalse(rs.next());
        
            query = "SELECT entity_id FROM " + tableName + " WHERE organization_id=? AND a_timestamp = CAST(? AS TIMESTAMP) LIMIT 1";
            statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setBigDecimal(2, dateAsDecimal);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(1));
            assertFalse(rs.next());


            query = "SELECT entity_id, CAST(a_timestamp AS BIGINT) FROM " + tableName + " WHERE organization_id=? AND a_timestamp IS NOT NULL LIMIT 1";
            statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(1));
            dateAsLong = rs.getLong(2);
            assertFalse(rs.next());
        
            query = "SELECT entity_id FROM " + tableName + " WHERE organization_id=? AND a_timestamp = CAST(? AS TIMESTAMP) LIMIT 1";
            statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setLong(2, dateAsLong);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(ROW1, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
}
