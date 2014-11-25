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

import static org.apache.phoenix.util.TestUtil.CUSTOM_ENTITY_DATA_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;



public class CustomEntityDataIT extends BaseClientManagedTimeIT {
    
    protected static void initTableValues(String tenantId, byte[][] splits, long ts) throws Exception {
        ensureTableCreated(getUrl(),CUSTOM_ENTITY_DATA_FULL_NAME,null, ts-2);
            
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        // Insert all rows at ts
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " +
                "CORE.CUSTOM_ENTITY_DATA(" +
                "    ORGANIZATION_ID, " +
                "    KEY_PREFIX, " +
                "    CUSTOM_ENTITY_DATA_ID, " +
                "    CREATED_BY, " +
                "    CREATED_DATE, " +
                "    CURRENCY_ISO_CODE, " +
                "    DELETED, " +
                "    DIVISION, " +
                "    LAST_UPDATE, " +
                "    LAST_UPDATE_BY," +
                "    NAME," +
                "    OWNER," +
                "    SYSTEM_MODSTAMP," +
                "    VAL0," +
                "    VAL1," +
                "    VAL2," +
                "    VAL3," +
                "    VAL4," +
                "    VAL5," +
                "    VAL6," +
                "    VAL7," +
                "    VAL8," +
                "    VAL9)" +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            
        // Insert all rows at ts
        stmt.setString(1, tenantId);
        stmt.setString(2, ROW2.substring(0,3));
        stmt.setString(3, ROW2.substring(3));
        stmt.setString(4, "Curly");
        stmt.setDate(5, new Date(1));
        stmt.setString(6, "ISO");
        stmt.setString(7, "0");
        stmt.setBigDecimal(8, new BigDecimal(1));
        stmt.setDate(9, new Date(2));
        stmt.setString(10, "Curly");
        stmt.setString(11, "Curly");
        stmt.setString(12, "Curly");
        stmt.setDate(13, new Date(2));
        stmt.setString(14, "2");
        stmt.setString(15, "2");
        stmt.setString(16, "2");
        stmt.setString(17, "2");
        stmt.setString(18, "2");
        stmt.setString(19, "2");
        stmt.setString(20, "2");
        stmt.setString(21, "2");
        stmt.setString(22, "2");
        stmt.setString(23, "2");
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setString(2, ROW5.substring(0,3));
        stmt.setString(3, ROW5.substring(3));
        stmt.setString(4, "Moe");
        stmt.setDate(5, new Date(1));
        stmt.setString(6, "ISO");
        stmt.setString(7, "0");
        stmt.setBigDecimal(8, new BigDecimal(1));
        stmt.setDate(9, new Date(2));
        stmt.setString(10, "Moe");
        stmt.setString(11, "Moe");
        stmt.setString(12, "Moe");
        stmt.setDate(13, new Date(2));
        stmt.setString(14, "5");
        stmt.setString(15, "5");
        stmt.setString(16, "5");
        stmt.setString(17, "5");
        stmt.setString(18, "5");
        stmt.setString(19, "5");
        stmt.setString(20, "5");
        stmt.setString(21, "5");
        stmt.setString(22, "5");
        stmt.setString(23, "5");
        stmt.execute();

        stmt.setString(1, tenantId);
        stmt.setString(2, ROW9.substring(0,3));
        stmt.setString(3, ROW9.substring(3));
        stmt.setString(4, "Larry");
        stmt.setDate(5, new Date(1));
        stmt.setString(6, "ISO");
        stmt.setString(7, "0");
        stmt.setBigDecimal(8, new BigDecimal(1));
        stmt.setDate(9, new Date(2));
        stmt.setString(10, "Larry");
        stmt.setString(11, "Larry");
        stmt.setString(12, "Larry");
        stmt.setDate(13, new Date(2));
        stmt.setString(14, "v9");
        stmt.setString(15, "v9");
        stmt.setString(16, "v9");
        stmt.setString(17, "v9");
        stmt.setString(18, "v9");
        stmt.setString(19, "v9");
        stmt.setString(20, "v9");
        stmt.setString(21, "v9");
        stmt.setString(22, "v9");
        stmt.setString(23, "v9");
        stmt.execute();
        
        conn.commit();
        conn.close();
    }    

    @Test
    public void testUngroupedAggregation() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT count(1) FROM CORE.CUSTOM_ENTITY_DATA WHERE organization_id=?";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Connection conn = DriverManager.getConnection(url, PropertiesUtil.deepCopy(TEST_PROPERTIES));
        try {
            initTableValues(tenantId, getDefaultSplits(getOrganizationId()), ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals(3, rs.getLong(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScan() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        String query = "SELECT CREATED_BY,CREATED_DATE,CURRENCY_ISO_CODE,DELETED,DIVISION,LAST_UPDATE,LAST_UPDATE_BY,NAME,OWNER,SYSTEM_MODSTAMP,VAL0,VAL1,VAL2,VAL3,VAL4,VAL5,VAL6,VAL7,VAL8,VAL9 FROM CORE.CUSTOM_ENTITY_DATA WHERE organization_id=?";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Connection conn = DriverManager.getConnection(url, PropertiesUtil.deepCopy(TEST_PROPERTIES));
        try {
            initTableValues(tenantId, getDefaultSplits(getOrganizationId()), ts);
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("Curly", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("Moe", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("Larry", rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testWhereStringConcatExpression() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        initTableValues(tenantId, getDefaultSplits(getOrganizationId()), ts);
        String query = "SELECT KEY_PREFIX||CUSTOM_ENTITY_DATA_ID FROM CORE.CUSTOM_ENTITY_DATA where '00A'||val0 LIKE '00A2%'";
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs=statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW2, rs.getString(1));
            assertFalse(rs.next());
        }
        finally {
            conn.close();
        }
    }
}
