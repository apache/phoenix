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

import static org.apache.phoenix.util.TestUtil.A_VALUE;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.phoenix.thirdparty.com.google.common.primitives.Doubles;
import org.apache.phoenix.thirdparty.com.google.common.primitives.Floats;

@Category(ParallelStatsDisabledTest.class)
public class NumericArithmeticIT extends ParallelStatsDisabledIT {
    private String tableName;
    
    @Before
    public void setup() throws Exception {
        String tenantId = getOrganizationId();
        this.tableName =
                initATableValues(generateUniqueName(), tenantId, getDefaultSplits(tenantId),
                    new Date(System.currentTimeMillis()), null, getUrl(), "COLUMN_ENCODED_BYTES=0");
    }
    
    @Test
    public void testDecimalAddExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where A_INTEGER + X_DECIMAL > 11";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
   
    @Test
    public void testDecimalSubtraction1Expression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where A_INTEGER - 3.5  <= 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertValueEqualsResultSet(rs, Arrays.<Object>asList(ROW1, ROW2, ROW3));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDecimalSubtraction2Expression() throws Exception {// check if decimal part makes a difference
        String query = "SELECT entity_id FROM " + tableName + " where X_DECIMAL - 3.5  > 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDecimalMultiplyExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where X_DECIMAL * A_INTEGER > 29.5";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertValueEqualsResultSet(rs, Arrays.<Object>asList(ROW8, ROW9));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTernarySubtractionExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where  X_INTEGER - X_LONG - 10  < 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW7);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW9);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testLongSubtractionExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where X_LONG - 1  < 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSmallIntDivideExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where a_short / 135 = 1";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testLongMultiplyExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where X_LONG * 2 * 2 = 20";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW7, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSmallIntSubtractionExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where a_short - 129  = 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDoubleAddExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where a_double + a_float > 0.08";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testUnsignedDoubleAddExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where a_unsigned_double + a_unsigned_float > 0.08";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW8, rs.getString(1));
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDoubleSubtractionExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where a_double - CAST(0.0002 AS DOUBLE)  < 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW1);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDoubleDivideExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where a_double / CAST(3.0 AS DOUBLE) = 0.0003";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDoubleMultiplyExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where A_DOUBLE * CAST(2.0 AS DOUBLE) = 0.0002";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW1, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    @Test
    public void testScanByByteValue() throws Exception {
        String query = "SELECT a_string, b_string, a_byte FROM " + tableName + " WHERE ?=organization_id and 1=a_byte";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, getOrganizationId());
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertEquals(rs.getByte(3), 1);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByShortValue() throws Exception {
        String query = "SELECT a_string, b_string, a_short FROM " + tableName + " WHERE ?=organization_id and 128=a_short";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, getOrganizationId());
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertEquals(rs.getShort("a_short"), 128);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByFloatValue() throws Exception {
        String query = "SELECT a_string, b_string, a_float FROM " + tableName + " WHERE ?=organization_id and ?=a_float";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, getOrganizationId());
            statement.setFloat(2, 0.01f);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByUnsignedFloatValue() throws Exception {
        String query = "SELECT a_string, b_string, a_unsigned_float FROM " + tableName + " WHERE ?=organization_id and ?=a_unsigned_float";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, getOrganizationId());
            statement.setFloat(2, 0.01f);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByDoubleValue() throws Exception {
        String query = "SELECT a_string, b_string, a_double FROM " + tableName + " WHERE ?=organization_id and ?=a_double";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, getOrganizationId());
            statement.setDouble(2, 0.0001);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Doubles.compare(rs.getDouble(3), 0.0001) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanByUnsignedDoubleValue() throws Exception {
        String query = "SELECT a_string, b_string, a_unsigned_double FROM " + tableName + " WHERE ?=organization_id and ?=a_unsigned_double";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, getOrganizationId());
            statement.setDouble(2, 0.0001);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), A_VALUE);
            assertEquals(rs.getString("B_string"), B_VALUE);
            assertTrue(Doubles.compare(rs.getDouble(3), 0.0001) == 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testScanWithNoWhereClause() throws Exception {
        String query = "SELECT y_integer FROM " + tableName;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            for (int i =0; i < 8; i++) {
                assertTrue (rs.next());
                assertEquals(0, rs.getInt(1));
                assertTrue(rs.wasNull());
            }
            assertTrue (rs.next());
            assertEquals(300, rs.getInt(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
   
}
