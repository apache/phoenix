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

import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.E_VALUE;
import static org.apache.phoenix.util.TestUtil.MILLIS_IN_DAY;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW5;
import static org.apache.phoenix.util.TestUtil.ROW6;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;


@RunWith(Parameterized.class)
public class ClientTimeArithmeticQueryIT extends BaseQueryIT {

    public ClientTimeArithmeticQueryIT(String indexDDL) {
        super(indexDDL);
    }
    
    @Parameters(name="{0}")
    public static Collection<Object> data() {
        return QueryIT.data();
    }
    
    @Test
    public void testDateAdd() throws Exception {
        String query = "SELECT entity_id, b_string FROM ATABLE WHERE a_date + CAST(0.5 AS DOUBLE) < ?";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setDate(1, new Date(System.currentTimeMillis() + MILLIS_IN_DAY));
            ResultSet rs = statement.executeQuery();
            @SuppressWarnings("unchecked")
            List<List<Object>> expectedResults = Lists.newArrayList(
                    Arrays.<Object>asList(ROW1, B_VALUE),
                    Arrays.<Object>asList( ROW4, B_VALUE), 
                    Arrays.<Object>asList(ROW7, B_VALUE));
            assertValuesEqualsResultSet(rs, expectedResults);
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDecimalAddExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where A_INTEGER + X_DECIMAL > 11";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testDoubleAddExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where a_double + a_float > 0.08";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
        String query = "SELECT entity_id FROM aTable where a_unsigned_double + a_unsigned_float > 0.08";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(
            value="RV_RETURN_VALUE_IGNORED",
            justification="Test code.")
    @Test
    public void testValidArithmetic() throws Exception {
        String[] queries = new String[] { 
                "SELECT entity_id,organization_id FROM atable where (A_DATE - A_DATE) * 5 < 0",
                "SELECT entity_id,organization_id FROM atable where 1 + A_DATE  < A_DATE",
                "SELECT entity_id,organization_id FROM atable where A_DATE - 1 < A_DATE",
                "SELECT entity_id,organization_id FROM atable where A_INTEGER - 45 < 0",
                "SELECT entity_id,organization_id FROM atable where X_DECIMAL / 45 < 0", };

        for (String query : queries) {
            Properties props = new Properties();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            Connection conn = DriverManager.getConnection(getUrl(), props);
            try {
                PreparedStatement statement = conn.prepareStatement(query);
                statement.executeQuery();
            }
            finally {
                conn.close();
            }
        }
    }
    
    @Test
    public void testIntSubtractionExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where A_INTEGER - 4  <= 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertValueEqualsResultSet(rs, Arrays.<Object>asList(ROW1, ROW2, ROW3, ROW4));
        } finally {
            conn.close();
        }
    }
    @Test
    public void testDecimalSubtraction1Expression() throws Exception {
        String query = "SELECT entity_id FROM aTable where A_INTEGER - 3.5  <= 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
        String query = "SELECT entity_id FROM aTable where X_DECIMAL - 3.5  > 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testLongSubtractionExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where X_LONG - 1  < 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testDoubleSubtractionExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where a_double - CAST(0.0002 AS DOUBLE)  < 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testSmallIntSubtractionExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where a_short - 129  = 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testTernarySubtractionExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where  X_INTEGER - X_LONG - 10  < 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testSelectWithSubtractionExpression() throws Exception {
        String query = "SELECT entity_id, x_integer - 4 FROM aTable where  x_integer - 4 = 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW8);
            assertEquals(rs.getInt(2), 0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    @Test
    public void testConstantSubtractionExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where A_INTEGER = 5 - 1 - 2";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testIntDivideExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where A_INTEGER / 3 > 2";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testDoubleDivideExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where a_double / CAST(3.0 AS DOUBLE) = 0.0003";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testSmallIntDivideExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where a_short / 135 = 1";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testIntToDecimalDivideExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where A_INTEGER / 3.0 > 2";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertValueEqualsResultSet(rs, Arrays.<Object>asList(ROW7, ROW8, ROW9));
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testConstantDivideExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where A_INTEGER = 9 / 3 / 3";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testSelectWithDivideExpression() throws Exception {
        String query = "SELECT entity_id, a_integer/3 FROM aTable where  a_integer = 9";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW9, rs.getString(1));
            assertEquals(3, rs.getInt(2));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testNegateExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where A_INTEGER - 4 = -1";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW3, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testIntMultiplyExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where A_INTEGER * 2 = 16";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testDoubleMultiplyExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where A_DOUBLE * CAST(2.0 AS DOUBLE) = 0.0002";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testLongMultiplyExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where X_LONG * 2 * 2 = 20";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testIntToDecimalMultiplyExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where A_INTEGER * 1.5 > 9";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertValueEqualsResultSet(rs, Arrays.<Object>asList(ROW7, ROW8, ROW9));
        } finally {
            conn.close();
        }
    }
    

    @Test
    public void testDecimalMultiplyExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where X_DECIMAL * A_INTEGER > 29.5";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
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
    public void testIntAddExpression() throws Exception {
        String query = "SELECT entity_id FROM aTable where A_INTEGER + 2 = 4";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(ROW2, rs.getString(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testCoalesceFunction() throws Exception {
        String query = "SELECT entity_id FROM aTable WHERE coalesce(X_DECIMAL,0.0) = 0.0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO aTable(organization_id,entity_id,x_decimal) values(?,?,?)");
        stmt.setString(1, getOrganizationId());
        stmt.setString(2, ROW1);
        stmt.setBigDecimal(3, BigDecimal.valueOf(1.0));
        stmt.execute();
        stmt.setString(2, ROW3);
        stmt.setBigDecimal(3, BigDecimal.valueOf(2.0));
        stmt.execute();
        stmt.setString(2, ROW4);
        stmt.setBigDecimal(3, BigDecimal.valueOf(3.0));
        stmt.execute();
        stmt.setString(2, ROW5);
        stmt.setBigDecimal(3, BigDecimal.valueOf(0.0));
        stmt.execute();
        stmt.setString(2, ROW6);
        stmt.setBigDecimal(3, BigDecimal.valueOf(4.0));
        stmt.execute();
        conn.commit();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20)); // Execute at timestamp 2
        conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW2);
            assertTrue (rs.next());
            assertEquals(rs.getString(1), ROW5);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testDateSubtract() throws Exception {
        String query = "SELECT entity_id, b_string FROM ATABLE WHERE a_date - CAST(0.5 AS DOUBLE) > ?";
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 5); // Run query at timestamp 5
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setDate(1, new Date(System.currentTimeMillis() + MILLIS_IN_DAY));
            ResultSet rs = statement.executeQuery();
            @SuppressWarnings("unchecked")
            List<List<Object>> expectedResults = Lists.newArrayList(
                    Arrays.<Object>asList(ROW3, E_VALUE),
                    Arrays.<Object>asList( ROW6, E_VALUE), 
                    Arrays.<Object>asList(ROW9, E_VALUE));
            assertValuesEqualsResultSet(rs, expectedResults);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDateDateSubtract() throws Exception {
        String url;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        
        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 15);
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement statement = conn.prepareStatement("UPSERT INTO ATABLE(organization_id,entity_id,a_time) VALUES(?,?,?)");
        statement.setString(1, getOrganizationId());
        statement.setString(2, ROW2);
        statement.setDate(3, date);
        statement.execute();
        statement.setString(2, ROW3);
        statement.setDate(3, date);
        statement.execute();
        statement.setString(2, ROW4);
        statement.setDate(3, new Date(date.getTime() + TestUtil.MILLIS_IN_DAY - 1));
        statement.execute();
        statement.setString(2, ROW6);
        statement.setDate(3, new Date(date.getTime() + TestUtil.MILLIS_IN_DAY - 1));
        statement.execute();
        statement.setString(2, ROW9);
        statement.setDate(3, date);
        statement.execute();
        conn.commit();
        conn.close();

        url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + (ts + 25);
        conn = DriverManager.getConnection(url, props);
        try {
            statement = conn.prepareStatement("SELECT entity_id, b_string FROM ATABLE WHERE a_date - a_time > 1");
            ResultSet rs = statement.executeQuery();
            @SuppressWarnings("unchecked")
            List<List<Object>> expectedResults = Lists.newArrayList(
                    Arrays.<Object>asList(ROW3, E_VALUE),
                    Arrays.<Object>asList( ROW6, E_VALUE), 
                    Arrays.<Object>asList(ROW9, E_VALUE));
            assertValuesEqualsResultSet(rs, expectedResults);
        } finally {
            conn.close();
        }
    }

}
