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
import static org.apache.phoenix.util.TestUtil.ROW2;
import static org.apache.phoenix.util.TestUtil.ROW3;
import static org.apache.phoenix.util.TestUtil.ROW4;
import static org.apache.phoenix.util.TestUtil.ROW7;
import static org.apache.phoenix.util.TestUtil.ROW8;
import static org.apache.phoenix.util.TestUtil.ROW9;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;


@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class IntArithmeticIT extends BaseQueryIT {

    public IntArithmeticIT(String indexDDL, boolean columnEncoded, boolean keepDeletedCells) {
        super(indexDDL, columnEncoded, keepDeletedCells);
    }
    
    @Parameters(name="IntArithmeticIT_{index}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Object> data() {
        return BaseQueryIT.allIndexes();
    }
    
    @Test
    public void testIntSubtractionExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where A_INTEGER - 4  <= 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
    public void testSelectWithSubtractionExpression() throws Exception {
        String query = "SELECT entity_id, x_integer - 4 FROM " + tableName + " where  x_integer - 4 = 0";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
        String query = "SELECT entity_id FROM " + tableName + " where A_INTEGER = 5 - 1 - 2";
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
    public void testIntDivideExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where A_INTEGER / 3 > 2";
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
    public void testIntToDecimalDivideExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where A_INTEGER / 3.0 > 2";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
        String query = "SELECT entity_id FROM " + tableName + " where A_INTEGER = 9 / 3 / 3";
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
    public void testSelectWithDivideExpression() throws Exception {
        String query = "SELECT entity_id, a_integer/3 FROM " + tableName + " where  a_integer = 9";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
    public void testIntMultiplyExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where A_INTEGER * 2 = 16";
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
    public void testIntToDecimalMultiplyExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where A_INTEGER * 1.5 > 9";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
    public void testIntAddExpression() throws Exception {
        String query = "SELECT entity_id FROM " + tableName + " where A_INTEGER + 2 = 4";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
    
}
