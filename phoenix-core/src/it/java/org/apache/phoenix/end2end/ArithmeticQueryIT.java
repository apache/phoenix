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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;


public class ArithmeticQueryIT extends BaseHBaseManagedTimeIT {

    @Test
    public void testDecimalUpsertValue() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE testDecimalArithmetic" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, " +
                    "col1 DECIMAL(31,0), col2 DECIMAL(5), col3 DECIMAL(5,2), col4 DECIMAL)";
            createTestTable(getUrl(), ddl);
            
            // Test upsert correct values 
            String query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3, col4) VALUES(?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "valueOne");
            stmt.setBigDecimal(2, new BigDecimal("123456789123456789"));
            stmt.setBigDecimal(3, new BigDecimal("12345"));
            stmt.setBigDecimal(4, new BigDecimal("12.34"));
            stmt.setBigDecimal(5, new BigDecimal("12345.6789"));
            stmt.execute();
            conn.commit();
            
            query = "SELECT col1, col2, col3, col4 FROM testDecimalArithmetic WHERE pk = 'valueOne'";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("123456789123456789"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("12345"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("12.34"), rs.getBigDecimal(3));
            assertEquals(new BigDecimal("12345.6789"), rs.getBigDecimal(4));
            assertFalse(rs.next());
            
            query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3) VALUES(?,?,?,?)";
            stmt = conn.prepareStatement(query);
            stmt.setString(1, "valueTwo");
            stmt.setBigDecimal(2, new BigDecimal("1234567890123456789012345678901.12345"));
            stmt.setBigDecimal(3, new BigDecimal("12345.6789"));
            stmt.setBigDecimal(4, new BigDecimal("123.45678"));
            stmt.execute();
            conn.commit();
            
            query = "SELECT col1, col2, col3 FROM testDecimalArithmetic WHERE pk = 'valueTwo'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("1234567890123456789012345678901"), rs.getBigDecimal(1));
            assertEquals(new BigDecimal("12345"), rs.getBigDecimal(2));
            assertEquals(new BigDecimal("123.45"), rs.getBigDecimal(3));
            assertFalse(rs.next());
            
            // Test upsert incorrect values and confirm exceptions would be thrown.
            try {
                query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3) VALUES(?,?,?,?)";
                stmt = conn.prepareStatement(query);
                stmt.setString(1, "badValues");
                // one more than max_precision
                stmt.setBigDecimal(2, new BigDecimal("12345678901234567890123456789012"));
                stmt.setBigDecimal(3, new BigDecimal("12345")); 
                stmt.setBigDecimal(4, new BigDecimal("123.45"));
                stmt.execute();
                conn.commit();
                fail("Should have caught bad values.");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
            try {
                query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3) VALUES(?,?,?,?)";
                stmt = conn.prepareStatement(query);
                stmt.setString(1, "badValues");
                stmt.setBigDecimal(2, new BigDecimal("123456"));
                // Exceeds specified precision by 1
                stmt.setBigDecimal(3, new BigDecimal("123456"));
                stmt.setBigDecimal(4, new BigDecimal("123.45"));
                stmt.execute();
                conn.commit();
                fail("Should have caught bad values.");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDecimalUpsertSelect() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE source" + 
                    " (pk VARCHAR NOT NULL PRIMARY KEY, col1 DECIMAL(5,2), col2 DECIMAL(5,1), col3 DECIMAL(5,2), col4 DECIMAL(4,4))";
            createTestTable(getUrl(), ddl);
            ddl = "CREATE TABLE target" + 
                    " (pk VARCHAR NOT NULL PRIMARY KEY, col1 DECIMAL(5,1), col2 DECIMAL(5,2), col3 DECIMAL(4,4))";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO source(pk, col1) VALUES(?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "1");
            stmt.setBigDecimal(2, new BigDecimal("100.12"));
            stmt.execute();
            conn.commit();
            stmt.setString(1, "2");
            stmt.setBigDecimal(2, new BigDecimal("100.34"));
            stmt.execute();
            conn.commit();
            
            // Evaluated on client side.
            // source and target in different tables, values scheme compatible.
            query = "UPSERT INTO target(pk, col2) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col2 FROM target";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.12"), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.34"), rs.getBigDecimal(1));
            assertFalse(rs.next());
            // source and target in different tables, values requires scale chopping.
            query = "UPSERT INTO target(pk, col1) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col1 FROM target";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.1"), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.3"), rs.getBigDecimal(1));
            assertFalse(rs.next());
            // source and target in different tables, values scheme incompatible.
            try {
                query = "UPSERT INTO target(pk, col3) SELECT pk, col1 from source";
                stmt = conn.prepareStatement(query);
                stmt.execute();
                conn.commit();
                fail("Should have caught bad upsert.");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
            
            // Evaluate on server side.
            conn.setAutoCommit(true);
            // source and target in same table, values scheme compatible.
            query = "UPSERT INTO source(pk, col3) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col3 FROM source";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.12"), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.34"), rs.getBigDecimal(1));
            assertFalse(rs.next());
            // source and target in same table, values requires scale chopping.
            query = "UPSERT INTO source(pk, col2) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col2 FROM source";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.1"), rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.3"), rs.getBigDecimal(1));
            assertFalse(rs.next());
            // source and target in same table, values scheme incompatible.
            query = "UPSERT INTO source(pk, col4) SELECT pk, col1 from source";
            stmt = conn.prepareStatement(query);
            stmt.execute();
            conn.commit();
            query = "SELECT col4 FROM source";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertNull(rs.getBigDecimal(1));
            assertTrue(rs.next());
            assertNull(rs.getBigDecimal(1));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDecimalAveraging() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE testDecimalArithmetic" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, col1 DECIMAL(31, 11), col2 DECIMAL(31,1), col3 DECIMAL(38,1))";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3) VALUES(?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "1");
            stmt.setBigDecimal(2, new BigDecimal("99999999999999999999.1"));
            stmt.setBigDecimal(3, new BigDecimal("99999999999999999999.1"));
            stmt.setBigDecimal(4, new BigDecimal("9999999999999999999999999999999999999.1"));
            stmt.execute();
            conn.commit();
            stmt.setString(1, "2");
            stmt.setBigDecimal(2, new BigDecimal("0"));
            stmt.setBigDecimal(3, new BigDecimal("0"));
            stmt.setBigDecimal(4, new BigDecimal("0"));
            stmt.execute();
            conn.commit();
            stmt.setString(1, "3");
            stmt.setBigDecimal(2, new BigDecimal("0"));
            stmt.setBigDecimal(3, new BigDecimal("0"));
            stmt.setBigDecimal(4, new BigDecimal("0"));
            stmt.execute();
            conn.commit();
            
            // Averaging
            // result scale should be: max(max(ls, rs), 4).
            // We are not imposing restriction on precisioin.
            query = "SELECT avg(col1) FROM testDecimalArithmetic";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("33333333333333333333.03333333333"), result);
            
            query = "SELECT avg(col2) FROM testDecimalArithmetic";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("33333333333333333333.0333"), result);
            
            // We cap our decimal to a precision of 38.
            query = "SELECT avg(col3) FROM testDecimalArithmetic";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("3333333333333333333333333333333333333"), result);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testRandomFunction() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE testRandomFunction (pk VARCHAR NOT NULL PRIMARY KEY)";
            createTestTable(getUrl(), ddl);
            conn.createStatement().execute("upsert into testRandomFunction values ('x')");
            conn.createStatement().execute("upsert into testRandomFunction values ('y')");
            conn.createStatement().execute("upsert into testRandomFunction values ('z')");
            conn.commit();

            ResultSet rs = conn.createStatement().executeQuery("select rand(), rand(), rand(1), rand(2), rand(1) from testRandomFunction");
            assertTrue(rs.next());
            double rand0 = rs.getDouble(1);
            double rand1 = rs.getDouble(3);
            double rand2 = rs.getDouble(4);
            assertTrue(rs.getDouble(1) != rs.getDouble(2));
            assertTrue(rs.getDouble(2) != rs.getDouble(3));
            assertTrue(rs.getDouble(3) == rs.getDouble(5));
            assertTrue(rs.getDouble(4) != rs.getDouble(5));
            assertTrue(rs.next());
            assertTrue(rand0 != rs.getDouble(1));
            assertTrue(rand1 != rs.getDouble(3));
            assertTrue(rand2 != rs.getDouble(4));
            double rand01 = rs.getDouble(1);
            double rand11 = rs.getDouble(3);
            double rand21 = rs.getDouble(4);
            assertTrue(rs.getDouble(1) != rs.getDouble(2));
            assertTrue(rs.getDouble(2) != rs.getDouble(3));
            assertTrue(rs.getDouble(3) == rs.getDouble(5));
            assertTrue(rs.getDouble(4) != rs.getDouble(5));
            assertTrue(rs.next());
            assertTrue(rand01 != rs.getDouble(1));
            assertTrue(rand11 != rs.getDouble(3));
            assertTrue(rand21 != rs.getDouble(4));
            assertTrue(rs.getDouble(1) != rs.getDouble(2));
            assertTrue(rs.getDouble(2) != rs.getDouble(3));
            assertTrue(rs.getDouble(3) == rs.getDouble(5));
            assertTrue(rs.getDouble(4) != rs.getDouble(5));
            double rand12 = rs.getDouble(3);

            rs = conn.createStatement().executeQuery("select rand(), rand(), rand(1), rand(2), rand(1) from testRandomFunction");
            assertTrue(rs.next());
            assertTrue(rs.getDouble(1) != rs.getDouble(2));
            assertTrue(rs.getDouble(2) != rs.getDouble(3));
            assertTrue(rs.getDouble(3) == rs.getDouble(5));
            assertTrue(rs.getDouble(4) != rs.getDouble(5));
            assertTrue(rand0 != rs.getDouble(1));
            assertTrue(rand1 == rs.getDouble(3));
            assertTrue(rand2 == rs.getDouble(4));
            assertTrue(rs.next());
            assertTrue(rand01 != rs.getDouble(1));
            assertTrue(rand11 == rs.getDouble(3));
            assertTrue(rand21 == rs.getDouble(4));
            assertTrue(rs.next());
            assertTrue(rand12 == rs.getDouble(3));

            ddl = "CREATE TABLE testRandomFunction1 (pk VARCHAR NOT NULL PRIMARY KEY, v1 UNSIGNED_DOUBLE)";
            createTestTable(getUrl(), ddl);
            conn.createStatement().execute("upsert into testRandomFunction1 select pk, rand(1) from testRandomFunction");
            conn.commit();

            rs = conn.createStatement().executeQuery("select count(*) from testRandomFunction1 where v1 = rand(1)");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));

            rs = conn.createStatement().executeQuery("select count(*) from testRandomFunction1 where v1 = rand(2)");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));

            conn.createStatement().execute("delete from testRandomFunction1 where v1 = rand(2)");
            conn.commit();

            rs = conn.createStatement().executeQuery("select count(*) from testRandomFunction1 where v1 = rand(1)");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));

            conn.setAutoCommit(true);
            conn.createStatement().execute("upsert into testRandomFunction1 select pk, rand(2) from testRandomFunction1");

            rs = conn.createStatement().executeQuery("select count(*) from testRandomFunction1 where v1 = rand(1)");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));

            rs = conn.createStatement().executeQuery("select count(*) from testRandomFunction1 where v1 = rand(2)");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));

            conn.createStatement().execute("delete from testRandomFunction1 where v1 = rand(2)");

            rs = conn.createStatement().executeQuery("select count(*) from testRandomFunction1 where v1 = rand(2)");
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        } finally {
            conn.close();
        }
    }

    @Test
    public void testDecimalArithmeticWithIntAndLong() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "CREATE TABLE testDecimalArithmetic" + 
                    "  (pk VARCHAR NOT NULL PRIMARY KEY, " +
                    "col1 DECIMAL(38,0), col2 DECIMAL(5, 2), col3 INTEGER, col4 BIGINT, col5 DECIMAL)";
            createTestTable(getUrl(), ddl);
            
            String query = "UPSERT INTO testDecimalArithmetic(pk, col1, col2, col3, col4, col5) VALUES(?,?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, "testValueOne");
            stmt.setBigDecimal(2, new BigDecimal("1234567890123456789012345678901"));
            stmt.setBigDecimal(3, new BigDecimal("123.45"));
            stmt.setInt(4, 10);
            stmt.setLong(5, 10L);
            stmt.setBigDecimal(6, new BigDecimal("111.111"));
            stmt.execute();
            conn.commit();

            stmt.setString(1, "testValueTwo");
            stmt.setBigDecimal(2, new BigDecimal("12345678901234567890123456789012345678"));
            stmt.setBigDecimal(3, new BigDecimal("123.45"));
            stmt.setInt(4, 10);
            stmt.setLong(5, 10L);
            stmt.setBigDecimal(6, new BigDecimal("123456789.0123456789"));
            stmt.execute();
            conn.commit();
            
            // INT has a default precision and scale of (10, 0)
            // LONG has a default precision and scale of (19, 0)
            query = "SELECT col1 + col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345678911"), result);
            
            query = "SELECT col1 + col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345678911"), result);
            
            query = "SELECT col2 + col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("133.45"), result);
            
            query = "SELECT col2 + col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("133.45"), result);
            
            query = "SELECT col5 + col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("121.111"), result);
            
            query = "SELECT col5 + col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("121.111"), result);
            
            query = "SELECT col1 - col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345678891"), result);
            
            query = "SELECT col1 - col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890123456789012345678891"), result);
            
            query = "SELECT col2 - col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("113.45"), result);
            
            query = "SELECT col2 - col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("113.45"), result);
            
            query = "SELECT col5 - col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("101.111"), result);
            
            query = "SELECT col5 - col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("101.111"), result);
            
            query = "SELECT col1 * col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.234567890123456789012345678901E+31"), result);
            
            query = "SELECT col1 * col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.234567890123456789012345678901E+31"), result);

            query = "SELECT col1 * col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.234567890123456789012345678901E+31"), result);
            
            try {
            	query = "SELECT col1 * col3 FROM testDecimalArithmetic WHERE pk='testValueTwo'";
            	stmt = conn.prepareStatement(query);
            	rs = stmt.executeQuery();
            	assertTrue(rs.next());
            	result = rs.getBigDecimal(1);
            	fail("Should have caught error.");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
            
            try {
            	query = "SELECT col1 * col4 FROM testDecimalArithmetic WHERE pk='testValueTwo'";
            	stmt = conn.prepareStatement(query);
            	rs = stmt.executeQuery();
            	assertTrue(rs.next());
            	result = rs.getBigDecimal(1);
            	fail("Should have caught error.");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.DATA_EXCEEDS_MAX_CAPACITY.getErrorCode(),e.getErrorCode());
            }
            
            query = "SELECT col4 * col5 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(0, result.compareTo(new BigDecimal("1111.11")));

            query = "SELECT col3 * col5 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(0, result.compareTo(new BigDecimal("1111.11")));
            
            query = "SELECT col2 * col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234.5"), result);
            
            // Result scale has value of 0
            query = "SELECT col1 / col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.2345678901234567890123456789E+29"), result);
            
            query = "SELECT col1 / col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.2345678901234567890123456789E+29"), result);
            
            // Result scale is 2.
            query = "SELECT col2 / col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("12.34"), result);
            
            query = "SELECT col2 / col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("12.34"), result);
            
            // col5 has NO_SCALE, so the result's scale is not expected to be truncated to col5 value's scale of 4
            query = "SELECT col5 / col3 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("11.1111"), result);
            
            query = "SELECT col5 / col4 FROM testDecimalArithmetic WHERE pk='testValueOne'";
            stmt = conn.prepareStatement(query);
            rs = stmt.executeQuery();
            assertTrue(rs.next());
            result = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("11.1111"), result);
        } finally {
            conn.close();
        }
    }
    @Test
    public void testSumDouble() throws Exception {
        initSumDoubleValues(null, getUrl());
        String query = "SELECT SUM(d) FROM SumDoubleTest";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.015)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSumUnsignedDouble() throws Exception {
        initSumDoubleValues(null, getUrl());
        String query = "SELECT SUM(ud) FROM SumDoubleTest";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Doubles.compare(rs.getDouble(1), 0.015)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSumFloat() throws Exception {
        initSumDoubleValues(null, getUrl());
        String query = "SELECT SUM(f) FROM SumDoubleTest";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Floats.compare(rs.getFloat(1), 0.15f)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testSumUnsignedFloat() throws Exception {
        initSumDoubleValues(null, getUrl());
        String query = "SELECT SUM(uf) FROM SumDoubleTest";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue (rs.next());
            assertTrue(Floats.compare(rs.getFloat(1), 0.15f)==0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    private void initIntegerTable(Connection conn) throws SQLException {
        String ddl = "CREATE TABLE ARITHMETIC_TEST (six INTEGER PRIMARY KEY, four INTEGER, three INTEGER)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO ARITHMETIC_TEST VALUES(6, 4, 3)";
        conn.createStatement().execute(dml);
        conn.commit();        
    }
    
    @Test
    public void testOrderOfOperationsAdditionSubtraction() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initIntegerTable(conn);
        ResultSet rs;
        
        // 6 + 4 - 3
        // 10 - 3
        // 7
        rs = conn.createStatement().executeQuery("SELECT six + four - three FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(7, rs.getLong(1));
        assertFalse(rs.next());
        
        // 4 - 3 + 6
        // 1 + 6
        // 7
        rs = conn.createStatement().executeQuery("SELECT four - three + six FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(7, rs.getLong(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testOrderOfOperationsAdditionMultiplication() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initIntegerTable(conn);
        ResultSet rs;
        
        // 6 + 4 * 3
        // 6 + 12
        // 18
        rs = conn.createStatement().executeQuery("SELECT six + four * three FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(18, rs.getLong(1));
        assertFalse(rs.next());
        
        // 4 * 3 + 6
        // 12 * 6     
        // 18
        rs = conn.createStatement().executeQuery("SELECT four * three + six FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(18, rs.getLong(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testOrderOfOperationsAdditionDivision() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initIntegerTable(conn);
        ResultSet rs;
        
        // 6 + 4 / 3
        // 6 + 1
        // 7
        rs = conn.createStatement().executeQuery("SELECT six + four / three FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(7, rs.getLong(1));
        assertFalse(rs.next());
        
        // 4 / 3 + 6
        // 1 + 6     
        // 7
        rs = conn.createStatement().executeQuery("SELECT four / three + six FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(7, rs.getLong(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testOrderOfOperationsAdditionModulus() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initIntegerTable(conn);
        ResultSet rs;
        
        // 6 + 4 % 3
        // 6 + 1
        // 7
        rs = conn.createStatement().executeQuery("SELECT six + four % three FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(7, rs.getLong(1));
        assertFalse(rs.next());
        
        // 4 % 3 + 6
        // 1 + 6
        // 7
        rs = conn.createStatement().executeQuery("SELECT four % three + six FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(7, rs.getLong(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testOrderOfOperationsSubtrationMultiplication() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initIntegerTable(conn);
        ResultSet rs;
        
        // 6 - 4 * 3
        // 6 - 12
        // -6
        rs = conn.createStatement().executeQuery("SELECT six - four * three FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(-6, rs.getLong(1));
        assertFalse(rs.next());
        
        // 4 * 3 - 6
        // 12 - 6     
        // 6
        rs = conn.createStatement().executeQuery("SELECT four * three - six FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(6, rs.getLong(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testOrderOfOperationsSubtractionDivision() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initIntegerTable(conn);
        ResultSet rs;
        
        // 6 - 4 / 3
        // 6 - 1     (integer division)
        // 5
        rs = conn.createStatement().executeQuery("SELECT six - four / three FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(5, rs.getLong(1));
        assertFalse(rs.next());
        
        // 4 / 3 - 6
        // 1 - 6     (integer division)
        // -5
        rs = conn.createStatement().executeQuery("SELECT four / three - six FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(-5, rs.getLong(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testOrderOfOperationsSubtractionModulus() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initIntegerTable(conn);
        ResultSet rs;
        
        // 6 - 4 % 3
        // 6 - 1
        // 5
        rs = conn.createStatement().executeQuery("SELECT six - four % three FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(5, rs.getLong(1));
        assertFalse(rs.next());
        
        // 4 % 3 - 6
        // 1 - 6
        // -5
        rs = conn.createStatement().executeQuery("SELECT four % three - six FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(-5, rs.getLong(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testOrderOfOperationsMultiplicationDivision() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initIntegerTable(conn);
        ResultSet rs;
        
        // 6 * 4 / 3
        // 24 / 3
        // 8
        rs = conn.createStatement().executeQuery("SELECT six * four / three FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(8, rs.getLong(1));
        assertFalse(rs.next());
        
        // 4 / 3 * 6
        // 1 * 6     (integer division)
        // 6
        rs = conn.createStatement().executeQuery("SELECT four / three * six FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(6, rs.getLong(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testOrderOfOperationsMultiplicationModulus() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initIntegerTable(conn);
        ResultSet rs;
        
        // 6 * 4 % 3
        // 24 % 3
        // 0
        rs = conn.createStatement().executeQuery("SELECT six * four % three FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));
        assertFalse(rs.next());
        
        // 4 % 3 * 6
        // 1 * 6
        // 6
        rs = conn.createStatement().executeQuery("SELECT four % three * six FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(6, rs.getLong(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testOrderOfOperationsDivisionModulus() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        initIntegerTable(conn);
        ResultSet rs;
        
        // 6 / 4 % 3
        // 1 % 3     (integer division)
        // 1
        rs = conn.createStatement().executeQuery("SELECT six / four % three FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));
        assertFalse(rs.next());
        
        // 4 % 3 / 6
        // 1 / 6
        // 0         (integer division)
        rs = conn.createStatement().executeQuery("SELECT four % three / six FROM ARITHMETIC_TEST");
        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));
        assertFalse(rs.next());
    }
    
    @Test
    public void testCastingOnConstantAddInArithmeticEvaluation() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE IF NOT EXISTS test_table (k1 INTEGER NOT NULL, v1 INTEGER CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO test_table (k1, v1) VALUES (2, 2)";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1 / (v1 + 0.5) FROM test_table");
        assertTrue(rs.next());
        double d = rs.getDouble(1);
        assertEquals(0.8, d, 0.01);
    }

    @Test
    public void testCastingOnConstantSubInArithmeticEvaluation() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE IF NOT EXISTS test_table (k1 INTEGER NOT NULL, v1 INTEGER CONSTRAINT pk PRIMARY KEY (k1))";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO test_table (k1, v1) VALUES (2, 2)";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT k1 / (v1 - 0.5) FROM test_table");
        assertTrue(rs.next());
        assertEquals(1.333333333, rs.getDouble(1), 0.001);
    }

    @Test
    public void testFloatingPointUpsert() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE test (id VARCHAR not null primary key, name VARCHAR, lat FLOAT)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO test(id,name,lat) VALUES ('testid', 'testname', -1.00)";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT lat FROM test");
        assertTrue(rs.next());
        assertEquals(-1.0f, rs.getFloat(1), 0.001);
    }

    @Test
    public void testFloatingPointMultiplicationUpsert() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE test (id VARCHAR not null primary key, name VARCHAR, lat FLOAT)";
        conn.createStatement().execute(ddl);
        String dml = "UPSERT INTO test(id,name,lat) VALUES ('testid', 'testname', -1.00 * 1)";
        conn.createStatement().execute(dml);
        conn.commit();

        ResultSet rs = conn.createStatement().executeQuery("SELECT lat FROM test");
        assertTrue(rs.next());
        assertEquals(-1.0f, rs.getFloat(1), 0.001);
    }
}