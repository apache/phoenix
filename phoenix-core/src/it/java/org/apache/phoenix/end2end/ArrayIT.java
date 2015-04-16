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

import static org.apache.phoenix.util.PhoenixRuntime.CURRENT_SCN_ATTRIB;
import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.TABLE_WITH_ARRAY;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.junit.Test;

import com.google.common.primitives.Floats;

public class ArrayIT extends BaseClientManagedTimeIT {

	private static final String SIMPLE_TABLE_WITH_ARRAY = "SIMPLE_TABLE_WITH_ARRAY";

    private static void initTablesWithArrays(String tenantId, Date date, Long ts, boolean useNull, String url) throws Exception {
        Properties props = new Properties();
        if (ts != null) {
            props.setProperty(CURRENT_SCN_ATTRIB, ts.toString());
        }
        Connection conn = DriverManager.getConnection(url, props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " +
                            "TABLE_WITH_ARRAY(" +
                            "    ORGANIZATION_ID, " +
                            "    ENTITY_ID, " +
                            "    a_string_array, " +
                            "    B_STRING, " +
                            "    A_INTEGER, " +
                            "    A_DATE, " +
                            "    X_DECIMAL, " +
                            "    x_long_array, " +
                            "    X_INTEGER," +
                            "    a_byte_array," +
                            "    A_SHORT," +
                            "    A_FLOAT," +
                            "    a_double_array," +
                            "    A_UNSIGNED_FLOAT," +
                            "    A_UNSIGNED_DOUBLE)" +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW1);
            // Need to support primitive
            String[] strArr =  new String[4];
            strArr[0] = "ABC";
            if (useNull) {
                strArr[1] = null;
            } else {
                strArr[1] = "CEDF";
            }
            strArr[2] = "XYZWER";
            strArr[3] = "AB";
            Array array = conn.createArrayOf("VARCHAR", strArr);
            stmt.setArray(3, array);
            stmt.setString(4, B_VALUE);
            stmt.setInt(5, 1);
            stmt.setDate(6, date);
            stmt.setBigDecimal(7, null);
            // Need to support primitive
            Long[] longArr =  new Long[2];
            longArr[0] = 25l;
            longArr[1] = 36l;
            array = conn.createArrayOf("BIGINT", longArr);
            stmt.setArray(8, array);
            stmt.setNull(9, Types.INTEGER);
            // Need to support primitive
            Byte[] byteArr =  new Byte[2];
            byteArr[0] = 25;
            byteArr[1] = 36;
            array = conn.createArrayOf("TINYINT", byteArr);
            stmt.setArray(10, array);
            stmt.setShort(11, (short) 128);
            stmt.setFloat(12, 0.01f);
            // Need to support primitive
            Double[] doubleArr =  new Double[4];
            doubleArr[0] = 25.343;
            doubleArr[1] = 36.763;
            doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
            array = conn.createArrayOf("DOUBLE", doubleArr);
            stmt.setArray(13, array);
            stmt.setFloat(14, 0.01f);
            stmt.setDouble(15, 0.0001);
            stmt.execute();

            conn.commit();
        } finally {
            conn.close();
        }
    }

	@Test
	public void testScanByArrayValue() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false, getUrl());
		String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM table_with_array WHERE ?=organization_id and ?=a_float";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
		        Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
        analyzeTable(conn, TABLE_WITH_ARRAY);
		try {
		    PreparedStatement statement = conn.prepareStatement(query);
			statement.setString(1, tenantId);
			statement.setFloat(2, 0.01f);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			Double[] doubleArr = new Double[4];
			doubleArr[0] = 25.343;
			doubleArr[1] = 36.763;
		    doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
			Array array = conn.createArrayOf("DOUBLE",
					doubleArr);
			PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
			assertEquals(resultArray, array);
			assertEquals(rs.getString("B_string"), B_VALUE);
			assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

    private void analyzeTable(Connection conn, String tableWithArray) throws SQLException {
        String analyse = "UPDATE STATISTICS  "+tableWithArray;
		PreparedStatement statement = conn.prepareStatement(analyse);
        statement.execute();
    }

	@Test
	public void testScanWithArrayInWhereClause() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false, getUrl());
		String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM table_with_array WHERE ?=organization_id and ?=a_byte_array";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		analyzeTable(conn, TABLE_WITH_ARRAY);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			statement.setString(1, tenantId);
			// Need to support primitive
			Byte[] byteArr = new Byte[2];
			byteArr[0] = 25;
			byteArr[1] = 36;
			Array array = conn.createArrayOf("TINYINT", byteArr);
			statement.setArray(2, array);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			Double[] doubleArr = new Double[4];
			doubleArr[0] = 25.343;
			doubleArr[1] = 36.763;
		    doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
			array = conn.createArrayOf("DOUBLE", doubleArr);
			Array resultArray = rs.getArray(1);
			assertEquals(resultArray, array);
			assertEquals(rs.getString("B_string"), B_VALUE);
			assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testScanWithNonFixedWidthArrayInWhereClause() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false, getUrl());
		String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM table_with_array WHERE ?=organization_id and ?=a_string_array";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			statement.setString(1, tenantId);
			// Need to support primitive
			String[] strArr = new String[4];
			strArr[0] = "ABC";
			strArr[1] = "CEDF";
			strArr[2] = "XYZWER";
			strArr[3] = "AB";
			Array array = conn.createArrayOf("VARCHAR", strArr);
			statement.setArray(2, array);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			Double[] doubleArr = new Double[4];
			doubleArr[0] = 25.343;
			doubleArr[1] = 36.763;
		    doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
			array = conn.createArrayOf("DOUBLE", doubleArr);
			Array resultArray = rs.getArray(1);
			assertEquals(resultArray, array);
			assertEquals(rs.getString("B_string"), B_VALUE);
			assertTrue(Floats.compare(rs.getFloat(3), 0.01f) == 0);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testScanWithNonFixedWidthArrayInSelectClause() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false, getUrl());
		String query = "SELECT a_string_array FROM table_with_array";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			String[] strArr = new String[4];
			strArr[0] = "ABC";
			strArr[1] = "CEDF";
			strArr[2] = "XYZWER";
			strArr[3] = "AB";
			Array array = conn.createArrayOf("VARCHAR", strArr);
			PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
			assertEquals(resultArray, array);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testSelectSpecificIndexOfAnArrayAsArrayFunction()
			throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false, getUrl());
		String query = "SELECT ARRAY_ELEM(a_double_array,2) FROM table_with_array";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			Double[] doubleArr = new Double[1];
			doubleArr[0] = 36.763;
			conn.createArrayOf("DOUBLE", doubleArr);
			Double result =  rs.getDouble(1);
			assertEquals(doubleArr[0], result);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testSelectSpecificIndexOfAnArray() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false, getUrl());
		String query = "SELECT a_double_array[3] FROM table_with_array";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			Double[] doubleArr = new Double[1];
			doubleArr[0] = 37.56;
			Double result =  rs.getDouble(1);
			assertEquals(doubleArr[0], result);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

    @Test
    public void testCaseWithArray() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false, getUrl());
        String query = "SELECT CASE WHEN A_INTEGER = 1 THEN a_double_array ELSE null END [3] FROM table_with_array";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 37.56;
            Double result =  rs.getDouble(1);
            assertEquals(doubleArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertValuesWithArray() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        String query = "upsert into table_with_array(ORGANIZATION_ID,ENTITY_ID,a_double_array) values('" + tenantId
                + "','00A123122312312',ARRAY[2.0,345.8])";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts)); // Execute
                                                                                 // at
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            int executeUpdate = statement.executeUpdate();
            assertEquals(1, executeUpdate);
            conn.commit();
            statement.close();
            conn.close();
            // create another connection
            props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(getUrl(), props);
            query = "SELECT ARRAY_ELEM(a_double_array,2) FROM table_with_array";
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 345.8d;
            conn.createArrayOf("DOUBLE", doubleArr);
            Double result = rs.getDouble(1);
            assertEquals(doubleArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertSelectWithSelectAsSubQuery1() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        Connection conn = null;
        try {
            createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
            initSimpleArrayTable(tenantId, null, ts, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "upsert into table_with_array(ORGANIZATION_ID,ENTITY_ID,a_double_array) "
                    + "SELECT organization_id, entity_id, a_double_array  FROM " + SIMPLE_TABLE_WITH_ARRAY
                    + " WHERE a_double_array[2] = 89.96";
            PreparedStatement statement = conn.prepareStatement(query);
            int executeUpdate = statement.executeUpdate();
            assertEquals(1, executeUpdate);
            conn.commit();
            statement.close();
            conn.close();
            // create another connection
            props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
            conn = DriverManager.getConnection(getUrl(), props);
            query = "SELECT ARRAY_ELEM(a_double_array,2) FROM table_with_array";
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 89.96d;
            Double result = rs.getDouble(1);
            assertEquals(result, doubleArr[0]);
            assertFalse(rs.next());

        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testArraySelectWithORCondition() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        Connection conn = null;
        try {
            createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
            initSimpleArrayTable(tenantId, null, ts, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT a_double_array[1]  FROM " + SIMPLE_TABLE_WITH_ARRAY
                    + " WHERE a_double_array[2] = 89.96 or a_char_array[0] = 'a'";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 64.87d;
            Double result = rs.getDouble(1);
            assertEquals(result, doubleArr[0]);
            assertFalse(rs.next());

        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testArraySelectWithANY() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        Connection conn = null;
        try {
            createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
            initSimpleArrayTable(tenantId, null, ts, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT a_double_array[1]  FROM " + SIMPLE_TABLE_WITH_ARRAY
                    + " WHERE CAST(89.96 AS DOUBLE) = ANY(a_double_array)";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 64.87d;
            Double result = rs.getDouble(1);
            assertEquals(result, doubleArr[0]);
            assertFalse(rs.next());
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testArraySelectWithALL() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        Connection conn = null;
        try {
            createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
            initSimpleArrayTable(tenantId, null, ts, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT a_double_array[1]  FROM " + SIMPLE_TABLE_WITH_ARRAY
                    + " WHERE CAST(64.87 as DOUBLE) = ALL(a_double_array)";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertFalse(rs.next());
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testArraySelectWithANYCombinedWithOR() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        Connection conn = null;
        try {
            createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
            initSimpleArrayTable(tenantId, null, ts, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT a_double_array[1]  FROM " + SIMPLE_TABLE_WITH_ARRAY
                    + " WHERE  a_char_array[0] = 'f' or CAST(89.96 AS DOUBLE) > ANY(a_double_array)";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 64.87d;
            Double result = rs.getDouble(1);
            assertEquals(result, doubleArr[0]);
            assertFalse(rs.next());
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testArraySelectWithALLCombinedWithOR() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        Connection conn = null;
        try {
            createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
            initSimpleArrayTable(tenantId, null, ts, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT a_double_array[1], a_double_array[2]  FROM " + SIMPLE_TABLE_WITH_ARRAY
                    + " WHERE  a_char_array[0] = 'f' or CAST(100.0 AS DOUBLE) > ALL(a_double_array)";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 64.87d;
            Double result = rs.getDouble(1);
            assertEquals(result, doubleArr[0]);
            doubleArr = new Double[1];
            doubleArr[0] = 89.96d;
            result = rs.getDouble(2);
            assertEquals(result, doubleArr[0]);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testArraySelectWithANYUsingVarLengthArray() throws Exception {
        Connection conn = null;
        try {
            long ts = nextTimestamp();
            String tenantId = getOrganizationId();
            createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
            initTablesWithArrays(tenantId, null, ts, false, getUrl());
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "SELECT a_string_array[1]  FROM " + TABLE_WITH_ARRAY
                    + " WHERE 'XYZWER' = ANY(a_string_array)";
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = "ABC";
            String result = rs.getString(1);
            assertEquals(result, strArr[0]);
            assertFalse(rs.next());
            query = "SELECT a_string_array[1]  FROM " + TABLE_WITH_ARRAY + " WHERE 'AB' = ANY(a_string_array)";
            statement = conn.prepareStatement(query);
            rs = statement.executeQuery();
            assertTrue(rs.next());
            result = rs.getString(1);
            assertEquals(result, strArr[0]);
            assertFalse(rs.next());
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testSelectWithArrayWithColumnRef() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false, getUrl());
        String query = "SELECT a_integer,ARRAY[1,2,a_integer] FROM table_with_array where organization_id =  '"
                + tenantId + "'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            int val = rs.getInt(1);
            assertEquals(val, 1);
            Array array = rs.getArray(2);
            // Need to support primitive
            Integer[] intArr = new Integer[3];
            intArr[0] = 1;
            intArr[1] = 2;
            intArr[2] = 1;
            Array resultArr = conn.createArrayOf("INTEGER", intArr);
            assertEquals(resultArr, array);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectWithArrayWithColumnRefWithVarLengthArray() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false, getUrl());
        String query = "SELECT b_string,ARRAY['abc','defgh',b_string] FROM table_with_array where organization_id =  '"
                + tenantId + "'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals(val, "b");
            Array array = rs.getArray(2);
            // Need to support primitive
            String[] strArr = new String[3];
            strArr[0] = "abc";
            strArr[1] = "defgh";
            strArr[2] = "b";
            Array resultArr = conn.createArrayOf("VARCHAR", strArr);
            assertEquals(resultArr, array);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectWithArrayWithColumnRefWithVarLengthArrayWithNullValue() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false, getUrl());
        String query = "SELECT b_string,ARRAY['abc',null,'bcd',null,null,b_string] FROM table_with_array where organization_id =  '"
                + tenantId + "'";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals(val, "b");
            Array array = rs.getArray(2);
            // Need to support primitive
            String[] strArr = new String[6];
            strArr[0] = "abc";
            strArr[1] = null;
            strArr[2] = "bcd";
            strArr[3] = null;
            strArr[4] = null;
            strArr[5] = "b";
            Array resultArr = conn.createArrayOf("VARCHAR", strArr);
            assertEquals(resultArr, array);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertSelectWithColumnRef() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        Connection conn = null;
        try {
            createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
            initSimpleArrayTable(tenantId, null, ts, false);
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(getUrl(), props);
            String query = "upsert into table_with_array(ORGANIZATION_ID,ENTITY_ID, a_unsigned_double, a_double_array) "
                    + "SELECT organization_id, entity_id, x_double, ARRAY[23.4, 22.1, x_double]  FROM " + SIMPLE_TABLE_WITH_ARRAY
                    + " WHERE a_double_array[2] = 89.96";
            PreparedStatement statement = conn.prepareStatement(query);
            int executeUpdate = statement.executeUpdate();
            assertEquals(1, executeUpdate);
            conn.commit();
            statement.close();
            conn.close();
            // create another connection
            props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
            conn = DriverManager.getConnection(getUrl(), props);
            query = "SELECT ARRAY_ELEM(a_double_array,2) FROM table_with_array";
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 22.1d;
            Double result = rs.getDouble(1);
            assertEquals(result, doubleArr[0]);
            assertFalse(rs.next());

        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testCharArraySpecificIndex() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createSimpleTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        initSimpleArrayTable(tenantId, null, ts, false);
        String query = "SELECT a_char_array[2] FROM SIMPLE_TABLE_WITH_ARRAY";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] charArr = new String[1];
            charArr[0] = "b";
            String result = rs.getString(1);
            assertEquals(charArr[0], result);
        } finally {
            conn.close();
        }
    }

    @Test
    public void testArrayWithDescOrder() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(
                "CREATE TABLE t ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array DESC)) \n");
        conn.close();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?,?)");
        stmt.setString(1, "a");
        String[] s = new String[] { "abc", "def", "ghi", "jkll", null, null, "xxx" };
        Array array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "abc", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT b_string_array FROM t");
        assertTrue(rs.next());
        PhoenixArray strArr = (PhoenixArray)rs.getArray(1);
        assertEquals(array, strArr);
        conn.close();
    }

    @Test
    public void testArrayWithFloatArray() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE t ( k VARCHAR PRIMARY KEY, a Float ARRAY[])");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO t VALUES('a',ARRAY[2.0,3.0])");
        int res = stmt.executeUpdate();
        assertEquals(1, res);
        conn.commit();
        conn.close();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT ARRAY_ELEM(a,2) FROM t");
        assertTrue(rs.next());
        Float f = new Float(3.0);
        assertEquals(f, (Float)rs.getFloat(1));
        conn.close();
    }

    @Test
    public void testArraySelectSingleArrayElemWithCast() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE t ( k VARCHAR PRIMARY KEY, a bigint ARRAY[])");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?)");
        stmt.setString(1, "a");
        Long[] s = new Long[] {1l, 2l};
        Array array = conn.createArrayOf("BIGINT", s);
        stmt.setArray(2, array);
        stmt.execute();
        conn.commit();
        conn.close();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT k, CAST(a[2] AS DOUBLE) FROM t");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        Double d = new Double(2.0);
        assertEquals(d, (Double)rs.getDouble(2));
        conn.close();
    }

    @Test
    public void testArrayWithCast() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE t ( k VARCHAR PRIMARY KEY, a bigint ARRAY[])");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?)");
        stmt.setString(1, "a");
        Long[] s = new Long[] { 1l, 2l };
        Array array = conn.createArrayOf("BIGINT", s);
        stmt.setArray(2, array);
        stmt.execute();
        conn.commit();
        conn.close();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT CAST(a AS DOUBLE []) FROM t");
        assertTrue(rs.next());
        Double[] d = new Double[] { 1.0, 2.0 };
        array = conn.createArrayOf("DOUBLE", d);
        PhoenixArray arr = (PhoenixArray)rs.getArray(1);
        assertEquals(array, arr);
        conn.close();
    }

    @Test
    public void testArrayWithCastForVarLengthArr() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE t ( k VARCHAR PRIMARY KEY, a VARCHAR(5) ARRAY)");
        conn.close();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?)");
        stmt.setString(1, "a");
        String[] s = new String[] { "1", "2" };
        PhoenixArray array = (PhoenixArray)conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT CAST(a AS CHAR ARRAY) FROM t");
        assertTrue(rs.next());
        PhoenixArray arr = (PhoenixArray)rs.getArray(1);
        String[] array2 = (String[])array.getArray();
        String[] array3 = (String[])arr.getArray();
        assertEquals(array2[0], array3[0]);
        assertEquals(array2[1], array3[1]);
        conn.close();
    }

    @Test
    public void testFixedWidthCharArray() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE t ( k VARCHAR PRIMARY KEY, a CHAR(5) ARRAY)");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.getMetaData().getColumns(null, null, "T", "A");
        assertTrue(rs.next());
        assertEquals(5, rs.getInt("COLUMN_SIZE"));
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?)");
        stmt.setString(1, "a");
        String[] s = new String[] {"1","2"};
        Array array = conn.createArrayOf("CHAR", s);
        stmt.setArray(2, array);
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT k, a[2] FROM t");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("2",rs.getString(2));
        conn.close();
    }

	@Test
	public void testSelectArrayUsingUpsertLikeSyntax() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false, getUrl());
		String query = "SELECT a_double_array FROM TABLE_WITH_ARRAY WHERE a_double_array = CAST(ARRAY [ 25.343, 36.763, 37.56,386.63] AS DOUBLE ARRAY)";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			Double[] doubleArr =  new Double[4];
            doubleArr[0] = 25.343;
            doubleArr[1] = 36.763;
            doubleArr[2] = 37.56;
            doubleArr[3] = 386.63;
			Array array = conn.createArrayOf("DOUBLE", doubleArr);
			PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
			assertEquals(resultArray, array);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testArrayIndexUsedInWhereClause() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false, getUrl());
		int a_index = 0;
		String query = "SELECT a_double_array[2] FROM table_with_array where a_double_array["+a_index+"2]<?";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			Double[] doubleArr = new Double[1];
			doubleArr[0] = 40.0;
			conn.createArrayOf("DOUBLE", doubleArr);
			statement.setDouble(1, 40.0d);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			// Need to support primitive
			doubleArr = new Double[1];
			doubleArr[0] = 36.763;
			Double result =  rs.getDouble(1);
			assertEquals(doubleArr[0], result);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testArrayIndexUsedInGroupByClause() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false, getUrl());
		String query = "SELECT a_double_array[2] FROM table_with_array  GROUP BY a_double_array[2]";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			Double[] doubleArr = new Double[1];
			doubleArr[0] = 40.0;
			conn.createArrayOf("DOUBLE", doubleArr);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			doubleArr = new Double[1];
			doubleArr[0] = 36.763;
			Double result =  rs.getDouble(1);
			assertEquals(doubleArr[0], result);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testVariableLengthArrayWithNullValue() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, true, getUrl());
		String query = "SELECT a_string_array[2] FROM table_with_array";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			String[] strArr = new String[1];
			strArr[0] = "XYZWER";
			String result = rs.getString(1);
			assertNull(result);
		} finally {
			conn.close();
		}
	}

	@Test
	public void testSelectSpecificIndexOfAVariableArrayAlongWithAnotherColumn1() throws Exception {
	    long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false, getUrl());
        String query = "SELECT a_string_array[3],A_INTEGER FROM table_with_array";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = "XYZWER";
            String result = rs.getString(1);
            assertEquals(strArr[0], result);
            int a_integer = rs.getInt(2);
            assertEquals(1, a_integer);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
	}

    @Test
    public void testSelectSpecificIndexOfAVariableArrayAlongWithAnotherColumn2() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false, getUrl());
        String query = "SELECT A_INTEGER, a_string_array[3] FROM table_with_array";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = "XYZWER";
            int a_integer = rs.getInt(1);
            assertEquals(1, a_integer);
            String result = rs.getString(2);
            assertEquals(strArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectMultipleArrayColumns() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false, getUrl());
        String query = "SELECT  a_string_array[3], a_double_array[2] FROM table_with_array";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = "XYZWER";
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 36.763d;
            Double a_double = rs.getDouble(2);
            assertEquals(doubleArr[0], a_double);
            String result = rs.getString(1);
            assertEquals(strArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectSameArrayColumnMultipleTimesWithDifferentIndices() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false, getUrl());
        String query = "SELECT a_string_array[1], a_string_array[2], " +
                "a_string_array[3], a_double_array[1], a_double_array[2], a_double_array[3] " +
                "FROM table_with_array";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            assertEquals("ABC", rs.getString(1));
            assertEquals("CEDF", rs.getString(2));
            assertEquals("XYZWER", rs.getString(3));
            assertEquals(25.343, rs.getDouble(4), 0.0);
            assertEquals(36.763, rs.getDouble(5), 0.0);
            assertEquals(37.56, rs.getDouble(6), 0.0);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSelectSameArrayColumnMultipleTimesWithSameIndices() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false, getUrl());
        String query = "SELECT a_string_array[3], a_string_array[3] FROM table_with_array";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = "XYZWER";
            String result = rs.getString(1);
            assertEquals(strArr[0], result);
            result = rs.getString(2);
            assertEquals(strArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

	@Test
	public void testSelectSpecificIndexOfAVariableArray() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false, getUrl());
		String query = "SELECT a_string_array[3] FROM table_with_array";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			String[] strArr = new String[1];
			strArr[0] = "XYZWER";
			String result = rs.getString(1);
			assertEquals(strArr[0], result);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

	@Test
	public void testWithOutOfRangeIndex() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false, getUrl());
		String query = "SELECT a_double_array[100] FROM table_with_array";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
			assertNull(resultArray);
		} finally {
			conn.close();
		}
	}

	@Test
	public void testArrayLengthFunctionForVariableLength() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false, getUrl());
		String query = "SELECT ARRAY_LENGTH(a_string_array) FROM table_with_array";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			int result = rs.getInt(1);
			assertEquals(result, 4);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}


	@Test
	public void testArrayLengthFunctionForFixedLength() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false, getUrl());
		String query = "SELECT ARRAY_LENGTH(a_double_array) FROM table_with_array";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(getUrl(), props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			int result = rs.getInt(1);
			assertEquals(result, 4);
			assertFalse(rs.next());
		} finally {
			conn.close();
		}
	}

    @Test
    public void testArraySizeRoundtrip() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            ResultSet rs = conn.getMetaData().getColumns(null, null, StringUtil.escapeLike(TABLE_WITH_ARRAY), StringUtil.escapeLike(SchemaUtil.normalizeIdentifier("x_long_array")));
            assertTrue(rs.next());
            assertEquals(5, rs.getInt("ARRAY_SIZE"));
            assertFalse(rs.next());

            rs = conn.getMetaData().getColumns(null, null, StringUtil.escapeLike(TABLE_WITH_ARRAY), StringUtil.escapeLike(SchemaUtil.normalizeIdentifier("a_string_array")));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt("ARRAY_SIZE"));
            assertFalse(rs.next());

            rs = conn.getMetaData().getColumns(null, null, StringUtil.escapeLike(TABLE_WITH_ARRAY), StringUtil.escapeLike(SchemaUtil.normalizeIdentifier("a_double_array")));
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("ARRAY_SIZE"));
            assertTrue(rs.wasNull());
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testVarLengthArrComparisonInWhereClauseWithSameArrays() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement()
                .execute(
                        "CREATE TABLE t_same_size ( k VARCHAR PRIMARY KEY, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4])");
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO t_same_size VALUES(?,?,?)");
        stmt.setString(1, "a");
        String[] s = new String[] {"abc","def", "ghi","jkl"};
        Array array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] {"abc","def", "ghi","jkl"};
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("SELECT k, a_string_array[2] FROM t_same_size where a_string_array=b_string_array");
        assertTrue(rs.next());
        assertEquals("a",rs.getString(1));
        assertEquals("def",rs.getString(2));
        conn.close();
    }

    @Test
    public void testVarLengthArrComparisonInWhereClauseWithDiffSizeArrays() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement()
                .execute(
                        "CREATE TABLE t ( k VARCHAR PRIMARY KEY, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4])");
        conn.close();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?,?)");
        stmt.setString(1, "a");
        String[] s = new String[] { "abc", "def", "ghi", "jkll" };
        Array array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "abc", "def", "ghi", "jklm" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery(
                "SELECT k, a_string_array[2] FROM t where a_string_array<b_string_array");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("def", rs.getString(2));
        conn.close();
    }

    @Test
    public void testVarLengthArrComparisonWithNulls() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;

        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement()
                .execute(
                        "CREATE TABLE t ( k VARCHAR PRIMARY KEY, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4])");
        conn.close();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?,?)");
        stmt.setString(1, "a");
        String[] s = new String[] { "abc", "def", "ghi", "jkll", null, null, "xxx" };
        Array array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "abc", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery(
                "SELECT k, a_string_array[2] FROM t where a_string_array>b_string_array");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("def", rs.getString(2));
        conn.close();
    }

    @Test
    public void testUpsertValuesWithNull() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        String query = "upsert into table_with_array(ORGANIZATION_ID,ENTITY_ID,a_double_array) values('" + tenantId
                + "','00A123122312312',null)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts)); // Execute
                                                                                 // at
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            int executeUpdate = statement.executeUpdate();
            assertEquals(1, executeUpdate);
            conn.commit();
            statement.close();
            conn.close();
            // create another connection
            props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(getUrl(), props);
            query = "SELECT ARRAY_ELEM(a_double_array,2) FROM table_with_array";
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            // Need to support primitive
            Double[] doubleArr = new Double[1];
            doubleArr[0] = 0.0d;
            conn.createArrayOf("DOUBLE", doubleArr);
            Double result = rs.getDouble(1);
            assertEquals(doubleArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testUpsertValuesWithNullUsingPreparedStmt() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        String query = "upsert into table_with_array(ORGANIZATION_ID,ENTITY_ID,a_string_array) values(?, ?, ?)";
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts)); // Execute
                                                                                 // at
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            statement.setString(1, tenantId);
            statement.setString(2, "00A123122312312");
            statement.setNull(3, Types.ARRAY);
            int executeUpdate = statement.executeUpdate();
            assertEquals(1, executeUpdate);
            conn.commit();
            statement.close();
            conn.close();
            // create another connection
            props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(getUrl(), props);
            query = "SELECT ARRAY_ELEM(a_string_array,1) FROM table_with_array";
            statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[1];
            strArr[0] = null;
            conn.createArrayOf("VARCHAR", strArr);
            String result = rs.getString(1);
            assertEquals(strArr[0], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testPKWithArray() throws Exception {
        Connection conn;
        PreparedStatement stmt;
        ResultSet rs;
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement()
                .execute(
                        "CREATE TABLE t ( k VARCHAR, a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4] \n"
                        + " CONSTRAINT pk PRIMARY KEY (k, b_string_array)) \n");
        conn.close();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        conn = DriverManager.getConnection(getUrl(), props);
        stmt = conn.prepareStatement("UPSERT INTO t VALUES(?,?,?)");
        stmt.setString(1, "a");
        String[] s = new String[] { "abc", "def", "ghi", "jkll", null, null, "xxx" };
        Array array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(2, array);
        s = new String[] { "abc", "def", "ghi", "jkll", null, null, null, "xxx" };
        array = conn.createArrayOf("VARCHAR", s);
        stmt.setArray(3, array);
        stmt.execute();
        conn.commit();
        conn.close();

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery(
                "SELECT k, a_string_array[2] FROM t where b_string_array[8]='xxx'");
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("def", rs.getString(2));
        conn.close();
    }

    @Test
    public void testPKWithArrayNotInEnd() throws Exception {
        Connection conn;
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(
                    "CREATE TABLE t ( a_string_array VARCHAR(100) ARRAY[4], b_string_array VARCHAR(100) ARRAY[4], k VARCHAR  \n"
                            + " CONSTRAINT pk PRIMARY KEY (b_string_array, k))");
            conn.close();
            fail();
        } catch (SQLException e) {
        } finally {
            if (conn != null) {
                conn.close();
            }
        }

    }

    @Test
    public void testArrayRefToLiteral() throws Exception {
        Connection conn;
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        try {
            PreparedStatement stmt = conn.prepareStatement("select ?[2] from system.\"catalog\" limit 1");
            Array array = conn.createArrayOf("CHAR", new String[] {"a","b","c"});
            stmt.setArray(1, array);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("b", rs.getString(1));
            assertFalse(rs.next());
        } catch (SQLException e) {
        } finally {
            if (conn != null) {
                conn.close();
            }
        }

    }

    static void createTableWithArray(String url, byte[][] bs, Object object,
			long ts) throws SQLException {
		String ddlStmt = "create table "
				+ TABLE_WITH_ARRAY
				+ "   (organization_id char(15) not null, \n"
				+ "    entity_id char(15) not null,\n"
				+ "    a_string_array varchar(100) array[3],\n"
				+ "    b_string varchar(100),\n"
				+ "    a_integer integer,\n"
				+ "    a_date date,\n"
				+ "    a_time time,\n"
				+ "    a_timestamp timestamp,\n"
				+ "    x_decimal decimal(31,10),\n"
				+ "    x_long_array bigint[5],\n"
				+ "    x_integer integer,\n"
				+ "    a_byte_array tinyint array,\n"
				+ "    a_short smallint,\n"
				+ "    a_float float,\n"
				+ "    a_double_array double array[],\n"
				+ "    a_unsigned_float unsigned_float,\n"
				+ "    a_unsigned_double unsigned_double \n"
				+ "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id)\n"
				+ ")";
		BaseTest.createTestTable(url, ddlStmt, bs, ts);
	}

	static void createSimpleTableWithArray(String url, byte[][] bs, Object object,
			long ts) throws SQLException {
		String ddlStmt = "create table "
				+ SIMPLE_TABLE_WITH_ARRAY
				+ "   (organization_id char(15) not null, \n"
				+ "    entity_id char(15) not null,\n"
				+ "    x_double double,\n"
				+ "    a_double_array double array[],\n"
				+ "    a_char_array char(5) array[],\n"
				+ "    CONSTRAINT pk PRIMARY KEY (organization_id, entity_id)\n"
				+ ")";
		BaseTest.createTestTable(url, ddlStmt, bs, ts);
	}

	protected static void initSimpleArrayTable(String tenantId, Date date, Long ts, boolean useNull) throws Exception {
   	 Properties props = new Properties();
        if (ts != null) {
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, ts.toString());
        }
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // Insert all rows at ts
            PreparedStatement stmt = conn.prepareStatement(
                    "upsert into " +SIMPLE_TABLE_WITH_ARRAY+
                    "(" +
                    "    ORGANIZATION_ID, " +
                    "    ENTITY_ID, " +
                    "    x_double, " +
                    "    a_double_array, a_char_array)" +
                    "VALUES (?, ?, ?, ?, ?)");
            stmt.setString(1, tenantId);
            stmt.setString(2, ROW1);
            stmt.setDouble(3, 1.2d);
            // Need to support primitive
            Double[] doubleArr =  new Double[2];
            doubleArr[0] = 64.87;
            doubleArr[1] = 89.96;
            //doubleArr[2] = 9.9;
            Array array = conn.createArrayOf("DOUBLE", doubleArr);
            stmt.setArray(4, array);

            // create character array
            String[] charArr =  new String[2];
            charArr[0] = "a";
            charArr[1] = "b";
            array = conn.createArrayOf("CHAR", charArr);
            stmt.setArray(5, array);
            stmt.execute();

            conn.commit();
        } finally {
            conn.close();
        }
   }
}
