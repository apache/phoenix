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

import static org.apache.phoenix.util.TestUtil.B_VALUE;
import static org.apache.phoenix.util.TestUtil.PHOENIX_JDBC_URL;
import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.TABLE_WITH_ARRAY;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.primitives.Floats;

public class ArrayTest extends BaseClientManagedTimeTest {

	private static final String SIMPLE_TABLE_WITH_ARRAY = "SIMPLE_TABLE_WITH_ARRAY";

	@Test
	public void testScanByArrayValue() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM table_with_array WHERE ?=organization_id and ?=a_float";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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

	@Test
	public void testScanWithArrayInWhereClause() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM table_with_array WHERE ?=organization_id and ?=a_byte_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array, /* comment ok? */ b_string, a_float FROM table_with_array WHERE ?=organization_id and ?=a_string_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_string_array FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT ARRAY_ELEM(a_double_array,1) FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array[2] FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
        createTableWithArray(BaseConnectedQueryTest.getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false);
        String query = "SELECT CASE WHEN A_INTEGER = 1 THEN a_double_array ELSE null END [2] FROM table_with_array";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
        createTableWithArray(BaseConnectedQueryTest.getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        String query = "upsert into table_with_array(ORGANIZATION_ID,ENTITY_ID,a_double_array) values('" + tenantId
                + "','00A123122312312',ARRAY[2.0d,345.8d])";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts)); // Execute
                                                                                 // at
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            int executeUpdate = statement.executeUpdate();
            assertEquals(1, executeUpdate);
            conn.commit();
            statement.close();
            conn.close();
            // create another connection
            props = new Properties(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
            query = "SELECT ARRAY_ELEM(a_double_array,1) FROM table_with_array";
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
        createTableWithArray(BaseConnectedQueryTest.getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        Connection conn = null;
        try {
            createSimpleTableWithArray(BaseConnectedQueryTest.getUrl(), getDefaultSplits(tenantId), null, ts - 2);
            initSimpleArrayTable(tenantId, null, ts, false);
            Properties props = new Properties(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
            String query = "upsert into table_with_array(ORGANIZATION_ID,ENTITY_ID,a_double_array) "
                    + "SELECT organization_id, entity_id, a_double_array  FROM " + SIMPLE_TABLE_WITH_ARRAY
                    + " WHERE a_double_array[1] = 89.96d";
            PreparedStatement statement = conn.prepareStatement(query);
            int executeUpdate = statement.executeUpdate();
            assertEquals(1, executeUpdate);
            conn.commit();
            statement.close();
            conn.close();
            // create another connection
            props = new Properties(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
            conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
            query = "SELECT ARRAY_ELEM(a_double_array,1) FROM table_with_array";
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
    public void testSelectWithArrayWithColumnRef() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(BaseConnectedQueryTest.getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false);
        String query = "SELECT a_integer,ARRAY[1,2,a_integer] FROM table_with_array where organization_id =  '"
                + tenantId + "'";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
    public void testUpsertSelectWithColumnRef() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(BaseConnectedQueryTest.getUrl(), getDefaultSplits(tenantId), null, ts - 2);
        Connection conn = null;
        try {
            createSimpleTableWithArray(BaseConnectedQueryTest.getUrl(), getDefaultSplits(tenantId), null, ts - 2);
            initSimpleArrayTable(tenantId, null, ts, false);
            Properties props = new Properties(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2)); // Execute at timestamp 2
            conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
            String query = "upsert into table_with_array(ORGANIZATION_ID,ENTITY_ID, a_unsigned_double, a_double_array) "
                    + "SELECT organization_id, entity_id, x_double, ARRAY[23.4d, 22.1d, x_double]  FROM " + SIMPLE_TABLE_WITH_ARRAY
                    + " WHERE a_double_array[1] = 89.96d";
            PreparedStatement statement = conn.prepareStatement(query);
            int executeUpdate = statement.executeUpdate();
            assertEquals(1, executeUpdate);
            conn.commit();
            statement.close();
            conn.close();
            // create another connection
            props = new Properties(TEST_PROPERTIES);
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 4));
            conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
            query = "SELECT ARRAY_ELEM(a_double_array,1) FROM table_with_array";
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
	public void testSelectArrayUsingUpsertLikeSyntax() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array FROM TABLE_WITH_ARRAY WHERE a_double_array = ARRAY [ 25.343d, 36.763d, 37.56d,386.63d]";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		int a_index = 0;
		String query = "SELECT a_double_array[1] FROM table_with_array where a_double_array["+a_index+"1]<?";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array[1] FROM table_with_array  GROUP BY a_double_array[1]";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, true);
		String query = "SELECT a_string_array[1] FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
        createTableWithArray(BaseConnectedQueryTest.getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false);
        String query = "SELECT a_string_array[2],A_INTEGER FROM table_with_array";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
        createTableWithArray(BaseConnectedQueryTest.getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false);
        String query = "SELECT A_INTEGER, a_string_array[2] FROM table_with_array";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
        createTableWithArray(BaseConnectedQueryTest.getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false);
        String query = "SELECT  a_string_array[2], a_double_array[1] FROM table_with_array";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
        createTableWithArray(BaseConnectedQueryTest.getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false);
        String query = "SELECT a_string_array[0], a_string_array[2] FROM table_with_array";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            PreparedStatement statement = conn.prepareStatement(query);
            ResultSet rs = statement.executeQuery();
            assertTrue(rs.next());
            String[] strArr = new String[2];
            strArr[0] = "ABC";
            strArr[1] = "XYZWER";
            String result = rs.getString(1);
            assertEquals(strArr[0], result);
            result = rs.getString(2);
            assertEquals(strArr[1], result);
            assertFalse(rs.next());
        } finally {
            conn.close();
        } 
    }
    
    @Test
    public void testSelectSameArrayColumnMultipleTimesWithSameIndices() throws Exception {
        long ts = nextTimestamp();
        String tenantId = getOrganizationId();
        createTableWithArray(BaseConnectedQueryTest.getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        initTablesWithArrays(tenantId, null, ts, false);
        String query = "SELECT a_string_array[2], a_string_array[2] FROM table_with_array";
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_string_array[2] FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT a_double_array[100] FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
		try {
			PreparedStatement statement = conn.prepareStatement(query);
			ResultSet rs = statement.executeQuery();
			assertTrue(rs.next());
			Double[] doubleArr = new Double[1];
			doubleArr[0] = 36.763;
			Array array = conn.createArrayOf("DOUBLE", doubleArr);
			PhoenixArray resultArray = (PhoenixArray) rs.getArray(1);
			assertEquals(resultArray, array);
			Assert.fail("Should have failed");
		} catch (Exception e) {
			System.out.println("");
		} finally {
			conn.close();
		}
	}

	@Test
	public void testArrayLengthFunctionForVariableLength() throws Exception {
		long ts = nextTimestamp();
		String tenantId = getOrganizationId();
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT ARRAY_LENGTH(a_string_array) FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
		createTableWithArray(BaseConnectedQueryTest.getUrl(),
				getDefaultSplits(tenantId), null, ts - 2);
		initTablesWithArrays(tenantId, null, ts, false);
		String query = "SELECT ARRAY_LENGTH(a_double_array) FROM table_with_array";
		Properties props = new Properties(TEST_PROPERTIES);
		props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
				Long.toString(ts + 2)); // Execute at timestamp 2
		Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
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
        createTableWithArray(BaseConnectedQueryTest.getUrl(),
                getDefaultSplits(tenantId), null, ts - 2);
        Properties props = new Properties(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB,
                Long.toString(ts + 2)); // Execute at timestamp 2
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_URL, props);
        try {
            ResultSet rs = conn.getMetaData().getColumns(null, null, StringUtil.escapeLike(TABLE_WITH_ARRAY), StringUtil.escapeLike("x_long_array"));
            assertTrue(rs.next());          
            assertEquals(5, rs.getInt("ARRAY_SIZE"));
            assertFalse(rs.next());

            rs = conn.getMetaData().getColumns(null, null, StringUtil.escapeLike(TABLE_WITH_ARRAY), StringUtil.escapeLike("a_string_array"));
            assertTrue(rs.next());          
            assertEquals(3, rs.getInt("ARRAY_SIZE"));
            assertFalse(rs.next());

            rs = conn.getMetaData().getColumns(null, null, StringUtil.escapeLike(TABLE_WITH_ARRAY), StringUtil.escapeLike("a_double_array"));
            assertTrue(rs.next());          
            assertEquals(0, rs.getInt("ARRAY_SIZE"));
            assertTrue(rs.wasNull());
            assertFalse(rs.next());
        } finally {
            conn.close();
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
                    "    a_double_array)" +
                    "VALUES (?, ?, ?, ?)");
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
            stmt.execute();
                
            conn.commit();
        } finally {
            conn.close();
        }
   }
}
