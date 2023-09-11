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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.phoenix.schema.TypeMismatchException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelStatsDisabledTest.class)
public class ArrayRemoveFunctionIT extends ParallelStatsDisabledIT {

	private Connection conn;
	private String tableName;

	@Before
	public void setup() throws Exception {
		conn = DriverManager.getConnection(getUrl());
		tableName = initTables(conn);
	}

	private String initTables(Connection conn) throws Exception {
		String tableName = generateUniqueName();
		String ddl = "CREATE TABLE " + tableName
				+ " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[],integers INTEGER[],doubles DOUBLE[],bigints BIGINT[],"
				+ "chars CHAR(15)[],double1 DOUBLE,char1 CHAR(17),nullcheck INTEGER,chars2 CHAR(15)[], nullVarchar VARCHAR[], nullBigInt BIGINT[],double2 DOUBLE,integer1 INTEGER,oneItem VARCHAR[],char2 char(15),varchar1 VARCHAR)";
		conn.createStatement().execute(ddl);
		String dml = "UPSERT INTO " + tableName
				+ "(region_name,varchars,integers,doubles,bigints,chars,double1,char1,nullcheck,chars2,double2,integer1,oneItem,char2,varchar1) VALUES('SF Bay Area',"
				+ "ARRAY['2345','46345','23234']," + "ARRAY[2345,46345,23234,456],"
				+ "ARRAY[10.0,23.45,46.345,23.234,45.6,5.78]," + "ARRAY[12,34,56,78,910],"
				+ "ARRAY['a','bbbb','c','ddd','e','c']," + "23.45," + "'wert'," + "NULL,"
				+ "ARRAY['a','bbbb','c','ddd','e','foo']," + "12," + "12," + "ARRAY['alone'],'2345','a')";
		PreparedStatement stmt = conn.prepareStatement(dml);
		stmt.execute();
		conn.commit();
		return tableName;
	}

	@Test
	public void testEmptyArrayModification() throws Exception {
		ResultSet rs = conn.createStatement()
				.executeQuery("SELECT ARRAY_REMOVE(nullVarChar,'34567') FROM " + tableName + " LIMIT 1");
		assertTrue(rs.next());

		assertNull(rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionVarchar() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(varchars,'23234') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("VARCHAR", new String[] { "2345", "46345" }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionInteger() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(integers,456) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("INTEGER", new Integer[] { 2345, 46345, 23234 }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionDouble() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(doubles,double1) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("DOUBLE", new Double[] { 10.0, 46.345, 23.234, 45.6, 5.78 }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionDoubleWithInt() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(doubles,10),doubles FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("DOUBLE", new Double[] { 23.45, 46.345, 23.234, 45.6, 5.78 }), rs.getArray(1));
		assertEquals(conn.createArrayOf("DOUBLE", new Double[] { 10.0, 23.45, 46.345, 23.234, 45.6, 5.78 }),
				rs.getArray(2));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionBigint() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(bigints,56),bigints FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("BIGINT", new Long[] { 12l, 34l, 78l, 910l }), rs.getArray(1));
		assertEquals(conn.createArrayOf("BIGINT", new Long[] { 12l, 34l, 56l, 78l, 910l }), rs.getArray(2));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionBigintWithInteger() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(bigints,integer1) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("BIGINT", new Long[] { 34l, 56l, 78l, 910l }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test(expected = TypeMismatchException.class)
	public void testArrayRemoveFunctionBigintWithDouble() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(bigints,double2) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("BIGINT", new Long[] { 34l, 56l, 78l, 910l }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionChar() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(chars,'ddd') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("CHAR", new String[] { "a", "bbbb", "c", "e", "c" }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test(expected = TypeMismatchException.class)
	public void testArrayRemoveFunctionIntToCharArray() throws Exception {
		conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(varchars,234) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
	}

	@Test(expected = TypeMismatchException.class)
	public void testArrayRemoveFunctionVarcharToIntegerArray() throws Exception {
		conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(integers,'234') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
	}

	@Test
	public void testArrayRemoveFunctionWithNestedFunctions1() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery("SELECT ARRAY_REMOVE(ARRAY[23,2345],integers[1]) FROM "
				+ tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("INTEGER", new Integer[] { 23 }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionWithNestedFunctions2() throws Exception {
		ResultSet rs = conn.createStatement()
				.executeQuery("SELECT ARRAY_REMOVE(integers,ARRAY_ELEM(ARRAY[2345,4],1)) FROM " + tableName
						+ " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("INTEGER", new Integer[] { 46345, 23234, 456 }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionWithUpsert1() throws Exception {
		String uniqueName = generateUniqueName();
		String ddl = "CREATE TABLE " + uniqueName + " (region_name VARCHAR PRIMARY KEY,varchars VARCHAR[])";
		conn.createStatement().execute(ddl);

		String dml = "UPSERT INTO " + uniqueName
				+ "(region_name,varchars) VALUES('SF Bay Area',ARRAY_REMOVE(ARRAY['hello','world'],'world'))";
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement()
				.executeQuery("SELECT varchars FROM " + uniqueName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("VARCHAR", new String[] { "hello" }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionWithUpsert2() throws Exception {
		String tableName = generateUniqueName();
		String ddl = "CREATE TABLE " + tableName + " (region_name VARCHAR PRIMARY KEY,integers INTEGER[])";
		conn.createStatement().execute(ddl);

		String dml = "UPSERT INTO " + tableName
				+ "(region_name,integers) VALUES('SF Bay Area',ARRAY_REMOVE(ARRAY[4,5],5))";
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement()
				.executeQuery("SELECT integers FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("INTEGER", new Integer[] { 4 }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionWithUpsertSelect1() throws Exception {
		String sourceTableName = generateUniqueName();
		String targetTableName = generateUniqueName();

		String ddl = "CREATE TABLE " + sourceTableName + " (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
		conn.createStatement().execute(ddl);

		ddl = "CREATE TABLE " + targetTableName + " (region_name VARCHAR PRIMARY KEY,doubles DOUBLE[])";
		conn.createStatement().execute(ddl);

		String dml = "UPSERT INTO " + sourceTableName
				+ "(region_name,doubles) VALUES('SF Bay Area',ARRAY_APPEND(ARRAY[5.67,7.87],9))";
		conn.createStatement().execute(dml);

		dml = "UPSERT INTO " + sourceTableName
				+ "(region_name,doubles) VALUES('SF Bay Area2',ARRAY_APPEND(ARRAY[56.7,7.87],9))";
		conn.createStatement().execute(dml);
		conn.commit();

		dml = "UPSERT INTO " + targetTableName
				+ "(region_name, doubles) SELECT region_name, ARRAY_REMOVE(doubles,9) FROM " + sourceTableName;
		conn.createStatement().execute(dml);
		conn.commit();

		ResultSet rs = conn.createStatement().executeQuery("SELECT doubles FROM " + targetTableName);
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("DOUBLE", new Double[] { 5.67, 7.87 }), rs.getArray(1));
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("DOUBLE", new Double[] { 56.7, 7.87 }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionInWhere1() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT region_name FROM " + tableName + " WHERE ARRAY[2345,46345,23234]=ARRAY_REMOVE(integers,456)");
		assertTrue(rs.next());

		assertEquals("SF Bay Area", rs.getString(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionVarcharWithNull() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(varchars,NULL) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("VARCHAR", new String[] { "2345", "46345", "23234" }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionDoublesWithNull() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(doubles,NULL) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("DOUBLE", new Double[] { 10.0, 23.45, 46.345, 23.234, 45.6, 5.78 }),
				rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionCharsWithNull() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(chars,NULL) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("CHAR", new String[] { "a", "bbbb", "c", "ddd", "e", "c" }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionWithNull() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(integers,nullcheck) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("INTEGER", new Integer[] { 2345, 46345, 23234, 456 }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionFirstElement() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(varchars,'2345') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("VARCHAR", new String[] { "46345", "23234" }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionMiddleElement() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(varchars,'46345') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("VARCHAR", new String[] { "2345", "23234" }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionLastElement() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(varchars,'23234') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("VARCHAR", new String[] { "2345", "46345" }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionOneElement() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(oneItem,'alone') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("VARCHAR", new String[0]), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionRepeatingElements() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(chars,'c') FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("CHAR", new String[] { "a", "bbbb", "ddd", "e" }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionCharFromVarcharArray() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(varchars,char2) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("VARCHAR", new String[] { "46345", "23234" }), rs.getArray(1));
		assertFalse(rs.next());
	}

	@Test
	public void testArrayRemoveFunctionVarcharFromCharArray() throws Exception {
		ResultSet rs = conn.createStatement().executeQuery(
				"SELECT ARRAY_REMOVE(chars,varchar1) FROM " + tableName + " WHERE region_name = 'SF Bay Area'");
		assertTrue(rs.next());

		assertEquals(conn.createArrayOf("CHAR", new String[] { "bbbb", "c", "ddd", "e", "c" }), rs.getArray(1));
		assertFalse(rs.next());
	}

}
