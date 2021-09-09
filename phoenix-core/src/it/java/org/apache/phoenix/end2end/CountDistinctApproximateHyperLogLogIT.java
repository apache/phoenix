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

import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.*;

@Category(ParallelStatsDisabledTest.class)
public class CountDistinctApproximateHyperLogLogIT extends ParallelStatsDisabledIT {
	private String tableName;

	@Before
	public void generateTableNames() {
		tableName = "T_" + generateUniqueName();
	}

	@Test(expected = ColumnNotFoundException.class)
	public void testDistinctCountException() throws Exception {
		String query = "SELECT APPROX_COUNT_DISTINCT(x) FROM " + tableName;
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		
		try (Connection conn = DriverManager.getConnection(getUrl(), props);
				PreparedStatement statement = conn.prepareStatement(query);) {
			prepareTableWithValues(conn, 100);
			ResultSet rs = statement.executeQuery();
		}
	}

	@Test
	public void testDistinctCountOnConstant() throws Exception {
		String query = "SELECT APPROX_COUNT_DISTINCT(20) FROM " + tableName;
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		
		try (Connection conn = DriverManager.getConnection(getUrl(), props);
				PreparedStatement statement = conn.prepareStatement(query);) {
			prepareTableWithValues(conn, 100);
			ResultSet rs = statement.executeQuery();

			assertTrue(rs.next());
			assertEquals(1, rs.getLong(1));
			assertFalse(rs.next());
		}
	}

	@Test
	public void testDistinctCountOnSingleColumn() throws Exception {
		String query = "SELECT APPROX_COUNT_DISTINCT(i2) FROM " + tableName;
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

		try (Connection conn = DriverManager.getConnection(getUrl(), props);
				PreparedStatement statement = conn.prepareStatement(query);) {
			prepareTableWithValues(conn, 100);
			ResultSet rs = statement.executeQuery();

			assertTrue(rs.next());
			assertEquals(10, rs.getLong(1));
			assertFalse(rs.next());
		}
	}

	@Test
	public void testDistinctCountOnMutlipleColumns() throws Exception {
		String query = "SELECT APPROX_COUNT_DISTINCT(i1||i2) FROM " + tableName;
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);

		try (Connection conn = DriverManager.getConnection(getUrl(), props);
				PreparedStatement statement = conn.prepareStatement(query);) {
			prepareTableWithValues(conn, 100);

			ResultSet rs = statement.executeQuery();

			assertTrue(rs.next());
			assertEquals(100, rs.getLong(1));
			assertFalse(rs.next());
		}
	}

	@Test
	public void testDistinctCountOnjoining() throws Exception {
		String query = "SELECT APPROX_COUNT_DISTINCT(a.i1||a.i2||b.i2) FROM " + tableName + " a, " + tableName
				+ " b where a.i1=b.i1 and a.i2 = b.i2";
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		
		try(Connection conn = DriverManager.getConnection(getUrl(), props);
			PreparedStatement statement = conn.prepareStatement(query);) {
			prepareTableWithValues(conn, 100);
			ResultSet rs = statement.executeQuery();

			assertTrue(rs.next());
			assertEquals(100, rs.getLong(1));
			assertFalse(rs.next());
		}
	}

	@Test
	public void testDistinctCountPlanExplain() throws Exception {
		Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
		String query = "SELECT APPROX_COUNT_DISTINCT(i1||i2) FROM " + tableName;
		try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
			prepareTableWithValues(conn, 100);
			ExplainPlan plan = conn.prepareStatement(query)
				.unwrap(PhoenixPreparedStatement.class).optimizeQuery()
				.getExplainPlan();
			ExplainPlanAttributes explainPlanAttributes =
				plan.getPlanStepsAsAttributes();
			assertEquals(tableName, explainPlanAttributes.getTableName());
			assertEquals("PARALLEL 1-WAY",
				explainPlanAttributes.getIteratorTypeAndScanSize());
			assertEquals("FULL SCAN ", explainPlanAttributes.getExplainScanType());
			assertEquals("SERVER FILTER BY FIRST KEY ONLY",
				explainPlanAttributes.getServerWhereFilter());
			assertEquals("SERVER AGGREGATE INTO SINGLE ROW",
				explainPlanAttributes.getServerAggregate());
		}
	}

	/**
	 * Prepare tables with stats updated. format of first table such as i1, i2
	 * 1, 10 2, 20 3, 30 ...
	 * 
	 * @param conn
	 * @param nRows
	 * @throws Exception
	 */
	final private void prepareTableWithValues(final Connection conn, final int nRows) throws Exception {
		conn.createStatement().execute("create table " + tableName + "\n"
				+ "   (i1 integer not null, i2 integer not null\n" + "    CONSTRAINT pk PRIMARY KEY (i1,i2))");

		final PreparedStatement stmt = conn.prepareStatement("upsert into " + tableName + " VALUES (?, ?)");
		for (int i = 0; i < nRows; i++) {
			stmt.setInt(1, i);
			stmt.setInt(2, (i * 10) % 100);
			stmt.execute();
		}
		conn.commit();
	}
}
