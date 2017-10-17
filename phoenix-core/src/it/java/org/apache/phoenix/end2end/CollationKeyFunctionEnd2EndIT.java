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

import static org.apache.phoenix.util.TestUtil.closeStmtAndConn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 * End2End test that tests the COLLATION_KEY in an ORDER BY clause
 * 
 * @author snakhoda-sfdc
 *
 */
public class CollationKeyFunctionEnd2EndIT extends ParallelStatsDisabledIT {

	private String tableName;
	private String[] dataArray = new String[] { "\u963f", "\u55c4", "\u963e", "\u554a", "\u4ec8", "\u3d9a", "\u9f51" };

	@Before
	public void initAndPopulateTable() throws Exception {
		Connection conn = null;
		PreparedStatement stmt = null;
		tableName = generateUniqueName();
		try {
			conn = DriverManager.getConnection(getUrl());
			String ddl = "CREATE TABLE " + tableName + " (id INTEGER PRIMARY KEY, data VARCHAR)";
			conn.createStatement().execute(ddl);

			// insert dataArray into the table, with the index into the array as
			// the id
			for (int i = 0; i < dataArray.length; i++) {
				PreparedStatement ps = conn.prepareStatement("upsert into " + tableName + " values(?, ?)");
				ps.setInt(1, i);
				ps.setString(2, dataArray[i]);
				ps.executeUpdate();
			}
			conn.commit();
		} finally {
			closeStmtAndConn(stmt, conn);
		}
	}

	@Test
	public void testZhSort() throws Exception {
		queryDataColumnWithCollKeyOrdering("zh", new Integer[] { 3, 0, 1, 6, 5, 4, 2 });
	}

	@Test
	public void testZhTwSort() throws Exception {
		queryDataColumnWithCollKeyOrdering("zh_TW", new Integer[] { 0, 3, 4, 1, 5, 2, 6 });
	}

	@Test
	public void testZhTwStrokeSort() throws Exception {
		queryDataColumnWithCollKeyOrdering("zh_TW_STROKE", new Integer[] { 4, 2, 0, 3, 1, 6, 5 });
	}

	@Test
	public void testZhStrokeSort() throws Exception {
		queryDataColumnWithCollKeyOrdering("zh__STROKE", new Integer[] { 0, 1, 3, 4, 6, 2, 5 });
	}

	@Test
	public void testZhPinyinSort() throws Exception {
		queryDataColumnWithCollKeyOrdering("zh__PINYIN", new Integer[] { 0, 1, 3, 4, 6, 2, 5 });
	}

	/**
	 * Issue a query ordered by the collation key of the data column according to the provided localeString,
	 * and compare the ID and data columns to the expected order.
	 * 
	 * @param expectedIndexOrder
	 *            an array of indexes into the dataArray in the order we expect.
	 *            This is the same as the ID column
	 * @throws SQLException
	 */
	private void queryDataColumnWithCollKeyOrdering(String localeString, Integer[] expectedIndexOrder)
			throws SQLException {
		String query = String.format("SELECT id, data FROM %s ORDER BY COLLATION_KEY(data, '%s')", tableName, localeString);

		Connection conn = DriverManager.getConnection(getUrl());
		PreparedStatement ps = conn.prepareStatement(query);
		ResultSet rs = ps.executeQuery();
		int i = 0;
		while (rs.next()) {
			int expectedId = expectedIndexOrder[i];
			assertEquals("For row " + i + "The ID did not match the expected index", expectedId, rs.getInt(1));
			assertEquals("For row " + i + "The data did not match the expected entry from the data array",
					dataArray[expectedId], rs.getString(2));
			i++;
		}
		assertEquals("The result set returned a different number of rows from the data array", dataArray.length, i);
	}
}
