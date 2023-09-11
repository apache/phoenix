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
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.Collator;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * End2End test that tests the COLLATION_KEY in an ORDER BY clause
 * 
 */
@Category(ParallelStatsDisabledTest.class)
public class CollationKeyFunctionIT extends ParallelStatsDisabledIT {

	private String tableName;
	private String[] dataArray = new String[] {
			// (0-6) chinese characters
			"\u963f", "\u55c4", "\u963e", "\u554a", "\u4ec8", "\u3d9a", "\u9f51",
			// (7-13) western characters, some with accent
			"a", "b", "ä", "A", "a", "ä", "A",
		    // null for null-input tests 
			null
		};

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
		queryWithCollKeyDefaultArgsWithExpectedOrder("zh", false, 0, 6, new Integer[] { 4, 3, 1, 5, 2, 0, 6 });
	}

	@Test
	public void testZhTwSort() throws Exception {
		queryWithCollKeyDefaultArgsWithExpectedOrder("zh_TW", false, 0, 6, new Integer[] { 4, 3, 1, 5, 2, 0, 6 });
	}

	@Test
	public void testZhTwStrokeSort() throws Exception {
		queryWithCollKeyDefaultArgsWithExpectedOrder("zh_TW_STROKE", false, 0, 6, new Integer[] { 4, 2, 0, 3, 1, 6, 5 });
	}

	@Test
	public void testZhStrokeSort() throws Exception {
		queryWithCollKeyDefaultArgsWithExpectedOrder("zh__STROKE", false, 0, 6, new Integer[] { 4, 2, 0, 3, 1, 6, 5 });
	}

	@Test
	public void testZhPinyinSort() throws Exception {
		queryWithCollKeyDefaultArgsWithExpectedOrder("zh__PINYIN", false, 0, 6, new Integer[] { 0, 1, 3, 4, 6, 2, 5 });
	}

	@Test
	public void testUpperCaseSort() throws Exception {
		queryWithCollKeyUpperCaseWithExpectedOrder("en", 7, 13, new Integer[] { 7, 10, 11, 13, 9, 12, 8 });
	}

	@Test
	public void testPrimaryStrengthSort() throws Exception {
		queryWithCollKeyWithStrengthWithExpectedOrder("en", Collator.PRIMARY, false, 7, 13,
				new Integer[] { 7, 9, 10, 11, 12, 13, 8 });
	}
	
	@Test
	public void testSecondaryStrengthSort() throws Exception {
		queryWithCollKeyWithStrengthWithExpectedOrder("en", Collator.SECONDARY, false, 7, 13,
				new Integer[] { 7, 10, 11, 13, 9, 12, 8 });
	}

	@Test
	public void testTertiaryStrengthSort() throws Exception {
		queryWithCollKeyWithStrengthWithExpectedOrder("en", Collator.TERTIARY, false, 7, 13,
				new Integer[] { 7, 11, 10, 13, 9, 12, 8 });
	}

	@Test
	public void testTertiaryStrengthSortDesc() throws Exception {
		queryWithCollKeyWithStrengthWithExpectedOrder("en", Collator.TERTIARY, true, 7, 13,
				new Integer[] { 8, 12, 9, 13, 10, 11, 7 });
	}
	
	// Null before anything else when doing ascending sort
	@Test
	public void testSortWithNullInputAsc() throws Exception {
		queryWithCollKeyDefaultArgsWithExpectedOrder("en", false, 13, 14, new Integer[] {14, 13});
	}
	
	// Null before anything else when doing descending sort (same behavior when doing order by without collation_key)
	@Test
	public void testSortWithNullInputDesc() throws Exception {
		queryWithCollKeyDefaultArgsWithExpectedOrder("en", true, 13, 14, new Integer[] {14, 13});
	}

	
	/**
	 * Issue a query ordered by the collation key (with COLLATION_KEY called
	 * with default args) of the data column according to the provided
	 * localeString, and compare the ID and data columns to the expected order.
	 * 
	 * @param expectedIndexOrder
	 *            an array of indexes into the dataArray in the order we expect.
	 *            This is the same as the ID column
	 * @throws SQLException
	 */
	private void queryWithCollKeyDefaultArgsWithExpectedOrder(String localeString, boolean isDescending, Integer beginIndex, Integer endIndex,
			Integer[] expectedIndexOrder) throws Exception {
		String sortOrder = isDescending ? "DESC" : "";

		String query = String.format(
				"SELECT id, data FROM %s WHERE ID BETWEEN %d AND %d ORDER BY COLLATION_KEY(data, '%s') %s", tableName,
				beginIndex, endIndex, localeString, sortOrder);
		queryWithExpectedOrder(query, expectedIndexOrder);
	}
	
	/**
	 * Same as above, except the upperCase collator argument is set to true
	 */
	private void queryWithCollKeyUpperCaseWithExpectedOrder(String localeString, Integer beginIndex, Integer endIndex,
			Integer[] expectedIndexOrder) throws Exception {
		String query = String.format(
				"SELECT id, data FROM %s WHERE ID BETWEEN %d AND %d ORDER BY COLLATION_KEY(data, '%s', true), id",
				tableName, beginIndex, endIndex, localeString);
		queryWithExpectedOrder(query, expectedIndexOrder);
	}

	/**
	 * Same as above, except the collator strength is set
	 */
	private void queryWithCollKeyWithStrengthWithExpectedOrder(String localeString, Integer strength, boolean isDescending,
			Integer beginIndex, Integer endIndex, Integer[] expectedIndexOrder) throws Exception {
		String sortOrder = isDescending ? "DESC" : "";
		
		String query = String.format(
				"SELECT id, data FROM %s WHERE ID BETWEEN %d AND %d ORDER BY COLLATION_KEY(data, '%s', false, %d) %s, id %s",
				tableName, beginIndex, endIndex, localeString, strength, sortOrder, sortOrder);
		queryWithExpectedOrder(query, expectedIndexOrder);
	}

	private void queryWithExpectedOrder(String query, Integer[] expectedIndexOrder) throws Exception {
		Connection conn = DriverManager.getConnection(getUrl());
		PreparedStatement ps = conn.prepareStatement(query);
		ResultSet rs = ps.executeQuery();
		int i = 0;
		while (rs.next()) {
			int expectedId = expectedIndexOrder[i];
			assertEquals("For row " + i + ": The ID did not match the expected index", expectedId, rs.getInt(1));
			assertEquals("For row " + i + ": The data did not match the expected entry from the data array",
					dataArray[expectedId], rs.getString(2));
			i++;
		}
		assertEquals("The result set returned a different number of rows from the data array", expectedIndexOrder.length, i);
	}
}
