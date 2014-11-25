/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.phoenix.end2end;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests for the LPAD built-in function.
 */

public class LpadFunctionIT extends BaseHBaseManagedTimeIT {
    
    /**
     * Helper to test LPAD function
     * 
     * @param conn
     *            connection to be used
     * @param colName
     *            name of column to query
     * @param length
     *            length of the output string
     * @param fillString
     *            fill characters to be used while prepending
     * @param sortOrder
     *            sort order of the pk column
     * @param expectedOutput
     *            expected output of LPAD function
     */
    private void testLpadHelper(Connection conn, String colName, int length, List<String> fillStringList,
        List<String> expectedOutputList, String sortOrder) throws Exception {
        assertEquals("fillStringList and expectedOutputList should be of equal size", fillStringList.size(),
            expectedOutputList.size());
        for (int id = 0; id < fillStringList.size(); ++id) {
            String fillString = fillStringList.get(id);
            String lPadExpr = fillString != null ? "LPAD(%s,?,?)" : "LPAD(%s,?)";
            String sql = String.format("SELECT " + lPadExpr + " FROM TEST_TABLE_%s WHERE id=?", colName, sortOrder);
            PreparedStatement stmt = conn.prepareStatement(sql);
            int index = 1;
            stmt.setInt(index++, length);
            if (fillString != null)
                stmt.setString(index++, fillString);
            stmt.setInt(index++, id);

            ResultSet rs = stmt.executeQuery();
            assertTrue("Expected exactly one row to be returned ", rs.next());
            assertEquals("LPAD returned incorrect result ", expectedOutputList.get(id), rs.getString(1));
            assertFalse("Expected exactly one row to be returned ", rs.next());
        }
    }

    /**
     * Helper to test LPAD function
     * 
     * @param conn
     *            connection to phoenix
     * @param inputList
     *            list of values to test
     * @param length
     *            length to be used in lpad function
     * @param fillStringList
     *            list of fill string to be used while testing
     * @param colName
     *            name of column to be used as function input
     * @param expecetedOutputList
     *            list of expected output values
     * @param expectedOutputList
     *            expected output of lpad function
     */
    private void testLpad(Connection conn, List<String> inputList, int length, List<String> fillStringList,
        String colName, List<String> expectedOutputList) throws Exception {
        TestUtil.initTables(conn, "VARCHAR", new ArrayList<Object>(inputList));
        testLpadHelper(conn, colName, length, fillStringList, expectedOutputList, "ASC");
        testLpadHelper(conn, colName, length, fillStringList, expectedOutputList, "DESC");
    }

    private void testLpad(Connection conn, List<String> inputList, int length, List<String> fillStringList,
        List<String> expectedOutputList) throws Exception {
        testLpad(conn, inputList, length, fillStringList, "pk", expectedOutputList);
    }

    @Test
    public void testNullInputStringSB() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        testLpad(conn, Lists.newArrayList("X", "X"), 4, Lists.newArrayList("", ""), "kv",
            Lists.<String> newArrayList(null, null));
    }

    @Test
    public void testEmptyFillExpr() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        testLpad(conn, Lists.newArrayList("ABCD", "ണഫɰɸ"), 6, Lists.newArrayList("", ""),
            Lists.<String> newArrayList(null, null));
    }

    @Test
    public void testDefaultFill() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        testLpad(conn, Lists.newArrayList("ABCD", "ണഫɰɸ"), 6, Lists.<String> newArrayList(null, null),
            Lists.newArrayList("  ABCD", "  ണഫɰɸ"));
    }

    @Test
    public void testLpadFillLengthGreaterThanPadLength() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        testLpad(conn, Lists.newArrayList("ABCD", "ണഫɰɸ", "ണഫɰɸ", "ABCD"), 8,
            Lists.newArrayList("123456", "ɚɚɦɚɚɦ", "123456", "ണഫɰɸണഫ"),
            Lists.newArrayList("1234ABCD", "ɚɚɦɚണഫɰɸ", "1234ണഫɰɸ", "ണഫɰɸABCD"));
    }

    @Test
    public void testLpadFillLengthLessThanPadLength() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        testLpad(conn, Lists.newArrayList("ABCD", "ɰɸɰɸ", "ɰɸɰɸ", "ABCD"), 8,
            Lists.newArrayList("12", "ഫɰ", "12", "ഫɰ"),
            Lists.newArrayList("1212ABCD", "ഫɰഫɰɰɸɰɸ", "1212ɰɸɰɸ", "ഫɰഫɰABCD"));
    }

    @Test
    public void testLpadFillLengthEqualPadLength() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        testLpad(conn, Lists.newArrayList("ABCD", "ɰɸɰɸ", "ɰɸɰɸ", "ABCD"), 8,
            Lists.newArrayList("1234", "ണഫɰɸ", "1234", "ണഫɰɸ"),
            Lists.newArrayList("1234ABCD", "ണഫɰɸɰɸɰɸ", "1234ɰɸɰɸ", "ണഫɰɸABCD"));
    }

    @Test
    public void testLpadZeroPadding() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        ArrayList<String> inputList = Lists.newArrayList("ABCD", "ണഫɰɸ", "ണഫɰɸ", "ABCD");
        testLpad(conn, inputList, 4, Lists.newArrayList("1234", "ɚɦɚɦ", "1234", "ɚɦɚɦ"), inputList);
    }

    @Test
    public void testLpadTrucate() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        testLpad(conn, Lists.newArrayList("ABCD", "ണഫɰɸ", "ണഫɰɸ", "ABCD"), 2,
            Lists.newArrayList("12", "ɚɦ", "12", "ɚɦ"), Lists.newArrayList("AB", "ണഫ", "ണഫ", "AB"));
    }

    @Test
    public void testLpadZeroOutputStringLength() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        testLpad(conn, Lists.newArrayList("ABCD", "ണഫɰɸ", "ണഫɰɸ", "ABCD"), 0,
            Lists.newArrayList("12", "ɚɦ", "12", "ɚɦ"), Lists.<String> newArrayList(null, null, null, null));
    }

    @Test
    public void testNegativeOutputStringLength() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        testLpad(conn, Lists.newArrayList("ABCD", "ണഫɰɸ", "ണഫɰɸ", "ABCD"), -1,
            Lists.newArrayList("12", "ɚɦ", "12", "ɚɦ"), Lists.<String> newArrayList(null, null, null, null));
    }

}