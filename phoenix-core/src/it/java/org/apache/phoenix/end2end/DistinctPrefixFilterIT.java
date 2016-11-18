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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Before;
import org.junit.Test;

public class DistinctPrefixFilterIT extends ParallelStatsDisabledIT {
    private static final String PREFIX = "SERVER DISTINCT PREFIX";
    private String testTableF;
    private String testTableV;
    private String testSeq;
    private Connection conn;

    @Before
    public void initTables() throws Exception {
        testTableF = generateUniqueName();
        testTableV = generateUniqueName();
        testSeq = "SEQ_" + generateUniqueName();

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String ddl = "CREATE TABLE " + testTableF +
                "  (prefix1 INTEGER NOT NULL, prefix2 INTEGER NOT NULL, prefix3 INTEGER NOT NULL, " +
                "col1 FLOAT, col2 INTEGER, CONSTRAINT pk PRIMARY KEY(prefix1, prefix2, prefix3)) DISABLE_WAL=true, IMMUTABLE_ROWS=true";
        createTestTable(getUrl(), ddl);

        ddl = "CREATE TABLE " + testTableV +
                "  (prefix1 varchar NOT NULL, prefix2 varchar NOT NULL, prefix3 INTEGER NOT NULL, " +
                "col1 FLOAT, col2 INTEGER, CONSTRAINT pk PRIMARY KEY(prefix1, prefix2, prefix3)) DISABLE_WAL=true, IMMUTABLE_ROWS=true, SALT_BUCKETS=8";
        createTestTable(getUrl(), ddl);

        conn.prepareStatement("CREATE INDEX " + testTableF + "_idx ON "+testTableF+"(col2) DISABLE_WAL=true").execute();
        conn.prepareStatement("CREATE LOCAL INDEX " + testTableV + "_idx ON "+testTableV+"(col2) DISABLE_WAL=true").execute();

        conn.prepareStatement("CREATE SEQUENCE " + testSeq + " CACHE 1000").execute();

        insertPrefixF(1, 1);
        insertPrefixF(1, 2);
        insertPrefixF(1, 3);
        insertPrefixF(2, 1);
        insertPrefixF(2, 2);
        insertPrefixF(2, 3);
        insertPrefixF(3, 1);
        insertPrefixF(3, 2);
        insertPrefixF(2147483647, 2147483647); // all xFF
        insertPrefixF(3, 2147483647); // all xFF
        insertPrefixF(3, 3);
        conn.commit();

        insertPrefixV("1", "1");
        insertPrefixV("1", "2");
        insertPrefixV("1", "3");
        insertPrefixV("2", "1");
        insertPrefixV("2", "2");
        insertPrefixV("2", "3");
        insertPrefixV("22", "1");
        insertPrefixV("3", "22");
        insertPrefixV("3", "1");
        insertPrefixV("3", "2");
        insertPrefixV("3", "3");
        conn.commit();
        ResultSet rs;
        rs = conn.createStatement().executeQuery("select /*+ NO_INDEX */ count(*) from " + testTableV);
        assertTrue(rs.next());
        long count1 = rs.getLong(1);
        rs = conn.createStatement().executeQuery("select count(*) from " + testTableV + "_idx");
        assertTrue(rs.next());
        long count2 = rs.getLong(1);
        assertEquals(count1,count2);

        multiply();
        multiply();
        multiply();
        multiply();
        multiply();
        multiply();
        multiply();
        multiply(); // 256 per unique prefix
    }

    @Test
    public void testCornerCases() throws Exception {
        String testTable = generateUniqueName();
        String ddl = "CREATE TABLE " + testTable +
                "  (prefix1 INTEGER NOT NULL, prefix2 SMALLINT NOT NULL, prefix3 INTEGER NOT NULL, " +
                "col1 FLOAT, CONSTRAINT pk PRIMARY KEY(prefix1, prefix2, prefix3))";
        createTestTable(getUrl(), ddl);

        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + testTable
                + "(prefix1, prefix2, prefix3, col1) VALUES(?,?,NEXT VALUE FOR "+testSeq+",rand())");
        stmt.setInt(1, 1);
        stmt.setInt(2, 2);
        stmt.execute();

        stmt = conn.prepareStatement("UPSERT INTO " + testTable
                + "(prefix1, prefix2, prefix3, col1) VALUES(?,?,NEXT VALUE FOR "+testSeq+",rand())");
        stmt.setInt(1, 2);
        stmt.setInt(2, 32767);
        stmt.execute();

        stmt = conn.prepareStatement("UPSERT INTO " + testTable
                + "(prefix1, prefix2, prefix3, col1) VALUES(?,?,NEXT VALUE FOR "+testSeq+",rand())");
        stmt.setInt(1, 3);
        stmt.setInt(2, 1);
        stmt.execute();

        stmt = conn.prepareStatement("UPSERT INTO " + testTable
                + "(prefix1, prefix2, prefix3, col1) VALUES(?,?,NEXT VALUE FOR "+testSeq+",rand())");
        stmt.setInt(1, 3);
        stmt.setInt(2, 2);
        stmt.execute();
        conn.commit();

        testSkipRange("SELECT %s prefix1 FROM "+ testTable + " GROUP BY prefix1 ORDER BY prefix1 DESC", 3);
        testSkipRange("SELECT %s DISTINCT prefix1 FROM " + testTable + " ORDER BY prefix1 DESC", 3);
        testSkipRange("SELECT %s DISTINCT prefix1 FROM " + testTable + " ORDER BY prefix1 DESC LIMIT 2", 2);
    }

    @Test
    public void testPlans() throws Exception {
        // use the filter even when the SkipScan filter is used
        testPlan("SELECT DISTINCT prefix1, prefix2 FROM "+testTableF+ " WHERE prefix1 IN (1,2)", true);
        testPlan("SELECT prefix1, 1, 2 FROM "+testTableF+" GROUP BY prefix1 HAVING prefix1 = 1", true);
        testPlan("SELECT prefix1 FROM "+testTableF+" GROUP BY prefix1, TRUNC(prefix1), TRUNC(prefix2)", true);
        testPlan("SELECT DISTINCT prefix1, prefix2 FROM "+testTableV+ " WHERE prefix1 IN ('1','2')", true);
        testPlan("SELECT prefix1, 1, 2 FROM "+testTableV+" GROUP BY prefix1 HAVING prefix1 = '1'", true);
        // make sure we do not mis-optimize this case
        testPlan("SELECT DISTINCT SUM(prefix1) FROM "+testTableF+" GROUP BY prefix1", false);

        testCommonPlans(testTableF, PREFIX);
        testCommonPlans(testTableV, PREFIX);
    }

    private void testCommonPlans(String testTable, String contains) throws Exception {
        testPlan("SELECT DISTINCT prefix1 FROM "+testTable, true);

        testPlan("SELECT COUNT(DISTINCT prefix1) FROM "+testTable, true);
        testPlan("SELECT COUNT(DISTINCT prefix1), COUNT(DISTINCT prefix2) FROM "+testTable, true);
        testPlan("SELECT COUNT(DISTINCT prefix1), COUNT(DISTINCT (prefix1,prefix2)) FROM "+testTable, true);
        // a plain aggregate, cannot optimize
        testPlan("SELECT COUNT(prefix1), COUNT(DISTINCT prefix1) FROM "+testTable, false);
        testPlan("SELECT COUNT(*) FROM (SELECT DISTINCT(prefix1) FROM "+testTable+")", true);
        testPlan("SELECT /*+ RANGE_SCAN */ DISTINCT prefix1 FROM "+testTable, false);
        testPlan("SELECT DISTINCT prefix1, prefix2 FROM "+testTable, true);
        // do not use the filter when the distinct is on the entire key
        testPlan("SELECT DISTINCT prefix1, prefix2, prefix3 FROM "+testTable, false);
        testPlan("SELECT DISTINCT (prefix1, prefix2, prefix3) FROM "+testTable, false);
        testPlan("SELECT DISTINCT prefix1, prefix2, col1, prefix3 FROM "+testTable, false);
        testPlan("SELECT DISTINCT prefix1, prefix2, col1 FROM "+testTable, false);
        testPlan("SELECT DISTINCT col1, prefix1, prefix2 FROM "+testTable, false);
        testPlan("SELECT DISTINCT col1 FROM "+testTable, false);
        testPlan("SELECT COUNT(DISTINCT col1) FROM "+testTable, false);
        testPlan("SELECT DISTINCT col2 FROM "+testTable, true);
        testPlan("SELECT COUNT(DISTINCT col2) FROM "+testTable, true);
        testPlan("SELECT prefix1 FROM "+testTable+" GROUP BY prefix1", true);
        testPlan("SELECT COUNT(prefix1) FROM (SELECT prefix1 FROM "+testTable+" GROUP BY prefix1)", true);
        // aggregate over the group by, cannot optimize
        testPlan("SELECT prefix1, count(*) FROM "+testTable+" GROUP BY prefix1", false);
        testPlan("SELECT prefix1 FROM "+testTable+" GROUP BY prefix1, prefix2", true);
        // again using full key
        testPlan("SELECT prefix1 FROM "+testTable+" GROUP BY prefix1, prefix2, prefix3", false);
        testPlan("SELECT (prefix1, prefix2, prefix3) FROM "+testTable+" GROUP BY (prefix1, prefix2, prefix3)", false);
        testPlan("SELECT prefix1, 1, 2 FROM "+testTable+" GROUP BY prefix1", true);
        testPlan("SELECT prefix1 FROM "+testTable+" GROUP BY prefix1, col1", false);

        testPlan("SELECT COUNT(DISTINCT prefix1) FROM "+testTable+" HAVING COUNT(col1) > 10", false);
        testPlan("SELECT COUNT(DISTINCT prefix1) FROM "+testTable+" ORDER BY COUNT(col1)", true);
        testPlan("SELECT COUNT(DISTINCT prefix1) FROM "+testTable+" ORDER BY COUNT(prefix1)", true);
        testPlan("SELECT COUNT(DISTINCT prefix1) FROM "+testTable+" ORDER BY COUNT(prefix2)", true);

        // can't optimize the following, yet, even though it would be possible
        testPlan("SELECT COUNT(DISTINCT prefix1) FROM "+testTable+" HAVING COUNT(DISTINCT prefix2) > 10", false);
        testPlan("SELECT COUNT(DISTINCT prefix1) FROM "+testTable+" HAVING COUNT(DISTINCT prefix1) > 10", false);
        testPlan("SELECT COUNT(DISTINCT prefix1) / 10 FROM "+testTable, false);
        // do not use the filter when the boolean expression filter is used
        testPlan("SELECT DISTINCT prefix1, prefix2 FROM "+testTable+" WHERE col1 > 0.5", false);
    }

    private void testPlan(String query, boolean optimizable) throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN "+query);
        assertEquals(optimizable, QueryUtil.getExplainPlan(rs).contains(PREFIX));
    }

    @Test
    public void testGroupBy() throws Exception {
        testSkipRange("SELECT %s prefix1 FROM "+ testTableF + " GROUP BY prefix1, prefix2 HAVING prefix1 IN (1,2)", 6);
        testSkipRange("SELECT %s prefix1 FROM "+ testTableF + " GROUP BY prefix1, prefix2 HAVING prefix1 IN (1,2) AND prefix2 IN (1,2)", 4);
        // this leads to a scan along [prefix1,prefix2], but work correctly
        testSkipRange("SELECT %s prefix1 FROM "+ testTableF + " GROUP BY prefix1, prefix2 HAVING prefix2 = 2", 3);
        testSkipRange("SELECT %s prefix1 FROM "+ testTableF + " GROUP BY prefix1, prefix2 HAVING prefix2 = 2147483647", 2);
        testSkipRange("SELECT %s prefix1 FROM "+ testTableF + " GROUP BY prefix1, prefix2 HAVING prefix1 = 2147483647", 1);
        testSkipRange("SELECT %s prefix1 FROM "+ testTableF + " WHERE col1 > 0.99 GROUP BY prefix1, prefix2 HAVING prefix2 = 2", -1);
        testSkipRange("SELECT %s prefix1 FROM "+ testTableF + " WHERE col1 >=0 and col2 > 990 GROUP BY prefix1, prefix2 HAVING prefix2 = 2", -1);

        testSkipRange("SELECT %s prefix1 FROM "+ testTableV + " GROUP BY prefix1, prefix2 HAVING prefix1 IN ('1','2')", 6);
        testSkipRange("SELECT %s prefix1 FROM "+ testTableV + " GROUP BY prefix1, prefix2 HAVING prefix1 IN ('1','2') AND prefix2 IN ('1','2')", 4);
        // this leads to a scan along [prefix1,prefix2], but work correctly
        testSkipRange("SELECT %s prefix1 FROM "+ testTableV + " GROUP BY prefix1, prefix2 HAVING prefix2 = '2'", 3);
        testSkipRange("SELECT %s prefix1 FROM "+ testTableV + " GROUP BY prefix1, prefix2 HAVING prefix2 = '22'", 1);
        testSkipRange("SELECT %s prefix1 FROM "+ testTableV + " GROUP BY prefix1, prefix2 HAVING prefix1 = '22'", 1);
        testSkipRange("SELECT %s prefix1 FROM "+ testTableV + " WHERE col1 > 0.99 GROUP BY prefix1, prefix2 HAVING prefix2 = '2'", -1);
        testSkipRange("SELECT %s prefix1 FROM "+ testTableV + " WHERE col1 >= 0 and col2 > 990 GROUP BY prefix1, prefix2 HAVING prefix2 = '2'", -1);

        testCommonGroupBy(testTableF);
        testCommonGroupBy(testTableV);
    }

    private void testCommonGroupBy(String testTable) throws Exception {
        testSkipRange("SELECT %s prefix1 FROM "+ testTable + " GROUP BY prefix1", 4);
        testSkipRange("SELECT %s prefix1 FROM "+ testTable + " GROUP BY prefix1 ORDER BY prefix1 DESC", 4);
        testSkipRange("SELECT %s prefix1 FROM "+ testTable + " GROUP BY prefix1, prefix2", 11);
        testSkipRange("SELECT %s prefix1 FROM "+ testTable + " GROUP BY prefix1, prefix2 ORDER BY prefix1 DESC", 11);
        testSkipRange("SELECT %s prefix1 FROM "+ testTable + " GROUP BY prefix1, prefix2 ORDER BY prefix2 DESC", 11);
        testSkipRange("SELECT %s prefix1 FROM "+ testTable + " GROUP BY prefix1, prefix2 ORDER BY prefix1, prefix2 DESC", 11);
    }

    @Test
    public void testDistinct() throws Exception {
        // mix distinct prefix and SkipScan filters
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTableF + " WHERE prefix1 IN (1,2)", 6);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTableF + " WHERE prefix1 IN (3,2147483647)", 5);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTableF + " WHERE prefix1 IN (3,2147483647) ORDER BY prefix1 DESC", 5);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTableF + " WHERE prefix1 IN (3,2147483647) ORDER BY prefix2 DESC", 5);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTableF + " WHERE prefix1 IN (2147483647,2147483647)", 1);
        // mix distinct and boolean expression filters
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTableF + " WHERE col1 > 0.99 AND prefix1 IN (1,2)", -1);

        testCount("SELECT %s COUNT(DISTINCT prefix1), COUNT(DISTINCT (prefix1, prefix2)) FROM " + testTableF + " WHERE prefix2=2", 3, 3);
        testCount("SELECT %s COUNT(DISTINCT prefix1), COUNT(DISTINCT (prefix1, prefix2)) FROM " + testTableF + " WHERE prefix1=2", 1, 3);

        // mix distinct prefix and SkipScan filters
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTableV + " WHERE prefix1 IN ('1','2')", 6);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTableV + " WHERE prefix1 IN ('3','22')", 5);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTableV + " WHERE prefix1 IN ('3','22') ORDER BY prefix1 DESC", 5);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTableV + " WHERE prefix1 IN ('3','22') ORDER BY prefix2 DESC", 5);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTableV + " WHERE prefix1 IN ('2','22')", 4);
        // mix distinct and boolean expression filters
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTableV + " WHERE col1 > 0.99 AND prefix1 IN ('1','2')", -1);

        testCommonDistinct(testTableF);
        testCommonDistinct(testTableV);
    }

    private void testCommonDistinct(String testTable) throws Exception {
        testSkipRange("SELECT %s DISTINCT prefix1 FROM " + testTable, 4);
        testSkipRange("SELECT %s DISTINCT prefix1 FROM " + testTable + " ORDER BY prefix1 DESC", 4);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTable, 11);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTable + " ORDER BY prefix1 DESC", 11);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTable + " ORDER BY prefix2 DESC", 11);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTable + " ORDER BY prefix1, prefix2 DESC", 11);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTable + " WHERE col1 > 0.99", -1);
        testSkipRange("SELECT %s DISTINCT prefix1, prefix2 FROM " + testTable + " WHERE col1 > 0.99 ORDER BY prefix1, prefix2 DESC", -1);

        testCount("SELECT %s COUNT(DISTINCT prefix1) FROM " + testTable, 4);
        testCount("SELECT COUNT(*) FROM (SELECT %s DISTINCT prefix1, prefix2 FROM " + testTable + ")", 11);
        testCount("SELECT %s COUNT(DISTINCT prefix1) FROM " + testTable + " WHERE col1 > 0.99", -1);
        testCount("SELECT COUNT(*) FROM (SELECT %s DISTINCT prefix1, prefix2 FROM " + testTable + " WHERE col1 > 0.99)", -1);
        testCount("SELECT %s COUNT(DISTINCT prefix1), COUNT(DISTINCT prefix2) FROM " + testTable, 4, 4);
        testCount("SELECT %s COUNT(DISTINCT prefix1), COUNT(DISTINCT (prefix1, prefix2)) FROM " + testTable, 4, 11);
        testCount("SELECT %s COUNT(DISTINCT prefix1), COUNT(DISTINCT (prefix1, prefix2)) FROM " + testTable + " WHERE col1 > 0.99", -1, -1);

        testCount("SELECT %s COUNT(DISTINCT col1) FROM " + testTable, -1);
        testCount("SELECT %s COUNT(DISTINCT col2) FROM " + testTable, -1);

        testCount("SELECT %s COUNT(DISTINCT prefix1) FROM " + testTable + " WHERE col1 < 0", -1);
    }

    @Test
    public void testRVC() throws Exception {
        int count = 0;
        ResultSet res1 = conn.createStatement().executeQuery("SELECT (prefix1, prefix2) FROM "+ testTableF + " GROUP BY (prefix1, prefix2)");
        ResultSet res2 = conn.createStatement().executeQuery("SELECT /*+ RANGE_SCAN */ (prefix1, prefix2) FROM "+ testTableF + " GROUP BY (prefix1, prefix2)");
        ResultSet res3 = conn.createStatement().executeQuery("SELECT DISTINCT(prefix1, prefix2) FROM "+ testTableF);
        ResultSet res4 = conn.createStatement().executeQuery("SELECT /*+ RANGE_SCAN */ DISTINCT(prefix1, prefix2) FROM "+ testTableF);
        while (res1.next()) {
            byte[] r1 = res1.getBytes(1);
    
            assertTrue(res2.next());
            byte[] r2 = res2.getBytes(1);
            assertArrayEquals(r1, r2);
    
            assertTrue(res3.next());
            byte[] r3 = res3.getBytes(1);
            assertArrayEquals(r1, r3);
    
            assertTrue(res4.next());
            byte[] r4 = res4.getBytes(1);
            assertArrayEquals(r1, r4);

            count++;
        }
        assertFalse(res2.next());
        assertFalse(res3.next());
        assertFalse(res4.next());
        assertEquals(11,count);
    }

    private void testSkipRange(String q, int expected) throws SQLException {
        String q1 = String.format(q, "");
        PreparedStatement stmt = conn.prepareStatement(q1);
        ResultSet res = stmt.executeQuery();
        int count = 0;
        while(res.next()) {
            count++;
        }

        if (expected > 0) assertEquals(expected, count);

        q1 = String.format(q, "/*+ RANGE_SCAN */");
        stmt = conn.prepareStatement(q1);
        res = stmt.executeQuery();
        int count1 = 0;
        while(res.next()) {
            count1++;
        }
        assertEquals(count, count1);
    }

    private void testCount(String q, int... expected) throws SQLException {
        String q1 = String.format(q, "");
        PreparedStatement stmt = conn.prepareStatement(q1);
        ResultSet res = stmt.executeQuery();
        int[] count = new int[expected.length];
        assertTrue(res.next());
        for (int i=0; i<expected.length; i++) {
            count[i] = res.getInt(i+1);
            if (expected[i] > 0) assertEquals(expected[i], count[i]);
        }
        assertFalse(res.next());

        q1 = String.format(q, "/*+ RANGE_SCAN */");
        stmt = conn.prepareStatement(q1);
        res = stmt.executeQuery();
        assertTrue(res.next());
        for (int i=0; i<expected.length; i++) {
            assertEquals(count[i], res.getInt(i+1));
        }
        assertFalse(res.next());
    }

    private void insertPrefixF(int prefix1, int prefix2) throws SQLException {
        String query = "UPSERT INTO " + testTableF
                + "(prefix1, prefix2, prefix3, col1, col2) VALUES(?,?,NEXT VALUE FOR "+testSeq+",rand(), trunc(rand()*1000))";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setInt(1, prefix1);
            stmt.setInt(2, prefix2);
            stmt.execute();
    }

    private void insertPrefixV(String prefix1, String prefix2) throws SQLException {
        String query = "UPSERT INTO " + testTableV
                + "(prefix1, prefix2, prefix3, col1, col2) VALUES(?,?,NEXT VALUE FOR "+testSeq+",rand(), trunc(rand()*1000))";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, prefix1);
            stmt.setString(2, prefix2);
            stmt.execute();
    }

    private void multiply() throws SQLException {
        conn.prepareStatement("UPSERT INTO " + testTableF
                + " SELECT prefix1,prefix2,NEXT VALUE FOR "+testSeq+",rand(), trunc(rand()*1000) FROM "+testTableF).execute();
        conn.prepareStatement("UPSERT INTO " + testTableV
                + " SELECT prefix1,prefix2,NEXT VALUE FOR "+testSeq+",rand(), trunc(rand()*1000) FROM "+testTableV).execute();
        conn.commit();
    }
}
