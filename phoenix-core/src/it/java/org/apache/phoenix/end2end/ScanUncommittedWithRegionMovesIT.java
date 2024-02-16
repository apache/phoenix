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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.phoenix.iterate.ScanningResultPostDummyResultCaller;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests to ensure we perform READ_COMMITTED scan i.e. after the scanner is opened, any updated
 * data should not be visible to the scanner until it is closed. The ongoing scanner should
 * only be able to cover the region state that was present before the scanner was opened. HBase
 * provides this guarantee by using region level MVCC.
 */
@Category(NeedsOwnMiniClusterTest.class)
public class ScanUncommittedWithRegionMovesIT extends ParallelStatsDisabledIT {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(ScanUncommittedWithRegionMovesIT.class);

    private static boolean hasTestStarted = false;
    private static int countOfDummyResults = 0;
    private static final Set<String> TABLE_NAMES = new HashSet<>();

    private static class TestScanningResultPostDummyResultCaller extends
            ScanningResultPostDummyResultCaller {

        @Override
        public void postDummyProcess() {
            if (hasTestStarted && (countOfDummyResults++ % 5) == 0 &&
                    (countOfDummyResults < 17 ||
                            countOfDummyResults > 28 && countOfDummyResults < 40)) {
                LOGGER.info("Moving regions of tables {}. current count of dummy results: {}",
                        TABLE_NAMES, countOfDummyResults);
                TABLE_NAMES.forEach(table -> {
                    try {
                        moveRegionsOfTable(table);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(2);
        props.put(QueryServices.PHOENIX_SERVER_PAGE_SIZE_MS, Long.toString(0));
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, String.valueOf(true));
        props.put(QueryServices.TESTS_MINI_CLUSTER_NUM_REGION_SERVERS, String.valueOf(2));
        props.put(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY, String.valueOf(1));
        props.put(QueryServices.PHOENIX_POST_DUMMY_PROCESS,
                TestScanningResultPostDummyResultCaller.class.getName());
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @After
    public void tearDown() throws Exception {
        TABLE_NAMES.clear();
        hasTestStarted = false;
        countOfDummyResults = 0;
    }

    private static void moveRegionsOfTable(String tableName)
            throws IOException {
        if (StringUtils.isEmpty(tableName)) {
            return;
        }
        Admin admin = getUtility().getAdmin();
        List<ServerName> servers =
                new ArrayList<>(admin.getRegionServers());
        ServerName server1 = servers.get(0);
        ServerName server2 = servers.get(1);
        List<RegionInfo> regionsOnServer1;
        try {
            regionsOnServer1 = admin.getRegions(server1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        List<RegionInfo> regionsOnServer2;
        try {
            regionsOnServer2 = admin.getRegions(server2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        regionsOnServer1.forEach(regionInfo -> {
            if (regionInfo.getTable().equals(TableName.valueOf(tableName))) {
                try {
                    admin.move(regionInfo.getEncodedNameAsBytes(), server2);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        regionsOnServer2.forEach(regionInfo -> {
            if (regionInfo.getTable().equals(TableName.valueOf(tableName))) {
                try {
                    admin.move(regionInfo.getEncodedNameAsBytes(), server1);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Test
    public void testUncoveredIndex() throws Exception {
        hasTestStarted = true;
        String dataTableName = generateUniqueName();
        populateTable(dataTableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE UNCOVERED INDEX "
                    + indexTableName + " on " + dataTableName + " (val1) ");
            populateTableWithAdditionalRows(dataTableName, conn);
            conn.commit();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            String selectSql;

            // Verify that an index hint is not necessary for an uncovered index
            selectSql = "SELECT  val2, val3 from " + dataTableName +
                    " WHERE val1 IS NOT NULL";

            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertTrue(rs.next());
                assertEquals("abc", rs.getString(1));
                assertEquals("abcd", rs.getString(2));
                moveRegionsOfTable(dataTableName);
                moveRegionsOfTable(indexTableName);
                try (Connection newConn = DriverManager.getConnection(getUrl())) {
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('c1', 'cd1','cde1', 'cdef1')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('g1', 'gh1','gh11', 'gh111')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('l', 'lm','lmn', 'lmno')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('m', 'mn','mno', 'mnop')");
                    newConn.commit();
                }
                assertRowsWithOldScanner(dataTableName, indexTableName, rs, true);
            }

            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertRowsWithNewScanner(dataTableName, indexTableName, rs, true);
            }
        }
    }

    @Test
    public void testCoveredIndex() throws Exception {
        hasTestStarted = true;
        String dataTableName = generateUniqueName();
        populateTable(dataTableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX "
                    + indexTableName + " on " + dataTableName + " (val1) INCLUDE (val2, val3)");
            populateTableWithAdditionalRows(dataTableName, conn);
            conn.commit();
            TABLE_NAMES.add(dataTableName);
            TABLE_NAMES.add(indexTableName);
            String selectSql;

            // Verify that an index hint is not necessary for an uncovered index
            selectSql = "SELECT  val2, val3 from " + dataTableName +
                    " WHERE val1 IS NOT NULL";

            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertTrue(rs.next());
                assertEquals("abc", rs.getString(1));
                assertEquals("abcd", rs.getString(2));
                moveRegionsOfTable(dataTableName);
                moveRegionsOfTable(indexTableName);
                try (Connection newConn = DriverManager.getConnection(getUrl())) {
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('c1', 'cd1','cde1', 'cdef1')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('g1', 'gh1','gh11', 'gh111')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('l', 'lm','lmn', 'lmno')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('m', 'mn','mno', 'mnop')");
                    newConn.commit();
                }
                assertRowsWithOldScanner(dataTableName, indexTableName, rs, true);
            }

            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertRowsWithNewScanner(dataTableName, indexTableName, rs, true);
            }
        }
    }

    @Test
    public void testNoIndex() throws Exception {
        hasTestStarted = true;
        String dataTableName = generateUniqueName();
        populateTable(dataTableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTableWithAdditionalRows(dataTableName, conn);
            conn.commit();
            TABLE_NAMES.add(dataTableName);
            String selectSql;

            // Verify that an index hint is not necessary for an uncovered index
            selectSql = "SELECT  val2, val3 from " + dataTableName +
                    " WHERE val1 IS NOT NULL";

            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertTrue(rs.next());
                assertEquals("abc", rs.getString(1));
                assertEquals("abcd", rs.getString(2));
                moveRegionsOfTable(dataTableName);
                try (Connection newConn = DriverManager.getConnection(getUrl())) {
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('c1', 'cd1','cde1', 'cdef1')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('g1', 'gh1','gh11', 'gh111')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('l', 'lm','lmn', 'lmno')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('m', 'mn','mno', 'mnop')");
                    newConn.commit();
                }
                assertRowsWithOldScanner(dataTableName, "", rs, true);
            }

            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertRowsWithNewScanner(dataTableName, "", rs, true);
            }
        }
    }

    @Test
    public void testUncoveredIndex2() throws Exception {
        hasTestStarted = true;
        String dataTableName = generateUniqueName();
        populateTable(dataTableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE UNCOVERED INDEX "
                    + indexTableName + " on " + dataTableName + " (val1) ");
            populateTableWithAdditionalRows(dataTableName, conn);
            conn.commit();
            String selectSql;

            // Verify that an index hint is not necessary for an uncovered index
            selectSql = "SELECT  val2, val3 from " + dataTableName +
                    " WHERE val1 IS NOT NULL";

            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertTrue(rs.next());
                assertEquals("abc", rs.getString(1));
                assertEquals("abcd", rs.getString(2));
                try (Connection newConn = DriverManager.getConnection(getUrl())) {
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('c1', 'cd1','cde1', 'cdef1')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('g1', 'gh1','gh11', 'gh111')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('l', 'lm','lmn', 'lmno')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('m', 'mn','mno', 'mnop')");
                    newConn.commit();
                }
                assertRowsWithOldScanner(dataTableName, indexTableName, rs, false);
            }

            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertRowsWithNewScanner(dataTableName, indexTableName, rs, false);
            }
        }
    }

    @Test
    public void testCoveredIndex2() throws Exception {
        hasTestStarted = true;
        String dataTableName = generateUniqueName();
        populateTable(dataTableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX "
                    + indexTableName + " on " + dataTableName + " (val1) INCLUDE (val2, val3)");
            populateTableWithAdditionalRows(dataTableName, conn);
            conn.commit();
            String selectSql;

            // Verify that an index hint is not necessary for an uncovered index
            selectSql = "SELECT  val2, val3 from " + dataTableName +
                    " WHERE val1 IS NOT NULL";

            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertTrue(rs.next());
                assertEquals("abc", rs.getString(1));
                assertEquals("abcd", rs.getString(2));
                try (Connection newConn = DriverManager.getConnection(getUrl())) {
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('c1', 'cd1','cde1', 'cdef1')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('g1', 'gh1','gh11', 'gh111')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('l', 'lm','lmn', 'lmno')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('m', 'mn','mno', 'mnop')");
                    newConn.commit();
                }
                assertRowsWithOldScanner(dataTableName, indexTableName, rs, false);
            }

            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertRowsWithNewScanner(dataTableName, indexTableName, rs, false);
            }
        }
    }

    @Test
    public void testNoIndex2() throws Exception {
        hasTestStarted = true;
        String dataTableName = generateUniqueName();
        populateTable(dataTableName);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTableWithAdditionalRows(dataTableName, conn);
            conn.commit();
            TABLE_NAMES.add(dataTableName);
            String selectSql;

            // Verify that an index hint is not necessary for an uncovered index
            selectSql = "SELECT  val2, val3 from " + dataTableName +
                    " WHERE val1 IS NOT NULL";

            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertTrue(rs.next());
                assertEquals("abc", rs.getString(1));
                assertEquals("abcd", rs.getString(2));
                try (Connection newConn = DriverManager.getConnection(getUrl())) {
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('c1', 'cd1','cde1', 'cdef1')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('g1', 'gh1','gh11', 'gh111')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('l', 'lm','lmn', 'lmno')");
                    newConn.createStatement().execute("upsert into " + dataTableName
                            + " (id, val1, val2, val3) values ('m', 'mn','mno', 'mnop')");
                    newConn.commit();
                }
                assertRowsWithOldScanner(dataTableName, "", rs, false);
            }

            try (ResultSet rs = conn.createStatement().executeQuery(selectSql)) {
                assertRowsWithNewScanner(dataTableName, "", rs, false);
            }
        }
    }

    private static void populateTableWithAdditionalRows(String dataTableName, Connection conn)
            throws SQLException {
        conn.createStatement().execute("upsert into " + dataTableName
                + " (id, val1, val2, val3) values ('c', 'cd','cde', 'cdef')");
        conn.createStatement().execute("upsert into " + dataTableName
                + " (id, val1, val2, val3) values ('d', 'de','de1', 'de11')");
        conn.createStatement().execute("upsert into " + dataTableName
                + " (id, val1, val2, val3) values ('e', 'ef','ef1', 'ef11')");
        conn.createStatement().execute("upsert into " + dataTableName
                + " (id, val1, val2, val3) values ('f', 'fg','fg1', 'fg11')");
        conn.createStatement().execute("upsert into " + dataTableName
                + " (id, val1, val2, val3) values ('g', 'gh','gh1', 'gh11')");
        conn.createStatement().execute("upsert into " + dataTableName
                + " (id, val1, val2, val3) values ('h', 'hi','hi1', 'hi11')");
        conn.createStatement().execute("upsert into " + dataTableName
                + " (id, val1, val2, val3) values ('i', 'ij','ij1', 'ij11')");
        conn.createStatement().execute("upsert into " + dataTableName
                + " (id, val1, val2, val3) values ('j', 'jk','jk1', 'jk11')");
        conn.createStatement().execute("upsert into " + dataTableName
                + " (id, val1, val2, val3) values ('k', 'kl','kl1', 'kl11')");
    }

    private static void assertRowsWithNewScanner(String dataTableName, String indexTableName,
                                                 ResultSet rs, boolean moveRegions)
            throws SQLException, IOException {
        assertTrue(rs.next());
        assertEquals("abc", rs.getString(1));
        assertEquals("abcd", rs.getString(2));
        if (moveRegions) {
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
        }
        assertTrue(rs.next());
        assertEquals("bcd", rs.getString(1));
        assertEquals("bcde", rs.getString(2));
        if (moveRegions) {
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
        }
        assertTrue(rs.next());
        assertEquals("cde", rs.getString(1));
        assertEquals("cdef", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("cde1", rs.getString(1));
        assertEquals("cdef1", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("de1", rs.getString(1));
        assertEquals("de11", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("ef1", rs.getString(1));
        assertEquals("ef11", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("fg1", rs.getString(1));
        assertEquals("fg11", rs.getString(2));
        if (moveRegions) {
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
        }
        assertTrue(rs.next());
        assertEquals("gh1", rs.getString(1));
        assertEquals("gh11", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("gh11", rs.getString(1));
        assertEquals("gh111", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("hi1", rs.getString(1));
        assertEquals("hi11", rs.getString(2));
        if (moveRegions) {
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
        }
        assertTrue(rs.next());
        assertEquals("ij1", rs.getString(1));
        assertEquals("ij11", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("jk1", rs.getString(1));
        assertEquals("jk11", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("kl1", rs.getString(1));
        assertEquals("kl11", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("lmn", rs.getString(1));
        assertEquals("lmno", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("mno", rs.getString(1));
        assertEquals("mnop", rs.getString(2));
        assertFalse(rs.next());
    }

    private static void assertRowsWithOldScanner(String dataTableName, String indexTableName,
                                                 ResultSet rs, boolean moveRegions)
            throws SQLException, IOException {
        assertTrue(rs.next());
        assertEquals("bcd", rs.getString(1));
        assertEquals("bcde", rs.getString(2));
        if (moveRegions) {
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
        }
        assertTrue(rs.next());
        assertEquals("cde", rs.getString(1));
        assertEquals("cdef", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("de1", rs.getString(1));
        assertEquals("de11", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("ef1", rs.getString(1));
        assertEquals("ef11", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("fg1", rs.getString(1));
        assertEquals("fg11", rs.getString(2));
        if (moveRegions) {
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
        }
        assertTrue(rs.next());
        assertEquals("gh1", rs.getString(1));
        assertEquals("gh11", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("hi1", rs.getString(1));
        assertEquals("hi11", rs.getString(2));
        if (moveRegions) {
            moveRegionsOfTable(dataTableName);
            moveRegionsOfTable(indexTableName);
        }
        assertTrue(rs.next());
        assertEquals("ij1", rs.getString(1));
        assertEquals("ij11", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("jk1", rs.getString(1));
        assertEquals("jk11", rs.getString(2));
        assertTrue(rs.next());
        assertEquals("kl1", rs.getString(1));
        assertEquals("kl11", rs.getString(2));
        assertFalse(rs.next());
    }

    private void populateTable(String tableName) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("create table " + tableName +
                " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10)," +
                " val3 varchar(10)) SPLIT ON ('d', 'h', 'k')");
        conn.createStatement().execute("upsert into " + tableName
                + " values ('a', 'ab', 'abc', 'abcd')");
        conn.commit();
        conn.createStatement().execute("upsert into " + tableName
                + " values ('b', 'bc', 'bcd', 'bcde')");
        conn.commit();
        conn.close();
    }
}
