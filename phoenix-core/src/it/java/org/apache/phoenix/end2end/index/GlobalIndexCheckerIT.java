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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.mapreduce.PhoenixJobCounters.INPUT_RECORDS;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_OLD_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.BEFORE_REBUILD_VALID_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.REBUILT_INDEX_ROW_COUNT;
import static org.apache.phoenix.mapreduce.index.PhoenixIndexToolJobCounters.SCANNED_DATA_ROW_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Maps;

import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class GlobalIndexCheckerIT extends BaseUniqueNamesOwnClusterIT {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalIndexCheckerIT.class);
    private final boolean async;
    private String tableDDLOptions;
    private StringBuilder optionBuilder;
    private final boolean encoded;
    public GlobalIndexCheckerIT(boolean async, boolean encoded) {
        this.async = async;
        this.encoded = encoded;
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.GLOBAL_INDEX_ROW_AGE_THRESHOLD_TO_DELETE_MS_ATTRIB, Long.toString(0));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Before
    public void beforeTest(){
        optionBuilder = new StringBuilder();
        if (!encoded) {
            optionBuilder.append(" COLUMN_ENCODED_BYTES=0");
        }
        this.tableDDLOptions = optionBuilder.toString();
    }

    @Parameters(
            name = "async={0},encoded={1}")
    public static synchronized Collection<Object[]> data() {
        List<Object[]> list = Lists.newArrayListWithExpectedSize(4);
        boolean[] Booleans = new boolean[]{true, false};
            for (boolean async : Booleans) {
                for (boolean encoded : Booleans) {
                    list.add(new Object[]{async, encoded});
                }
            }
        return list;
    }

    @After
    public void unsetFailForTesting() {
        IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
        IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
        IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
    }

    public static void assertExplainPlan(Connection conn, String selectSql,
                                         String dataTableFullName, String indexTableFullName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
        String actualExplainPlan = QueryUtil.getExplainPlan(rs);
        IndexToolIT.assertExplainPlan(false, actualExplainPlan, dataTableFullName, indexTableFullName);
    }

    private void populateTable(String tableName) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("create table " + tableName +
                " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), val3 varchar(10))" + tableDDLOptions);
        conn.createStatement().execute("upsert into " + tableName + " values ('a', 'ab', 'abc', 'abcd')");
        conn.commit();
        conn.createStatement().execute("upsert into " + tableName + " values ('b', 'bc', 'bcd', 'bcde')");
        conn.commit();
        conn.close();
    }

    @Test
    public void testDelete() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
            String dml = "DELETE from " + dataTableName + " WHERE id  = 'a'";
            assertEquals(1, conn.createStatement().executeUpdate(dml));
            conn.commit();
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : ""));
            if (async) {
                // run the index MR job.
                IndexToolIT.runIndexTool(true, false, null, dataTableName, indexTableName);
            }
            // Count the number of index rows
            String query = "SELECT COUNT(*) from " + indexTableName;
            // There should be one row in the index table
            ResultSet rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testDeleteNonExistingRow() throws Exception {
        if (async) {
            // No need to run the same test twice one for async = true and the other for async = false
            return;
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)");

            String dml = "DELETE from " + dataTableName + " WHERE id  = 'a'";
            conn.createStatement().executeUpdate(dml);
            conn.commit();
            // Attempt to delete a row that does not exist
            conn.createStatement().executeUpdate(dml);
            conn.commit();
            // Make sure this delete attempt did not make the index and data table inconsistent
            IndexToolIT.runIndexTool(true, false, "", dataTableName, indexTableName, null,
                    0, IndexTool.IndexVerifyType.ONLY);
        }
    }

    @Test
    public void testIndexRowWithoutEmptyColumn() throws Exception {
        if (async) {
            // No need to run the same test twice one for async = true and the other for async = false
            return;
        }
        long scn;
        String dataTableName = generateUniqueName();
        String indexTableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)");
            scn = EnvironmentEdgeManager.currentTimeMillis();
            // Configure IndexRegionObserver to fail the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName + " values ('a', 'abc','abcc', 'abccd')");
            commitWithException(conn);
            // The above upsert will create an unverified index row with row key {'abc', 'a'} and will make the existing
            // index row with row key {'ab', 'a'} unverified
            // Configure IndexRegionObserver to allow the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            // Do major compaction which will remove the empty column cell from the index row with row key {'ab', 'a'}
            TestUtil.doMajorCompaction(conn, indexTableName);
        }
        Properties props = new Properties();
        props.setProperty("CurrentSCN", Long.toString(scn));
        try (Connection connWithSCN = DriverManager.getConnection(getUrl(), props)) {
            String selectSql =  "SELECT * from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the index table
            assertExplainPlan(connWithSCN, selectSql, dataTableName, indexTableName);
            ResultSet rs = connWithSCN.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("abc", rs.getString(3));
            assertEquals("abcd", rs.getString(4));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testLimitWithUnverifiedRows() throws Exception {
        if (async) {
            return;
        }
        String dataTableName = generateUniqueName();
        populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)");
            conn.commit();
            // Read all index rows and rewrite them back directly. This will overwrite existing rows with newer
            // timestamps and set the empty column to value "x". This will make them unverified
            conn.createStatement().execute("UPSERT INTO " + indexTableName + " SELECT * FROM " +
                    indexTableName);
            conn.commit();
            // Verified that read repair will not reduce the number of rows returned for LIMIT queries
            String selectSql = "SELECT * from " + indexTableName + " LIMIT 1";
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("ab", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertEquals("abc", rs.getString(3));
            assertEquals("abcd", rs.getString(4));
            assertFalse(rs.next());
            selectSql = "SELECT val3 from " + dataTableName + " WHERE val1 = 'bc' LIMIT 1";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("bcde", rs.getString(1));
            assertFalse(rs.next());
            // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update phase) where the verify flag is set
            // to true and/or index rows are deleted and check that this does not impact the correctness
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            // This is to cover the case where there is no data table row for an unverified index row
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1, val2) values ('c', 'aa','cde')");
            commitWithException(conn);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            // Verified that read repair will not reduce the number of rows returned for LIMIT queries
            selectSql = "SELECT * from " + indexTableName + " LIMIT 1";
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("ab", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertEquals("abc", rs.getString(3));
            assertEquals("abcd", rs.getString(4));
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);

        }
    }

    @Test
    public void testSimulateConcurrentUpdates() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : ""));
            if (async) {
                // run the index MR job.
                IndexToolIT.runIndexTool(true, false, null, dataTableName, indexTableName);
            }
            // For the concurrent updates on the same row, the last write phase is ignored.
            // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update phase) where the
            // verify flag is set to true and/or index rows are deleted and check that this does not impact the
            // correctness.
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            // Do multiple updates on the same data row
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1) values ('a', 'aa')");
            conn.commit();
            // The expected state of the index table is  {('aa', 'a', 'abcc', 'abcd'), ('bc', 'b', 'bcd', 'bcde')}
            // Do more multiple updates on the same data row
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1, val3) values ('a', null, null)");
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1) values ('a', 'ab')");
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1) values ('b', 'ab')");
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1, val2) values ('b', 'ab', null)");
            conn.commit();
            // Now the expected state of the index table is  {('ab', 'a', 'abcc' , null), ('ab', 'b', null, 'bcde')}
            ResultSet rs = conn.createStatement().executeQuery("SELECT * from "  + indexTableName);
            assertTrue(rs.next());
            assertEquals("ab", rs.getString(1));
            assertEquals("a", rs.getString(2));
            assertEquals("abcc", rs.getString(3));
            assertEquals(null, rs.getString(4));
            assertTrue(rs.next());
            assertEquals("ab", rs.getString(1));
            assertEquals("b", rs.getString(2));
            assertEquals(null, rs.getString(3));
            assertEquals("bcde", rs.getString(4));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testFailPostIndexDeleteUpdate() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : ""));
            if (async) {
                // run the index MR job.
                IndexToolIT.runIndexTool(true, false, null, dataTableName, indexTableName);
            }
            String selectSql = "SELECT id from " + dataTableName + " WHERE val1  = 'ab'";
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertFalse(rs.next());

            // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update phase) where the verify flag is set
            // to true and/or index rows are deleted and check that this does not impact the correctness
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            String dml = "DELETE from " + dataTableName + " WHERE id  = 'a'";
            assertEquals(1, conn.createStatement().executeUpdate(dml));
            conn.commit();

            // The index rows are actually not deleted yet because IndexRegionObserver failed delete operation. However, they are
            // made unverified in the pre index update phase (i.e., the first write phase)
            dml = "DELETE from " + dataTableName + " WHERE val1  = 'ab'";
            // This DML will scan the Index table and detect unverified index rows. This will trigger read repair which
            // result in deleting these rows since the corresponding data table rows are deleted already. So, the number of
            // rows to be deleted by the "DELETE" DML will be zero since the rows deleted by read repair will not be visible
            // to the DML
            assertEquals(0, conn.createStatement().executeUpdate(dml));

            // Count the number of index rows
            String query = "SELECT COUNT(*) from " + indexTableName;
            // There should be one row in the index table
            rs = conn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testPartialRowUpdateForMutable() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        Connection conn = DriverManager.getConnection(getUrl());
        String indexTableName = generateUniqueName();
        conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : ""));
        if (async) {
            // run the index MR job.
            IndexToolIT.runIndexTool(true, false, null, dataTableName, indexTableName);
        }
        conn.createStatement().execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
        conn.commit();
        conn.createStatement().execute("upsert into " + dataTableName + " (id, val2) values ('c', 'cde')");
        conn.commit();
        String selectSql =  "SELECT * from " + dataTableName + " WHERE val1  = 'ab'";
        // Verify that we will read from the index table
        assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
        ResultSet rs = conn.createStatement().executeQuery(selectSql);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("ab", rs.getString(2));
        assertEquals("abcc", rs.getString(3));
        assertEquals("abcd", rs.getString(4));
        assertFalse(rs.next());

        conn.createStatement().execute("upsert into " + dataTableName + " (id, val1, val3) values ('a', 'ab', 'abcdd')");
        conn.commit();
        // Verify that we will read from the index table
        assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
        rs = conn.createStatement().executeQuery(selectSql);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("ab", rs.getString(2));
        assertEquals("abcc", rs.getString(3));
        assertEquals("abcdd", rs.getString(4));
        assertFalse(rs.next());
        conn.close();
    }

    /**
     * Phoenix allows immutable tables with indexes to be overwritten partially as long as the indexed columns are not
     * updated during partial overwrites. However, there is no check/enforcement for this. The immutable index mutations
     * are prepared at the client side without reading existing data table rows. This means the index mutations
     * prepared by the client will be partial when the data table row mutations are partial. The new indexing design
     * assumes index rows are always full and all cells within an index row have the same timestamp. On the read path,
     * GlobalIndexChecker returns only the cells with the most recent timestamp of the row. This means that if the
     * client updates the same row multiple times, the client will read back only the most recent update which could be
     * partial. To support the partial updates for immutable indexes, GlobalIndexChecker allows cells with different
     * timestamps to be be returned to the client for immutable tables even though this breaks the design assumption.
     * This test is to verify the partial row update support for immutable tables.
     *
     * Allowing partial overwrites on immutable indexes is a broken model in the first place. Assuring correctness in
     * the presence of failures does not seem possible without using something similar to the solution for mutable
     * indexes.
     *
     * Immutable indexes should be used only for really immutable tables and for these tables, overwrites
     * should be due to failures and should be idempotent. For everything else, mutable tables should be used
     *
     * @throws Exception
     */
    @Test
    public void testPartialRowUpdateForImmutable() throws Exception {
        if (async || encoded) {
            // No need to run this test more than once
            return;
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), val3 varchar(10))" +
                    " IMMUTABLE_ROWS=true, IMMUTABLE_STORAGE_SCHEME="+ PTableImpl.ImmutableStorageScheme.ONE_CELL_PER_COLUMN);
            conn.createStatement().execute("upsert into " + dataTableName + " values ('a', 'ab', 'abc', 'abcd')");
            conn.commit();
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)");
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1, val2) values ('a', 'ab', 'abcc')");
            conn.commit();
            String selectSql = "SELECT * from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("a", rs.getString(1));
            assertEquals("ab", rs.getString(2));
            assertEquals("abcc", rs.getString(3));
            assertEquals("abcd", rs.getString(4));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testFailPreIndexRowUpdate() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : ""));
            if (async) {
                // run the index MR job.
                IndexToolIT.runIndexTool(true, false, null, dataTableName, indexTableName);
            }
            // Configure IndexRegionObserver to fail the first write phase (i.e., the pre index update phase). This should not
            // lead to any change on index or data table rows
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
            commitWithException(conn);
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1, val2) values ('c', 'cd','cde')");
            commitWithException(conn);
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
            String selectSql = "SELECT val2, val3 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("abcd", rs.getString(2));
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testFailPostIndexRowUpdate() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : ""));
            if (async) {
                // run the index MR job.
                IndexToolIT.runIndexTool(true, false, null, dataTableName, indexTableName);
            }
            // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update phase) where the verify flag is set
            // to true and/or index rows are deleted and check that this does not impact the correctness
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
            conn.commit();
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1, val2) values ('c', 'cd','cde')");
            conn.commit();
            IndexTool indexTool = IndexToolIT.runIndexTool(true, false, "", dataTableName, indexTableName, null, 0, IndexTool.IndexVerifyType.ONLY);
            assertEquals(3, indexTool.getJob().getCounters().findCounter(INPUT_RECORDS).getValue());
            assertEquals(3, indexTool.getJob().getCounters().findCounter(SCANNED_DATA_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(REBUILT_INDEX_ROW_COUNT).getValue());
            assertEquals(3, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_VALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_EXPIRED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_MISSING_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_BEYOND_MAXLOOKBACK_INVALID_INDEX_ROW_COUNT).getValue());
            assertEquals(2, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_UNVERIFIED_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_OLD_INDEX_ROW_COUNT).getValue());
            assertEquals(0, indexTool.getJob().getCounters().findCounter(BEFORE_REBUILD_UNKNOWN_INDEX_ROW_COUNT).getValue());
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            String selectSql = "SELECT val2, val3 from " + dataTableName + " WHERE val1  = 'ab' and val2 = 'abcc'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("abcc", rs.getString(1));
            assertEquals("abcd", rs.getString(2));
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testUnverifiedValuesAreNotVisible() throws Exception {
        if (async) {
            // No need to run the same test twice one for async = true and the other for async = false
            return;
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar(10) not null primary key, val1 varchar(10), val2 varchar(10), val3 varchar(10))" + tableDDLOptions);
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)");

            // Configure IndexRegionObserver to fail the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName + " values ('a', 'ab','abc', 'abcd')");
            commitWithException(conn);
            // The above upsert will create an unverified index row
            // Configure IndexRegionObserver to allow the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            // Insert the same row with missing value for val3
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1, val2) values ('a', 'ab','abc')");
            conn.commit();
            // At this moment val3 in the data table row has null value
            String selectSql = "SELECT val3 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            // Verify that we do not read from the unverified row
            assertTrue(rs.next());
            assertEquals(null, rs.getString(1));
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testUnverifiedRowRepair() throws Exception {
        if (async) {
            // No need to run the same test twice one for async = true and the other for async = false
            return;
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (id varchar(10) not null primary key, a.val1 varchar(10), b.val2 varchar(10), c.val3 varchar(10))" + tableDDLOptions);
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + " on " +
                    dataTableName + " (val1) include (val2, val3)");
            // Configure IndexRegionObserver to fail the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1, val3) values ('a', 'ab','abcde')");
            commitWithException(conn);
            // The above upsert will create an unverified index row
            // Configure IndexRegionObserver to allow the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            // Verify that this row is not visible
            String selectSql = "SELECT * from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            // Verify that we do not read from the unverified row
            assertFalse(rs.next());
            // Insert the same row with a value for val3
            conn.createStatement().execute("upsert into " + dataTableName + " values ('a', 'ab','abc', 'abcd')");
            conn.commit();
            // At this moment val3 in the data table row should not have null value
            selectSql = "SELECT val3 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("abcd", rs.getString(1));
            assertFalse(rs.next());
            // Configure IndexRegionObserver to fail the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val1, val3) values ('a', 'ab','abcde')");
            commitWithException(conn);
            // The above upsert will create an unverified index row
            // Configure IndexRegionObserver to allow the data write phase
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            // Verify that this row is still read back correctly
            for (int i = 0; i < 2; i++) {
                selectSql = "SELECT val3 from " + dataTableName + " WHERE val1  = 'ab'";
                // Verify that we will read from the index table
                assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
                rs = conn.createStatement().executeQuery(selectSql);
                // Verify that the repair of the unverified row did not affect the valid index row
                assertTrue(rs.next());
                assertEquals("abcd", rs.getString(1));
                assertFalse(rs.next());
            }
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testOnePhaseOverwiteFollowingTwoPhaseWrite() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
            String indexTableName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexTableName + "1 on " +
                    dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : ""));
            conn.createStatement().execute("CREATE INDEX " + indexTableName + "2 on " +
                    dataTableName + " (val2) include (val1, val3)" + (async ? "ASYNC" : ""));
            if (async) {
                // run the index MR job.
                IndexToolIT.runIndexTool(true, false, null, dataTableName, indexTableName + "1");
                IndexToolIT.runIndexTool(true, false, null, dataTableName, indexTableName + "2");
            }
            // Two Phase write. This write is recoverable
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName + " values ('c', 'cd', 'cde', 'cdef')");
            conn.commit();
            // One Phase write. This write is not recoverable
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName + " values ('c', 'cd', 'cdee', 'cdfg')");
            commitWithException(conn);
            // Let three phase writes happen as in the normal case
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            String selectSql = "SELECT val2, val3 from " + dataTableName + " WHERE val1  = 'cd'";
            // Verify that we will read from the first index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "1");
            // Verify the first write is visible but the second one is not
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("cde", rs.getString(1));
            assertEquals("cdef", rs.getString(2));
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    @Test
    public void testOnePhaseOverwrite() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexTableName = generateUniqueName();
            createTableAndIndexes(conn, dataTableName, indexTableName);
            // Configure IndexRegionObserver to skip the last two write phase (i.e., the data table update and post index
            // update phase) and check that this does not impact the correctness (one overwrite)
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val2, val3) values ('a', 'abcc', 'abccc')");
            commitWithException(conn);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            // Read only one column and verify that this is sufficient for the read repair to fix
            // all the columns of the unverified index row that was generated due to doing only one phase write above
            String selectSql =  "SELECT val2 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the first index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "1");
            // Verify that one phase write has no effect
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertFalse(rs.next());
            // Now read the other column and verify that it is also fixed
            selectSql =  "SELECT val3 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the first index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "1");
            // Verify that one phase write has no effect
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("abcd", rs.getString(1));
            assertFalse(rs.next());
            selectSql =  "SELECT val2, val3 from " + dataTableName + " WHERE val2  = 'abcc'";
            // Verify that we will read from the second index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "2");
            rs = conn.createStatement().executeQuery(selectSql);
            // Verify that one phase writes have no effect
            assertFalse(rs.next());
            // Configure IndexRegionObserver to skip the last two write phase (i.e., the data table update and post index
            // update phase) and check that this does not impact the correctness  (two overwrites)
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abccc')");
            commitWithException(conn);
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcccc')");
            commitWithException(conn);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            selectSql =  "SELECT val2, val3 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the first index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "1");
            // Verify that one phase writes have no effect
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("abcd", rs.getString(2));
            assertFalse(rs.next());
            selectSql =  "SELECT val2, val3 from " + dataTableName + " WHERE val2  = 'abccc'";
            // Verify that we will read from the second index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "2");
            rs = conn.createStatement().executeQuery(selectSql);
            // Verify that one phase writes have no effect
            assertFalse(rs.next());
            selectSql =  "SELECT val2, val3 from " + dataTableName + " WHERE val2  = 'abcccc'";
            // Verify that we will read from the second index table
            assertExplainPlan(conn, selectSql, dataTableName, indexTableName + "2");
            rs = conn.createStatement().executeQuery(selectSql);
            // Verify that one phase writes have no effect
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexTableName);
        }
    }

    private void createTableAndIndexes(Connection conn, String dataTableName,
                                       String indexTableName) throws Exception {
        createTableAndIndexes(conn, dataTableName, indexTableName, 1);
    }

    private void createTableAndIndexes(Connection conn, String dataTableName,
                                       String indexTableName, int indexVersions) throws Exception {
        populateTable(dataTableName); // with two rows ('a', 'ab', 'abc', 'abcd') and ('b', 'bc', 'bcd', 'bcde')
        conn.createStatement().execute("CREATE INDEX " + indexTableName + "1 on " +
                dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : "") +
            " VERSIONS=" + indexVersions);
        conn.createStatement().execute("CREATE INDEX " + indexTableName + "2 on " +
                dataTableName + " (val2) include (val1, val3)" + (async ? "ASYNC" : "")+
            " VERSIONS=" + indexVersions);
        conn.commit();
        if (async) {
            // run the index MR job.
            IndexToolIT.runIndexTool(true, false, null, dataTableName, indexTableName + "1");
            IndexToolIT.runIndexTool(true, false, null, dataTableName, indexTableName + "2");
        }
    }

    @Test
    public void testFailDataTableAndPostIndexRowUpdate() throws Exception {

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTableName = generateUniqueName();
            String indexName = generateUniqueName();
            createTableAndIndexes(conn, dataTableName, indexName);
            // Configure IndexRegionObserver to fail the last two write phase (i.e., the data table update and post index update phase)
            // and check that this does not impact the correctness
            IndexRegionObserver.setFailDataTableUpdatesForTesting(true);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
            commitWithException(conn);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            conn.createStatement().execute("upsert into " + dataTableName + " (id, val3) values ('a', 'abcdd')");
            conn.commit();
            String selectSql = "SELECT val2, val3 from " + dataTableName + " WHERE val1  = 'ab'";
            // Verify that we will read from the first index table
            assertExplainPlan(conn, selectSql, dataTableName, indexName + "1");
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("abcdd", rs.getString(2));
            assertFalse(rs.next());
            selectSql = "SELECT val2, val3 from " + dataTableName + " WHERE val2  = 'abc'";
            // Verify that we will read from the second index table
            assertExplainPlan(conn, selectSql, dataTableName, indexName + "2");
            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertEquals("abc", rs.getString(1));
            assertEquals("abcdd", rs.getString(2));
            assertFalse(rs.next());
            // Add rows and check everything is still okay
            verifyTableHealth(conn, dataTableName, indexName);
        }
    }

    @Test
    public void testViewIndexRowUpdate() throws Exception {
        if (async) {
            // No need to run the same test twice one for async = true and the other for async = false
            return;
        }
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Create a base table
            String dataTableName = generateUniqueName();
            conn.createStatement().execute("create table " + dataTableName +
                    " (oid varchar(10) not null, kp char(3) not null, val1 varchar(10)" +
                    "CONSTRAINT pk PRIMARY KEY (oid, kp)) COLUMN_ENCODED_BYTES=0, MULTI_TENANT=true");
            // Create a view on the base table
            String viewName = generateUniqueName();
            conn.createStatement().execute("CREATE VIEW " + viewName + " (id char(10) not null, " +
                    "val2 varchar, val3 varchar, CONSTRAINT pk PRIMARY KEY (id)) AS SELECT * FROM " + dataTableName +
                    " WHERE kp = '0EC'");
            // Create an index on the view
            String indexName = generateUniqueName();
            conn.createStatement().execute("CREATE INDEX " + indexName + " on " +
                    viewName + " (val2) include (val3)");
            Properties props = new Properties();
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, "o1");
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), props)) {
                // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update phase)
                // This will leave index rows unverified
                // Create a view of the view
                String childViewName = generateUniqueName();
                tenantConn.createStatement().execute("CREATE VIEW " + childViewName + " (zid CHAR(15)) " +
                        "AS SELECT * FROM " + viewName);
                // Create another view of the child view
                String grandChildViewName = generateUniqueName();
                tenantConn.createStatement().execute("CREATE VIEW " + grandChildViewName + " (val4 CHAR(15)) " +
                        "AS SELECT * FROM " + childViewName);
                IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
                tenantConn.createStatement().execute("upsert into " + childViewName + " (zid, id, val1, val2, val3) VALUES('z1','1', 'a1','b1','c1')");
                tenantConn.commit();
                IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
                // Activate read repair
                String selectSql = "select val3 from  " + childViewName + " WHERE val2  = 'b1'";
                ResultSet rs = tenantConn.createStatement().executeQuery(selectSql);
                assertTrue(rs.next());
                assertEquals("c1", rs.getString("val3"));
                // Configure IndexRegionObserver to fail the last write phase (i.e., the post index update phase)
                // This will leave index rows unverified
                IndexRegionObserver.setFailPostIndexUpdatesForTesting(true);
                tenantConn.createStatement().execute("upsert into " + grandChildViewName + " (zid, id, val2, val3, val4) VALUES('z1', '2', 'b2', 'c2', 'd1')");
                tenantConn.commit();
                IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
                // Activate read repair
                selectSql = "select id, val3 from  " + grandChildViewName + " WHERE val2  = 'b2'";
                rs = tenantConn.createStatement().executeQuery(selectSql);
                assertTrue(rs.next());
                assertEquals("2", rs.getString("id"));
                assertEquals("c2", rs.getString("val3"));
            }
        }
    }

    static private void commitWithException(Connection conn) {
        try {
            conn.commit();
            IndexRegionObserver.setFailPreIndexUpdatesForTesting(false);
            IndexRegionObserver.setFailDataTableUpdatesForTesting(false);
            IndexRegionObserver.setFailPostIndexUpdatesForTesting(false);
            fail();
        } catch (Exception e) {
            // this is expected
        }
    }

    static private void verifyTableHealth(Connection conn, String dataTableName, String indexTableName) throws Exception {
        // Add two rows and check everything is still okay
        conn.createStatement().execute("upsert into " + dataTableName + " values ('a', 'ab', 'abc', 'abcd')");
        conn.createStatement().execute("upsert into " + dataTableName + " values ('z', 'za', 'zab', 'zabc')");
        conn.commit();
        String selectSql = "SELECT * from " + dataTableName + " WHERE val1  = 'ab'";
        ///Verify that we will read from the index table
        assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
        ResultSet rs = conn.createStatement().executeQuery(selectSql);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("ab", rs.getString(2));
        assertEquals("abc", rs.getString(3));
        assertEquals("abcd", rs.getString(4));
        assertFalse(rs.next());
        selectSql = "SELECT * from " + dataTableName + " WHERE val1  = 'za'";
        ///Verify that we will read from the index table
        assertExplainPlan(conn, selectSql, dataTableName, indexTableName);
        rs = conn.createStatement().executeQuery(selectSql);
        conn.commit();
        assertTrue(rs.next());
        assertEquals("z", rs.getString(1));
        assertEquals("za", rs.getString(2));
        assertEquals("zab", rs.getString(3));
        assertEquals("zabc", rs.getString(4));
        assertFalse(rs.next());
    }
}
