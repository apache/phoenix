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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.phoenix.end2end.BaseUniqueNamesOwnClusterIT;
import org.apache.phoenix.end2end.IndexToolIT;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class GlobalIndexCheckerIT extends BaseUniqueNamesOwnClusterIT {
    private final boolean async;
    private final String tableDDLOptions;

    public GlobalIndexCheckerIT(boolean async, boolean encoded) {
        this.async = async;
        StringBuilder optionBuilder = new StringBuilder();
        if (!encoded) {
            optionBuilder.append(" COLUMN_ENCODED_BYTES=0 ");
        }
        this.tableDDLOptions = optionBuilder.toString();
    }

    @BeforeClass
    public static void doSetup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Parameters(
            name = "async={0},encoded={1}")
    public static Collection<Object[]> data() {
        List<Object[]> list = Lists.newArrayListWithExpectedSize(4);
        boolean[] Booleans = new boolean[]{true, false};
            for (boolean async : Booleans) {
                for (boolean encoded : Booleans) {
                    list.add(new Object[]{async, encoded});
                }
            }
        return list;
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
    public void testSkipPostIndexDeleteUpdate() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(dataTableName);
        Connection conn = DriverManager.getConnection(getUrl());
        String indexName = generateUniqueName();
        conn.createStatement().execute("CREATE INDEX " + indexName + " on " +
                dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : ""));
        if (async) {
            // run the index MR job.
            IndexToolIT.runIndexTool(true, false, null, dataTableName, indexName);
        }
        String selectSql =  "SELECT id from " + dataTableName + " WHERE val1  = 'ab'";
        ResultSet rs = conn.createStatement().executeQuery(selectSql);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertFalse(rs.next());

        // Configure Indexer to skip the last write phase (i.e., the post index update phase) where the verify flag is set
        // to true and/or index rows are deleted and check that this does not impact the correctness
        IndexRegionObserver.setSkipPostIndexUpdatesForTesting(true);
        String dml = "DELETE from " + dataTableName + " WHERE id  = 'a'";
        assertEquals(1, conn.createStatement().executeUpdate(dml));
        conn.commit();

        // The index rows are actually not deleted yet because Indexer skipped delete operation. However, they are
        // made unverified in the pre index update phase (i.e., the first write phase)
        dml = "DELETE from " + dataTableName + " WHERE val1  = 'ab'";
        // This DML will scan the Index table and detect unverified index rows. This will trigger read repair which
        // result in deleting these rows since the corresponding data table rows are deleted already. So, the number of
        // rows to be deleted by the "DELETE" DML will be zero since the rows deleted by read repair will not be visible
        // to the DML
        assertEquals(0,conn.createStatement().executeUpdate(dml));

        // Count the number of index rows
        String query = "SELECT COUNT(*) from " + indexName;
        // There should be one row in the index table
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        conn.close();
    }

    @Test
    public void testPartialRowUpdate() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(dataTableName);
        Connection conn = DriverManager.getConnection(getUrl());
        String indexName = generateUniqueName();
        conn.createStatement().execute("CREATE INDEX " + indexName + " on " +
                dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : ""));
        if (async) {
            // run the index MR job.
            IndexToolIT.runIndexTool(true, false, null, dataTableName, indexName);
        }
        conn.createStatement().execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
        conn.commit();
        conn.createStatement().execute("upsert into " + dataTableName + " (id, val2) values ('c', 'cde')");
        conn.commit();
        String selectSql =  "SELECT * from " + dataTableName + " WHERE val1  = 'ab'";
        // Verify that we will read from the index table
        assertExplainPlan(conn, selectSql, dataTableName, indexName);
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
        assertExplainPlan(conn, selectSql, dataTableName, indexName);
        rs = conn.createStatement().executeQuery(selectSql);
        assertTrue(rs.next());
        assertEquals("a", rs.getString(1));
        assertEquals("ab", rs.getString(2));
        assertEquals("abcc", rs.getString(3));
        assertEquals("abcdd", rs.getString(4));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testSkipPostIndexPartialRowUpdate() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(dataTableName);
        Connection conn = DriverManager.getConnection(getUrl());
        String indexName = generateUniqueName();
        conn.createStatement().execute("CREATE INDEX " + indexName + " on " +
                dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : ""));
        if (async) {
            // run the index MR job.
            IndexToolIT.runIndexTool(true, false, null, dataTableName, indexName);
        }
        // Configure Indexer to skip the last write phase (i.e., the post index update phase) where the verify flag is set
        // to true and/or index rows are deleted and check that this does not impact the correctness
        IndexRegionObserver.setSkipPostIndexUpdatesForTesting(true);
        conn.createStatement().execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
        conn.commit();
        conn.createStatement().execute("upsert into " + dataTableName + " (id, val1, val2) values ('c', 'cd','cde')");
        conn.commit();
        String selectSql =  "SELECT val2, val3 from " + dataTableName + " WHERE val1  = 'ab'";
        // Verify that we will read from the index table
        assertExplainPlan(conn, selectSql, dataTableName, indexName);
        ResultSet rs = conn.createStatement().executeQuery(selectSql);
        assertTrue(rs.next());
        assertEquals("abcc", rs.getString(1));
        assertEquals("abcd", rs.getString(2));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testSkipDataTableAndPostIndexPartialRowUpdate() throws Exception {
        String dataTableName = generateUniqueName();
        populateTable(dataTableName);
        Connection conn = DriverManager.getConnection(getUrl());
        String indexName = generateUniqueName();
        conn.createStatement().execute("CREATE INDEX " + indexName + "1 on " +
                dataTableName + " (val1) include (val2, val3)" + (async ? "ASYNC" : ""));
        conn.createStatement().execute("CREATE INDEX " + indexName + "2 on " +
                dataTableName + " (val2) include (val1, val3)" + (async ? "ASYNC" : ""));
        if (async) {
            // run the index MR job.
            IndexToolIT.runIndexTool(true, false, null, dataTableName, indexName + "1");
            IndexToolIT.runIndexTool(true, false, null, dataTableName, indexName + "2");
        }
        // Configure Indexer to skip the last two write phase (i.e., the data table update and post index update phase)
        // and check that this does not impact the correctness
        IndexRegionObserver.setSkipDataTableUpdatesForTesting(true);
        IndexRegionObserver.setSkipPostIndexUpdatesForTesting(true);
        conn.createStatement().execute("upsert into " + dataTableName + " (id, val2) values ('a', 'abcc')");
        conn.commit();
        IndexRegionObserver.setSkipDataTableUpdatesForTesting(false);
        IndexRegionObserver.setSkipPostIndexUpdatesForTesting(false);
        conn.createStatement().execute("upsert into " + dataTableName + " (id, val3) values ('a', 'abcdd')");
        conn.commit();
        String selectSql =  "SELECT val2, val3 from " + dataTableName + " WHERE val1  = 'ab'";
        // Verify that we will read from the first index table
        assertExplainPlan(conn, selectSql, dataTableName, indexName + "1");
        ResultSet rs = conn.createStatement().executeQuery(selectSql);
        assertTrue(rs.next());
        assertEquals("abc", rs.getString(1));
        assertEquals("abcdd", rs.getString(2));
        assertFalse(rs.next());
        selectSql =  "SELECT val2, val3 from " + dataTableName + " WHERE val2  = 'abc'";
        // Verify that we will read from the second index table
        assertExplainPlan(conn, selectSql, dataTableName, indexName + "2");
        rs = conn.createStatement().executeQuery(selectSql);
        assertTrue(rs.next());
        assertEquals("abc", rs.getString(1));
        assertEquals("abcdd", rs.getString(2));
        assertFalse(rs.next());
        conn.close();
    }
}
