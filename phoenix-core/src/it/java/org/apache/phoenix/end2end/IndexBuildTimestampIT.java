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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import com.google.common.collect.Lists;

@RunWith(Parameterized.class)
public class IndexBuildTimestampIT extends BaseUniqueNamesOwnClusterIT {
    private final boolean localIndex;
    private final boolean async;
    private final boolean view;
    private final String tableDDLOptions;

    public IndexBuildTimestampIT(boolean mutable, boolean localIndex,
                            boolean async, boolean view) {
        this.localIndex = localIndex;
        this.async = async;
        this.view = view;
        StringBuilder optionBuilder = new StringBuilder();
        if (!mutable) {
            optionBuilder.append(" IMMUTABLE_ROWS=true ");
        }
        optionBuilder.append(" SPLIT ON(1,2)");
        this.tableDDLOptions = optionBuilder.toString();
    }

    @BeforeClass
    public static synchronized void setup() throws Exception {
        IndexToolIT.setup();
    }

    @Parameters(
            name = "mutable={0},localIndex={1},async={2},view={3}")
    public static synchronized Collection<Object[]> data() {
        List<Object[]> list = Lists.newArrayListWithExpectedSize(16);
        boolean[] Booleans = new boolean[]{false, true};
        for (boolean mutable : Booleans) {
            for (boolean localIndex : Booleans) {
                for (boolean async : Booleans) {
                    for (boolean view : Booleans) {
                        list.add(new Object[]{mutable, localIndex, async, view});
                    }
                }
            }
        }
        return list;
    }

    public static void assertExplainPlan(Connection conn, boolean localIndex, String selectSql,
                                         String dataTableFullName, String indexTableFullName) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN " + selectSql);
        String actualExplainPlan = QueryUtil.getExplainPlan(rs);

        IndexToolIT.assertExplainPlan(localIndex, actualExplainPlan, dataTableFullName, indexTableFullName);
    }

    private class MyClock extends EnvironmentEdge {
        long initialTime;
        long delta;

        public MyClock(long delta) {
            initialTime = System.currentTimeMillis() + delta;
            this.delta = delta;
        }

        @Override
        public long currentTime() {
            return System.currentTimeMillis() + delta;
        }

        public long initialTime() {
            return initialTime;
        }
    }

    private void populateTable(String tableName, MyClock clock1, MyClock clock2) throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        conn.createStatement().execute("create table " + tableName +
                " (id varchar(10) not null primary key, val varchar(10), ts timestamp)" + tableDDLOptions);

        EnvironmentEdgeManager.injectEdge(clock1);
        conn.createStatement().execute("upsert into " + tableName + " values ('aaa', 'abc', current_date())");
        conn.commit();

        EnvironmentEdgeManager.injectEdge(clock2);
        conn.createStatement().execute("upsert into " + tableName + " values ('bbb', 'bcd', current_date())");
        conn.commit();
        conn.close();

        Properties props = new Properties();
        props.setProperty("CurrentSCN", Long.toString(clock1.initialTime()));
        conn = DriverManager.getConnection(getUrl(), props);
        ResultSet rs = conn.createStatement().executeQuery("select * from " + tableName);
        assertFalse(rs.next());
        conn.close();

        props.setProperty("CurrentSCN", Long.toString(clock2.initialTime()));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("select * from " + tableName);

        assertTrue(rs.next());
        assertEquals("aaa", rs.getString(1));
        assertEquals("abc", rs.getString(2));
        assertNotNull(rs.getDate(3));

        assertFalse(rs.next());
        conn.close();

        props.setProperty("CurrentSCN", Long.toString(clock2.currentTime()));
        conn = DriverManager.getConnection(getUrl(), props);
        rs = conn.createStatement().executeQuery("select * from " + tableName);

        assertTrue(rs.next());
        assertEquals("aaa", rs.getString(1));
        assertEquals("abc", rs.getString(2));
        assertNotNull(rs.getDate(3));

        assertTrue(rs.next());
        assertEquals("bbb", rs.getString(1));
        assertEquals("bcd", rs.getString(2));
        assertNotNull(rs.getDate(3));
        assertFalse(rs.next());
        conn.close();
    }

    @Test
    public void testCellTimestamp() throws Exception {
        EnvironmentEdgeManager.reset();
        MyClock clock1 = new MyClock(100000);
        MyClock clock2 = new MyClock(200000);
        String dataTableName = generateUniqueName();
        populateTable(dataTableName, clock1, clock2);

        MyClock clock3 = new MyClock(300000);
        EnvironmentEdgeManager.injectEdge(clock3);
        try {

            Properties props = new Properties();
            props.setProperty(QueryServices.ENABLE_SERVER_SIDE_UPSERT_MUTATIONS, "true");
            props.setProperty(QueryServices.ENABLE_SERVER_SIDE_DELETE_MUTATIONS, "true");
            Connection conn = DriverManager.getConnection(getUrl(), props);

            String viewName = null;
            if (view) {
                viewName = generateUniqueName();
                conn.createStatement().execute("CREATE VIEW " + viewName + " AS SELECT * FROM " +
                        dataTableName);
            }
            String indexName = generateUniqueName();
            conn.createStatement().execute("CREATE " + (localIndex ? "LOCAL " : "") + " INDEX " + indexName + " on " +
                    (view ? viewName : dataTableName) + " (val) include (ts)" + (async ? "ASYNC" : ""));

            conn.close();

            if (async) {
                // run the index MR job.
                IndexToolIT.runIndexTool(true, false, null, (view ? viewName : dataTableName), indexName);
            }

            // Verify the index timestamps via Phoenix
            String selectSql = String.format("SELECT * FROM %s WHERE val = 'abc'", (view ? viewName : dataTableName));
            conn = DriverManager.getConnection(getUrl());
            // assert we are pulling from index table
            assertExplainPlan(conn, localIndex, selectSql, dataTableName, (view ? "_IDX_" + dataTableName : indexName));
            ResultSet rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertTrue(rs.unwrap(PhoenixResultSet.class).getCurrentRow().getValue(0).getTimestamp() < clock2.initialTime() &&
                    rs.unwrap(PhoenixResultSet.class).getCurrentRow().getValue(0).getTimestamp() >= clock1.initialTime());

            selectSql =
                    String.format("SELECT * FROM %s WHERE val = 'bcd'", (view ? viewName : dataTableName));
            // assert we are pulling from index table
            assertExplainPlan(conn, localIndex, selectSql, dataTableName, (view ? "_IDX_" + dataTableName : indexName));

            rs = conn.createStatement().executeQuery(selectSql);
            assertTrue(rs.next());
            assertTrue(rs.unwrap(PhoenixResultSet.class).getCurrentRow().getValue(0).getTimestamp() < clock3.initialTime() &&
                    rs.unwrap(PhoenixResultSet.class).getCurrentRow().getValue(0).getTimestamp() >= clock2.initialTime()
            );
            assertFalse(rs.next());

            // Verify the index timestamps via HBase
            PTable pIndexTable = PhoenixRuntime.getTable(conn, indexName);
            Table table = conn.unwrap(PhoenixConnection.class).getQueryServices()
                    .getTable(pIndexTable.getPhysicalName().getBytes());

            Scan scan = new Scan();
            scan.setTimeRange(clock3.initialTime(), clock3.currentTime());
            ResultScanner scanner = table.getScanner(scan);
            assertTrue(scanner.next() == null);


            scan = new Scan();
            scan.setTimeRange(clock2.initialTime(), clock3.initialTime());
            scanner = table.getScanner(scan);
            assertTrue(scanner.next() != null);


            scan = new Scan();
            scan.setTimeRange(clock1.initialTime(), clock2.initialTime());
            scanner = table.getScanner(scan);
            assertTrue(scanner.next() != null);
            conn.close();
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }
}
