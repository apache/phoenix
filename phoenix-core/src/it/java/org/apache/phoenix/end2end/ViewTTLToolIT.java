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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.phoenix.mapreduce.ViewTTLTool;
import org.apache.phoenix.mapreduce.util.PhoenixMultiInputUtil;
import org.apache.phoenix.query.HBaseFactoryProvider;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class ViewTTLToolIT extends ParallelStatsDisabledIT {

    private final int ONE = 1;
    private final int ZERO = 0;
    private final int NUMBER_OF_UPSERT_ROWS = 202;
    private final long VIEW_TTL_EXPIRE_IN_A_MILLISECOND = 1;
    private final long VIEW_TTL_EXPIRE_IN_A_DAY = 1000 * 60 * 60 * 24;

    private void alterViewTtl(Connection conn, String viewName, long view_ttl_value)
            throws SQLException {
        conn.createStatement().execute(
                String.format("ALTER VIEW %s SET VIEW_TTL= %d", viewName, view_ttl_value));
    }

    private void createViewAndUpsertData(Connection conn, String tableName, String viewName,
                                         long view_ttl_value) throws SQLException {
        String ddl = "CREATE VIEW " + viewName +
                " (PK1 BIGINT NOT NULL, PK2 BIGINT NOT NULL CONSTRAINT PKVIEW PRIMARY KEY(PK1,PK2)) " +
                "AS SELECT * FROM " + tableName + " VIEW_TTL=" + view_ttl_value;

        conn.createStatement().execute(ddl);

        for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
            PreparedStatement stmt = conn.prepareStatement(
                    "UPSERT INTO " + viewName + " (ID, PK1, PK2) VALUES(?,?,?)");
            stmt.setString(1, generateUniqueName());
            stmt.setInt(2, i);
            stmt.setInt(3, i);
            stmt.execute();
            stmt.close();
        }
        conn.commit();
    }

    private void createViewOnGlobalViewAndUpsertData(Connection conn, String tableName, String viewName,
                                                     long view_ttl_value) throws SQLException {
        String ddl = "CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName + " VIEW_TTL= " + view_ttl_value;
        conn.createStatement().execute(ddl);

        for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
            PreparedStatement stmt = conn.prepareStatement(
                    "UPSERT INTO " + viewName + " (ID, PK1, PK2, NUM) VALUES(?,?,?,?)");
            stmt.setString(1, generateUniqueName());
            stmt.setInt(2, i);
            stmt.setInt(3, i);
            stmt.setInt(4, i);
            stmt.execute();
            stmt.close();
        }
        conn.commit();
    }

    private void verifyGlobalTableNumberOfRows(String tableName, int expectedRows) throws Exception {
        try (Table table  = HBaseFactoryProvider.getHConnectionFactory().createConnection(config).getTable(tableName)) {
            assertEquals(expectedRows, getRowCount(table, new Scan()));
        }
    }

    private void verifyIndexTableNumberOfRowsForATenant(String tableName, String regrex, int expectedRows)
            throws Exception {
        try (Table table  = HBaseFactoryProvider.getHConnectionFactory().createConnection(config).getTable(tableName)) {
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regrex));
            Scan scan = new Scan();
            scan.setFilter(filter);
            assertEquals(expectedRows, getRowCount(table,scan));
        } catch (Exception e) {
            throw e;
        }
    }

    private void verifyNumberOfRows(String tableName, String tenantId, int expectedRows,
                                    Connection conn) throws Exception {
        String query = "SELECT COUNT(*) FROM " + tableName;
        if (tenantId != null) {
            query = query + " WHERE TENANT_ID = '" + tenantId + "'";
        }
        try (Statement stm = conn.createStatement()) {

            ResultSet rs = stm.executeQuery(query);
            assertTrue(rs.next());
            assertEquals(expectedRows, rs.getInt(1));
        }
    }

    private long getRowCount(Table table, Scan scan) throws Exception {
        ResultScanner scanner = table.getScanner(scan);
        int count = 0;
        for (Result dummy : scanner) {
            count++;
        }
        scanner.close();
        return count;
    }

    private void createMultiTenantTable(Connection conn, String tableName) throws Exception {
        String ddl = "CREATE TABLE " + tableName +
                " (TENANT_ID CHAR(10) NOT NULL, ID CHAR(10) NOT NULL, NUM BIGINT CONSTRAINT " +
                "PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true, COLUMN_ENCODED_BYTES = 0";

        conn.createStatement().execute(ddl);
    }

    private void createGlobalViewWithPk(Connection conn, String tableName, String globalViewName) throws Exception {
        String ddl = "CREATE VIEW " + globalViewName +
                " (PK1 BIGINT NOT NULL, PK2 BIGINT NOT NULL CONSTRAINT PKVIEW PRIMARY KEY(PK1,PK2)) AS SELECT * FROM "
                + tableName;
        conn.createStatement().execute(ddl);
    }

    @Test
    public void testViewOnMultiTenantTable() throws Exception {
        String schema = generateUniqueName();
        String baseTable = generateUniqueName();
        String fullTableName = schema + "." + baseTable;

        String tenant1 = generateUniqueName();
        String tenant2 = generateUniqueName();

        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();

        String fullViewName1 = schema + "." + viewName1;
        String fullViewName2 = schema + "." + viewName2;

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenant1Connection = PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant1);
             Connection tenant2Connection = PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant2)) {

            createMultiTenantTable(globalConn, fullTableName);
            createViewAndUpsertData(tenant1Connection, fullTableName, fullViewName1, VIEW_TTL_EXPIRE_IN_A_MILLISECOND);
            createViewAndUpsertData(tenant2Connection, fullTableName, fullViewName2, VIEW_TTL_EXPIRE_IN_A_DAY);

            verifyNumberOfRows(fullTableName, null, NUMBER_OF_UPSERT_ROWS * 2, globalConn);
            verifyNumberOfRows(fullTableName, tenant1, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyNumberOfRows(fullTableName, tenant2, NUMBER_OF_UPSERT_ROWS, globalConn);

            //running MR job to delete expired rows.
            ViewTTLTool viewTtlTool = new ViewTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            viewTtlTool.setConf(conf);
            int status = viewTtlTool.run(new String[]{"-runfg", "-a"});
            assertEquals(0, status);

            verifyNumberOfRows(fullTableName, null, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyNumberOfRows(fullTableName, tenant1, 0, globalConn);
            verifyNumberOfRows(fullTableName, tenant2, NUMBER_OF_UPSERT_ROWS, globalConn);
        }
    }

    @Test
    public void testTenantViewOnMultiTenantTableCases() throws Exception {
        String schema = generateUniqueName();
        String baseTable = generateUniqueName();
        String fullTableName = schema + "." + baseTable;

        String tenant1 = generateUniqueName();
        String tenant2 = generateUniqueName();

        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();

        String fullViewName1 = schema + "." + viewName1;
        String fullViewName2 = schema + "." + viewName2;

        String indexView = viewName2 + "_INDEX";
        String indexTable = "_IDX_" + fullTableName;

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenant1Connection = PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant1);
             Connection tenant2Connection = PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant2)) {

            createMultiTenantTable(globalConn, fullTableName);
            createViewAndUpsertData(tenant1Connection, fullTableName, fullViewName1, VIEW_TTL_EXPIRE_IN_A_MILLISECOND);
            createViewAndUpsertData(tenant2Connection, fullTableName, fullViewName2, VIEW_TTL_EXPIRE_IN_A_DAY);

            tenant2Connection.createStatement().execute(
                    "CREATE INDEX " + indexView + " ON " + fullViewName2 + "(NUM) INCLUDE (ID)");

            // before running MR deleting job, all rows should be present in multi tenant table.
            verifyNumberOfRows(fullTableName, tenant1, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyNumberOfRows(fullTableName, tenant2, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyGlobalTableNumberOfRows(indexTable, NUMBER_OF_UPSERT_ROWS);

            // running MR job to delete expired rows.
            ViewTTLTool viewTtlTool = new ViewTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            viewTtlTool.setConf(conf);
            int status = viewTtlTool.run(new String[]{"-runfg", "-a"});
            assertEquals(0, status);

            // first run should delete expired rows for tenant1 but not tenant2
            verifyNumberOfRows(fullTableName, tenant1, 0, globalConn);
            verifyNumberOfRows(fullTableName, tenant2, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyGlobalTableNumberOfRows(indexTable, NUMBER_OF_UPSERT_ROWS);

            // alter the view ttl and all rows should expired immediately.
            alterViewTtl(tenant2Connection, fullViewName2, VIEW_TTL_EXPIRE_IN_A_MILLISECOND);

            status = viewTtlTool.run(new String[]{"-runfg", "-a"});
            assertEquals(0, status);

            // MR job should delete rows from multi-tenant table and index table for tenant2.
            verifyGlobalTableNumberOfRows(indexTable, 0);
            verifyNumberOfRows(fullTableName, tenant2, 0, globalConn);
        }
    }

    @Test
    public void testTenantViewOnGlobalViewWithPkCases() throws Exception {
        String schema = generateUniqueName();
        String baseTable = generateUniqueName();
        String fullTableName = schema + "." + baseTable;

        String tenant1 = generateUniqueName();
        String tenant2 = generateUniqueName();

        String globalViewName = generateUniqueName();
        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();

        String fullViewName1 = schema + "." + viewName1;
        String fullViewName2 = schema + "." + viewName2;

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenant1Connection = PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant1);
             Connection tenant2Connection = PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant2)) {

            createMultiTenantTable(globalConn, fullTableName);
            //create global views with PK
            createGlobalViewWithPk(globalConn, fullTableName, globalViewName);

            //create global views on top of global view
            createViewOnGlobalViewAndUpsertData(tenant1Connection, globalViewName, fullViewName1,
                    VIEW_TTL_EXPIRE_IN_A_MILLISECOND);
            createViewOnGlobalViewAndUpsertData(tenant2Connection, globalViewName, fullViewName2,
                    VIEW_TTL_EXPIRE_IN_A_DAY);

            // before running MR deleting job, all rows should be present in multi tenant table.
            verifyNumberOfRows(fullTableName, tenant1, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyNumberOfRows(fullTableName, tenant2, NUMBER_OF_UPSERT_ROWS, globalConn);

            // running MR job to delete expired rows.
            ViewTTLTool viewTtlTool = new ViewTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            viewTtlTool.setConf(conf);
            int status = viewTtlTool.run(new String[]{"-runfg","-a"});
            assertEquals(0, status);

            // first run should delete expired rows for tenant1 but not tenant2
            verifyNumberOfRows(fullTableName, tenant1, 0, globalConn);
            verifyNumberOfRows(fullTableName, tenant2, NUMBER_OF_UPSERT_ROWS, globalConn);

            // alter the view ttl and all rows should expired immediately.
            alterViewTtl(tenant2Connection, fullViewName2, VIEW_TTL_EXPIRE_IN_A_MILLISECOND);

            status = viewTtlTool.run(new String[]{"-runfg","-a"});
            assertEquals(0, status);

            // MR job should delete rows from the multi-tenant table
            verifyNumberOfRows(fullTableName, tenant2, 0, globalConn);
        }
    }

    @Test
    public void testAllViewCases() throws Exception {
        String schema1 = generateUniqueName();
        String baseTable1 = generateUniqueName();
        String baseTable2 = generateUniqueName();
        String globalTable = generateUniqueName();

        String fullTable11 = schema1 + "." + baseTable1;
        String fullTable12 = schema1 + "." + baseTable2;

        String tenant1 = generateUniqueName();
        String tenant2 = generateUniqueName();

        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();

        String indexView = viewName2 + "_INDEX";
        String indexTable = "_IDX_" + fullTable12;

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenant1Connection = PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant1);
             Connection tenant2Connection = PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant2)) {
            createMultiTenantTable(globalConn, fullTable11);
            createMultiTenantTable(globalConn, fullTable12);

            // NOT MULTI_TENANT TABLE
            globalConn.createStatement().execute(String.format(
                    "CREATE TABLE %s (ID BIGINT PRIMARY KEY, NUM BIGINT) COLUMN_ENCODED_BYTES = 0", globalTable));
            for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
                PreparedStatement stmt = globalConn.prepareStatement(
                        "UPSERT INTO " + globalTable + " (ID, NUM) VALUES(?,?)");
                stmt.setInt(1, i);
                stmt.setInt(2, i);
                stmt.execute();
                stmt.close();
            }
            globalConn.commit();

            globalConn.createStatement().execute(String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE NUM = 1", viewName1, globalTable));
            globalConn.createStatement().execute(
                    String.format("ALTER VIEW %s SET VIEW_TTL= %d", viewName1, VIEW_TTL_EXPIRE_IN_A_DAY));

            globalConn.createStatement().execute(
                    String.format("CREATE VIEW %s AS SELECT * FROM %s " +
                    "WHERE NUM = 2", viewName2, globalTable));
            globalConn.createStatement().execute(
                    String.format("ALTER VIEW %s SET VIEW_TTL= %d", viewName2, VIEW_TTL_EXPIRE_IN_A_DAY));

            // diff tenants create same view name
            createViewAndUpsertData(tenant1Connection, fullTable11, schema1 + "." + viewName1,
                    VIEW_TTL_EXPIRE_IN_A_MILLISECOND);
            createViewAndUpsertData(tenant2Connection, fullTable11, schema1 + "." + viewName1,
                    VIEW_TTL_EXPIRE_IN_A_DAY);

            createViewAndUpsertData(tenant1Connection, fullTable12, schema1 + "." + viewName2,
                    VIEW_TTL_EXPIRE_IN_A_DAY);
            createViewAndUpsertData(tenant2Connection, fullTable12, schema1 + "." + viewName2,
                    VIEW_TTL_EXPIRE_IN_A_DAY);

            tenant2Connection.createStatement().execute(
                    "CREATE INDEX " + indexView + " ON " + schema1 + "." + viewName2 + "(NUM) INCLUDE (ID)");

            verifyNumberOfRows(fullTable11, tenant1, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyNumberOfRows(fullTable11, tenant2, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyNumberOfRows(fullTable12, tenant1, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyNumberOfRows(fullTable12, tenant2, NUMBER_OF_UPSERT_ROWS, globalConn);


            verifyGlobalTableNumberOfRows(indexTable, NUMBER_OF_UPSERT_ROWS);
            verifyGlobalTableNumberOfRows(globalTable, NUMBER_OF_UPSERT_ROWS);

            ViewTTLTool viewTtlTool = new ViewTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            viewTtlTool.setConf(conf);

            int status = viewTtlTool.run(new String[]{"-runfg","-a"});
            assertEquals(0, status);

            verifyNumberOfRows(fullTable11, tenant1, ZERO, globalConn);
            verifyNumberOfRows(fullTable11, tenant2, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyNumberOfRows(fullTable12, tenant1, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyNumberOfRows(fullTable12, tenant2, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyGlobalTableNumberOfRows(indexTable, NUMBER_OF_UPSERT_ROWS);
            verifyGlobalTableNumberOfRows(globalTable, NUMBER_OF_UPSERT_ROWS);

            // modify VIEW_TTL time and rerun the MR job for the table level.
            // it should only delete views on
            alterViewTtl(tenant2Connection, schema1 + "." + viewName1, VIEW_TTL_EXPIRE_IN_A_MILLISECOND);
            alterViewTtl(tenant1Connection, schema1 + "." + viewName2, VIEW_TTL_EXPIRE_IN_A_MILLISECOND);
            alterViewTtl(tenant2Connection, schema1 + "." + viewName2, VIEW_TTL_EXPIRE_IN_A_MILLISECOND);
            alterViewTtl(globalConn, viewName2, VIEW_TTL_EXPIRE_IN_A_MILLISECOND);

            viewTtlTool.run(new String[]{"-runfg","-a"});
            assertEquals(0, status);

            verifyNumberOfRows(fullTable11, tenant1, ZERO, globalConn);
            verifyNumberOfRows(fullTable11, tenant2, ZERO, globalConn);
            verifyNumberOfRows(fullTable12, tenant1, ZERO, globalConn);
            verifyNumberOfRows(fullTable12, tenant2, ZERO, globalConn);
            verifyNumberOfRows(viewName1, null, ONE, globalConn);
            verifyNumberOfRows(viewName2, null, ZERO, globalConn);
            // index view
            verifyGlobalTableNumberOfRows(indexTable, ZERO);
            verifyGlobalTableNumberOfRows(globalTable, NUMBER_OF_UPSERT_ROWS - ONE);
        }
    }

    @Test
    public void testDeleteATenantViewCase() throws Exception {
        String schema = generateUniqueName();
        String baseTable = generateUniqueName();
        String fullTableName = schema + "." + baseTable;

        String tenant1 = generateUniqueName();
        String tenant2 = generateUniqueName();

        String globalViewName = generateUniqueName();
        String viewName = generateUniqueName();
        String fullViewName = schema + "." + viewName;

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenant1Connection = PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant1);
             Connection tenant2Connection = PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant2)) {

            createMultiTenantTable(globalConn, fullTableName);
            //create global views with PK
            createGlobalViewWithPk(globalConn, fullTableName, globalViewName);

            //create global views on top of global view
            createViewOnGlobalViewAndUpsertData(tenant1Connection, globalViewName, fullViewName,
                    VIEW_TTL_EXPIRE_IN_A_MILLISECOND);
            createViewOnGlobalViewAndUpsertData(tenant2Connection, globalViewName, fullViewName,
                    VIEW_TTL_EXPIRE_IN_A_MILLISECOND);

            // before running MR deleting job, all rows should be present in multi tenant table.
            verifyNumberOfRows(fullTableName, tenant1, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyNumberOfRows(fullTableName, tenant2, NUMBER_OF_UPSERT_ROWS, globalConn);

            // running MR job to delete expired rows.
            ViewTTLTool viewTtlTool = new ViewTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            viewTtlTool.setConf(conf);
            int status = viewTtlTool.run(new String[]{"-runfg", "-v", fullViewName});
            assertEquals(0, status);

            //should NOT delete any expired rows for tenant1 and tenant2
            verifyNumberOfRows(fullTableName, tenant1, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyNumberOfRows(fullTableName, tenant2, NUMBER_OF_UPSERT_ROWS, globalConn);

            status = viewTtlTool.run(new String[]{"-runfg", "-id", tenant2, "-v", fullViewName});
            assertEquals(0, status);

            //should delete expired rows for tenant2 but not tenant1
            verifyNumberOfRows(fullTableName, tenant1, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyNumberOfRows(fullTableName, tenant2, 0, globalConn);
        }
    }

    @Test
    public void testDeleteExpiredRowsWForMultiTenantOnAnIndexTableCase() throws Exception {
        String baseTable = generateUniqueName();
        String indexTable = "_IDX_" + baseTable;
        String tenant1 = generateUniqueName();
        String tenant2 = generateUniqueName();
        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();

        String indexView1 = viewName1 + "_IDX";
        String indexView2 = viewName2 + "_IDX";

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenant1Connection = PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant1);
             Connection tenant2Connection = PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant2)) {

            createMultiTenantTable(globalConn, baseTable);
            createViewAndUpsertData(tenant1Connection, baseTable, viewName1, VIEW_TTL_EXPIRE_IN_A_DAY);
            createViewAndUpsertData(tenant2Connection, baseTable, viewName2, VIEW_TTL_EXPIRE_IN_A_DAY);

            tenant1Connection.createStatement().execute(
                    "CREATE INDEX " + indexView1 + " ON " + viewName1 + "(NUM) INCLUDE (ID)");

            tenant2Connection.createStatement().execute(
                    "CREATE INDEX " + indexView2 + " ON " + viewName2 + "(NUM) INCLUDE (ID)");

            // before running MR deleting job, all rows should be present in multi tenant table.
            verifyNumberOfRows(baseTable, tenant1, NUMBER_OF_UPSERT_ROWS, globalConn);
            verifyNumberOfRows(baseTable, tenant2, NUMBER_OF_UPSERT_ROWS, globalConn);

            verifyIndexTableNumberOfRowsForATenant(indexTable,
                    ".*" + tenant1 + ".*", NUMBER_OF_UPSERT_ROWS);
            verifyIndexTableNumberOfRowsForATenant(indexTable,
                    ".*" + tenant2 + ".*", NUMBER_OF_UPSERT_ROWS);

            // alter the view ttl and all rows should expired immediately.
            alterViewTtl(tenant2Connection, viewName2, VIEW_TTL_EXPIRE_IN_A_MILLISECOND);

            // running MR job to delete expired rows.
            ViewTTLTool viewTtlTool = new ViewTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            viewTtlTool.setConf(conf);
            int status = viewTtlTool.run(new String[]{"-runfg", "-a"});
            assertEquals(0, status);

            verifyIndexTableNumberOfRowsForATenant(indexTable, ".*" + tenant1 + ".*", NUMBER_OF_UPSERT_ROWS);
            verifyIndexTableNumberOfRowsForATenant(indexTable, ".*" + tenant2 + ".*", 0);
        }
    }

    @Test
    public void testDeleteExpiredRowsForATenantHasMultiViewsOnAnIndexTableCase() throws Exception {
        String baseTable = generateUniqueName();
        String indexTable = "_IDX_" + baseTable;
        String tenant = generateUniqueName();
        String viewName1 = generateUniqueName();
        String viewName2 = generateUniqueName();
        String viewName3 = generateUniqueName();

        String indexView1 = viewName1 + "_IDX";
        String indexView2 = viewName2 + "_IDX";
        String indexView3 = viewName3 + "_IDX";

        try (Connection globalConn = DriverManager.getConnection(getUrl());
             Connection tenantConnection = PhoenixMultiInputUtil.buildTenantConnection(getUrl(), tenant)) {

            String ddl = "CREATE TABLE " + baseTable + "(TENANT_ID CHAR(10) NOT NULL, ID CHAR(10) NOT NULL, " +
                    "AGE BIGINT NOT NULL, NUM BIGINT CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID, AGE)) " +
                    "MULTI_TENANT=true, COLUMN_ENCODED_BYTES = 0";
            globalConn.createStatement().execute(ddl);

            ddl = "CREATE VIEW " + viewName1 + " AS SELECT * FROM " + baseTable + " WHERE ID = 'A' VIEW_TTL= "
                    + VIEW_TTL_EXPIRE_IN_A_DAY;
            tenantConnection.createStatement().execute(ddl);

            ddl = "CREATE VIEW " + viewName2 + " AS SELECT * FROM " + baseTable + " WHERE ID = 'B' VIEW_TTL= "
                    + VIEW_TTL_EXPIRE_IN_A_DAY;
            tenantConnection.createStatement().execute(ddl);

            ddl = "CREATE VIEW " + viewName3 + " AS SELECT * FROM " + baseTable + " WHERE ID = 'C'";
            tenantConnection.createStatement().execute(ddl);

            for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
                PreparedStatement stmt = tenantConnection.prepareStatement(
                        "UPSERT INTO " + viewName1 + " (AGE, NUM) VALUES(?,?)");
                stmt.setInt(1, i);
                stmt.setInt(2, i);
                stmt.execute();
                stmt.close();
            }
            tenantConnection.commit();

            for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
                PreparedStatement stmt = tenantConnection.prepareStatement(
                        "UPSERT INTO " + viewName2 + " (AGE, NUM) VALUES(?,?)");
                stmt.setInt(1, i);
                stmt.setInt(2, i);
                stmt.execute();
                stmt.close();
            }
            tenantConnection.commit();

            for (int i = 0; i < NUMBER_OF_UPSERT_ROWS; i++) {
                PreparedStatement stmt = tenantConnection.prepareStatement(
                        "UPSERT INTO " + viewName3 + " (AGE, NUM) VALUES(?,?)");
                stmt.setInt(1, i);
                stmt.setInt(2, i);
                stmt.execute();
                stmt.close();
            }
            tenantConnection.commit();

            tenantConnection.createStatement().execute(
                    "CREATE INDEX " + indexView1 + " ON " + viewName1 + "(NUM) INCLUDE (AGE)");
            tenantConnection.createStatement().execute(
                    "CREATE INDEX " + indexView2 + " ON " + viewName2 + "(NUM) INCLUDE (AGE)");
            tenantConnection.createStatement().execute(
                    "CREATE INDEX " + indexView3 + " ON " + viewName3 + "(NUM) INCLUDE (AGE)");

            verifyIndexTableNumberOfRowsForATenant(indexTable, ".*" + tenant + ".*", NUMBER_OF_UPSERT_ROWS * 3);

            alterViewTtl(tenantConnection, viewName1, VIEW_TTL_EXPIRE_IN_A_MILLISECOND);

            // running MR job to delete expired rows.
            ViewTTLTool viewTtlTool = new ViewTTLTool();
            Configuration conf = new Configuration(getUtility().getConfiguration());
            viewTtlTool.setConf(conf);
            int status = viewTtlTool.run(new String[]{"-runfg", "-a"});
            assertEquals(0, status);

            verifyIndexTableNumberOfRowsForATenant(indexTable, ".*" + tenant + ".*", NUMBER_OF_UPSERT_ROWS * 2);
            String query = "SELECT COUNT(*) FROM %s";

            ResultSet rs = tenantConnection.createStatement().executeQuery(String.format(query, indexView1));
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));

            rs = tenantConnection.createStatement().executeQuery(String.format(query, indexView2));
            assertTrue(rs.next());
            assertEquals(NUMBER_OF_UPSERT_ROWS, rs.getInt(1));

            rs = tenantConnection.createStatement().executeQuery(String.format(query, indexView3));
            assertTrue(rs.next());
            assertEquals(NUMBER_OF_UPSERT_ROWS, rs.getInt(1));
        }
    }
}