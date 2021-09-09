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

import static org.apache.phoenix.thirdparty.com.google.common.collect.Lists
        .newArrayListWithExpectedSize;
import static org.apache.phoenix.coprocessor.PhoenixMetaDataCoprocessorHost
        .PHOENIX_META_DATA_COPROCESSOR_CONF_KEY;
import static org.apache.phoenix.util.TestUtil.analyzeTable;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.transaction.PhoenixTransactionProvider.Feature;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;

/**
 * Basic test suite for views
 */
@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class ViewIT extends SplitSystemCatalogIT {

    protected String tableDDLOptions;
    protected String transactionProvider;
    protected boolean columnEncoded;

    public ViewIT(String transactionProvider, boolean columnEncoded) {
        StringBuilder optionBuilder = new StringBuilder();
        this.transactionProvider = transactionProvider;
        this.columnEncoded = columnEncoded;
        if (transactionProvider != null) {
            optionBuilder.append(" TRANSACTION_PROVIDER='")
                    .append(transactionProvider)
                    .append("'");
        }
        if (!columnEncoded) {
            if (optionBuilder.length()!=0)
                optionBuilder.append(",");
            optionBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        this.tableDDLOptions = optionBuilder.toString();
    }

    // name is used by failsafe as file name in reports
    @Parameters(name="ViewIT_transactionProvider={0}, columnEncoded={1}")
    public static synchronized Collection<Object[]> data() {
        return TestUtil.filterTxParamData(Arrays.asList(new Object[][] { 
            { "TEPHRA", false }, { "TEPHRA", true },
            { "OMID", false }, 
            { null, false }, { null, true }}),0);
    }
    
    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        NUM_SLAVES_BASE = 6;
        boolean splitSystemCatalog = (driver == null);
        Map<String, String> serverProps = Maps.newHashMapWithExpectedSize(1);
        serverProps.put(QueryServices.PHOENIX_ACLS_ENABLED, "true");
        serverProps.put(PHOENIX_META_DATA_COPROCESSOR_CONF_KEY,
            ViewConcurrencyAndFailureIT.TestMetaDataRegionObserver.class
                    .getName());
        serverProps.put("hbase.coprocessor.abortonerror", "false");
        setUpTestDriver(new ReadOnlyProps(serverProps.entrySet().iterator()),
                ReadOnlyProps.EMPTY_PROPS);
        // Split SYSTEM.CATALOG once after the mini-cluster is started
        if (splitSystemCatalog) {
            // splitSystemCatalog is incompatible with the balancer chore
            getUtility().getHBaseCluster().getMaster().balanceSwitch(false);
            splitSystemCatalog();
        }
    }

    @Test
    public void testReadOnlyOnUpdatableView() throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String fullViewName1 = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());
        String fullViewName2 = SchemaUtil.getTableName(SCHEMA3,
                generateUniqueName());
        String ddl = "CREATE VIEW " + fullViewName2 + " AS SELECT * FROM "
                + fullViewName1
                + " WHERE k3 > 1 and k3 < 50";
        testUpdatableView(fullTableName, fullViewName1, fullViewName2, ddl,
                null, tableDDLOptions);
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT k1, k2, k3 FROM "
                    + fullViewName2);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(109, rs.getInt(2));
            assertEquals(2, rs.getInt(3));
            assertFalse(rs.next());
            try {
                stmt.execute("UPSERT INTO " + fullViewName2 + " VALUES(1)");
                fail();
            } catch (ReadOnlyTableException ignored) {
            }

            stmt.execute("UPSERT INTO " + fullTableName
                    + "(k1, k2,k3) VALUES(1, 122, 5)");
            conn.commit();
            rs = stmt.executeQuery("SELECT k1, k2, k3 FROM " + fullViewName2
                    + " WHERE k2 >= 120");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(122, rs.getInt(2));
            assertEquals(5, rs.getInt(3));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testReadOnlyViewWithCaseSensitiveTableNames() throws Exception {
        try (Connection earlierCon = DriverManager.getConnection(getUrl());
                Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String schemaName = TestUtil.DEFAULT_SCHEMA_NAME + "_"
                    + generateUniqueName();
            String caseSensitiveTableName = "\"t_" + generateUniqueName()
                    + "\"";
            String fullTableName = SchemaUtil.getTableName(schemaName,
                    caseSensitiveTableName);
            String caseSensitiveViewName = "\"v_" + generateUniqueName() + "\"";

            String ddl = "CREATE TABLE " + fullTableName
                    + " (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)"
                    + tableDDLOptions;
            stmt.execute(ddl);
            ddl = "CREATE VIEW " + caseSensitiveViewName
                    + " (v2 VARCHAR) AS SELECT * FROM "
                    + fullTableName + " WHERE k > 5";
            stmt.execute(ddl);

            try {
                stmt.execute("UPSERT INTO " + caseSensitiveViewName
                        + " VALUES(1)");
                fail();
            } catch (ReadOnlyTableException ignored) {
            }

            for (int i = 0; i < 10; i++) {
                stmt.execute("UPSERT INTO " + fullTableName
                        + " VALUES(" + i + ")");
            }
            conn.commit();

            int count = 0;
            ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM "
                    + caseSensitiveViewName);
            while (rs.next()) {
                count++;
                assertEquals(count + 5, rs.getInt(1));
            }
            assertEquals(4, count);
            count = 0;
            try (Statement earlierStmt = earlierCon.createStatement()) {
                rs = earlierStmt.executeQuery("SELECT k FROM "
                        + caseSensitiveViewName);
                while (rs.next()) {
                    count++;
                    assertEquals(count + 5, rs.getInt(1));
                }
                assertEquals(4, count);
            }
        }
    }
    
    @Test
    public void testReadOnlyViewWithCaseSensitiveColumnNames()
            throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String viewName = SchemaUtil.getTableName(SCHEMA2,
                    generateUniqueName());
            String ddl = "CREATE TABLE " + fullTableName
                    + " (\"k\" INTEGER NOT NULL PRIMARY KEY, \"v1\" INTEGER, "
                    + "\"a\".v2 VARCHAR)"
                    + tableDDLOptions;

            stmt.execute(ddl);
            ddl = "CREATE VIEW " + viewName + " (v VARCHAR) AS SELECT * FROM "
                    + fullTableName + " WHERE \"k\" > 5 and \"v1\" > 1";
            stmt.execute(ddl);

            try {
                stmt.execute("UPSERT INTO " + viewName + " VALUES(1)");
                fail();
            } catch (ReadOnlyTableException ignored) {

            }
            for (int i = 0; i < 10; i++) {
                stmt.execute("UPSERT INTO " + fullTableName + " VALUES(" + i
                        + ", " + (i + 10) + ",'A')");
            }
            conn.commit();

            int count = 0;
            ResultSet rs = stmt.executeQuery(
                    "SELECT \"k\", \"v1\",\"a\".v2 FROM " + viewName);
            while (rs.next()) {
                count++;
                assertEquals(count + 5, rs.getInt(1));
            }
            assertEquals(4, count);
        }
    }
    
    @Test
    public void testViewWithCurrentDate() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String viewName = SchemaUtil.getTableName(SCHEMA2,
                    generateUniqueName());

            String ddl = "CREATE TABLE " + fullTableName
                    + " (k INTEGER NOT NULL PRIMARY KEY, "
                    + "v1 INTEGER, v2 DATE)" + tableDDLOptions;
            stmt.execute(ddl);
            ddl = "CREATE VIEW " + viewName + " (v VARCHAR) AS SELECT * FROM "
                    + fullTableName
                    + " WHERE v2 > CURRENT_DATE()-5 AND v2 > DATE '2010-01-01'";
            stmt.execute(ddl);

            try {
                stmt.execute("UPSERT INTO " + viewName + " VALUES(1)");
                fail();
            } catch (ReadOnlyTableException ignored) {

            }
            for (int i = 0; i < 10; i++) {
                stmt.execute("UPSERT INTO " + fullTableName + " VALUES(" + i
                        + ", " + (i + 10) + ",CURRENT_DATE()-" + i + ")");
            }
            conn.commit();

            int count = 0;
            ResultSet rs = stmt.executeQuery("SELECT k FROM " + viewName);
            while (rs.next()) {
                assertEquals(count, rs.getInt(1));
                count++;
            }
            assertEquals(5, count);
        }
    }

    @Test
    public void testViewUsesTableGlobalIndex() throws Exception {
        testViewUsesTableIndex(false);
    }
    
    @Test
    public void testViewUsesTableLocalIndex() throws Exception {
        if (transactionProvider == null ||
                !TransactionFactory.getTransactionProvider(
                        TransactionFactory.Provider.valueOf(
                                transactionProvider))
                        .isUnsupported(Feature.ALLOW_LOCAL_INDEX)) {
            testViewUsesTableIndex(true);
        }
    }

    @Test
    public void testCreateViewTimestamp() throws Exception {
        String tenantId = null;
        createViewTimestampHelper(tenantId);
    }

    @Test
    public void testCreateTenantViewTimestamp() throws Exception {
        createViewTimestampHelper(TENANT1);
    }

    private void createViewTimestampHelper(String tenantId) throws SQLException {
        Properties props = new Properties();
        if (tenantId != null) {
            props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        }
        final String schemaName = "S_" + generateUniqueName();
        final String tableName = "T_" + generateUniqueName();
        final String viewName = "V_" + generateUniqueName();
        final String dataTableFullName = SchemaUtil.getTableName(schemaName, tableName);
        final String viewFullName = SchemaUtil.getTableName(schemaName, viewName);
        String tableDDL =
            "CREATE TABLE " + dataTableFullName + " (\n" + "ID1 VARCHAR(15) NOT NULL,\n"
                + "ID2 VARCHAR(15) NOT NULL,\n" + "CREATED_DATE DATE,\n"
                + "CREATION_TIME BIGINT,\n" + "LAST_USED DATE,\n"
                + "CONSTRAINT PK PRIMARY KEY (ID1, ID2)) ";
        String viewDDL = "CREATE VIEW " + viewFullName  + " AS SELECT * " +
            "FROM " + dataTableFullName;
        long startTS = EnvironmentEdgeManager.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(tableDDL);
        }
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(viewDDL);
            CreateTableIT.verifyLastDDLTimestamp(viewFullName,
                startTS,
                conn);
        }
    }

    private void testViewUsesTableIndex(boolean localIndex) throws Exception {
        ResultSet rs;
        // Use unique name for table with local index as otherwise we run
        // into issues when we attempt to drop the table (with the drop
        // metadata option set to false
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName()) +
                (localIndex ? "_WITH_LI" : "_WITHOUT_LI");
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()){
            String ddl = "CREATE TABLE " + fullTableName
                    + " (k1 INTEGER NOT NULL, "
                    + "k2 INTEGER NOT NULL, k3 DECIMAL, s1 VARCHAR, s2 VARCHAR"
                    + " CONSTRAINT pk PRIMARY KEY (k1, k2, k3))"
                    + tableDDLOptions;
            stmt.execute(ddl);
            String indexName1 = "I_" + generateUniqueName();
            String fullIndexName1 = SchemaUtil.getTableName(SCHEMA1,
                    indexName1);
            stmt.execute("CREATE " + (localIndex ? "LOCAL " : "") + " INDEX "
                    + indexName1 + " ON "
                    + fullTableName + "(k3, k2) INCLUDE(s1, s2)");
            String indexName2 = "I_" + generateUniqueName();
            stmt.execute("CREATE INDEX " + indexName2 + " ON " + fullTableName
                    + "(k3, k2, s2)");

            String fullViewName = SchemaUtil.getTableName(SCHEMA2,
                    generateUniqueName());
            ddl = "CREATE VIEW " + fullViewName + " AS SELECT * FROM "
                    + fullTableName + " WHERE s1 = 'foo'";
            stmt.execute(ddl);
            String[] s1Values = { "foo", "bar" };
            for (int i = 0; i < 10; i++) {
                stmt.execute("UPSERT INTO " + fullTableName + " VALUES("
                        + (i % 4) + "," + (i + 100)
                        + "," + (i > 5 ? 2 : 1) + ",'" + s1Values[i % 2]
                        + "','bas')");
            }
            conn.commit();

            rs = stmt.executeQuery("SELECT count(*) FROM " + fullViewName);
            assertTrue(rs.next());
            assertEquals(5, rs.getLong(1));
            assertFalse(rs.next());

            String viewIndexName = "I_" + generateUniqueName();
            stmt.execute("CREATE INDEX " + viewIndexName + " on "
                    + fullViewName + "(k2)");

            String query = "SELECT k2 FROM " + fullViewName
                    + " WHERE k2 IN (100,109) AND k3 IN"
                    + " (1,2) AND s2='bas'";
            rs = stmt.executeQuery(query);
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
            assertFalse(rs.next());

            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("PARALLEL 1-WAY",
                explainPlanAttributes.getIteratorTypeAndScanSize());
            assertEquals("SKIP SCAN ON 4 KEYS ",
                explainPlanAttributes.getExplainScanType());
            assertEquals("SERVER FILTER BY (\"S2\" = 'bas' AND \"S1\" = 'foo')",
                explainPlanAttributes.getServerWhereFilter());

            // Assert that in either case (local & global) that index from
            // physical table used for query on view.
            if (localIndex) {
                assertEquals(fullTableName, explainPlanAttributes.getTableName());
                assertEquals(" [1,1,100] - [1,2,109]",
                    explainPlanAttributes.getKeyRanges());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals(fullIndexName1, explainPlanAttributes.getTableName());
                assertEquals(" [1,100] - [2,109]",
                    explainPlanAttributes.getKeyRanges());
                assertNull(explainPlanAttributes.getClientSortAlgo());
            }
        }
    }

    @Test
    public void testCreateChildViewWithBaseTableLocalIndex() throws Exception {
        testCreateChildViewWithBaseTableIndex(true);
    }

    @Test
    public void testCreateChildViewWithBaseTableGlobalIndex() throws Exception {
        testCreateChildViewWithBaseTableIndex(false);
    }

    public void testCreateChildViewWithBaseTableIndex(boolean localIndex)
            throws Exception {
        String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                generateUniqueName());
        String fullViewName = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());
        String indexName = "I_" + generateUniqueName();
        String fullChildViewName = SchemaUtil.getTableName(SCHEMA2,
                generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String sql = "CREATE TABLE " + fullTableName
                    + " (ID INTEGER NOT NULL PRIMARY KEY, HOST VARCHAR(10),"
                    + " FLAG BOOLEAN)";
            stmt.execute(sql);
            sql = "CREATE VIEW " + fullViewName
                    + " (COL1 INTEGER, COL2 INTEGER, COL3 INTEGER,"
                    + " COL4 INTEGER) AS SELECT * FROM "
                    + fullTableName + " WHERE ID > 5";
            stmt.execute(sql);
            sql = "CREATE " + (localIndex ? "LOCAL " : "") + " INDEX "
                    + indexName + " ON " + fullTableName + "(HOST)";
            stmt.execute(sql);
            sql = "CREATE VIEW " + fullChildViewName + " AS SELECT * FROM "
                    + fullViewName + " WHERE COL1 > 2";
            stmt.execute(sql);
            // Sanity upserts in baseTable, view, child view
            stmt.executeUpdate("upsert into " + fullTableName
                    + " values (1, 'host1', TRUE)");
            stmt.executeUpdate("upsert into " + fullTableName
                    + " values (5, 'host5', FALSE)");
            stmt.executeUpdate("upsert into " + fullTableName
                    + " values (7, 'host7', TRUE)");
            conn.commit();
            // View is not updateable
            try {
                stmt.executeUpdate("upsert into " + fullViewName
                        + " (ID, HOST, FLAG, COL1)"
                        + " values (7, 'host7', TRUE, 1)");
                fail();
            } catch (Exception ignore) {
            }
            // Check view inherits index, but child view doesn't
            PTable table = PhoenixRuntime.getTable(conn, fullViewName);
            assertEquals(1, table.getIndexes().size());
            table = PhoenixRuntime.getTable(conn, fullChildViewName);
            assertEquals(0, table.getIndexes().size());

            ResultSet rs = stmt.executeQuery("select count(*) from "
                    + fullTableName);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));

            rs = stmt.executeQuery("select count(*) from " + fullViewName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    public void testCreateViewDefinesPKColumn() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String fullViewName = SchemaUtil.getTableName(SCHEMA2,
                    generateUniqueName());

            String ddl = "CREATE TABLE " + fullTableName
                    + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, "
                    + "CONSTRAINT pk PRIMARY KEY (k1, k2))" + tableDDLOptions;
            stmt.execute(ddl);
            ddl = "CREATE VIEW " + fullViewName + "(v2 VARCHAR, k3 VARCHAR "
                    + "PRIMARY KEY) "
                    + "AS SELECT * FROM " + fullTableName + " WHERE K1 = 1";
            stmt.execute(ddl);

            // assert PK metadata
            ResultSet rs = conn.getMetaData().getPrimaryKeys(null,
                    SchemaUtil.getSchemaNameFromFullName(fullViewName),
                    SchemaUtil.getTableNameFromFullName(fullViewName));
            assertPKs(rs, new String[] { "K1", "K2", "K3" });

            // sanity check upserts into base table and view
            stmt.executeUpdate("upsert into " + fullTableName + " (k1, k2, v1)"
                    + " values (1, 1, 1)");
            stmt.executeUpdate("upsert into " + fullViewName
                    + " (k1, k2, k3, v2) values (1, 1, 'abc', 'def')");
            conn.commit();

            // expect 2 rows in the base table
            rs = stmt.executeQuery("select count(*) from " + fullTableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));

            // expect 2 row in the view
            rs = stmt.executeQuery("select count(*) from " + fullViewName);
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }
    
    @Test
    public void testQueryViewStatementOptimization() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String fullTableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String fullViewName1 = SchemaUtil.getTableName(SCHEMA2,
                    generateUniqueName());
            String fullViewName2 = SchemaUtil.getTableName(SCHEMA3,
                    generateUniqueName());
            String sql = "CREATE TABLE " + fullTableName
                    + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, "
                    + "CONSTRAINT pk PRIMARY KEY (k1, k2))" + tableDDLOptions;
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
                sql = "CREATE VIEW " + fullViewName1 + "  AS SELECT * FROM "
                        + fullTableName;
                stmt.execute(sql);
                sql = "CREATE VIEW " + fullViewName2 + "  AS SELECT * FROM "
                        + fullTableName
                        + " WHERE k1 = 1.0";
                stmt.execute(sql);
            }
            sql = "SELECT * FROM " + fullViewName1 + " order by k1, k2";
            QueryPlan plan;
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                plan = PhoenixRuntime.getOptimizedQueryPlan(stmt);
                assertEquals(0, plan.getOrderBy().getOrderByExpressions()
                        .size());
            }
            sql = "SELECT * FROM " + fullViewName2 + " order by k1, k2";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                plan = PhoenixRuntime.getOptimizedQueryPlan(stmt);
                assertEquals(0, plan.getOrderBy().getOrderByExpressions()
                        .size());
            }
        }
    }

    private void assertPKs(ResultSet rs, String[] expectedPKs)
            throws SQLException {
        List<String> pkCols = newArrayListWithExpectedSize(expectedPKs.length);
        while (rs.next()) {
            pkCols.add(rs.getString("COLUMN_NAME"));
        }
        String[] actualPKs = pkCols.toArray(new String[0]);
        assertArrayEquals(expectedPKs, actualPKs);
    }

    @Test
    public void testCompositeDescPK() throws Exception {
        Properties props = new Properties();
        try (Connection globalConn = DriverManager.getConnection(getUrl(),
                props)) {
            String tableName = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String viewName1 = SchemaUtil.getTableName(SCHEMA2,
                    generateUniqueName());
            String viewName2 = SchemaUtil.getTableName(SCHEMA3,
                    generateUniqueName());
            String viewName3 = SchemaUtil.getTableName(SCHEMA1,
                    generateUniqueName());
            String viewName4 = SchemaUtil.getTableName(SCHEMA2,
                    generateUniqueName());

            String myTableDDLOptions = tableDDLOptions;
            if (myTableDDLOptions.length() != 0) myTableDDLOptions += ",";
            myTableDDLOptions += "VERSIONS=1, MULTI_TENANT=true, "
                    + "IMMUTABLE_ROWS=TRUE, REPLICATION_SCOPE=1";

            // create global base table
            try (Statement stmt = globalConn.createStatement()) {
                stmt.execute("CREATE TABLE " + tableName
                        + " (TENANT_ID CHAR(15) NOT NULL, KEY_PREFIX CHAR(3)"
                        + " NOT NULL, "
                        + "CREATED_DATE DATE, CREATED_BY CHAR(15), "
                        + "SYSTEM_MODSTAMP DATE CONSTRAINT "
                        + "PK PRIMARY KEY (TENANT_ID, KEY_PREFIX)) "
                        + myTableDDLOptions);
            }

            String tenantId = "tenantId";
            Properties tenantProps = new Properties();
            tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            // create a tenant specific view
            try (Connection tenantConn = DriverManager.getConnection(getUrl(),
                    tenantProps);
                    Statement tenantStmt = tenantConn.createStatement()) {
                // create various tenant specific views
                // view with composite PK with multiple PK values of VARCHAR
                // values DESC
                tenantStmt.execute("CREATE VIEW " + viewName1
                        + " (pk1 VARCHAR(10) NOT NULL, pk2 VARCHAR(10) "
                        + "NOT NULL, col1 DATE,"
                        + " col3 DECIMAL CONSTRAINT PK PRIMARY KEY "
                        + "(pk1 DESC, pk2 DESC)) "
                        + "AS SELECT * FROM " + tableName
                        + " WHERE KEY_PREFIX = 'abc' ");
                // view with composite PK with single pk value DESC
                tenantStmt.execute("CREATE VIEW " + viewName2
                        + " (pk1 VARCHAR(10) NOT NULL, pk2 VARCHAR(10) "
                        + "NOT NULL, col1 DATE,"
                        + " col3 DECIMAL CONSTRAINT PK PRIMARY KEY "
                        + "(pk1 DESC, pk2 DESC)) "
                        + "AS SELECT * FROM " + tableName
                        + " WHERE KEY_PREFIX = 'abc' ");

                // view with composite PK with multiple Date PK values DESC
                tenantStmt.execute("CREATE VIEW " + viewName3
                        + " (pk1 DATE(10) NOT NULL, pk2 DATE(10) "
                        + "NOT NULL, "
                        + "col1 VARCHAR(10), col3 DECIMAL CONSTRAINT PK "
                        + "PRIMARY KEY "
                        + "(pk1 DESC, pk2 DESC)) AS SELECT * FROM "
                        + tableName + " WHERE KEY_PREFIX = 'ab3' ");
                
                tenantStmt.execute("CREATE VIEW " + viewName4
                        + " (pk1 DATE(10) NOT NULL, pk2 DECIMAL NOT NULL,"
                        + " pk3 VARCHAR(10) NOT NULL,"
                        + " col3 DECIMAL CONSTRAINT PK PRIMARY KEY "
                        + "(pk1 DESC, pk2 DESC, pk3 DESC)) "
                        + "AS SELECT * FROM "
                        + tableName + " WHERE KEY_PREFIX = 'ab4' ");

                // upsert rows
                upsertRows(viewName1, tenantConn);
                upsertRows(viewName2, tenantConn);

                // run queries
                String[] whereClauses = new String[] {
                        "pk1 = 'testa'", "", "pk1 >= 'testa'", "pk1 <= 'testa'",
                        "pk1 > 'testa'", "pk1 < 'testa'" };
                long[] expectedArray = new long[] { 4, 5, 5, 4, 1, 0 };
                validate(viewName1, tenantConn, whereClauses, expectedArray);
                validate(viewName2, tenantConn, whereClauses, expectedArray);

                tenantStmt.execute("UPSERT INTO " + viewName3
                        + " (pk1, pk2, col1, col3) VALUES "
                        + "(TO_DATE('2017-10-16 22:00:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), "
                        + "TO_DATE('2017-10-16 21:00:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), 'txt1', 10)");
                tenantStmt.execute("UPSERT INTO " + viewName3
                        + " (pk1, pk2, col1, col3) VALUES "
                        + "(TO_DATE('2017-10-16 22:00:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), "
                        + "TO_DATE('2017-10-16 21:01:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), 'txt1', 10)");
                tenantStmt.execute("UPSERT INTO " + viewName3
                        + " (pk1, pk2, col1, col3) VALUES "
                        + "(TO_DATE('2017-10-16 22:00:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), "
                        + "TO_DATE('2017-10-16 21:02:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), 'txt1', 10)");
                tenantStmt.execute("UPSERT INTO " + viewName3
                        + " (pk1, pk2, col1, col3) VALUES "
                        + "(TO_DATE('2017-10-16 22:00:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), "
                        + "TO_DATE('2017-10-16 21:03:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), 'txt1', 10)");
                tenantStmt.execute("UPSERT INTO " + viewName3
                        + " (pk1, pk2, col1, col3) VALUES "
                        + "(TO_DATE('2017-10-16 23:00:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), "
                        + "TO_DATE('2017-10-16 21:04:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), 'txt1', 10)");
                tenantConn.commit();

                String[] view3WhereClauses = new String[] {
                        "pk1 = TO_DATE('2017-10-16 22:00:00', "
                                + "'yyyy-MM-dd HH:mm:ss')", "",
                        "pk1 >= TO_DATE('2017-10-16 22:00:00', "
                                + "'yyyy-MM-dd HH:mm:ss')",
                        "pk1 <= TO_DATE('2017-10-16 22:00:00', "
                                + "'yyyy-MM-dd HH:mm:ss')",
                        "pk1 > TO_DATE('2017-10-16 22:00:00', "
                                + "'yyyy-MM-dd HH:mm:ss')",
                        "pk1 < TO_DATE('2017-10-16 22:00:00', "
                                + "'yyyy-MM-dd HH:mm:ss')" };
                validate(viewName3, tenantConn, view3WhereClauses,
                        expectedArray);

                tenantStmt.execute("UPSERT INTO " + viewName4
                        + " (pk1, pk2, pk3, col3) VALUES "
                        + "(TO_DATE('2017-10-16 22:00:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), 1, 'txt1', 10)");
                tenantStmt.execute("UPSERT INTO " + viewName4
                        + " (pk1, pk2, pk3, col3) VALUES "
                        + "(TO_DATE('2017-10-16 22:00:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), 2, 'txt2', 10)");
                tenantStmt.execute("UPSERT INTO " + viewName4
                        + " (pk1, pk2, pk3, col3) VALUES"
                        + " (TO_DATE('2017-10-16 22:00:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), 3, 'txt3', 10)");
                tenantStmt.execute("UPSERT INTO " + viewName4
                        + " (pk1, pk2, pk3, col3) VALUES "
                        + "(TO_DATE('2017-10-16 22:00:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), 4, 'txt4', 10)");
                tenantStmt.execute("UPSERT INTO " + viewName4
                        + " (pk1, pk2, pk3, col3) VALUES "
                        + "(TO_DATE('2017-10-16 23:00:00', "
                        + "'yyyy-MM-dd HH:mm:ss'), 1, 'txt1', 10)");
                tenantConn.commit();

                String[] view4WhereClauses = new String[] {
                        "pk1 = TO_DATE('2017-10-16 22:00:00', "
                                + "'yyyy-MM-dd HH:mm:ss')",
                        "pk1 = TO_DATE('2017-10-16 22:00:00', "
                                + "'yyyy-MM-dd HH:mm:ss') AND pk2 = 2",
                        "pk1 = TO_DATE('2017-10-16 22:00:00', "
                                + "'yyyy-MM-dd HH:mm:ss') AND pk2 > 2",
                        "", "pk1 >= TO_DATE('2017-10-16 22:00:00', "
                        + "'yyyy-MM-dd HH:mm:ss')",
                        "pk1 <= TO_DATE('2017-10-16 22:00:00', "
                                + "'yyyy-MM-dd HH:mm:ss')",
                        "pk1 > TO_DATE('2017-10-16 22:00:00', "
                                + "'yyyy-MM-dd HH:mm:ss')",
                        "pk1 < TO_DATE('2017-10-16 22:00:00', "
                                + "'yyyy-MM-dd HH:mm:ss')" };
                long[] view4ExpectedArray = new long[]
                        { 4, 1, 2, 5, 5, 4, 1, 0 };
                validate(viewName4, tenantConn, view4WhereClauses,
                        view4ExpectedArray);
            }
        }
    }

    private void validate(String viewName, Connection tenantConn,
            String[] whereClauseArray,
            long[] expectedArray) throws SQLException {
        for (int i = 0; i < whereClauseArray.length; ++i) {
            String where = !whereClauseArray[i].isEmpty() ?
                    (" WHERE " + whereClauseArray[i]) : "";
            try (Statement stmt = tenantConn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT count(*) FROM "
                        + viewName + where);
                assertTrue(rs.next());
                assertEquals(expectedArray[i], rs.getLong(1));
                assertFalse(rs.next());
            }
        }
    }

    private void upsertRows(String viewName, Connection tenantConn)
            throws SQLException {
        try (Statement stmt = tenantConn.createStatement()) {
            stmt.execute("UPSERT INTO " + viewName
                    + " (pk1, pk2, col1, col3) VALUES "
                    + "('testa', 'testb', TO_DATE('2017-10-16 22:00:00', "
                    + "'yyyy-MM-dd HH:mm:ss'), 10)");
            stmt.execute("UPSERT INTO " + viewName
                    + " (pk1, pk2, col1, col3) VALUES "
                    + "('testa', 'testc', TO_DATE('2017-10-16 22:00:00', "
                    + "'yyyy-MM-dd HH:mm:ss'), 10)");
            stmt.execute("UPSERT INTO " + viewName
                    + " (pk1, pk2, col1, col3) "
                    + "VALUES ('testa', 'testd', "
                    + "TO_DATE('2017-10-16 22:00:00', "
                    + "'yyyy-MM-dd HH:mm:ss'), 10)");
            stmt.execute("UPSERT INTO " + viewName
                    + " (pk1, pk2, col1, col3) "
                    + "VALUES ('testa', 'teste', "
                    + "TO_DATE('2017-10-16 22:00:00', "
                    + "'yyyy-MM-dd HH:mm:ss'), 10)");
            stmt.execute("UPSERT INTO " + viewName
                    + " (pk1, pk2, col1, col3) "
                    + "VALUES ('testb', 'testa', "
                    + "TO_DATE('2017-10-16 22:00:00', "
                    + "'yyyy-MM-dd HH:mm:ss'), 10)");
            tenantConn.commit();
        }
    }
    
    public static void testUpdatableView(String fullTableName,
            String fullViewName, String fullChildViewName, String childViewDDL,
            Integer saltBuckets, String tableDDLOptions) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            if (saltBuckets != null) {
                if (tableDDLOptions.length() != 0) tableDDLOptions += ",";
                tableDDLOptions += (" SALT_BUCKETS=" + saltBuckets);
            }
            String ddl = "CREATE TABLE " + fullTableName
                    + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, k3 DECIMAL,"
                    + " s VARCHAR "
                    + "CONSTRAINT pk PRIMARY KEY (k1, k2, k3))"
                    + tableDDLOptions;
            stmt.execute(ddl);
            ddl = "CREATE VIEW " + fullViewName + " AS SELECT * FROM "
                    + fullTableName
                    + " WHERE k1 = 1";
            stmt.execute(ddl);
            ArrayList<String> splitPoints = Lists.newArrayList(fullTableName,
                    fullViewName);
            if (fullChildViewName != null) {
                stmt.execute(childViewDDL);
                splitPoints.add(fullChildViewName);
            }

            for (int i = 0; i < 10; i++) {
                stmt.execute("UPSERT INTO " + fullTableName
                        + " VALUES(" + (i % 4) + ","
                        + (i + 100) + "," + (i > 5 ? 2 : 1) + ")");
            }
            conn.commit();

            ResultSet rs;
            rs = stmt.executeQuery("SELECT count(*) FROM " + fullTableName);
            assertTrue(rs.next());
            assertEquals(10, rs.getInt(1));
            rs = stmt.executeQuery("SELECT count(*) FROM " + fullViewName);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            rs = stmt.executeQuery("SELECT k1, k2, k3 FROM " + fullViewName);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(101, rs.getInt(2));
            assertEquals(1, rs.getInt(3));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(105, rs.getInt(2));
            assertEquals(1, rs.getInt(3));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(109, rs.getInt(2));
            assertEquals(2, rs.getInt(3));
            assertFalse(rs.next());

            stmt.execute("UPSERT INTO " + fullViewName
                    + "(k2,S,k3) VALUES(120,'foo',50.0)");
            stmt.execute("UPSERT INTO " + fullViewName
                    + "(k2,S,k3) VALUES(121,'bar',51.0)");
            conn.commit();

            rs = stmt.executeQuery("SELECT k1, k2 FROM " + fullViewName
                    + " WHERE k2 >= 120");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(120, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(121, rs.getInt(2));
            assertFalse(rs.next());
        }
    }

    public static Pair<String, Scan> testUpdatableViewIndex(
            String fullTableName, Integer saltBuckets,
            boolean localIndex, String viewName) throws Exception {
        ResultSet rs;
        try (Connection conn = DriverManager.getConnection(getUrl());
                Statement stmt = conn.createStatement()) {
            String viewIndexName1 = "I_" + generateUniqueName();
            String viewIndexPhysicalName = MetaDataUtil.
                    getViewIndexPhysicalName(fullTableName);
            if (localIndex) {
                stmt.execute("CREATE LOCAL INDEX " + viewIndexName1 + " on "
                        + viewName + "(k3)");
            } else {
                stmt.execute(
                        "CREATE INDEX " + viewIndexName1 + " on " + viewName
                                + "(k3) include (s)");
            }
            stmt.execute("UPSERT INTO " + viewName + "(k2,S,k3) "
                    + "VALUES(120,'foo',50.0)");
            conn.commit();

            analyzeTable(conn, viewName);
            List<KeyRange> splits = getAllSplits(conn, viewIndexName1);
            // More guideposts with salted, since it's already pre-split at salt
            // buckets
            assertEquals(saltBuckets == null ? 6 : 8, splits.size());

            String query = "SELECT k1, k2, k3, s FROM " + viewName
                    + " WHERE k3 = 51.0";
            rs = stmt.executeQuery(query);
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(121, rs.getInt(2));
            assertEquals(0, BigDecimal.valueOf(51.0).compareTo(
                    rs.getBigDecimal(3)));
            assertEquals("bar", rs.getString(4));
            assertFalse(rs.next());

            ExplainPlan plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes =
                plan.getPlanStepsAsAttributes();
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());

            if (localIndex) {
                assertEquals("PARALLEL "
                        + (saltBuckets == null ? 1 : saltBuckets) + "-WAY",
                    explainPlanAttributes.getIteratorTypeAndScanSize());
                assertEquals(fullTableName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [1,51]", explainPlanAttributes.getKeyRanges());
                assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                    explainPlanAttributes.getServerWhereFilter());
                assertEquals("CLIENT MERGE SORT",
                    explainPlanAttributes.getClientSortAlgo());
            } else {
                assertEquals(viewIndexPhysicalName,
                    explainPlanAttributes.getTableName());
                if (saltBuckets == null) {
                    assertEquals("PARALLEL 1-WAY",
                        explainPlanAttributes.getIteratorTypeAndScanSize());
                    assertEquals(" [" + Short.MIN_VALUE + ",51]",
                        explainPlanAttributes.getKeyRanges());
                    assertNull(explainPlanAttributes.getClientSortAlgo());
                } else {
                    assertEquals("PARALLEL " + saltBuckets + "-WAY",
                        explainPlanAttributes.getIteratorTypeAndScanSize());
                    assertEquals(" [0," + Short.MIN_VALUE + ",51] - ["
                        + (saltBuckets - 1) + "," + Short.MIN_VALUE + ",51]",
                        explainPlanAttributes.getKeyRanges());
                    assertEquals("CLIENT MERGE SORT",
                        explainPlanAttributes.getClientSortAlgo());
                }
            }

            String viewIndexName2 = "I_" + generateUniqueName();
            if (localIndex) {
                stmt.execute("CREATE LOCAL INDEX " + viewIndexName2 + " on "
                        + viewName + "(s)");
            } else {
                stmt.execute("CREATE INDEX " + viewIndexName2 + " on "
                        + viewName + "(s)");
            }

            // new index hasn't been analyzed yet
            splits = getAllSplits(conn, viewIndexName2);
            assertEquals(saltBuckets == null ? 1 : 3, splits.size());

            // analyze table should analyze all view data
            analyzeTable(conn, fullTableName);
            splits = getAllSplits(conn, viewIndexName2);
            assertEquals(saltBuckets == null ? 6 : 8, splits.size());

            query = "SELECT k1, k2, s FROM " + viewName + " WHERE s = 'foo'";
            rs = stmt.executeQuery(query);
            Scan scan = stmt.unwrap(PhoenixStatement.class).getQueryPlan()
                    .getContext().getScan();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals(120, rs.getInt(2));
            assertEquals("foo", rs.getString(3));
            assertFalse(rs.next());
            String physicalTableName;

            plan = conn.prepareStatement(query)
                .unwrap(PhoenixPreparedStatement.class).optimizeQuery()
                .getExplainPlan();
            explainPlanAttributes = plan.getPlanStepsAsAttributes();
            assertEquals("RANGE SCAN ",
                explainPlanAttributes.getExplainScanType());
            assertEquals("SERVER FILTER BY FIRST KEY ONLY",
                explainPlanAttributes.getServerWhereFilter());
            if (localIndex) {
                physicalTableName = fullTableName;
                assertEquals("PARALLEL " + (saltBuckets == null ? 1 : saltBuckets)
                    + "-WAY", explainPlanAttributes.getIteratorTypeAndScanSize());
                assertEquals(fullTableName,
                    explainPlanAttributes.getTableName());
                assertEquals(" [" + (2) + ",'foo']",
                    explainPlanAttributes.getKeyRanges());
            } else {
                physicalTableName = viewIndexPhysicalName;
                assertEquals(viewIndexPhysicalName,
                    explainPlanAttributes.getTableName());
                if (saltBuckets == null) {
                    assertEquals("PARALLEL 1-WAY",
                        explainPlanAttributes.getIteratorTypeAndScanSize());
                    assertEquals(" [" + (Short.MIN_VALUE + 1)
                        + ",'foo']", explainPlanAttributes.getKeyRanges());
                    assertNull(explainPlanAttributes.getClientSortAlgo());
                } else {
                    assertEquals("PARALLEL " + saltBuckets + "-WAY",
                        explainPlanAttributes.getIteratorTypeAndScanSize());
                    assertEquals(" [0," + (Short.MIN_VALUE + 1) + ",'foo'] - ["
                        + (saltBuckets - 1) + "," + (Short.MIN_VALUE + 1)
                        + ",'foo']", explainPlanAttributes.getKeyRanges());
                    assertEquals("CLIENT MERGE SORT",
                        explainPlanAttributes.getClientSortAlgo());
                }
            }
            return new Pair<>(physicalTableName, scan);
        }
    }
    
    @Test
    public void testDisallowCreatingViewsOnSystemTable() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String viewDDL = "CREATE VIEW " + generateUniqueName()
                    + " AS SELECT * FROM SYSTEM.CATALOG";
            try (Statement stmt = conn.createStatement()){
                stmt.execute(viewDDL);
                fail("Should have thrown an exception");
            } catch (SQLException sqlE) {
                assertEquals("Expected a different Error code",
                        SQLExceptionCode.CANNOT_CREATE_VIEWS_ON_SYSTEM_TABLES
                                .getErrorCode(), sqlE.getErrorCode());
            }
        }
    }

}
