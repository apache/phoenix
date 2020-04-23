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

import com.clearspring.analytics.util.Lists;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.function.FunctionExpression;
import org.apache.phoenix.expression.visitor.KeyValueExpressionVisitor;
import org.apache.phoenix.expression.visitor.StatelessTraverseAllExpressionVisitor;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeVisitor;
import org.apache.phoenix.parse.PhoenixRowTimestampParseNode;
import org.apache.phoenix.parse.StatelessTraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.TraverseAllParseNodeVisitor;
import org.apache.phoenix.parse.UDFParseNode;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TableViewFinderResult;
import org.apache.phoenix.util.ViewUtil;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.fail;
import static org.apache.phoenix.coprocessor.MetaDataProtocol.MIN_SPLITTABLE_SYSTEM_CATALOG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_LINK_HBASE_TABLE_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ViewUtilIT extends ParallelStatsDisabledIT {

    @Test
    public void testGetSystemTableForChildLinks() throws Exception {
        assertEquals(SYSTEM_LINK_HBASE_TABLE_NAME, ViewUtil.getSystemTableForChildLinks(
                MIN_SPLITTABLE_SYSTEM_CATALOG, config));

        // lower version should also give CHILD_LINK table as server upgrade to advanced version
        assertEquals(SYSTEM_LINK_HBASE_TABLE_NAME, ViewUtil.getSystemTableForChildLinks(
                MIN_SPLITTABLE_SYSTEM_CATALOG - 1, config));
    }

    @Test
    public void testHasChildViewsInGlobalViewCase() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        TableName catalogOrChildTableName = ViewUtil.getSystemTableForChildLinks(0, config);
        String schema = generateUniqueName();
        byte[] schemaInBytes = schema.getBytes(StandardCharsets.UTF_8);
        byte[] tenantIdInBytes = new byte[0];
        String fullTableName = schema + "." + generateUniqueName();
        String secondLevelViewName = schema + "." + generateUniqueName();
        String thirdLevelViewName = schema + "." + generateUniqueName();
        String leafViewName1 = schema + "." + generateUniqueName();
        String leafViewName2 = schema + "." + generateUniqueName();

        String tableDDLQuery = "CREATE TABLE " + fullTableName + " (A BIGINT PRIMARY KEY, B BIGINT)";
        String viewDDLQuery = "CREATE VIEW %s AS SELECT * FROM %s";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(tableDDLQuery);
            conn.createStatement().execute(
                    String.format(viewDDLQuery, secondLevelViewName, fullTableName));
            conn.createStatement().execute(
                    String.format(viewDDLQuery, leafViewName1, secondLevelViewName));
            conn.createStatement().execute(
                    String.format(viewDDLQuery, thirdLevelViewName, secondLevelViewName));
            conn.createStatement().execute(
                    String.format(viewDDLQuery, leafViewName2, thirdLevelViewName));

            try (PhoenixConnection phoenixConnection =
                    DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class)) {
                Table catalogOrChildTable = phoenixConnection.getQueryServices().getTable(
                        SchemaUtil.getPhysicalName(catalogOrChildTableName.toBytes(),
                                phoenixConnection.getQueryServices().getProps()).getName());

                assertTrue(ViewUtil.hasChildViews(catalogOrChildTable,
                        tenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(fullTableName).
                                getBytes(StandardCharsets.UTF_8),
                        System.currentTimeMillis()));
                assertTrue(ViewUtil.hasChildViews(catalogOrChildTable,
                        tenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(secondLevelViewName).
                                getBytes(StandardCharsets.UTF_8)
                        , System.currentTimeMillis()));
                assertTrue(ViewUtil.hasChildViews(catalogOrChildTable,
                        tenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(thirdLevelViewName).
                                getBytes(StandardCharsets.UTF_8),
                        System.currentTimeMillis()));
                assertFalse(ViewUtil.hasChildViews(catalogOrChildTable,
                        tenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(leafViewName1).
                                getBytes(StandardCharsets.UTF_8),
                        System.currentTimeMillis()));
                assertFalse(ViewUtil.hasChildViews(catalogOrChildTable,
                        tenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(leafViewName2).
                                getBytes(StandardCharsets.UTF_8),
                        System.currentTimeMillis()));
            }
        }
    }

    @Test
    public void testHasChildViewsInTenantViewCase() throws Exception {
        String tenantId = generateUniqueName();
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        TableName catalogOrChildTableName = ViewUtil.getSystemTableForChildLinks(0, config);
        String schema = generateUniqueName();
        byte[] schemaInBytes = schema.getBytes(StandardCharsets.UTF_8);
        byte[] tenantIdInBytes = tenantId.getBytes(StandardCharsets.UTF_8);
        byte[] emptyTenantIdInBytes = new byte[0];
        String multiTenantTableName = schema + "." + generateUniqueName();
        String globalViewName = schema + "." + generateUniqueName();
        String tenantViewOnMultiTenantTable1 = schema + "." + generateUniqueName();
        String viewName2 = generateUniqueName();
        String tenantViewOnMultiTenantTable2 = schema + "." + viewName2;
        String tenantViewIndex = viewName2 + "_INDEX";
        String tenantViewOnGlobalView = schema + "." + generateUniqueName();

        String multiTenantTableDDL = "CREATE TABLE " + multiTenantTableName +
                "(TENANT_ID CHAR(10) NOT NULL, ID CHAR(10) NOT NULL, NUM BIGINT " +
                "CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true";
        String globalViewDDL = "CREATE VIEW " + globalViewName + "(PK1 BIGINT, PK2 BIGINT) " +
                "AS SELECT * FROM " + multiTenantTableName + " WHERE NUM > -1";
        String viewDDL = "CREATE VIEW %s AS SELECT * FROM %s";
        String viewIndexDDL = "CREATE INDEX " + tenantViewIndex + " ON " +
                tenantViewOnMultiTenantTable2 + "(NUM DESC) INCLUDE (ID)";

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(multiTenantTableDDL);
            conn.createStatement().execute(globalViewDDL);

            try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
                tenantConn.createStatement().execute(
                        String.format(viewDDL, tenantViewOnGlobalView, globalViewName));
                tenantConn.createStatement().execute(
                        String.format(viewDDL, tenantViewOnMultiTenantTable1, multiTenantTableName));
                tenantConn.createStatement().execute(
                        String.format(viewDDL, tenantViewOnMultiTenantTable2, multiTenantTableName));
                tenantConn.createStatement().execute(viewIndexDDL);
            }

            try (PhoenixConnection phoenixConnection = DriverManager.getConnection(getUrl(),
                    props).unwrap(PhoenixConnection.class)) {
                Table catalogOrChildTable = phoenixConnection.getQueryServices().getTable(
                        SchemaUtil.getPhysicalName(catalogOrChildTableName.toBytes(),
                                phoenixConnection.getQueryServices().getProps()).getName());

                assertTrue(ViewUtil.hasChildViews(catalogOrChildTable,
                        emptyTenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(multiTenantTableName).
                                getBytes(StandardCharsets.UTF_8),
                        System.currentTimeMillis()));
                assertTrue(ViewUtil.hasChildViews(catalogOrChildTable,
                        emptyTenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(globalViewName).
                                getBytes(StandardCharsets.UTF_8)
                        , System.currentTimeMillis()));
                assertFalse(ViewUtil.hasChildViews(catalogOrChildTable,
                        tenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(tenantViewOnMultiTenantTable1).
                                getBytes(StandardCharsets.UTF_8),
                        System.currentTimeMillis()));
                assertFalse(ViewUtil.hasChildViews(catalogOrChildTable,
                        tenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(tenantViewOnMultiTenantTable2).
                                getBytes(StandardCharsets.UTF_8),
                        System.currentTimeMillis()));
                assertFalse(ViewUtil.hasChildViews(catalogOrChildTable,
                        tenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(tenantViewOnGlobalView).
                                getBytes(StandardCharsets.UTF_8),
                        System.currentTimeMillis()));
            }
        }
    }

    @Test
    public void testFindAllRelativesForGlobalConnection() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        TableName catalogOrChildTableName = ViewUtil.getSystemTableForChildLinks(0, config);
        String schema = generateUniqueName();
        byte[] schemaInBytes = schema.getBytes(StandardCharsets.UTF_8);
        byte[] tenantIdInBytes = new byte[0];
        String fullTableName = schema + "." + generateUniqueName();
        String middleLevelViewName = schema + "." + generateUniqueName();
        String leafViewName1 = schema + "." + generateUniqueName();
        String leafViewName2 = schema + "." + generateUniqueName();
        int NUMBER_OF_VIEWS = 3;

        String tableDDLQuery = "CREATE TABLE " + fullTableName + " (A BIGINT PRIMARY KEY, B BIGINT)";
        String viewDDLQuery = "CREATE VIEW %s AS SELECT * FROM %s";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(tableDDLQuery);
            conn.createStatement().execute(
                    String.format(viewDDLQuery, middleLevelViewName, fullTableName));
            conn.createStatement().execute(
                    String.format(viewDDLQuery, leafViewName1, middleLevelViewName));
            conn.createStatement().execute(
                    String.format(viewDDLQuery, leafViewName2, middleLevelViewName));

            try (PhoenixConnection phoenixConnection = DriverManager.getConnection(getUrl(),
                    props).unwrap(PhoenixConnection.class)) {
                Table catalogOrChildTable = phoenixConnection.getQueryServices().getTable(
                        SchemaUtil.getPhysicalName(catalogOrChildTableName.toBytes(),
                                phoenixConnection.getQueryServices().getProps()).getName());

                TableViewFinderResult result = new TableViewFinderResult();
                ViewUtil.findAllRelatives(catalogOrChildTable, tenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(fullTableName).
                                getBytes(StandardCharsets.UTF_8),
                        PTable.LinkType.CHILD_TABLE, result);
                assertEquals(NUMBER_OF_VIEWS, result.getLinks().size());

                result = new TableViewFinderResult();
                ViewUtil.findAllRelatives(catalogOrChildTable, tenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(middleLevelViewName).
                                getBytes(StandardCharsets.UTF_8),
                        PTable.LinkType.CHILD_TABLE, result);
                assertEquals(2, result.getLinks().size());

                result = new TableViewFinderResult();
                ViewUtil.findAllRelatives(catalogOrChildTable, tenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(leafViewName1).
                                getBytes(StandardCharsets.UTF_8),
                        PTable.LinkType.CHILD_TABLE, result);
                assertEquals(0, result.getLinks().size());

                result = new TableViewFinderResult();
                ViewUtil.findAllRelatives(catalogOrChildTable, tenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(leafViewName2).
                                getBytes(StandardCharsets.UTF_8),
                        PTable.LinkType.CHILD_TABLE, result);
                assertEquals(0, result.getLinks().size());
            }
        }
    }

    @Test
    public void testFindAllRelativesForTenantConnection() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String tenantId1 = generateUniqueName();
        Properties tenantProps1 = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        tenantProps1.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId1);
        String tenantId2 = generateUniqueName();
        Properties tenantProps2 = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        tenantProps2.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId2);
        TableName catalogOrChildTableName = ViewUtil.getSystemTableForChildLinks(0, config);
        String schema = generateUniqueName();
        byte[] schemaInBytes = schema.getBytes(StandardCharsets.UTF_8);
        byte[] tenantId1InBytes = tenantId1.getBytes(StandardCharsets.UTF_8);
        byte[] tenantId2InBytes = tenantId2.getBytes(StandardCharsets.UTF_8);
        byte[] emptyTenantIdInBytes = new byte[0];
        String multiTenantTableName = schema + "." + generateUniqueName();
        String tenant1MiddleLevelViewOnMultiTenantTable = schema + "." + generateUniqueName();
        String tenant1LeafViewName = schema + "." + generateUniqueName();
        String tenant2LeafViewName = schema + "." + generateUniqueName();
        int NUMBER_OF_VIEWS = 3;

        String multiTenantTableDDL = "CREATE TABLE " + multiTenantTableName +
                "(TENANT_ID CHAR(10) NOT NULL, ID CHAR(10) NOT NULL, NUM BIGINT " +
                "CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true";
        String viewDDL = "CREATE VIEW %s AS SELECT * FROM %s";

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(multiTenantTableDDL);

            try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps1)) {
                tenantConn.createStatement().execute(
                        String.format(viewDDL, tenant1MiddleLevelViewOnMultiTenantTable,
                                multiTenantTableName));
                tenantConn.createStatement().execute(
                        String.format(viewDDL, tenant1LeafViewName,
                                tenant1MiddleLevelViewOnMultiTenantTable));
            }
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps2)) {
                tenantConn.createStatement().execute(
                        String.format(viewDDL, tenant2LeafViewName, multiTenantTableName));
            }

            try (PhoenixConnection phoenixConnection = DriverManager.getConnection(getUrl(),
                    props).unwrap(PhoenixConnection.class)) {
                Table catalogOrChildTable = phoenixConnection.getQueryServices().getTable(
                        SchemaUtil.getPhysicalName(catalogOrChildTableName.toBytes(),
                                phoenixConnection.getQueryServices().getProps()).getName());

                TableViewFinderResult result = new TableViewFinderResult();
                ViewUtil.findAllRelatives(catalogOrChildTable, emptyTenantIdInBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(multiTenantTableName).
                                getBytes(StandardCharsets.UTF_8),
                        PTable.LinkType.CHILD_TABLE, result);
                assertEquals(NUMBER_OF_VIEWS, result.getLinks().size());

                result = new TableViewFinderResult();
                ViewUtil.findAllRelatives(catalogOrChildTable, tenantId1InBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(
                                tenant1MiddleLevelViewOnMultiTenantTable).
                                getBytes(StandardCharsets.UTF_8),
                        PTable.LinkType.CHILD_TABLE, result);
                assertEquals(1, result.getLinks().size());

                result = new TableViewFinderResult();
                ViewUtil.findAllRelatives(catalogOrChildTable, tenantId1InBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(tenant1LeafViewName).
                                getBytes(StandardCharsets.UTF_8),
                        PTable.LinkType.CHILD_TABLE, result);
                assertEquals(0, result.getLinks().size());

                result = new TableViewFinderResult();
                ViewUtil.findAllRelatives(catalogOrChildTable, tenantId2InBytes, schemaInBytes,
                        SchemaUtil.getTableNameFromFullName(tenant2LeafViewName).
                                getBytes(StandardCharsets.UTF_8),
                        PTable.LinkType.CHILD_TABLE, result);
                assertEquals(0, result.getLinks().size());
            }
        }
    }

    @Test public void testGetViewWhereWithViewTTL() throws Exception {
        String tenantId = generateUniqueName();
        Properties tenantProps = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        String schema = generateUniqueName();

        String multiTenantTableName = schema + "." + generateUniqueName();
        String globalViewName = schema + "." + generateUniqueName();

        String view1 = generateUniqueName();
        String view2 = generateUniqueName();
        String customObjectView1 = schema + "." + view1;
        String customObjectView2 = schema + "." + view2;

        String tenantView = generateUniqueName();
        String tenantViewOnGlobalView = schema + "." + tenantView;
        String tenantViewIndex = tenantView + "_INDEX";
        String tenantIndex = schema + "." + tenantViewIndex;

        String
                multiTenantTableDDL =
                "CREATE TABLE IF NOT EXISTS " + multiTenantTableName
                        + "(TENANT_ID CHAR(15) NOT NULL, KP CHAR(3) NOT NULL, ID VARCHAR, NUM BIGINT "
                        + "CONSTRAINT PK PRIMARY KEY (TENANT_ID, KP)) MULTI_TENANT=true";

        String
                globalViewDDL =
                "CREATE VIEW IF NOT EXISTS " + globalViewName + "(G1 BIGINT, G2 BIGINT) "
                        + "AS SELECT * FROM " + multiTenantTableName + " WHERE KP = '001' VIEW_TTL=30";

        String viewDDL = "CREATE VIEW IF NOT EXISTS %s (V1 BIGINT, V2 BIGINT) AS SELECT * FROM %s VIEW_TTL=30";
        String
                viewIndexDDL =
                "CREATE INDEX IF NOT EXISTS " + tenantViewIndex + " ON " + tenantViewOnGlobalView
                        + "(NUM DESC) INCLUDE (ID)";

        String
                customObjectViewDDL =
                "CREATE VIEW IF NOT EXISTS %s (V1 BIGINT, V2 BIGINT) " + "AS SELECT * FROM "
                        + multiTenantTableName + " WHERE KP = '%s' VIEW_TTL=30";

        String selectFromViewSQL = "SELECT * FROM %s";

        List<String> dmls = Arrays.asList(new String[] {
                String.format(viewDDL, tenantViewOnGlobalView, globalViewName),
                String.format(customObjectViewDDL, customObjectView1, view1),
                String.format(customObjectViewDDL, customObjectView2, view2),
                viewIndexDDL
        });

        // Create the various tables and views
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // Base table.
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(multiTenantTableDDL);
            }
            // Global view.
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(globalViewDDL);
            }
            // Tenant views and indexes.
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
                for (String dml : dmls) {
                    try (Statement stmt = tenantConn.createStatement()) {
                        stmt.execute(dml);
                    }
                }
            }
        }

        List<String> sqls = Arrays.asList(new String[] {
                String.format(selectFromViewSQL, tenantViewOnGlobalView),
                String.format(selectFromViewSQL, customObjectView1),
                String.format(selectFromViewSQL, customObjectView2),
                String.format(selectFromViewSQL, tenantIndex)
        });

        // Validate the view where clause for the above sqls.
        try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
            for (String sql : sqls) {
                try (Statement stmt = tenantConn.createStatement()) {
                    PhoenixStatement pstmt = stmt.unwrap(PhoenixStatement.class);
                    QueryPlan plan = pstmt.compileQuery(sql);
                    validatePhoenixRowTimestampParseNodeExists(plan);
                }
            }
        }

    }

    private void validatePhoenixRowTimestampParseNodeExists(QueryPlan plan) {
        ParseNode parsedNode;
        final AtomicInteger count = new AtomicInteger(0);
        try {
            parsedNode =
                    ViewUtil.getViewWhereWithViewTTL(plan.getContext(),
                            plan.getTableRef().getTable());
            if (parsedNode != null) {
                parsedNode.accept(new StatelessTraverseAllParseNodeVisitor() {
                    @Override
                    public boolean visitEnter(FunctionParseNode node) throws SQLException {
                        if(node instanceof PhoenixRowTimestampParseNode) {
                            count.set(1);
                        }
                        return super.visitEnter(node);
                    }
                });
            }
            Assert.assertEquals(1, count.get());
        } catch (SQLException e) {
            fail();
        }

    }

}
