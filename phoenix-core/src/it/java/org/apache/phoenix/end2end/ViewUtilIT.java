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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.TableInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TableViewFinderResult;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ViewUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.phoenix.coprocessor.MetaDataProtocol.MIN_SPLITTABLE_SYSTEM_CATALOG;
import static org.apache.phoenix.end2end.ViewMetadataIT.BASE_TABLE_SCHEMA;
import static org.apache.phoenix.end2end.ViewMetadataIT.CHILD_VIEW_LEVEL_1_SCHEMA;
import static org.apache.phoenix.end2end.ViewMetadataIT.CREATE_BASE_TABLE_DDL;
import static org.apache.phoenix.end2end.ViewMetadataIT.CREATE_CHILD_VIEW_LEVEL_1_DDL;
import static org.apache.phoenix.end2end.ViewMetadataIT.createOrphanLink;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_LINK_HBASE_TABLE_NAME;
import static org.apache.phoenix.util.ByteUtil.EMPTY_BYTE_ARRAY;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(ParallelStatsDisabledTest.class)
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

        String tableDDLQuery = "CREATE TABLE " + fullTableName
                + " (A BIGINT PRIMARY KEY, B BIGINT)";
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
                    DriverManager.getConnection(getUrl(), props).unwrap(PhoenixConnection.class);
                    Table catalogOrChildTable = phoenixConnection.getQueryServices().getTable(
                            SchemaUtil.getPhysicalName(catalogOrChildTableName.toBytes(),
                                    phoenixConnection.getQueryServices().getProps()).getName())) {

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
        tenantProps.setProperty(TENANT_ID_ATTRIB, tenantId);
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
                        String.format(viewDDL, tenantViewOnMultiTenantTable1,
                                multiTenantTableName));
                tenantConn.createStatement().execute(
                        String.format(viewDDL, tenantViewOnMultiTenantTable2,
                                multiTenantTableName));
                tenantConn.createStatement().execute(viewIndexDDL);
            }

            try (PhoenixConnection phoenixConnection = DriverManager.getConnection(getUrl(),
                    props).unwrap(PhoenixConnection.class);
                    Table catalogOrChildTable = phoenixConnection.getQueryServices().getTable(
                            SchemaUtil.getPhysicalName(catalogOrChildTableName.toBytes(),
                                    phoenixConnection.getQueryServices().getProps()).getName())) {

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

        String tableDDLQuery = "CREATE TABLE " + fullTableName
                + " (A BIGINT PRIMARY KEY, B BIGINT)";
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
                    props).unwrap(PhoenixConnection.class);
                    Table catalogOrChildTable = phoenixConnection.getQueryServices().getTable(
                            SchemaUtil.getPhysicalName(catalogOrChildTableName.toBytes(),
                                    phoenixConnection.getQueryServices().getProps()).getName())) {

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
        tenantProps1.setProperty(TENANT_ID_ATTRIB, tenantId1);
        String tenantId2 = generateUniqueName();
        Properties tenantProps2 = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        tenantProps2.setProperty(TENANT_ID_ATTRIB, tenantId2);
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
                    props).unwrap(PhoenixConnection.class);
                    Table catalogOrChildTable = phoenixConnection.getQueryServices().getTable(
                            SchemaUtil.getPhysicalName(catalogOrChildTableName.toBytes(),
                                    phoenixConnection.getQueryServices().getProps()).getName())) {

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

    @Test
    public void testFindLegitChildViews() throws Exception {
        final String parentTable = generateUniqueName();
        List<String> childViewNames = new ArrayList<>(3);
        childViewNames.add("A_" + generateUniqueName());
        childViewNames.add("B_" + generateUniqueName());
        childViewNames.add("C_" + generateUniqueName());
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(String.format(CREATE_BASE_TABLE_DDL, BASE_TABLE_SCHEMA,
                    parentTable));
            for (String childViewName : childViewNames) {
                conn.createStatement().execute(String.format(CREATE_CHILD_VIEW_LEVEL_1_DDL,
                        CHILD_VIEW_LEVEL_1_SCHEMA, childViewName, BASE_TABLE_SCHEMA, parentTable));
            }
            ConnectionQueryServices cqs = conn.unwrap(PhoenixConnection.class).getQueryServices();
            try (Table childLinkTable = cqs.getTable(SchemaUtil.getPhysicalName(
                    SYSTEM_LINK_HBASE_TABLE_NAME.toBytes(), cqs.getProps()).getName())) {
                Pair<List<PTable>, List<TableInfo>> allDescendants =
                        ViewUtil.findAllDescendantViews(childLinkTable, cqs.getConfiguration(),
                                EMPTY_BYTE_ARRAY, BASE_TABLE_SCHEMA.getBytes(),
                                parentTable.getBytes(), HConstants.LATEST_TIMESTAMP, true);
                assertTrue("No orphan views expected", allDescendants.getSecond().isEmpty());
                List<PTable> childViews = allDescendants.getFirst();
                assertEquals("Just 1 legit child view expected", 1, childViews.size());
                PTable childView = childViews.get(0);

                // Should have got the first child view as per alphabetical ordering of the
                // linking row scan result
                assertEquals(CHILD_VIEW_LEVEL_1_SCHEMA, childView.getSchemaName().getString());
                assertEquals(childViewNames.get(0), childView.getTableName().getString());
                assertEquals(BASE_TABLE_SCHEMA, childView.getParentSchemaName().getString());
                assertEquals(parentTable, childView.getParentTableName().getString());

                allDescendants = ViewUtil.findAllDescendantViews(childLinkTable,
                        cqs.getConfiguration(), EMPTY_BYTE_ARRAY, BASE_TABLE_SCHEMA.getBytes(),
                        parentTable.getBytes(), HConstants.LATEST_TIMESTAMP, false);
                assertTrue("No orphan views expected", allDescendants.getSecond().isEmpty());
                childViews = allDescendants.getFirst();
                assertEquals("All child views expected", childViewNames.size(), childViews.size());
                for (int i=0; i<childViewNames.size(); i++) {
                    childView = childViews.get(i);
                    assertEquals(CHILD_VIEW_LEVEL_1_SCHEMA, childView.getSchemaName().getString());
                    assertEquals(childViewNames.get(i), childView.getTableName().getString());
                    assertEquals(BASE_TABLE_SCHEMA, childView.getParentSchemaName().getString());
                    assertEquals(parentTable, childView.getParentTableName().getString());
                }
            }
        }
    }

    @Test
    public void testOrphanViewDetection() throws Exception {
        final String parent1TableName = generateUniqueName();
        final String parent2TableName = generateUniqueName();
        final String viewName = "V_" + generateUniqueName();
        createOrphanLink(BASE_TABLE_SCHEMA, parent1TableName, parent2TableName,
                CHILD_VIEW_LEVEL_1_SCHEMA, viewName);

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            ConnectionQueryServices cqs = conn.unwrap(PhoenixConnection.class).getQueryServices();
            try (Table childLinkTable = cqs.getTable(SchemaUtil.getPhysicalName(
                    SYSTEM_LINK_HBASE_TABLE_NAME.toBytes(), cqs.getProps()).getName())) {
                // The view is a legitimate child of parent1, so it should not be counted as
                // neither a legitimate view of parent2, nor an orphan view of parent2
                Pair<List<PTable>, List<TableInfo>> allDescendants =
                        ViewUtil.findAllDescendantViews(childLinkTable, cqs.getConfiguration(),
                                EMPTY_BYTE_ARRAY, BASE_TABLE_SCHEMA.getBytes(),
                                parent2TableName.getBytes(), HConstants.LATEST_TIMESTAMP, false);
                assertTrue("No orphan views expected", allDescendants.getSecond().isEmpty());
                assertTrue("No legitimate views expected", allDescendants.getFirst().isEmpty());

                // Drop that view
                conn.createStatement().execute(String.format("DROP VIEW %s.%s",
                        CHILD_VIEW_LEVEL_1_SCHEMA,
                        viewName));

                // Now this view is no longer a legitimate child view of parent1 either, so the
                // orphan parent2->view link should show up as an orphan view of parent2
                allDescendants = ViewUtil.findAllDescendantViews(childLinkTable,
                        cqs.getConfiguration(), EMPTY_BYTE_ARRAY, BASE_TABLE_SCHEMA.getBytes(),
                        parent2TableName.getBytes(), HConstants.LATEST_TIMESTAMP, false);
                assertTrue("No legitimate views expected", allDescendants.getFirst().isEmpty());
                List<TableInfo> orphanViews = allDescendants.getSecond();
                assertEquals("1 orphan view expected", 1, orphanViews.size());
                assertEquals(CHILD_VIEW_LEVEL_1_SCHEMA,
                        Bytes.toString(orphanViews.get(0).getSchemaName()));
                assertEquals(viewName, Bytes.toString(orphanViews.get(0).getTableName()));
            }
        }
    }

}
