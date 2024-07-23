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

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.PhoenixTestBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder;
import org.apache.phoenix.query.PhoenixTestBuilder.SchemaBuilder.TableOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TTL_NOT_DEFINED;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;

@Category(ParallelStatsDisabledTest.class)
@RunWith(Parameterized.class)
public class TTLIT extends ParallelStatsDisabledIT {

    private final boolean isMultiTenant;

    private static final int DEFAULT_TEST_TTL_VALUE = 800000;
    private static final int ALTER_TEST_TTL_VALUE = 100000;
    private static final int DEFAULT_TEST_TTL_VALUE_AT_GLOBAL = 400000;
    private static final int DEFAULT_TEST_TTL_VALUE_AT_TENANT = 200000;
    public static final String SKIP_ASSERT = "SKIP_ASSERT";

    public TTLIT (boolean isMultiTenant) {
        this.isMultiTenant = isMultiTenant;
    }

    @Parameterized.Parameters(name="isMultiTenant={0}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { true },
                { false },
        });
    }

    /**
     * BUILD methods to create Table/Index/View/ViewIndexes with or without TTL as table props
     * at different levels
     */

    private SchemaBuilder createTableWithTTL(boolean addTTL, boolean addIndex,
                                             SchemaBuilder.TableIndexOptions indexOptions) {
        SchemaBuilder schemaBuilder = new SchemaBuilder(getUrl());
        TableOptions tableOptions = TableOptions.withDefaults();
        tableOptions.setMultiTenant(isMultiTenant);
        if (addTTL) {
            tableOptions.setTableProps("TTL = " + DEFAULT_TEST_TTL_VALUE);
        }
        if (addIndex) {
            if (indexOptions == null) {
                indexOptions = SchemaBuilder.TableIndexOptions.withDefaults();
            }
            schemaBuilder.withTableIndexOptions(indexOptions);
        }
        return schemaBuilder.withTableOptions(tableOptions);
    }

    private SchemaBuilder createGlobalViewOnTable(boolean addTTL, boolean addIndex) {
        return createGlobalViewOnTable(createTableWithTTL(false, false, null), addTTL, addIndex, true);
    }

    private SchemaBuilder createGlobalViewOnTable(SchemaBuilder schemaBuilder, boolean addTTL,
                                                  boolean addIndex, boolean addPK) {
        SchemaBuilder.GlobalViewOptions globalViewOptions = SchemaBuilder.GlobalViewOptions.withDefaults();
        if (!addPK) {
            globalViewOptions.setGlobalViewPKColumns(Lists.newArrayList());
        }
        if (addTTL) {
            globalViewOptions.setTableProps("TTL = " + DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
        }
        if (addIndex) {
            schemaBuilder.withGlobalViewIndexDefaults();
        }
        return schemaBuilder.withGlobalViewOptions(globalViewOptions);
    }

    private SchemaBuilder createTenantViewOnTable(boolean addTTL, boolean addIndex) {
        return createTenantViewOnTableOrGlobalView(createTableWithTTL(false, false, null),
                addTTL, addIndex, true);
    }

    private SchemaBuilder createTenantViewOnGlobalView(boolean addTTL, boolean addIndex) {
        return createTenantViewOnTableOrGlobalView(createGlobalViewOnTable(false, false),
                addTTL, addIndex, true);
    }

    private SchemaBuilder createTenantViewOnTableOrGlobalView(SchemaBuilder schemaBuilder, boolean addTTL,
                                                              boolean addIndex, boolean addPK) {
        SchemaBuilder.TenantViewOptions tenantViewOptions = SchemaBuilder.TenantViewOptions.withDefaults();
        if (!addPK) {
            tenantViewOptions.setTenantViewPKColumns(Lists.newArrayList());
        }
        if (addTTL) {
            tenantViewOptions.setTableProps("TTL = " + DEFAULT_TEST_TTL_VALUE_AT_TENANT);
        }
        if (addIndex) {
            schemaBuilder.withTenantViewIndexDefaults();
        }
        return schemaBuilder.withTenantViewOptions(tenantViewOptions);
    }

    /**
     * Assert Methods
     */

    private void assertTTLForGivenPTable(PTable table, int ttl) {
        Assert.assertEquals(ttl, table.getTTL());
    }


    private void assertTTLForGivenEntity(Connection connection, String entityName, int ttl) throws SQLException {
        PTable pTable = PhoenixRuntime.getTable(connection, entityName);
        Assert.assertEquals(ttl,pTable.getTTL());
    }

    private void assertTTLForIndexName(Connection connection, String indexName, int ttl) throws SQLException {
        if (!indexName.equals(SKIP_ASSERT)) {
            PTable index = PhoenixRuntime.getTable(connection, indexName);
            Assert.assertEquals(ttl,index.getTTL());
        }
    }

    private void assertTTLForIndexFromParentPTable(Connection connection, String baseEntity, int ttl) throws SQLException {
        PTable entity = PhoenixRuntime.getTable(connection, baseEntity);
        List<PTable> indexes = entity.getIndexes();
        for (PTable index : indexes) {
            assertTTLForGivenPTable(index, ttl);
        }
    }

    /*
     * TEST METHODS
     */

    @Test
    public void testTTLAtTableLevelWithIndex() throws Exception {
        SchemaBuilder schemaBuilder = createTableWithTTL(true, true,null);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String indexName = schemaBuilder.getEntityTableIndexName();

        try (Connection globalConnection = DriverManager.getConnection(getUrl())) {
            assertTTLForGivenPTable(table, DEFAULT_TEST_TTL_VALUE);
            assertTTLForIndexName(globalConnection, indexName, DEFAULT_TEST_TTL_VALUE);
            //Assert TTL for index by getting PTable from table's PTable
            assertTTLForIndexFromParentPTable(globalConnection, schemaBuilder.getEntityTableName(), DEFAULT_TEST_TTL_VALUE);
        }
    }


    @Test
    public void testSettingTTLForIndex() throws Exception {
        SchemaBuilder.TableIndexOptions indexOptions = new SchemaBuilder.TableIndexOptions();
        indexOptions.setTableIndexColumns(PhoenixTestBuilder.DDLDefaults.TABLE_INDEX_COLUMNS);
        indexOptions.setTableIncludeColumns(PhoenixTestBuilder.DDLDefaults.TABLE_INCLUDE_COLUMNS);
        indexOptions.setIndexProps("TTL = " + DEFAULT_TEST_TTL_VALUE);
        SchemaBuilder schemaBuilder = createTableWithTTL(false, true, indexOptions);
        try {
            schemaBuilder.build();
            Assert.fail();
        } catch (SQLException sqe) {
            Assert.assertEquals(SQLExceptionCode.CANNOT_SET_OR_ALTER_PROPERTY_FOR_INDEX.getErrorCode()
                    ,sqe.getErrorCode());
        }
    }

    /**
     * Test TTL defined at Table level for 2 level View Hierarchy, TTL value should be passed down
     * to view at Global Level as well as Child Views of Global View.
     * @throws Exception
     */

    @Test
    public void testTTLInHierarchyDefinedAtTableLevel() throws Exception {
        //Create Table with TTL as table option.
        SchemaBuilder schemaBuilder = createTableWithTTL(true, false, null);
        //Create Global View on top of table without defining TTL.
        schemaBuilder = createGlobalViewOnTable(schemaBuilder, false, false, true);
        //Create Tenant View on top of Global View without defining TTL.
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, false, false, true);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String globalView = schemaBuilder.getEntityGlobalViewName();
        String childView = schemaBuilder.getEntityTenantViewName();

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
            Connection tenantConnection = DriverManager.getConnection(
                    getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, DEFAULT_TEST_TTL_VALUE);
            assertTTLForGivenEntity(globalConnection, globalView, DEFAULT_TEST_TTL_VALUE);
            assertTTLForGivenEntity(tenantConnection, childView, DEFAULT_TEST_TTL_VALUE);
        }
    }

    @Test
    public void testSettingTTLForViewsInHierarchyWithTTLAtTableLevel() throws Exception {
        SchemaBuilder schemaBuilder = createTableWithTTL(true, false, null);
        schemaBuilder = createGlobalViewOnTable(schemaBuilder, true,false,true);
        try {
            schemaBuilder.build();
            Assert.fail();
        } catch (SQLException sqe) {
            Assert.assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode()
                    ,sqe.getErrorCode());
        }

        schemaBuilder = createTableWithTTL(true, false, null);
        schemaBuilder = createGlobalViewOnTable(schemaBuilder, false,false,true);
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, true, false, true);
        try {
            schemaBuilder.build();
            Assert.fail();
        } catch (SQLException sqe) {
            Assert.assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode()
                    ,sqe.getErrorCode());
        }
    }

    @Test
    public void testAlteringTTLForViewsInHierarchyWithTTLAtTableLevel() throws Exception {
        SchemaBuilder schemaBuilder = createTableWithTTL(true, false, null);
        schemaBuilder = createGlobalViewOnTable(schemaBuilder, false, false, true);
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, false, false, true);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String globalViewName = schemaBuilder.getEntityGlobalViewName();
        String childViewName = schemaBuilder.getEntityTenantViewName();

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, DEFAULT_TEST_TTL_VALUE);
            assertTTLForGivenEntity(globalConnection, globalViewName, DEFAULT_TEST_TTL_VALUE);
            assertTTLForGivenEntity(tenantConnection, childViewName, DEFAULT_TEST_TTL_VALUE);

            //Altering TTL at globalView should fail
            String dml = "ALTER VIEW " + globalViewName + " SET TTL = " + ALTER_TEST_TTL_VALUE;
            try {
                globalConnection.createStatement().execute(dml);
                Assert.fail();
            } catch (SQLException sqe) {
                Assert.assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode()
                        ,sqe.getErrorCode());
            }

            //Altering TTL at tenantView should fail
            dml = "ALTER VIEW " + childViewName + " SET TTL = " + ALTER_TEST_TTL_VALUE;
            try {
                tenantConnection.createStatement().execute(dml);
                Assert.fail();
            } catch (SQLException sqe) {
                Assert.assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode()
                        ,sqe.getErrorCode());
            }
        }


    }


    @Test
    public void testTTLInHierarchyDefinedAtTableLevelWithIndex() throws Exception {
        //Create Table with TTL as table option and also build index.
        SchemaBuilder schemaBuilder = createTableWithTTL(true, true, null);
        //Create Global View on top of table without defining TTL but don't extend PK as we are building index on table.
        schemaBuilder = createGlobalViewOnTable(schemaBuilder, false, false, false);
        //Create Tenant View on top of Global View without defining TTL but don't extend PK as we are building index on table.
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, false, false, false);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String indexName = schemaBuilder.getEntityTableIndexName();
        String globalView = schemaBuilder.getEntityGlobalViewName();
        String childView = schemaBuilder.getEntityTenantViewName();

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, DEFAULT_TEST_TTL_VALUE);
            assertTTLForIndexName(globalConnection, indexName, DEFAULT_TEST_TTL_VALUE);
            //Assert TTL for index by getting PTable from table's PTable
            assertTTLForIndexFromParentPTable(globalConnection, schemaBuilder.getEntityTableName(), DEFAULT_TEST_TTL_VALUE);
            assertTTLForGivenEntity(globalConnection, globalView, DEFAULT_TEST_TTL_VALUE);
            assertTTLForGivenEntity(tenantConnection, childView, DEFAULT_TEST_TTL_VALUE);
        }
    }

    @Test
    public void testTTLInHierarchyDefinedAtTableLevelWithIndexAtGlobalLevel() throws Exception {
        //Create Table with TTL as table option.
        SchemaBuilder schemaBuilder = createTableWithTTL(true, false,null);
        //Create Global View on top of table without defining TTL and create index on top of view.
        schemaBuilder = createGlobalViewOnTable(schemaBuilder, false, true,true);
        //Create Tenant View on top of Global View without defining TTL but don't extend PK as we are building index on global view.
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, false, false, false);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();

        String globalView = schemaBuilder.getEntityGlobalViewName();
        String globalViewIndexName = schemaBuilder.getEntityGlobalViewIndexName();

        String childView = schemaBuilder.getEntityTenantViewName();

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, DEFAULT_TEST_TTL_VALUE);

            assertTTLForGivenEntity(globalConnection, globalView, DEFAULT_TEST_TTL_VALUE);
            assertTTLForIndexName(globalConnection, globalViewIndexName, DEFAULT_TEST_TTL_VALUE);
            assertTTLForIndexFromParentPTable(globalConnection, globalView, DEFAULT_TEST_TTL_VALUE);

            assertTTLForGivenEntity(tenantConnection, childView, DEFAULT_TEST_TTL_VALUE);
        }
    }

    @Test
    public void testTTLInHierarchyDefinedAtTableLevelWithIndexAtTenantLevel() throws Exception {
        //Create Table with TTL as table option.
        SchemaBuilder schemaBuilder = createTableWithTTL(true, false,null);
        //Create Global View on top of table without defining TTL.
        schemaBuilder = createGlobalViewOnTable(schemaBuilder, false, false,true);
        //Create Tenant View on top of Global View without defining TTL and create index on top of view
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, false, isMultiTenant, true);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String globalView = schemaBuilder.getEntityGlobalViewName();
        String childView = schemaBuilder.getEntityTenantViewName();
        String childViewIndexName = isMultiTenant ? schemaBuilder.getEntityTenantViewIndexName() : SKIP_ASSERT;

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, DEFAULT_TEST_TTL_VALUE);

            assertTTLForGivenEntity(globalConnection, globalView, DEFAULT_TEST_TTL_VALUE);

            assertTTLForGivenEntity(tenantConnection, childView, DEFAULT_TEST_TTL_VALUE);
            assertTTLForIndexName(tenantConnection, childViewIndexName, DEFAULT_TEST_TTL_VALUE);
            assertTTLForIndexFromParentPTable(tenantConnection, childView, DEFAULT_TEST_TTL_VALUE);

        }
    }

    /**
     * Test with TTL defined at Global View Level
     */

    @Test
    public void testTTLHierarchyDefinedAtGlobalViewLevel() throws Exception {
        SchemaBuilder schemaBuilder = createGlobalViewOnTable(true, true);
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, false, false,false);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String globalViewName = schemaBuilder.getEntityGlobalViewName();
        String globalViewIndexName = schemaBuilder.getEntityGlobalViewIndexName();
        String childViewName = schemaBuilder.getEntityTenantViewName();

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
            assertTTLForIndexName(globalConnection, globalViewIndexName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
            assertTTLForIndexFromParentPTable(globalConnection, globalViewName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
            assertTTLForGivenEntity(tenantConnection, childViewName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
        }
    }

    @Test
    public void testTTLHierarchyDefinedAtGlobalViewLevelWithTenantIndex() throws Exception {
        SchemaBuilder schemaBuilder = createGlobalViewOnTable(true, false);
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, false, isMultiTenant,true);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String globalViewName = schemaBuilder.getEntityGlobalViewName();
        String childViewName = schemaBuilder.getEntityTenantViewName();
        String childViewIndexName = isMultiTenant ? schemaBuilder.getEntityTenantViewIndexName() : SKIP_ASSERT;

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
            assertTTLForGivenEntity(tenantConnection, childViewName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
            assertTTLForIndexName(tenantConnection, childViewIndexName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
            assertTTLForIndexFromParentPTable(tenantConnection, childViewName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
        }
    }

    @Test
    public void testSettingTTLAtTenantViewLevelWithTTLDefinedAtGlobalView() throws Exception {
        SchemaBuilder schemaBuilder = createGlobalViewOnTable(true, false);
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, true, false,true);
        try {
            schemaBuilder.build();
            Assert.fail();
        } catch (SQLException sqe) {
        Assert.assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode()
                ,sqe.getErrorCode());
        }

        schemaBuilder = createGlobalViewOnTable(true, false);
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, false, false,true);
        schemaBuilder.build();
        PTable table = schemaBuilder.getBaseTable();
        String globalViewName = schemaBuilder.getEntityGlobalViewName();
        String childViewName = schemaBuilder.getEntityTenantViewName();

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {

            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
            assertTTLForGivenEntity(tenantConnection, childViewName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);


        }
    }

    @Test
    public void testAlteringTTLAtDifferentLevelsWithTTLDefinedAtGlobalView() throws Exception {
        SchemaBuilder schemaBuilder = createGlobalViewOnTable(true, false);
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, false, false,true);
        schemaBuilder.build();
        PTable table = schemaBuilder.getBaseTable();
        String tableName = schemaBuilder.getEntityTableName();
        String globalViewName = schemaBuilder.getEntityGlobalViewName();
        String childViewName = schemaBuilder.getEntityTenantViewName();

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
            assertTTLForGivenEntity(tenantConnection, childViewName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);

            //Altering TTL at tenantView should fail
            String dml = "ALTER VIEW " + childViewName + " SET TTL = " + ALTER_TEST_TTL_VALUE;
            try {
                tenantConnection.createStatement().execute(dml);
                Assert.fail();
            } catch (SQLException sqe) {
                Assert.assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode()
                        ,sqe.getErrorCode());
            }

            //Altering TTL at Table should fail
            dml = "ALTER TABLE " + tableName + " SET TTL = " + ALTER_TEST_TTL_VALUE;
            try {
                globalConnection.createStatement().execute(dml);
                Assert.fail();
            } catch (SQLException sqe) {
                Assert.assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode()
                        ,sqe.getErrorCode());
            }

        }
    }

    @Test
    public void testTTLHierarchyDefinedAtTenantViewLevel() throws Exception {
        SchemaBuilder schemaBuilder = createTenantViewOnGlobalView(true, isMultiTenant);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String globalView = schemaBuilder.getEntityGlobalViewName();
        String childView = schemaBuilder.getEntityTenantViewName();
        String childViewIndex = isMultiTenant ? schemaBuilder.getEntityTenantViewIndexName() : SKIP_ASSERT;

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalView, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(tenantConnection, childView, DEFAULT_TEST_TTL_VALUE_AT_TENANT);
            assertTTLForIndexName(tenantConnection, childViewIndex, DEFAULT_TEST_TTL_VALUE_AT_TENANT);
            assertTTLForIndexFromParentPTable(tenantConnection, childView, DEFAULT_TEST_TTL_VALUE_AT_TENANT);
        }
    }

    @Test
    public void testSettingAndAlteringTTLInHierarchyAboveOfTenantViewWithTTLDefined() throws Exception {
        SchemaBuilder schemaBuilder = createTenantViewOnGlobalView(true, isMultiTenant);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String tableName = schemaBuilder.getEntityTableName();
        String globalView = schemaBuilder.getEntityGlobalViewName();
        String childView = schemaBuilder.getEntityTenantViewName();
        String childViewIndex = isMultiTenant ? schemaBuilder.getEntityTenantViewIndexName() : SKIP_ASSERT;

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalView, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(tenantConnection, childView, DEFAULT_TEST_TTL_VALUE_AT_TENANT);
            assertTTLForIndexName(tenantConnection, childViewIndex, DEFAULT_TEST_TTL_VALUE_AT_TENANT);
            assertTTLForIndexFromParentPTable(tenantConnection, childView, DEFAULT_TEST_TTL_VALUE_AT_TENANT);

            //Altering TTL at globalView should fail
            String dml = "ALTER VIEW " + globalView + " SET TTL = " + ALTER_TEST_TTL_VALUE;
            try {
                globalConnection.createStatement().execute(dml);
                Assert.fail();
            } catch (SQLException sqe) {
                Assert.assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode()
                        ,sqe.getErrorCode());
            }

            //Altering TTL at Table should fail
            dml = "ALTER TABLE " + tableName + " SET TTL = " + ALTER_TEST_TTL_VALUE;
            try {
                globalConnection.createStatement().execute(dml);
                Assert.fail();
            } catch (SQLException sqe) {
                Assert.assertEquals(SQLExceptionCode.TTL_ALREADY_DEFINED_IN_HIERARCHY.getErrorCode()
                        ,sqe.getErrorCode());
            }

        }
    }


    @Test
    public void testAlteringTTLFromTableToLevel1() throws Exception {
        SchemaBuilder schemaBuilder = createTableWithTTL(true, false, null);
        schemaBuilder = createGlobalViewOnTable(schemaBuilder, false, false, true);
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, false, isMultiTenant, true);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String schemaName = schemaBuilder.getTableOptions().getSchemaName();
        String tableName = schemaBuilder.getEntityTableName();
        String globalViewName = schemaBuilder.getEntityGlobalViewName();
        String tenantViewName = schemaBuilder.getEntityTenantViewName();
        String tenantViewIndexName = isMultiTenant ? schemaBuilder.getEntityTenantViewIndexName() : SKIP_ASSERT;

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, DEFAULT_TEST_TTL_VALUE);
            assertTTLForGivenEntity(globalConnection, globalViewName, DEFAULT_TEST_TTL_VALUE);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, DEFAULT_TEST_TTL_VALUE);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, DEFAULT_TEST_TTL_VALUE);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, DEFAULT_TEST_TTL_VALUE);

            //Alter TTL to NONE
            String dml = "ALTER TABLE " + tableName + " SET TTL = NONE";
            globalConnection.createStatement().execute(dml);

            //Clearing cache as MetaDataCaching is not there for TTL usecase
            globalConnection.unwrap(PhoenixConnection.class).getQueryServices().clearCache();

            assertTTLForGivenEntity(globalConnection, tableName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, TTL_NOT_DEFINED);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, TTL_NOT_DEFINED);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, TTL_NOT_DEFINED);

            //Alter TTL From not defined to something at Global View Level
            dml = "ALTER VIEW " + globalViewName + " SET TTL = " + ALTER_TEST_TTL_VALUE;
            globalConnection.createStatement().execute(dml);

            //Clearing cache again
            globalConnection.unwrap(PhoenixConnection.class).getQueryServices().clearCache();

            assertTTLForGivenEntity(globalConnection, tableName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, ALTER_TEST_TTL_VALUE);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, ALTER_TEST_TTL_VALUE);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, ALTER_TEST_TTL_VALUE);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, ALTER_TEST_TTL_VALUE);

        }

    }

    @Test
    public void testAlteringTTLFromTableToLevel2() throws Exception {
        SchemaBuilder schemaBuilder = createTableWithTTL(true, false, null);
        schemaBuilder = createGlobalViewOnTable(schemaBuilder, false, false, true);
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, false, isMultiTenant, true);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String schemaName = schemaBuilder.getTableOptions().getSchemaName();
        String tableName = schemaBuilder.getEntityTableName();
        String globalViewName = schemaBuilder.getEntityGlobalViewName();
        String tenantViewName = schemaBuilder.getEntityTenantViewName();
        String tenantViewIndexName = isMultiTenant ? schemaBuilder.getEntityTenantViewIndexName() : SKIP_ASSERT;

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, DEFAULT_TEST_TTL_VALUE);
            assertTTLForGivenEntity(globalConnection, globalViewName, DEFAULT_TEST_TTL_VALUE);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, DEFAULT_TEST_TTL_VALUE);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, DEFAULT_TEST_TTL_VALUE);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, DEFAULT_TEST_TTL_VALUE);

            //Alter TTL to NONE
            String dml = "ALTER TABLE " + tableName + " SET TTL = NONE";
            globalConnection.createStatement().execute(dml);

            //Clearing cache as MetaDataCaching is not there for TTL usecase
            globalConnection.unwrap(PhoenixConnection.class).getQueryServices().clearCache();

            assertTTLForGivenEntity(globalConnection, tableName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, TTL_NOT_DEFINED);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, TTL_NOT_DEFINED);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, TTL_NOT_DEFINED);

            //Alter TTL From not defined to something at Global View Level
            dml = "ALTER VIEW " + tenantViewName + " SET TTL = " + ALTER_TEST_TTL_VALUE;
            tenantConnection.createStatement().execute(dml);

            //Clearing cache again
            globalConnection.unwrap(PhoenixConnection.class).getQueryServices().clearCache();

            assertTTLForGivenEntity(globalConnection, tableName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, ALTER_TEST_TTL_VALUE);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, ALTER_TEST_TTL_VALUE);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, ALTER_TEST_TTL_VALUE);

        }

    }

    @Test
    public void testAlteringTTLFromLevel1ToLevel2() throws Exception {
        SchemaBuilder schemaBuilder = createTableWithTTL(false, false, null);
        schemaBuilder = createGlobalViewOnTable(schemaBuilder, true, false, true);
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, false, isMultiTenant, true);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String schemaName = schemaBuilder.getTableOptions().getSchemaName();
        String tableName = schemaBuilder.getEntityTableName();
        String globalViewName = schemaBuilder.getEntityGlobalViewName();
        String tenantViewName = schemaBuilder.getEntityTenantViewName();
        String tenantViewIndexName = isMultiTenant ? schemaBuilder.getEntityTenantViewIndexName() : SKIP_ASSERT;

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, DEFAULT_TEST_TTL_VALUE_AT_GLOBAL);

            //Alter TTL to NONE
            String dml = "ALTER VIEW " + globalViewName + " SET TTL = NONE";
            globalConnection.createStatement().execute(dml);

            //Clearing cache as MetaDataCaching is not there for TTL usecase
            globalConnection.unwrap(PhoenixConnection.class).getQueryServices().clearCache();

            assertTTLForGivenEntity(globalConnection, tableName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, TTL_NOT_DEFINED);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, TTL_NOT_DEFINED);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, TTL_NOT_DEFINED);

            //Alter TTL From not defined to something at Global View Level
            dml = "ALTER VIEW " + tenantViewName + " SET TTL = " + ALTER_TEST_TTL_VALUE;
            tenantConnection.createStatement().execute(dml);

            //Clearing cache again
            globalConnection.unwrap(PhoenixConnection.class).getQueryServices().clearCache();

            assertTTLForGivenEntity(globalConnection, tableName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, ALTER_TEST_TTL_VALUE);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, ALTER_TEST_TTL_VALUE);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, ALTER_TEST_TTL_VALUE);

        }

    }

    @Test
    public void testAlteringTTLFromLevel2ToTable() throws Exception {
        SchemaBuilder schemaBuilder = createTableWithTTL(false, false, null);
        schemaBuilder = createGlobalViewOnTable(schemaBuilder, false, false, true);
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, true, isMultiTenant, true);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String schemaName = schemaBuilder.getTableOptions().getSchemaName();
        String tableName = schemaBuilder.getEntityTableName();
        String globalViewName = schemaBuilder.getEntityGlobalViewName();
        String tenantViewName = schemaBuilder.getEntityTenantViewName();
        String tenantViewIndexName = isMultiTenant ? schemaBuilder.getEntityTenantViewIndexName() : SKIP_ASSERT;

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, DEFAULT_TEST_TTL_VALUE_AT_TENANT);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, DEFAULT_TEST_TTL_VALUE_AT_TENANT);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, DEFAULT_TEST_TTL_VALUE_AT_TENANT);

            //Alter TTL to NONE
            String dml = "ALTER VIEW " + tenantViewName + " SET TTL = NONE";
            tenantConnection.createStatement().execute(dml);

            //Clearing cache as MetaDataCaching is not there for TTL usecase
            globalConnection.unwrap(PhoenixConnection.class).getQueryServices().clearCache();

            assertTTLForGivenEntity(globalConnection, tableName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, TTL_NOT_DEFINED);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, TTL_NOT_DEFINED);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, TTL_NOT_DEFINED);

            //Alter TTL From not defined to something at Global View Level
            dml = "ALTER TABLE " + tableName + " SET TTL = " + ALTER_TEST_TTL_VALUE;
            globalConnection.createStatement().execute(dml);

            //Clearing cache again
            globalConnection.unwrap(PhoenixConnection.class).getQueryServices().clearCache();

            assertTTLForGivenEntity(globalConnection, tableName, ALTER_TEST_TTL_VALUE);
            assertTTLForGivenEntity(globalConnection, globalViewName, ALTER_TEST_TTL_VALUE);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, ALTER_TEST_TTL_VALUE);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, ALTER_TEST_TTL_VALUE);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, ALTER_TEST_TTL_VALUE);

        }

    }


    @Test
    public void testAlteringTTLFromLevel2ToLevel1() throws Exception {
        SchemaBuilder schemaBuilder = createTableWithTTL(false, false, null);
        schemaBuilder = createGlobalViewOnTable(schemaBuilder, false, false, true);
        schemaBuilder = createTenantViewOnTableOrGlobalView(schemaBuilder, true, isMultiTenant, true);
        schemaBuilder.build();

        PTable table = schemaBuilder.getBaseTable();
        String schemaName = schemaBuilder.getTableOptions().getSchemaName();
        String tableName = schemaBuilder.getEntityTableName();
        String globalViewName = schemaBuilder.getEntityGlobalViewName();
        String tenantViewName = schemaBuilder.getEntityTenantViewName();
        String tenantViewIndexName = isMultiTenant ? schemaBuilder.getEntityTenantViewIndexName() : SKIP_ASSERT;

        try (Connection globalConnection = DriverManager.getConnection(getUrl());
             Connection tenantConnection = DriverManager.getConnection(
                     getUrl() + ';' + TENANT_ID_ATTRIB + '=' + schemaBuilder.getDataOptions().getTenantId())) {
            //All level entities should have TTL value equal to TTL defined at table level
            assertTTLForGivenPTable(table, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, DEFAULT_TEST_TTL_VALUE_AT_TENANT);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, DEFAULT_TEST_TTL_VALUE_AT_TENANT);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, DEFAULT_TEST_TTL_VALUE_AT_TENANT);

            //Alter TTL to NONE
            String dml = "ALTER VIEW " + tenantViewName + " SET TTL = NONE";
            tenantConnection.createStatement().execute(dml);

            //Clearing cache as MetaDataCaching is not there for TTL usecase
            //Clearing cache as MetaDataCaching is not there for TTL usecase
            globalConnection.unwrap(PhoenixConnection.class).getQueryServices().clearCache();

            assertTTLForGivenEntity(globalConnection, tableName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, TTL_NOT_DEFINED);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, TTL_NOT_DEFINED);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, TTL_NOT_DEFINED);

            //Alter TTL From not defined to something at Global View Level
            dml = "ALTER VIEW " + globalViewName + " SET TTL = " + ALTER_TEST_TTL_VALUE;
            globalConnection.createStatement().execute(dml);

            //Clearing cache again
            //Clearing cache as MetaDataCaching is not there for TTL usecase
            globalConnection.unwrap(PhoenixConnection.class).getQueryServices().clearCache();

            assertTTLForGivenEntity(globalConnection, tableName, TTL_NOT_DEFINED);
            assertTTLForGivenEntity(globalConnection, globalViewName, ALTER_TEST_TTL_VALUE);
            assertTTLForGivenEntity(tenantConnection, tenantViewName, ALTER_TEST_TTL_VALUE);
            assertTTLForIndexName(tenantConnection, tenantViewIndexName, ALTER_TEST_TTL_VALUE);
            assertTTLForIndexFromParentPTable(tenantConnection, tenantViewName, ALTER_TEST_TTL_VALUE);

        }

    }




}
