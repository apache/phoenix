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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.LINK_TYPE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_TYPE;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.mapreduce.OrphanViewTool;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class OrphanViewToolIT extends BaseOwnClusterIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrphanViewToolIT.class);

    private final boolean isMultiTenant;

    private static final long fanout = 2;
    private static final long childCount = fanout;
    private static final long grandChildCount = fanout * fanout;
    private static final long grandGrandChildCount = fanout * fanout * fanout;

    private static final String filePath = "/tmp/";
    private static final String viewFileName = "/tmp/" + OrphanViewTool.fileName[OrphanViewTool.VIEW];
    private static final String physicalLinkFileName = "/tmp/" + OrphanViewTool.fileName[OrphanViewTool.PHYSICAL_TABLE_LINK];
    private static final String parentLinkFileName = "/tmp/" + OrphanViewTool.fileName[OrphanViewTool.PARENT_TABLE_LINK];
    private static final String childLinkFileName = "/tmp/" + OrphanViewTool.fileName[OrphanViewTool.CHILD_TABLE_LINK];

    protected static String SCHEMA1 = "SCHEMA1";
    protected static String SCHEMA2 = "SCHEMA2";
    protected static String SCHEMA3 = "SCHEMA3";
    protected static String SCHEMA4 = "SCHEMA4";

    private final String TENANT_SPECIFIC_URL = getUrl() + ';' + TENANT_ID_ATTRIB + "=tenant";

    private static final String createBaseTableFirstPartDDL = "CREATE TABLE IF NOT EXISTS %s";
    private static final String createBaseTableSecondPartDDL = "(%s PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR " +
            " CONSTRAINT NAME_PK PRIMARY KEY (%s PK2)) %s";
    private static final String createBaseTableIndexDDL = "CREATE INDEX %s ON %s (V1)";
    private static final String deleteTableRows = "DELETE FROM " + SYSTEM_CATALOG_NAME +
            " WHERE " + TABLE_SCHEM + " %s AND " +
            TABLE_TYPE + " = '" + PTableType.TABLE.getSerializedValue() + "'";

    private static final String createViewDDL = "CREATE VIEW %s AS SELECT * FROM %s";
    private static final String countAllViewsQuery = "SELECT COUNT(*) FROM " + SYSTEM_CATALOG_NAME +
            " WHERE " + TABLE_TYPE + " = '" + PTableType.VIEW.getSerializedValue() + "'";
    private static final String countViewsQuery = "SELECT COUNT(*) FROM " + SYSTEM_CATALOG_NAME +
            " WHERE " + TABLE_SCHEM + " %s AND " +
            TABLE_TYPE + " = '" + PTableType.VIEW.getSerializedValue() + "'";
    private static final String deleteViewRows = "DELETE FROM " + SYSTEM_CATALOG_NAME +
            " WHERE " + TABLE_SCHEM + " %s AND " +
            TABLE_TYPE + " = '" + PTableType.VIEW.getSerializedValue() + "'";

    private static final String countChildLinksQuery = "SELECT COUNT(*) FROM " + SYSTEM_CHILD_LINK_NAME +
            " WHERE " + TABLE_SCHEM + " %s AND " +
            LINK_TYPE + " = " + PTable.LinkType.CHILD_TABLE.getSerializedValue();
    private static final String deleteChildLinks = "DELETE FROM " + SYSTEM_CHILD_LINK_NAME +
            " WHERE " + TABLE_SCHEM + " %s AND " +
            LINK_TYPE + " = " + PTable.LinkType.CHILD_TABLE.getSerializedValue();

    private static final String countParentLinksQuery = "SELECT COUNT(*) FROM " + SYSTEM_CATALOG_NAME +
            " WHERE " + TABLE_SCHEM + " %s AND " +
            LINK_TYPE + " = " + PTable.LinkType.PARENT_TABLE.getSerializedValue();
    private static final String deleteParentLinks = "DELETE FROM " + SYSTEM_CATALOG_NAME +
            " WHERE " + TABLE_SCHEM + " %s AND " +
            LINK_TYPE + " = " + PTable.LinkType.PARENT_TABLE.getSerializedValue();

    private static final String countPhysicalLinksQuery = "SELECT COUNT(*) FROM " + SYSTEM_CATALOG_NAME +
            " WHERE " + TABLE_SCHEM + " %s AND " +
            LINK_TYPE + " = " + PTable.LinkType.PHYSICAL_TABLE.getSerializedValue();
    private static final String deletePhysicalLinks = "DELETE FROM " + SYSTEM_CATALOG_NAME +
            " WHERE " + TABLE_SCHEM + " %s AND " +
            LINK_TYPE + " = " + PTable.LinkType.PHYSICAL_TABLE.getSerializedValue();

    private static final String deleteSchemaRows = "DELETE FROM %s WHERE " + TABLE_SCHEM + " %s";

    public OrphanViewToolIT(boolean isMultiTenant) {
        this.isMultiTenant = isMultiTenant;
    }

    @Parameters(name="OrphanViewToolIT_multiTenant={0}") // name is used by failsafe as file name in reports
    public static synchronized Collection<Boolean> data() {
        return Arrays.asList(false, true);
    }

    @BeforeClass
    public static synchronized void doSetup() throws Exception {
        setUpTestDriver(ReadOnlyProps.EMPTY_PROPS);
    }

    @AfterClass
    public static synchronized void cleanUp() throws Exception {
        boolean refCountLeaked = isAnyStoreRefCountLeaked();
        for (int i = OrphanViewTool.VIEW; i < OrphanViewTool.ORPHAN_TYPE_COUNT; i++) {
            File file = new File(filePath + OrphanViewTool.fileName[i]);
            if (file.exists()) {
                file.delete();
            }
        }
        assertFalse("refCount leaked", refCountLeaked);
    }

    private String generateDDL(String format) {
        return generateDDL("", format);
    }

    private String generateDDL(String options, String format) {
        StringBuilder optionsBuilder = new StringBuilder(options);
        if (isMultiTenant) {
            if (optionsBuilder.length()!=0)
                optionsBuilder.append(",");
            optionsBuilder.append("MULTI_TENANT=true");
        }
        return String.format(format, isMultiTenant ? "TENANT_ID VARCHAR NOT NULL, " : "",
                isMultiTenant ? "TENANT_ID, " : "", optionsBuilder.toString());
    }

    private void deleteRowsFrom(Connection connection, String systemTableName, String baseTableSchema,
                                String childViewSchemaName,
                                String grandchildViewSchemaName, String grandGrandChildViewSchemaName)
            throws SQLException {
        connection.createStatement().execute(String.format(deleteSchemaRows, systemTableName,
                baseTableSchema == null ? "IS NULL" : " = '" + baseTableSchema + "'"));
        connection.createStatement().execute(String.format(deleteSchemaRows, systemTableName,
                childViewSchemaName == null ? "IS NULL" : " = '" + childViewSchemaName + "'"));
        connection.createStatement().execute(String.format(deleteSchemaRows, systemTableName,
                grandchildViewSchemaName == null ? "IS NULL" : " = '" + grandchildViewSchemaName + "'"));
        connection.createStatement().execute(String.format(deleteSchemaRows, systemTableName,
                grandGrandChildViewSchemaName == null ? "IS NULL" : " = '" + grandGrandChildViewSchemaName + "'"));
    }

    private void deleteAllRows(Connection connection, String baseTableSchema,
                            String childViewSchemaName,
                            String grandchildViewSchemaName, String grandGrandChildViewSchemaName) throws SQLException {
        deleteRowsFrom(connection, SYSTEM_CATALOG_NAME, baseTableSchema, childViewSchemaName,
                grandchildViewSchemaName, grandGrandChildViewSchemaName);
        deleteRowsFrom(connection, SYSTEM_CHILD_LINK_NAME, baseTableSchema, childViewSchemaName,
                grandchildViewSchemaName, grandGrandChildViewSchemaName);
        connection.commit();
    }

    private void createBaseTableIndexAndViews(Connection baseTableConnection, String baseTableFullName,
                                              Connection viewConnection, String childViewSchemaName,
                                              String grandchildViewSchemaName, String grandGrandChildViewSchemaName)
            throws SQLException {
        baseTableConnection.createStatement().execute(generateDDL(String.format(createBaseTableFirstPartDDL,
                baseTableFullName) + createBaseTableSecondPartDDL));
        baseTableConnection.createStatement().execute(String.format(createBaseTableIndexDDL,
                generateUniqueName(), baseTableFullName));
        // Create a view tree (i.e., tree of views) with depth of 3
        for (int i = 0; i < fanout; i++) {
            String childView = SchemaUtil.getTableName(childViewSchemaName, generateUniqueName());
            viewConnection.createStatement().execute(String.format(createViewDDL, childView, baseTableFullName));
            for (int j = 0; j < fanout; j++) {
                String grandchildView = SchemaUtil.getTableName(grandchildViewSchemaName, generateUniqueName());
                viewConnection.createStatement().execute(String.format(createViewDDL, grandchildView, childView));
                for (int k = 0; k < fanout; k++) {
                    viewConnection.createStatement().execute(String.format(createViewDDL,
                            SchemaUtil.getTableName(grandGrandChildViewSchemaName, generateUniqueName()),
                            grandchildView));
                }
            }
        }
    }

    private void verifyLineCount(String fileName, long lineCount) throws IOException {
        LineNumberReader reader = new LineNumberReader(new FileReader(fileName));
        while (reader.readLine() != null) {
        }
        int count = reader.getLineNumber();
        if (count != lineCount)
            LOGGER.debug(count + " != " + lineCount);
        assertTrue(count == lineCount);
        reader.close();
    }

    private void verifyCountQuery(Connection connection, String query, String schemaName, long count)
            throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery(String.format(query,
                schemaName == null ? "IS NULL" : "= '" + schemaName + "'"));
        assertTrue(rs.next());
        assertTrue(rs.getLong(1) == count);
    }

    @Test
    public void testCreateTableAndViews() throws Exception {
        String baseTableName = generateUniqueName();
        String baseTableFullName = SchemaUtil.getTableName(SCHEMA1, baseTableName);
        try (Connection connection = DriverManager.getConnection(getUrl());
             Connection viewConnection =
                     isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL) : connection) {
            createBaseTableIndexAndViews(connection, baseTableFullName, viewConnection, SCHEMA2, SCHEMA3, SCHEMA4);
            // Run the orphan view tool to drop orphan views but no view should be dropped
            runOrphanViewTool(true, false, true, false);
            verifyOrphanFileLineCounts(0, 0, 0, 0);
            // Verify that the views we have created are still in the system catalog table
            ResultSet rs = connection.createStatement().executeQuery(countAllViewsQuery);
            assertTrue(rs.next());
            assertTrue(rs.getLong(1) == childCount + grandChildCount + grandGrandChildCount);
            deleteAllRows(connection, SCHEMA1, SCHEMA2, SCHEMA3, SCHEMA4);
        }
    }

    private void verifyNoChildLink(Connection connection, String viewSchemaName) throws Exception {
        // Verify that there there is no link in the system child link table
        verifyCountQuery(connection, countChildLinksQuery, viewSchemaName, 0);
    }

    private void verifyNoViewNoLinkInSystemCatalog(Connection connection, String viewSchemaName) throws Exception {
        // Verify that the views and links have been removed from the system catalog table
        verifyCountQuery(connection, countViewsQuery, viewSchemaName, 0);
        verifyCountQuery(connection, countParentLinksQuery, viewSchemaName, 0);
        verifyCountQuery(connection, countPhysicalLinksQuery, viewSchemaName, 0);
    }

    private void verifyOrphanFileLineCounts(long viewCount, long parentLinkCount,
                                            long physicalLinkCount, long childLinkCount)
        throws Exception {
        verifyLineCount(viewFileName, viewCount);
        verifyLineCount(parentLinkFileName, parentLinkCount);
        verifyLineCount(physicalLinkFileName, physicalLinkCount);
        verifyLineCount(childLinkFileName, childLinkCount);
    }
    private void executeDeleteQuery(Connection connection, String deleteQuery, String schemaName) throws Exception {
        connection.createStatement().execute(String.format(deleteQuery,
                schemaName == null ? "IS NULL" : "= '" + schemaName + "'"));
        connection.commit();
    }

    @Test
    public void testDeleteBaseTableRows() throws Exception {
        String baseTableName = generateUniqueName();
        String baseTableFullName = SchemaUtil.getTableName(SCHEMA1, baseTableName);
        try (Connection connection = DriverManager.getConnection(getUrl());
             Connection viewConnection =
                     isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL) : connection) {
            createBaseTableIndexAndViews(connection, baseTableFullName, viewConnection, SCHEMA2, SCHEMA2, SCHEMA2);
            // Delete the base table row from the system catalog
            executeDeleteQuery(connection, deleteTableRows, SCHEMA1);
            // Verify that the views we have created are still in the system catalog table
            ResultSet rs = connection.createStatement().executeQuery(countAllViewsQuery);
            assertTrue(rs.next());
            assertTrue(rs.getLong(1) == childCount + grandChildCount + grandGrandChildCount);
            // Run the orphan view tool to identify orphan views
            runOrphanViewTool(false, true, true, false);
            verifyOrphanFileLineCounts(childCount + grandChildCount + grandGrandChildCount,
                    0,
                    childCount + grandChildCount + grandGrandChildCount,
                    childCount);
            // Verify that orphan views have not yet dropped as we just identified them
            rs = connection.createStatement().executeQuery(countAllViewsQuery);
            assertTrue(rs.next());
            assertTrue(rs.getLong(1) == childCount + grandChildCount + grandGrandChildCount);
            // Drop the previously identified orphan views
            runOrphanViewTool(true, false, false, true);
            // Verify that the orphan views and links have been removed from the system catalog table
            verifyNoViewNoLinkInSystemCatalog(connection, SCHEMA2);
            // Verify that there there is no link in the system child link table
            verifyNoChildLink(connection, SCHEMA1);
            deleteAllRows(connection, SCHEMA1, SCHEMA2, SCHEMA3, SCHEMA4);
        }
    }

    @Test
    public void testDeleteChildViewRows() throws Exception {
        String baseTableName = generateUniqueName();
        String baseTableFullName = SchemaUtil.getTableName(SCHEMA1, baseTableName);
        try (Connection connection = DriverManager.getConnection(getUrl());
             Connection viewConnection =
                     isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL) : connection) {
            createBaseTableIndexAndViews(connection, baseTableFullName, viewConnection, null, SCHEMA3, SCHEMA3);
            // Delete the rows of the immediate child views of the base table from the system catalog
            executeDeleteQuery(connection, deleteViewRows, null);
            // Verify that the other views we have created are still in the system catalog table
            verifyCountQuery(connection, countViewsQuery, SCHEMA3, grandChildCount + grandGrandChildCount);
            // Run the orphan view tool to clean up orphan views
            runOrphanViewTool(true, false, true, false);
            // Verify that the tool attempt to remove all orphan views and links
            verifyOrphanFileLineCounts(grandChildCount + grandGrandChildCount,
                    grandChildCount,
                    childCount,
                    childCount + grandChildCount);
            // Verify that all views and links records of the views are removed from the system catalog table
            verifyNoViewNoLinkInSystemCatalog(connection, null);
            verifyNoViewNoLinkInSystemCatalog(connection, SCHEMA3);
            // Verify that there there is no link in the system child link table
            verifyNoChildLink(connection, SCHEMA1);
            verifyNoChildLink(connection, null);
            deleteAllRows(connection, SCHEMA1, null, SCHEMA3, SCHEMA4);
        }
    }

    @Test
    public void testDeleteGrandchildViewRows() throws Exception {
        String baseTableName = generateUniqueName();
        String baseTableFullName = SchemaUtil.getTableName(SCHEMA1, baseTableName);
        try (Connection connection = DriverManager.getConnection(getUrl());
             Connection viewConnection =
                     isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL) : connection) {
            createBaseTableIndexAndViews(connection, baseTableFullName, viewConnection, SCHEMA2, SCHEMA3, null);
            // Delete the grand child view rows from the system catalog
            executeDeleteQuery(connection, deleteViewRows, SCHEMA3);
            // Verify that grand grand child views are still in the system catalog table
            verifyCountQuery(connection, countViewsQuery, null, grandGrandChildCount);
            // Run the orphan view tool to clean up orphan views
            runOrphanViewTool(true, false, true, false);
            // Verify that the orphan views and links have been removed
            verifyOrphanFileLineCounts(grandGrandChildCount,
                    grandChildCount + grandGrandChildCount,
                    grandChildCount,
                    grandChildCount + grandGrandChildCount);
            // Verify that all views and links records for grand and grand grand child views are removed
            // from the system catalog table
            verifyNoViewNoLinkInSystemCatalog(connection, SCHEMA3);
            verifyNoViewNoLinkInSystemCatalog(connection, null);
            // Verify the child links are also removed
            verifyNoChildLink(connection, SCHEMA2);
            verifyNoChildLink(connection, SCHEMA3);
            deleteAllRows(connection, SCHEMA1, SCHEMA2, SCHEMA3, null);
        }
    }

    @Test
    public void testDeleteParentChildLinkRows() throws Exception {
        String baseTableName = generateUniqueName();
        String baseTableFullName = SchemaUtil.getTableName(SCHEMA1, baseTableName);
        try (Connection connection = DriverManager.getConnection(getUrl());
             Connection viewConnection =
                     isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL) : connection) {
            createBaseTableIndexAndViews(connection, baseTableFullName, viewConnection, SCHEMA2, SCHEMA3, SCHEMA4);
            // Delete the CHILD_TABLE links to grand child views
            executeDeleteQuery(connection, deleteChildLinks, SCHEMA2);
            // Verify that grand grand child views are still in the system catalog table
            verifyCountQuery(connection, countViewsQuery, SCHEMA4, grandGrandChildCount);
            // Run the orphan view tool to clean up orphan views and links
            runOrphanViewTool(true, false, true, false);
            // Verify that the orphan views have been removed
            verifyOrphanFileLineCounts(grandChildCount + grandGrandChildCount,
                    0, 0, 0);
            // Verify that all views and links records for grand and grand grand child views are removed
            // from the system catalog table
            verifyNoViewNoLinkInSystemCatalog(connection, SCHEMA3);
            verifyNoViewNoLinkInSystemCatalog(connection, SCHEMA4);
            // Verify the child links are also removed
            verifyNoChildLink(connection, SCHEMA2);
            verifyNoChildLink(connection, SCHEMA3);
            deleteAllRows(connection, SCHEMA1, SCHEMA2, SCHEMA3, SCHEMA4);
        }
    }

    @Test
    public void testDeleteChildParentLinkRows() throws Exception {
        String baseTableName = generateUniqueName();
        String baseTableFullName = SchemaUtil.getTableName(SCHEMA1, baseTableName);
        try (Connection connection = DriverManager.getConnection(getUrl());
             Connection viewConnection =
                     isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL) : connection) {
            createBaseTableIndexAndViews(connection, baseTableFullName, viewConnection, SCHEMA2, SCHEMA3, SCHEMA4);
            // Delete the PARENT_TABLE links from  grand grand child views
            executeDeleteQuery(connection, deleteParentLinks, SCHEMA4);
            // Verify that grand grand child views are still in the system catalog table
            verifyCountQuery(connection, countViewsQuery, SCHEMA4, grandGrandChildCount);
            // Run the orphan view tool to clean up orphan views and links
            runOrphanViewTool(true, false, true, false);
            // Verify that orphan views and links have been removed
            verifyOrphanFileLineCounts(grandGrandChildCount,
                    0, 0, 0);
            // Verify that all views and links records for grand grand child views are removed
            // from the system catalog table
            verifyNoViewNoLinkInSystemCatalog(connection, SCHEMA4);
            // Verify the child links to grand grand child views are also removed
            verifyNoChildLink(connection, SCHEMA3);
            deleteAllRows(connection, SCHEMA1, SCHEMA2, SCHEMA3, SCHEMA4);
        }
    }

    @Test
    public void testDeletePhysicalTableLinks() throws Exception {
        String baseTableName = generateUniqueName();
        String baseTableFullName = SchemaUtil.getTableName(SCHEMA1, baseTableName);
        try (Connection connection = DriverManager.getConnection(getUrl());
             Connection viewConnection =
                     isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL) : connection) {
            createBaseTableIndexAndViews(connection, baseTableFullName, viewConnection, SCHEMA2, SCHEMA3, SCHEMA3);
            // Delete the physical table link rows from the system catalog
            executeDeleteQuery(connection, deletePhysicalLinks, SCHEMA2);
            // Verify that the views we have created are still in the system catalog table
            verifyCountQuery(connection, countViewsQuery, SCHEMA2, childCount);
            verifyCountQuery(connection, countViewsQuery, SCHEMA3, grandChildCount + grandGrandChildCount);
            // Run the orphan view tool to remove orphan views
            runOrphanViewTool(true, false, true, false);
            // Verify that the orphan views have been removed
            verifyLineCount(viewFileName, childCount + grandChildCount + grandGrandChildCount);
            verifyNoViewNoLinkInSystemCatalog(connection, SCHEMA2);
            verifyNoViewNoLinkInSystemCatalog(connection, SCHEMA3);
            // Verify that there there is no link in the system child link table
            verifyNoChildLink(connection, SCHEMA1);
            verifyNoChildLink(connection, SCHEMA2);
            verifyNoChildLink(connection, SCHEMA3);
            deleteAllRows(connection, SCHEMA1, SCHEMA2, SCHEMA3, SCHEMA4);
        }
    }

    public static String[] getArgValues(boolean clean, boolean identify, boolean outputPath, boolean inputPath)
            throws InterruptedException{
        final List<String> args = Lists.newArrayList();
        if (outputPath) {
            args.add("-op");
            args.add(filePath);
        }
        if (inputPath) {
            args.add("-ip");
            args.add(filePath);
        }
        if (clean) {
            args.add("-c");
        }
        if (identify) {
            args.add("-i");
        }
        final long ageMs = 2000;
        Thread.sleep(ageMs);
        args.add("-a");
        args.add(Long.toString(ageMs));
        return args.toArray(new String[0]);
    }

    public static void runOrphanViewTool(boolean clean, boolean identify, boolean outputPath, boolean inputPath)
            throws Exception {
        OrphanViewTool orphanViewTool = new OrphanViewTool();
        Configuration conf = new Configuration(getUtility().getConfiguration());
        orphanViewTool.setConf(conf);
        final String[] cmdArgs =
                getArgValues(clean, identify, outputPath, inputPath);
        int status = orphanViewTool.run(cmdArgs);
        assertEquals(0, status);
    }
}
