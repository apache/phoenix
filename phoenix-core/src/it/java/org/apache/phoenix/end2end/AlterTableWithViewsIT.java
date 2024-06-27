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

import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_MUTATE_TABLE;
import static org.apache.phoenix.util.PhoenixRuntime.TENANT_ID_ATTRIB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

@Category(NeedsOwnMiniClusterTest.class)
@RunWith(Parameterized.class)
public class AlterTableWithViewsIT extends SplitSystemCatalogIT {

    private final boolean isMultiTenant;
    private final boolean columnEncoded;
    private final boolean salted;
    private final String TENANT_SPECIFIC_URL1 = getUrl() + ';' + TENANT_ID_ATTRIB + "=" + TENANT1;
    private final String TENANT_SPECIFIC_URL2 = getUrl() + ';' + TENANT_ID_ATTRIB + "=" + TENANT2;
    
    public AlterTableWithViewsIT(boolean columnEncoded, boolean isMultiTenant, boolean salted) {
        this.columnEncoded = columnEncoded;
        this.isMultiTenant = isMultiTenant;
        this.salted = salted;
    }
    
    // name is used by failsafe as file name in reports
    @Parameters(name = "AlterTableWithViewsIT_columnEncoded={0}, multiTenant={1}, salted={2}")
    public static synchronized Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] { { false, false, false }, { true, false, true },
                { true, true, false }, { true, true, true } });
    }
    
    // transform PColumn to String
    private Function<PColumn,String> function = new Function<PColumn,String>(){
        @Override
        public String apply(PColumn input) {
            return input.getName().getString();
        }
    };
    
    private String generateDDL(String format) {
        return generateDDL("", format);
    }
    
    private String generateDDL(String options, String format) {
        StringBuilder optionsBuilder = new StringBuilder(options);
        if (!columnEncoded) {
            if (optionsBuilder.length()!=0)
                optionsBuilder.append(",");
            optionsBuilder.append("COLUMN_ENCODED_BYTES=0");
        }
        if (isMultiTenant) {
            if (optionsBuilder.length()!=0)
                optionsBuilder.append(",");
            optionsBuilder.append("MULTI_TENANT=true");
        }
        if (salted) {
            if (optionsBuilder.length()!=0)
                optionsBuilder.append(",");
            optionsBuilder.append("SALT_BUCKETS=4");
        }
        return String.format(format, isMultiTenant ? "TENANT_ID VARCHAR NOT NULL, " : "",
            isMultiTenant ? "TENANT_ID, " : "", optionsBuilder.toString());
    }
    
    @Test
    public void testAddNewColumnsToBaseTableWithViews() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {       
            String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewOfTable = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());

            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                            + " %s ID char(1) NOT NULL,"
                            + " COL1 integer NOT NULL,"
                            + " COL2 bigint NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
                            + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, tableName, PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, true, "ID", "COL1", "COL2");
            
            viewConn.createStatement().execute("CREATE VIEW " + viewOfTable + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM " + tableName);
            assertTableDefinition(viewConn, viewOfTable, PTableType.VIEW, tableName, 0, 5, 3, true, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // adding a new pk column and a new regular column
            conn.createStatement().execute("ALTER TABLE " + tableName + " ADD COL3 varchar(10) PRIMARY KEY, COL4 integer");
            assertTableDefinition(conn, tableName, PTableType.TABLE, null, columnEncoded ? 2 : 1, 5, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, false, "ID", "COL1", "COL2", "COL3", "COL4");
            // TODO PHOENIX-4766 add/drop column to a base table are no longer propagated to child views
            // assertTableDefinition(viewConn, viewOfTable, PTableType.VIEW, tableName, 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            assertTableDefinition(viewConn, viewOfTable, PTableType.VIEW, tableName, 0, 5, 3, true, "ID", "COL1", "COL2", "COL3", "COL4", "VIEW_COL1", "VIEW_COL2");
        } 
    }
    
    @Test
    public void testAlterPropertiesOfParentTable() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {       
            String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewOfTable1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
            String viewOfTable2 = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
            
            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                            + " %s ID char(1) NOT NULL,"
                            + " COL1 integer NOT NULL,"
                            + " COL2 bigint NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
                            + " ) %s ";
            conn.createStatement().execute(generateDDL("UPDATE_CACHE_FREQUENCY=2", ddlFormat));
            viewConn.createStatement().execute("CREATE VIEW " + viewOfTable1 + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM " + tableName);
            viewConn.createStatement().execute("CREATE VIEW " + viewOfTable2 + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM " + tableName);
            PName tenantId = isMultiTenant ? PNameFactory.newName(TENANT1) : null;

            // Initially all property values should be the same for the base table and its views
            PTable table = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, tableName));
            PTable viewTable1 = viewConn.unwrap(PhoenixConnection.class).getTable(new PTableKey(tenantId, viewOfTable1));
            PTable viewTable2 = viewConn.unwrap(PhoenixConnection.class).getTable(new PTableKey(tenantId, viewOfTable2));
            assertFalse(table.isImmutableRows());
            assertFalse(viewTable1.isImmutableRows());
            assertFalse(viewTable2.isImmutableRows());
            assertEquals(2, table.getUpdateCacheFrequency());
            assertEquals(2, viewTable1.getUpdateCacheFrequency());
            assertEquals(2, viewTable2.getUpdateCacheFrequency());
            assertNull(table.useStatsForParallelization());
            assertNull(viewTable1.useStatsForParallelization());
            assertNull(viewTable2.useStatsForParallelization());

            // Alter a property value for one of the views
            viewConn.createStatement().execute("ALTER VIEW " + viewOfTable2
                    + " SET UPDATE_CACHE_FREQUENCY=1, USE_STATS_FOR_PARALLELIZATION=false");
            // query the view to force the table cache to be updated
            viewConn.createStatement().execute("SELECT * FROM "+viewOfTable2);
            viewTable2 = viewConn.unwrap(PhoenixConnection.class).getTable(new PTableKey(tenantId, viewOfTable2));
            assertEquals(1, viewTable2.getUpdateCacheFrequency());
            assertFalse(viewTable2.useStatsForParallelization());

            // Alter a property value for the base table. So the view for which this property was
            // not modified earlier should get the base table's altered property value
            conn.createStatement().execute("ALTER TABLE " + tableName
                    + " SET IMMUTABLE_ROWS=true, UPDATE_CACHE_FREQUENCY=3, "
                    + "USE_STATS_FOR_PARALLELIZATION=true");
            // query the views to force the table cache to be updated
            viewConn.createStatement().execute("SELECT * FROM "+viewOfTable1);
            viewConn.createStatement().execute("SELECT * FROM "+viewOfTable2);
            table = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, tableName));
            viewTable1 = viewConn.unwrap(PhoenixConnection.class).getTable(new PTableKey(tenantId, viewOfTable1));
            viewTable2 = viewConn.unwrap(PhoenixConnection.class).getTable(new PTableKey(tenantId, viewOfTable2));
            assertTrue(table.isImmutableRows());
            assertTrue(viewTable1.isImmutableRows());
            assertTrue(viewTable2.isImmutableRows());
            assertEquals(3, table.getUpdateCacheFrequency());
            // The updated property value in the base table is reflected in this view
            assertEquals(3, viewTable1.getUpdateCacheFrequency());
            // The update property is not propagated to this view since it was altered on the view
            assertEquals(1, viewTable2.getUpdateCacheFrequency());
            assertTrue(table.useStatsForParallelization());
            assertTrue(viewTable1.useStatsForParallelization());
            assertFalse(viewTable2.useStatsForParallelization());

            long gpw = 1000000;
            conn.createStatement().execute("ALTER TABLE " + tableName + " SET GUIDE_POSTS_WIDTH=" + gpw);
            
            ResultSet rs;
            DatabaseMetaData md = conn.getMetaData();
            rs =
                    md.getTables("", SchemaUtil.getSchemaNameFromFullName(tableName),
                        SchemaUtil.getTableNameFromFullName(tableName), null);
            assertTrue(rs.next());
            assertEquals(gpw, rs.getLong(PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH));
            
            rs = md.getTables(null, SchemaUtil.getSchemaNameFromFullName(viewOfTable1),
                SchemaUtil.getTableNameFromFullName(viewOfTable1), null);
            assertTrue(rs.next());
            rs.getLong(PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH);
            assertTrue(rs.wasNull());

            rs = md.getTables(null, SchemaUtil.getSchemaNameFromFullName(viewOfTable2),
                SchemaUtil.getTableNameFromFullName(viewOfTable2), null);
            assertTrue(rs.next());
            rs.getLong(PhoenixDatabaseMetaData.GUIDE_POSTS_WIDTH);
            assertTrue(rs.wasNull());
        } 
    }

    @Test
    public void testCreateViewWithPropsMaintainsOwnProps() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ?
                        DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn) {
            String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewOfTable1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
            String viewOfTable2 = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());

            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                    + " %s ID char(1) NOT NULL," + " COL1 integer NOT NULL, COL2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)) %s ";
            conn.createStatement().execute(generateDDL("UPDATE_CACHE_FREQUENCY=2", ddlFormat));

            viewConn.createStatement().execute("CREATE VIEW " + viewOfTable1
                    + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM "
                    + tableName + " UPDATE_CACHE_FREQUENCY=7");
            viewConn.createStatement().execute("CREATE VIEW " + viewOfTable2
                    + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM "
                    + tableName + " USE_STATS_FOR_PARALLELIZATION=true");
            PName tenantId = isMultiTenant ? PNameFactory.newName(TENANT1) : null;

            // Initially, property values not specified during view creation should be the same for
            // the base table and its views those specified should have corresponding values
            PTable table = conn.unwrap(PhoenixConnection.class)
                    .getTable(new PTableKey(null, tableName));
            PTable viewTable1 = viewConn.unwrap(PhoenixConnection.class)
                    .getTable(new PTableKey(tenantId, viewOfTable1));
            PTable viewTable2 = viewConn.unwrap(PhoenixConnection.class)
                    .getTable(new PTableKey(tenantId, viewOfTable2));
            assertEquals(2, table.getUpdateCacheFrequency());
            assertEquals(7, viewTable1.getUpdateCacheFrequency());
            assertEquals(2, viewTable2.getUpdateCacheFrequency());
            assertNull(table.useStatsForParallelization());
            assertNull(viewTable1.useStatsForParallelization());
            assertTrue(viewTable2.useStatsForParallelization());
            // Alter a property value for the base table. So the view for which this property was
            // not explicitly set or modified earlier should get the base table's new property value
            conn.createStatement().execute("ALTER TABLE " + tableName
                    + " SET UPDATE_CACHE_FREQUENCY=3, USE_STATS_FOR_PARALLELIZATION=false");
            Thread.sleep(2);
            // query the views to force the table cache to be updated
            viewConn.createStatement().execute("SELECT * FROM " + viewOfTable1);
            viewConn.createStatement().execute("SELECT * FROM " + viewOfTable2);
            table = conn.unwrap(PhoenixConnection.class)
                    .getTable(new PTableKey(null, tableName));
            viewTable1 = viewConn.unwrap(PhoenixConnection.class)
                    .getTable(new PTableKey(tenantId, viewOfTable1));
            viewTable2 = viewConn.unwrap(PhoenixConnection.class)
                    .getTable(new PTableKey(tenantId, viewOfTable2));
            assertEquals(3, table.getUpdateCacheFrequency());
            // The updated property value is only propagated to the view in which we did not specify
            // a value for the property during view creation or alter its value later on
            assertEquals(7, viewTable1.getUpdateCacheFrequency());
            assertEquals(3, viewTable2.getUpdateCacheFrequency());
            assertFalse(table.useStatsForParallelization());
            assertFalse(viewTable1.useStatsForParallelization());
            assertTrue(viewTable2.useStatsForParallelization());
        }
    }
    
    @Test
    public void testDropColumnsFromBaseTableWithView() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {
            String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewOfTable = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());

            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + " %s ID char(1) NOT NULL,"
                            + " COL1 integer NOT NULL," + " COL2 bigint NOT NULL,"
                            + " COL3 varchar(10)," + " COL4 varchar(10)," + " COL5 varchar(10),"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)" + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, tableName, PTableType.TABLE, null, 0, 6,
                QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, true, "ID", "COL1", "COL2", "COL3",
                "COL4", "COL5");

            viewConn.createStatement()
                    .execute(
                        "CREATE VIEW " + viewOfTable + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM " + tableName);
            assertTableDefinition(viewConn, viewOfTable, PTableType.VIEW, tableName, 0, 8, 6,
                true, "ID", "COL1", "COL2", "COL3", "COL4", "COL5", "VIEW_COL1", "VIEW_COL2");

            // drop two columns from the base table
            conn.createStatement().execute("ALTER TABLE " + tableName + " DROP COLUMN COL3, COL5");
            assertTableDefinition(conn, tableName, PTableType.TABLE, null, columnEncoded ? 2 : 1, 4,
                QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, false, "ID", "COL1", "COL2", "COL4");
            assertTableDefinition(viewConn, viewOfTable, PTableType.VIEW, tableName, 0, 8, 6,
                true, "ID", "COL1", "COL2", "COL4", "VIEW_COL1", "VIEW_COL2");
        }
    }
    
    @Test
    public void testAddExistingViewColumnToBaseTableWithViews() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {
            conn.setAutoCommit(false);
            viewConn.setAutoCommit(false);
            String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewOfTable = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
            
            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                            + " %s ID char(10) NOT NULL,"
                            + " COL1 integer NOT NULL,"
                            + " COL2 bigint NOT NULL,"
                            + " COL3 varchar,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
                            + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, tableName, PTableType.TABLE, null, 0, 4, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, true, "ID", "COL1", "COL2", "COL3");
            
            viewConn.createStatement().execute("CREATE VIEW " + viewOfTable + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256), VIEW_COL3 VARCHAR, VIEW_COL4 DECIMAL, VIEW_COL5 DECIMAL(10,2), VIEW_COL6 VARCHAR, CONSTRAINT pk PRIMARY KEY (VIEW_COL5, VIEW_COL6) ) AS SELECT * FROM " + tableName);
            assertTableDefinition(viewConn,viewOfTable, PTableType.VIEW, tableName, 0, 10, 4, true, "ID", "COL1", "COL2", "COL3", "VIEW_COL1", "VIEW_COL2", "VIEW_COL3", "VIEW_COL4", "VIEW_COL5", "VIEW_COL6");
            
            // upsert single row into view
            String dml = "UPSERT INTO " + viewOfTable + " VALUES(?,?,?,?,?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = viewConn.prepareStatement(dml);
            stmt.setString(1, "view1");
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setString(4, "view4");
            stmt.setInt(5, 15);
            stmt.setString(6, "view6");
            stmt.setString(7, "view7");
            stmt.setInt(8, 18);
            stmt.setInt(9, 19);
            stmt.setString(10, "view10");
            stmt.execute();
            viewConn.commit();
            
            try {
                // should fail because there is already a view column with same name of different type
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL1 char(10)");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }           
            
            try {
                // should fail because there is already a view column with same name with different scale
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL1 DECIMAL(10,1)");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            } 
            
            try {
                // should fail because there is already a view column with same name with different length
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL1 DECIMAL(9,2)");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            } 
            
            try {
                // should fail because there is already a view column with different length
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL2 VARCHAR");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            // validate that there were no columns added to the table or view, if its table is column encoded the sequence number changes when we increment the cq counter
            assertTableDefinition(conn, tableName, PTableType.TABLE, null, columnEncoded ? 1 : 0, 4, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, true, "ID", "COL1", "COL2", "COL3");
            assertTableDefinition(viewConn, viewOfTable, PTableType.VIEW, tableName, 0, 10, 4, true, "ID", "COL1", "COL2", "COL3", "VIEW_COL1", "VIEW_COL2", "VIEW_COL3", "VIEW_COL4", "VIEW_COL5", "VIEW_COL6");
            
            if (columnEncoded) {
                try {
                    // adding a key value column to the base table that already exists in the view is not allowed
                    conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL4 DECIMAL, VIEW_COL2 VARCHAR(256)");
                    fail();
                } catch (SQLException e) {
                    assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
                }
            }
            else {
                // should succeed 
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL4 DECIMAL, VIEW_COL2 VARCHAR(256)");
                assertTableDefinition(conn, tableName, PTableType.TABLE, null, columnEncoded ? 2 : 1, 6, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, false, "ID", "COL1", "COL2", "COL3", "VIEW_COL4", "VIEW_COL2");
                // even though we added columns to the base table, the view metadata remains the same as the base table metadata changes are no longer propagated to the chid view
                assertTableDefinition(viewConn, viewOfTable, PTableType.VIEW, tableName, 0, 10, 4, true, "ID", "COL1", "COL2", "COL3", "VIEW_COL4", "VIEW_COL2", "VIEW_COL1", "VIEW_COL3", "VIEW_COL5", "VIEW_COL6");
                
                // query table
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
                assertTrue(rs.next());
                assertEquals("view1", rs.getString("ID"));
                assertEquals(12, rs.getInt("COL1"));
                assertEquals(13, rs.getInt("COL2"));
                assertEquals("view4", rs.getString("COL3"));
                assertEquals("view6", rs.getString("VIEW_COL2"));
                assertEquals(18, rs.getInt("VIEW_COL4"));
                assertFalse(rs.next());
    
                // query view
                rs = stmt.executeQuery("SELECT * FROM " + viewOfTable);
                assertTrue(rs.next());
                assertEquals("view1", rs.getString("ID"));
                assertEquals(12, rs.getInt("COL1"));
                assertEquals(13, rs.getInt("COL2"));
                assertEquals("view4", rs.getString("COL3"));
                assertEquals(15, rs.getInt("VIEW_COL1"));
                assertEquals("view6", rs.getString("VIEW_COL2"));
                assertEquals("view7", rs.getString("VIEW_COL3"));
                assertEquals(18, rs.getInt("VIEW_COL4"));
                assertEquals(19, rs.getInt("VIEW_COL5"));
                assertEquals("view10", rs.getString("VIEW_COL6"));
                assertFalse(rs.next());
                
                // the base column count and ordinal positions of columns is updated in the ptable (at read time) 
                PName tenantId = isMultiTenant ? PNameFactory.newName(TENANT1) : null;
                PTable view = viewConn.unwrap(PhoenixConnection.class).getTable(new PTableKey(tenantId, viewOfTable));
                assertBaseColumnCount(4, view.getBaseColumnCount());
                assertColumnsMatch(view.getColumns(), "ID", "COL1", "COL2", "COL3", "VIEW_COL4", "VIEW_COL2", "VIEW_COL1", "VIEW_COL3", "VIEW_COL5", "VIEW_COL6");
            }
        } 
    }
    
    @Test
    public void testAddExistingViewPkColumnToBaseTableWithViews() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {      
            conn.setAutoCommit(false);
            viewConn.setAutoCommit(false);
            String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewOfTable = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());

            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + tableName + " ("
                            + " %s ID char(10) NOT NULL,"
                            + " COL1 integer NOT NULL,"
                            + " COL2 integer NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
                            + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, tableName, PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, true, "ID", "COL1", "COL2");
            
            viewConn.createStatement().execute("CREATE VIEW " + viewOfTable + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM " + tableName);
            assertTableDefinition(viewConn, viewOfTable, PTableType.VIEW, tableName, 0, 5, 3, true, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // upsert single row into view
            String dml = "UPSERT INTO " + viewOfTable + " VALUES(?,?,?,?,?)";
            PreparedStatement stmt = viewConn.prepareStatement(dml);
            stmt.setString(1, "view1");
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setInt(4, 14);
            stmt.setString(5, "view5");
            stmt.execute();
            viewConn.commit();
            
            try {
                // should fail because there we have to add both VIEW_COL1 and VIEW_COL2 to the pk
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL2 VARCHAR(256) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because there we have to add both VIEW_COL1 and VIEW_COL2  to the pk 
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because there we have to add both VIEW_COL1 and VIEW_COL2 to the pk
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because there we have to add both VIEW_COL1 and VIEW_COL2  to the pk 
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY, VIEW_COL2 VARCHAR(256)");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because there we have to add both VIEW_COL1 and VIEW_COL2 to the pk in the right order
                conn.createStatement().execute("ALTER TABLE " + tableName + "  ADD VIEW_COL2 VARCHAR(256) PRIMARY KEY, VIEW_COL1 DECIMAL(10,2) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because there we have to add both VIEW_COL1 and VIEW_COL2 with the right sort order
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY DESC, VIEW_COL2 VARCHAR(256) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            // add the pk column of the view to the base table
            conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY, VIEW_COL2 VARCHAR(256) PRIMARY KEY");
            assertTableDefinition(conn, tableName, PTableType.TABLE, null, 1, 5, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, false, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            // even though we added columns to the base table, the sequence number and base column count is not updated in the view metadata (in SYSTEM.CATALOG)
            assertTableDefinition(viewConn, viewOfTable, PTableType.VIEW, tableName, 0,  5, 3, true, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // query table
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());

            // query view
            rs = stmt.executeQuery("SELECT * FROM " + viewOfTable);
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());
            
            // the base column count is updated in the ptable
            PName tenantId = isMultiTenant ? PNameFactory.newName(TENANT1) : null;
            PTable view = viewConn.unwrap(PhoenixConnection.class).getTableNoCache(viewOfTable.toUpperCase());
            view = viewConn.unwrap(PhoenixConnection.class).getTable(new PTableKey(tenantId, viewOfTable));
            assertBaseColumnCount(3, view.getBaseColumnCount());
        } 
    }
    
    @Test
    public void testAddExistingViewPkColumnToBaseTableWithMultipleViews() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {
            String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewOfTable1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
            String viewOfTable2 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
            
            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + tableName + "("
                            + " %s ID char(10) NOT NULL,"
                            + " COL1 integer NOT NULL,"
                            + " COL2 integer NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
                            + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, tableName, PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, true, "ID", "COL1", "COL2");
            
            viewConn.createStatement().execute("CREATE VIEW " + viewOfTable1 + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM " + tableName);
            assertTableDefinition(viewConn, viewOfTable1, PTableType.VIEW, tableName, 0, 5, 3, true, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            viewConn.createStatement().execute("CREATE VIEW " + viewOfTable2 + " ( VIEW_COL3 VARCHAR(256), VIEW_COL4 DECIMAL(10,2) CONSTRAINT pk PRIMARY KEY (VIEW_COL3, VIEW_COL4)) AS SELECT * FROM " + tableName);
            assertTableDefinition(viewConn, viewOfTable2, PTableType.VIEW, tableName, 0, 5, 3, true, "ID", "COL1", "COL2", "VIEW_COL3", "VIEW_COL4");
            
            try {
                // should fail because there are two views with different pk columns
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY, VIEW_COL2 VARCHAR(256) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because there are two view with different pk columns
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL3 VARCHAR PRIMARY KEY, VIEW_COL4 DECIMAL PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because slot positions of pks are different
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL1 DECIMAL PRIMARY KEY, VIEW_COL2 VARCHAR PRIMARY KEY, VIEW_COL3 VARCHAR PRIMARY KEY, VIEW_COL4 DECIMAL PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because slot positions of pks are different
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL3 VARCHAR PRIMARY KEY, VIEW_COL4 DECIMAL PRIMARY KEY, VIEW_COL1 DECIMAL PRIMARY KEY, VIEW_COL2 VARCHAR PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
        } 
    }
    
    @Test
    public void testAddExistingViewPkColumnToBaseTableWithMultipleViewsHavingSamePks() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn;
                Connection viewConn2 = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn) {
            conn.setAutoCommit(false);
            viewConn.setAutoCommit(false);
            viewConn2.setAutoCommit(false);
            String tableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewOfTable1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
            String viewOfTable2 = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
            
            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + tableName + "("
                    + " %s ID char(10) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 integer NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
                    + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, tableName, PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, true, "ID", "COL1", "COL2");
            
            viewConn.createStatement().execute("CREATE VIEW " + viewOfTable1 + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM " + tableName);
            assertTableDefinition(viewConn, viewOfTable1, PTableType.VIEW, tableName, 0, 5, 3, true, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            viewConn2.createStatement().execute("CREATE VIEW " + viewOfTable2 + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM " + tableName);
            assertTableDefinition(viewConn2, viewOfTable2, PTableType.VIEW, tableName, 0, 5, 3,  true, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");

            // upsert single row into both view
            String dml = "UPSERT INTO " + viewOfTable1 + " VALUES(?,?,?,?,?)";
            PreparedStatement stmt = viewConn.prepareStatement(dml);
            stmt.setString(1, "view1");
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setInt(4, 14);
            stmt.setString(5, "view5");
            stmt.execute();
            viewConn.commit();
            dml = "UPSERT INTO " + viewOfTable2 + " VALUES(?,?,?,?,?)";
            stmt = viewConn2.prepareStatement(dml);
            stmt.setString(1, "view1");
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setInt(4, 14);
            stmt.setString(5, "view5");
            stmt.execute();
            viewConn2.commit();
            
            try {
                // should fail because the view have two extra columns in their pk
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because the view have two extra columns in their pk
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL2 VARCHAR(256) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because slot positions of pks are different
                conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL2 DECIMAL(10,2) PRIMARY KEY, VIEW_COL1 VARCHAR(256) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            conn.createStatement().execute("ALTER TABLE " + tableName + " ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY, VIEW_COL2 VARCHAR(256) PRIMARY KEY");
            assertTableDefinition(conn, tableName, PTableType.TABLE, null, 1, 5, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, false, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            // even though we added columns to the base table, the sequence number and base column count is not updated in the view metadata (in SYSTEM.CATALOG)
            assertTableDefinition(viewConn, viewOfTable1, PTableType.VIEW, tableName, 0, 5, 3, true, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            assertTableDefinition(viewConn, viewOfTable2, PTableType.VIEW, tableName, 0, 5, 3, true, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // query table
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());

            // query both views
            rs = viewConn.createStatement().executeQuery("SELECT * FROM " + viewOfTable1);
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());
            rs = viewConn2.createStatement().executeQuery("SELECT * FROM " + viewOfTable2);
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());
            
            // the column count is updated in the base table
            PTable table = conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, tableName));
            assertColumnsMatch(table.getColumns(), "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
        }
    }
    
    public void assertTableDefinition(Connection conn, String fullTableName, PTableType tableType, String parentTableName, int sequenceNumber, int columnCount, int baseColumnCount, boolean offsetCountsForSaltedTables, String... columnNames) throws Exception {
        int delta= 0;
        delta += isMultiTenant ? 1 : 0;
        // when we create a salted table we include the salt num in the column count, but after we
        // add or drop a column we don't include the salted table in the column count, so if a table
        // is salted take this into account for the column count but no the base column count
        if (offsetCountsForSaltedTables)
            delta += salted ? 1 : 0;
        String[] cols;
        if (isMultiTenant && tableType!=PTableType.VIEW) {
            cols = ArrayUtils.addAll(new String[]{"TENANT_ID"}, columnNames);
        }
        else {
            cols = columnNames;
        }
        AlterMultiTenantTableWithViewsIT.assertTableDefinition(conn, fullTableName, tableType, parentTableName, sequenceNumber, columnCount + delta,
            baseColumnCount==QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT ? baseColumnCount : baseColumnCount +delta, cols);
    }
    
    private void assertBaseColumnCount(int expected, int actual) {
        if (salted) ++expected;
        if (isMultiTenant) ++expected;
        assertEquals("Base column count does not match", expected, actual);
    }
    
    private void assertColumnsMatch(List<PColumn> actual, String... expected) {
        List<String> expectedCols = Lists.newArrayList(expected);
        if (isMultiTenant) {
            expectedCols.add(0, "TENANT_ID");
        }
        if (salted) {
            expectedCols.add(0, "_SALT");
        }
        assertEquals(expectedCols, Lists.transform(actual, function));
    }
    
    public static String getSystemCatalogEntriesForTable(Connection conn, String tableName, String message) throws Exception {
        StringBuilder sb = new StringBuilder(message);
        sb.append("\n\n\n");
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM \"SYSTEM\".\"CATALOG\" WHERE TABLE_NAME='"+ tableName +"'");
        ResultSetMetaData metaData = rs.getMetaData();
        int rowNum = 0;
        while (rs.next()) {
            sb.append(rowNum++).append("------\n");
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                sb.append("\t").append(metaData.getColumnLabel(i)).append("=").append(rs.getString(i)).append("\n");
            }
            sb.append("\n");
        }
        rs.close();
        return sb.toString();
    }
    
    
    @Test
    public void testAlteringViewThatHasChildViews() throws Exception {
        String baseTable = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String childView = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String grandChildView = SchemaUtil.getTableName(SCHEMA4, generateUniqueName());
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl());
                PhoenixConnection viewConn = isMultiTenant
                        ? (PhoenixConnection) DriverManager.getConnection(TENANT_SPECIFIC_URL1)
                        : conn) {
            String ddlFormat =
                    "CREATE TABLE IF NOT EXISTS " + baseTable + "  ("
                            + " %s PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s PK2)" + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));

            String childViewDDL = "CREATE VIEW " + childView + " AS SELECT * FROM " + baseTable;
            viewConn.createStatement().execute(childViewDDL);
            
            PTable view = viewConn.getTableNoCache(childView.toUpperCase());
            assertColumnsMatch(view.getColumns(), "PK2", "V1", "V2");

            String grandChildViewDDL =
                    "CREATE VIEW " + grandChildView + " AS SELECT * FROM " + childView;
            viewConn.createStatement().execute(grandChildViewDDL);
            
            String addColumnToChildViewDDL =
                    "ALTER VIEW " + childView + " ADD CHILD_VIEW_COL VARCHAR";
            viewConn.createStatement().execute(addColumnToChildViewDDL);

            view = viewConn.getTableNoCache(childView.toUpperCase());
            assertColumnsMatch(view.getColumns(), "PK2", "V1", "V2", "CHILD_VIEW_COL");

            PTable gcView = viewConn.getTableNoCache(grandChildView.toUpperCase());
            assertColumnsMatch(gcView.getColumns(), "PK2", "V1", "V2", "CHILD_VIEW_COL");

            // dropping base table column from child view should succeed
            String dropColumnFromChildView = "ALTER VIEW " + childView + " DROP COLUMN V2";
            viewConn.createStatement().execute(dropColumnFromChildView);
            view = viewConn.getTableNoCache(childView.toUpperCase());
            assertColumnsMatch(view.getColumns(), "PK2", "V1", "CHILD_VIEW_COL");

            // dropping view specific column from child view should succeed
            dropColumnFromChildView = "ALTER VIEW " + childView + " DROP COLUMN CHILD_VIEW_COL";
            viewConn.createStatement().execute(dropColumnFromChildView);
            view = viewConn.getTableNoCache(childView.toUpperCase());
            assertColumnsMatch(view.getColumns(), "PK2", "V1");

            // Adding column to view that has child views is allowed
            String addColumnToChildView = "ALTER VIEW " + childView + " ADD V5 VARCHAR";
            viewConn.createStatement().execute(addColumnToChildView);
            // V5 column should be visible now for both childView and grandChildView
            viewConn.createStatement().execute("SELECT V5 FROM " + childView);
            viewConn.createStatement().execute("SELECT V5 FROM " + grandChildView);

            view = viewConn.getTableNoCache(childView.toUpperCase());
            assertColumnsMatch(view.getColumns(), "PK2", "V1", "V5");

            // grand child view should have the same columns
            gcView = viewConn.getTableNoCache(grandChildView.toUpperCase());
            assertColumnsMatch(gcView.getColumns(), "PK2", "V1", "V5");
        }
    }
    
    @Test
    public void testDivergedViewsStayDiverged() throws Exception {
        String baseTable = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
        String view1 = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
        String view2 = SchemaUtil.getTableName(SCHEMA3, generateUniqueName());
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl());
                PhoenixConnection viewConn = isMultiTenant
                        ? (PhoenixConnection) DriverManager.getConnection(TENANT_SPECIFIC_URL1)
                        : conn ;
                PhoenixConnection viewConn2 = isMultiTenant
                        ? (PhoenixConnection) DriverManager.getConnection(TENANT_SPECIFIC_URL2)
                        : conn) {
            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + baseTable + " ("
                    + " %s PK1 VARCHAR NOT NULL, V0 VARCHAR, V1 VARCHAR, V2 VARCHAR "
                    + " CONSTRAINT NAME_PK PRIMARY KEY (%s PK1)"
                    + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            
            String viewDDL = "CREATE VIEW " + view1 + " AS SELECT * FROM " + baseTable;
            viewConn.createStatement().execute(viewDDL);
            
            viewDDL = "CREATE VIEW " + view2 + " AS SELECT * FROM " + baseTable;
            viewConn2.createStatement().execute(viewDDL);
            
            // Drop the column inherited from base table to make it diverged
            String dropColumn = "ALTER VIEW " + view1 + " DROP COLUMN V2";
            viewConn.createStatement().execute(dropColumn);
            PTable table = viewConn.getTableNoCache(view1);
            assertEquals(QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT, table.getBaseColumnCount());
            
            try {
                viewConn.createStatement().execute("SELECT V2 FROM " + view1);
                fail("V2 should have been droppped");
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
            }
            
            // Add a new regular column and pk column  to the base table
            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD V3 VARCHAR, PK2 VARCHAR PRIMARY KEY";
            conn.createStatement().execute(alterBaseTable);
            
            // Column V3 shouldn't have propagated to the diverged view.
            try {
                viewConn.createStatement().execute("SELECT V3 FROM " + view1);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
            }
            
            // However, column V3 should have propagated to the non-diverged view.
            viewConn2.createStatement().execute("SELECT V3 FROM " + view2);
            
            // PK2 should be in both views
            viewConn.createStatement().execute("SELECT PK2 FROM " + view1);
            viewConn2.createStatement().execute("SELECT PK2 FROM " + view2);
            
            // Drop a column from the base table
            alterBaseTable = "ALTER TABLE " + baseTable + " DROP COLUMN V1";
            conn.createStatement().execute(alterBaseTable);

            // V1 should be dropped from the base table
            try {
                conn.createStatement().execute("SELECT V1 FROM " + baseTable);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
            }
            
            // V1 should be dropped from both diverged and non-diverged views
            try {
                viewConn2.createStatement().execute("SELECT V1 FROM " + view2);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
            }
            try {
                viewConn.createStatement().execute("SELECT V1 FROM " + view1);
//              TODO since the view is diverged we can't filter out the parent table column metadata
//              while building the view. After the client stops sending parent table column metadata (see PHOENIX-4766)
//              while creating a view dropping a parent table column will also be reflected in a diverged view
//              fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
            }
            
            // V0 should be still exist in both diverged and non-diverged views
            viewConn.createStatement().execute("SELECT V0 FROM " + view1);
            viewConn2.createStatement().execute("SELECT V0 FROM " + view2);

            // we currently cannot add a column that was dropped back to the view because the excluded column
            // doesn't contain data type information see PHOENIX-4868
            try {
    			viewConn.createStatement().execute("ALTER VIEW " + view1 + " ADD V2 VARCHAR");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
			
			// V2 should not exist in the view
            try {
    			viewConn.createStatement().execute("SELECT V2 FROM " + view1);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
            } 
        }
    }
    
    @Test
    public void testMakeBaseTableTransactional() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(QueryServices.TRANSACTIONS_ENABLED, Boolean.TRUE.toString());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {  
            String baseTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewOfTable = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
            
            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + baseTableName + " ("
                            + " %s ID char(1) NOT NULL,"
                            + " COL1 integer NOT NULL,"
                            + " COL2 bigint NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
                            + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, baseTableName, PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, true, "ID", "COL1", "COL2");
            
            viewConn.createStatement().execute("CREATE VIEW " + viewOfTable + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM "+baseTableName);
            assertTableDefinition(viewConn, viewOfTable, PTableType.VIEW, baseTableName, 0, 5, 3, true, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            PName tenantId = isMultiTenant ? PNameFactory.newName(TENANT1) : null;
            PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
            Table htable = phoenixConn.getQueryServices().getTable(Bytes.toBytes(baseTableName));
            assertFalse(phoenixConn.getTable(new PTableKey(null, baseTableName)).isTransactional());
            assertFalse(viewConn.unwrap(PhoenixConnection.class).getTable(new PTableKey(tenantId, viewOfTable)).isTransactional());
        } 
    }

    @Test
    public void testAlterTablePropertyOnView() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {  
            String baseTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewOfTable = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
            
            String ddl = "CREATE TABLE " + baseTableName + " (\n"
                    +"%s ID VARCHAR(15) NOT NULL,\n"
                    + " COL1 integer NOT NULL,"
                    +"CREATED_DATE DATE,\n"
                    +"CONSTRAINT PK PRIMARY KEY (%s ID, COL1)) %s";
            conn.createStatement().execute(generateDDL(ddl));
            ddl = "CREATE VIEW " + viewOfTable + " AS SELECT * FROM " + baseTableName;
            viewConn.createStatement().execute(ddl);
            
            try {
                viewConn.createStatement().execute("ALTER VIEW " + viewOfTable + " SET IMMUTABLE_ROWS = true");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_ALTER_TABLE_PROPERTY_ON_VIEW.getErrorCode(), e.getErrorCode());
            }
            
            viewConn.createStatement().execute("ALTER VIEW " + viewOfTable + " SET UPDATE_CACHE_FREQUENCY = 100");
            viewConn.createStatement().execute("SELECT * FROM "+ viewOfTable);
            PName tenantId = isMultiTenant ? PNameFactory.newName(TENANT1) : null;
            assertEquals(100, viewConn.unwrap(PhoenixConnection.class).getTable(new PTableKey(tenantId, viewOfTable)).getUpdateCacheFrequency());
            
            try {
                viewConn.createStatement().execute("ALTER VIEW " + viewOfTable + " SET APPEND_ONLY_SCHEMA = true");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_ALTER_TABLE_PROPERTY_ON_VIEW.getErrorCode(), e.getErrorCode());
            }
        }
    }
    
    @Test
    public void testAlterAppendOnlySchema() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {  
            String baseTableName = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewOfTable = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
            
            String ddl = "CREATE TABLE " + baseTableName + " (\n"
                    +"%s ID VARCHAR(15) NOT NULL,\n"
                    + " COL1 integer NOT NULL,"
                    +"CREATED_DATE DATE,\n"
                    +"CONSTRAINT PK PRIMARY KEY (%s ID, COL1)) %s";
            conn.createStatement().execute(generateDDL(ddl));
            ddl = "CREATE VIEW " + viewOfTable + " AS SELECT * FROM " + baseTableName;
            viewConn.createStatement().execute(ddl);
            
            PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
            PTable table = phoenixConn.getTable(new PTableKey(null, baseTableName));
            PName tenantId = isMultiTenant ? PNameFactory.newName(TENANT1) : null;
            assertFalse(table.isAppendOnlySchema());
            PTable viewTable = viewConn.unwrap(PhoenixConnection.class).getTable(new PTableKey(tenantId, viewOfTable));
            assertFalse(viewTable.isAppendOnlySchema());
            
            try {
                viewConn.createStatement().execute("ALTER VIEW " + viewOfTable + " SET APPEND_ONLY_SCHEMA = true");
                fail();
            }
            catch(SQLException e){
                assertEquals(SQLExceptionCode.CANNOT_ALTER_TABLE_PROPERTY_ON_VIEW.getErrorCode(), e.getErrorCode());
            }
            
            conn.createStatement().execute("ALTER TABLE " + baseTableName + " SET APPEND_ONLY_SCHEMA = true");
            viewConn.createStatement().execute("SELECT * FROM "+viewOfTable);
            
            phoenixConn = conn.unwrap(PhoenixConnection.class);
            table = phoenixConn.getTable(new PTableKey(null, baseTableName));
            assertTrue(table.isAppendOnlySchema());
            viewTable = viewConn.unwrap(PhoenixConnection.class).getTable(new PTableKey(tenantId, viewOfTable));
            assertTrue(viewTable.isAppendOnlySchema());
        }
    }
    
    @Test
    public void testDroppingIndexedColDropsViewIndex() throws Exception {
        try (Connection conn =DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn  ) {
            String tableWithView = SchemaUtil.getTableName(SCHEMA1, generateUniqueName());
            String viewOfTable = SchemaUtil.getTableName(SCHEMA2, generateUniqueName());
            String viewSchemaName = SchemaUtil.getSchemaNameFromFullName(viewOfTable);
            String viewIndex1 = generateUniqueName();
            String viewIndex2 = generateUniqueName();
            String fullNameViewIndex1 = SchemaUtil.getTableName(viewSchemaName, viewIndex1);
            String fullNameViewIndex2 = SchemaUtil.getTableName(viewSchemaName, viewIndex2);
            
            conn.setAutoCommit(false);
            viewConn.setAutoCommit(false);
            String ddlFormat =
                    "CREATE TABLE " + tableWithView
                            + " (%s k VARCHAR NOT NULL, v1 VARCHAR, v2 VARCHAR, v3 VARCHAR, v4 VARCHAR CONSTRAINT PK PRIMARY KEY(%s k))%s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            viewConn.createStatement()
                    .execute(
                        "CREATE VIEW " + viewOfTable + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM " + tableWithView );
            // create an index with the column that will be dropped
            viewConn.createStatement().execute("CREATE INDEX " + viewIndex1 + " ON " + viewOfTable + "(v2) INCLUDE (v4)");
            // create an index without the column that will be dropped
            viewConn.createStatement().execute("CREATE INDEX " + viewIndex2 + " ON " + viewOfTable + "(v1) INCLUDE (v4)");
            // verify index was created
            try {
                viewConn.createStatement().execute("SELECT * FROM " + fullNameViewIndex1 );
            } catch (TableNotFoundException e) {
                fail("Index on view was not created");
            }
            
            // upsert a single row
            PreparedStatement stmt = viewConn.prepareStatement("UPSERT INTO " + viewOfTable + " VALUES(?,?,?,?,?,?,?)");
            stmt.setString(1, "a");
            stmt.setString(2, "b");
            stmt.setString(3, "c");
            stmt.setString(4, "d");
            stmt.setString(5, "e");
            stmt.setInt(6, 1);
            stmt.setString(7, "g");
            stmt.execute();
            viewConn.commit();

            // verify the index was created
            PhoenixConnection pconn = viewConn.unwrap(PhoenixConnection.class);
            PName tenantId = isMultiTenant ? PNameFactory.newName(TENANT1) : null; 
            PTable view = pconn.getTable(new PTableKey(tenantId,  viewOfTable ));
            PTable viewIndex = pconn.getTable(new PTableKey(tenantId,  fullNameViewIndex1 ));
            byte[] viewIndexPhysicalTable = viewIndex.getPhysicalName().getBytes();
            assertNotNull("Can't find view index", viewIndex);
            assertEquals("Unexpected number of indexes ", 2, view.getIndexes().size());
            assertEquals("Unexpected index ",  fullNameViewIndex1 , view.getIndexes().get(0).getName()
                    .getString());
            assertEquals("Unexpected index ",  fullNameViewIndex2 , view.getIndexes().get(1).getName()
                .getString());
            assertEquals("Unexpected salt buckets", view.getBucketNum(),
                view.getIndexes().get(0).getBucketNum());
            assertEquals("Unexpected salt buckets", view.getBucketNum(),
                view.getIndexes().get(1).getBucketNum());
            
            // drop two columns
            conn.createStatement().execute("ALTER TABLE " + tableWithView + " DROP COLUMN v2, v3 ");
            
            // verify columns were dropped
            try {
                conn.createStatement().execute("SELECT v2 FROM " + tableWithView );
                fail("Column should have been dropped");
            } catch (ColumnNotFoundException e) {
            }
            try {
                conn.createStatement().execute("SELECT v3 FROM " + tableWithView );
                fail("Column should have been dropped");
            } catch (ColumnNotFoundException e) {
            }
            
            // verify index metadata was dropped
            try {
                viewConn.createStatement().execute("SELECT * FROM " + fullNameViewIndex1 );
                fail("Index metadata should have been dropped");
            } catch (TableNotFoundException e) {
            }
            
            pconn = viewConn.unwrap(PhoenixConnection.class);
            view = pconn.getTable(new PTableKey(tenantId,  viewOfTable ));
            try {
                viewIndex = pconn.getTable(new PTableKey(tenantId,  fullNameViewIndex1 ));
                fail("View index should have been dropped");
            } catch (TableNotFoundException e) {
            }
            assertEquals("Unexpected number of indexes ", 1, view.getIndexes().size());
            assertEquals("Unexpected index ",  fullNameViewIndex2 , view.getIndexes().get(0).getName().getString());
            
            // verify that the physical index view table is *not* dropped
            conn.unwrap(PhoenixConnection.class).getQueryServices().getTableDescriptor(viewIndexPhysicalTable);
            
            // scan the physical table and verify there is a single row for the second local index
            Scan scan = new Scan();
            HTable table = (HTable) conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(viewIndexPhysicalTable);
            ResultScanner results = table.getScanner(scan);
            Result result = results.next();
            assertNotNull(result);
            PTable viewIndexPTable = pconn.getTable(new PTableKey(pconn.getTenantId(), fullNameViewIndex2));
            PColumn column = viewIndexPTable.getColumnForColumnName(IndexUtil.getIndexColumnName(QueryConstants.DEFAULT_COLUMN_FAMILY, "V4"));
            byte[] cq = column.getColumnQualifierBytes();
            // there should be a single row belonging to VIEWINDEX2 
            assertNotNull(fullNameViewIndex2 + " row is missing", result.getValue(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES, cq));
            assertNull(results.next());
        }
    }

    @Test
    public void testAddThenDropColumnTableDDLTimestamp() throws Exception {
        Properties props = new Properties();
        String schemaName = SCHEMA1;
        String dataTableName = "T_" + generateUniqueName();
        String viewName = "V_" + generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String viewFullName = SchemaUtil.getTableName(schemaName, viewName);

        String tableDDL = generateDDL("CREATE TABLE IF NOT EXISTS " + dataTableFullName + " ("
            + " %s ID char(1) NOT NULL,"
            + " COL1 integer NOT NULL,"
            + " COL2 bigint NOT NULL,"
            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
            + " ) %s");

        String viewDDL = "CREATE VIEW " + viewFullName + " AS SELECT * FROM " + dataTableFullName;

        String columnAddDDL = "ALTER VIEW " + viewFullName + " ADD COL3 varchar(50) NULL ";
        String columnDropDDL = "ALTER VIEW " + viewFullName + " DROP COLUMN COL3 ";
        long startTS = EnvironmentEdgeManager.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(tableDDL);
            //first get the original DDL timestamp when we created the table
            long tableDDLTimestamp = CreateTableIT.verifyLastDDLTimestamp(
                dataTableFullName, startTS,
                conn);
            Thread.sleep(1);
            conn.createStatement().execute(viewDDL);
            tableDDLTimestamp = CreateTableIT.verifyLastDDLTimestamp(
                viewFullName, tableDDLTimestamp + 1, conn);
            Thread.sleep(1);
            //now add a column and make sure the timestamp updates
            conn.createStatement().execute(columnAddDDL);
            tableDDLTimestamp = CreateTableIT.verifyLastDDLTimestamp(
                viewFullName,
                tableDDLTimestamp + 1, conn);
            Thread.sleep(1);
            conn.createStatement().execute(columnDropDDL);
            CreateTableIT.verifyLastDDLTimestamp(
                viewFullName,
                tableDDLTimestamp + 1 , conn);
        }
    }

    @Test
    public void testLastDDLTimestampForDivergedViews() throws Exception {
        //Phoenix allows users to "drop" columns from views that are inherited from their ancestor
        // views or tables. These columns are then excluded from the view schema, and the view is
        // considered "diverged" from its parents, and so no longer inherit any additional schema
        // changes that are applied to their ancestors. This test make sure that this behavior
        // extends to DDL timestamp
        String schemaName = SCHEMA1;
        String dataTableName = "T_" + generateUniqueName();
        String viewName = "V_" + generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String viewFullName = SchemaUtil.getTableName(schemaName, viewName);

        String tableDDL = generateDDL("CREATE TABLE IF NOT EXISTS " + dataTableFullName + " ("
            + " %s ID char(1) NOT NULL,"
            + " COL1 integer NOT NULL,"
            + " COL2 bigint,"
            + " COL3 bigint,"
            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1)"
            + " ) %s");

        String viewDDL = "CREATE VIEW " + viewFullName + " AS SELECT * FROM " + dataTableFullName;

        String divergeDDL = "ALTER VIEW " + viewFullName + " DROP COLUMN COL2";
        String viewColumnAddDDL = "ALTER VIEW " + viewFullName + " ADD COL4 varchar(50) NULL ";
        String viewColumnDropDDL = "ALTER VIEW " + viewFullName + " DROP COLUMN COL4 ";
        String tableColumnAddDDL = "ALTER TABLE " + dataTableFullName + " ADD COL5 varchar" +
            "(50) NULL";
        String tableColumnDropDDL = "ALTER TABLE " + dataTableFullName + " DROP COLUMN COL3 ";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(tableDDL);
            long tableDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn, dataTableFullName);
            conn.createStatement().execute(viewDDL);
            long viewDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn, viewFullName);
            Thread.sleep(1);
            conn.createStatement().execute(divergeDDL);
            //verify table DDL timestamp DID NOT change
            assertEquals(tableDDLTimestamp, CreateTableIT.getLastDDLTimestamp(conn, dataTableFullName));
            //verify view DDL timestamp changed
            viewDDLTimestamp = CreateTableIT.verifyLastDDLTimestamp(
                viewFullName, viewDDLTimestamp + 1, conn);
            Thread.sleep(1);
            conn.createStatement().execute(viewColumnAddDDL);
            //verify DDL timestamp changed because we added a column to the view
            viewDDLTimestamp = CreateTableIT.verifyLastDDLTimestamp(
                viewFullName, viewDDLTimestamp + 1, conn);
            Thread.sleep(1);
            conn.createStatement().execute(viewColumnDropDDL);
            //verify DDL timestamp changed because we dropped a column from the view
            viewDDLTimestamp = CreateTableIT.verifyLastDDLTimestamp(
                viewFullName, viewDDLTimestamp + 1, conn);
            Thread.sleep(1);
            conn.createStatement().execute(tableColumnAddDDL);
            //verify DDL timestamp DID change because we added a column from the base table
            viewDDLTimestamp = CreateTableIT.verifyLastDDLTimestamp(
                viewFullName, viewDDLTimestamp + 1, conn);
            //and that it did change because we dropped a column from the base table
            conn.createStatement().execute(tableColumnDropDDL);
            viewDDLTimestamp = CreateTableIT.verifyLastDDLTimestamp(
                viewFullName, viewDDLTimestamp + 1, conn);
        }
    }

    @Test
    public void testLastDDLTimestampWithChildViews() throws Exception {
        Assume.assumeTrue(isMultiTenant);
        Properties props = new Properties();
        String schemaName = SCHEMA1;
        String dataTableName = "T_" + generateUniqueName();
        String globalViewName = "V_" + generateUniqueName();
        String tenantViewName = "V_" + generateUniqueName();
        String dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        String globalViewFullName = SchemaUtil.getTableName(schemaName, globalViewName);
        String tenantViewFullName = SchemaUtil.getTableName(schemaName, tenantViewName);

        String tableDDL = generateDDL("CREATE TABLE IF NOT EXISTS " + dataTableFullName + " ("
            + " %s ID char(1) NOT NULL,"
            + " COL1 integer NOT NULL,"
            + " COL2 bigint NOT NULL,"
            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
            + " ) %s");

        //create a table with a child global view, who then has a child tenant view
        String globalViewDDL =
            "CREATE VIEW " + globalViewFullName + " AS SELECT * FROM " + dataTableFullName;

        String tenantViewDDL =
            "CREATE VIEW " + tenantViewFullName + " AS SELECT * FROM " + globalViewFullName;

        long startTS = EnvironmentEdgeManager.currentTimeMillis();
        long tableDDLTimestamp, globalViewDDLTimestamp;

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(tableDDL);
            conn.createStatement().execute(globalViewDDL);
            tableDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn, dataTableFullName);
            globalViewDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn, globalViewFullName);
        }
        props.setProperty(TENANT_ID_ATTRIB, TENANT1);
        try (Connection tenantConn = DriverManager.getConnection(getUrl(), props)) {
            tenantConn.createStatement().execute(tenantViewDDL);
        }
        // First, check that adding a child view to the base table didn't change the base table's
        // timestamp, and that adding a child tenant view to the global view didn't change the
        // global view's timestamp
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            long newTableDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn, dataTableFullName);
            assertEquals(tableDDLTimestamp, newTableDDLTimestamp);

            long newGlobalViewDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn, globalViewFullName);
            assertEquals(globalViewDDLTimestamp, newGlobalViewDDLTimestamp);
        }
        Thread.sleep(1);
        //now add / drop a column from the tenant view and make sure it doesn't change its
        // ancestors' timestamps
        String tenantViewColumnAddDDL = "ALTER VIEW " + tenantViewFullName + " ADD COL3 varchar" +
            "(50) " + "NULL ";
        String tenantViewColumnDropDDL = "ALTER VIEW " + tenantViewFullName + " DROP COLUMN COL3 ";

        try (Connection tenantConn = DriverManager.getConnection(getUrl(), props)) {
            tenantConn.createStatement().execute(tenantViewColumnAddDDL);
            long newTableDDLTimestamp = CreateTableIT.getLastDDLTimestamp(tenantConn, dataTableFullName);
            assertEquals(tableDDLTimestamp, newTableDDLTimestamp);

            long afterTenantColumnAddViewDDLTimestamp = CreateTableIT.getLastDDLTimestamp(tenantConn,
                globalViewFullName);
            assertEquals(globalViewDDLTimestamp, afterTenantColumnAddViewDDLTimestamp);

            tenantConn.createStatement().execute(tenantViewColumnDropDDL);
            //update the tenant view timestamp (we'll need it later)
            long afterTenantColumnDropTableDDLTimestamp = CreateTableIT.getLastDDLTimestamp(tenantConn,
                dataTableFullName);
            assertEquals(tableDDLTimestamp, afterTenantColumnDropTableDDLTimestamp);

            long afterTenantColumnDropViewDDLTimestamp = CreateTableIT.getLastDDLTimestamp(tenantConn,
                globalViewFullName);
            assertEquals(globalViewDDLTimestamp, afterTenantColumnDropViewDDLTimestamp);
        }
        Thread.sleep(1);
        //now add / drop a column from the base table and make sure it changes the timestamps for
        // both the global view (its child) and the tenant view (its grandchild)
        String tableColumnAddDDL = "ALTER TABLE " + dataTableFullName + " ADD COL4 varchar" +
            "(50) " + "NULL ";
        String tableColumnDropDDL = "ALTER TABLE " + dataTableFullName + " DROP COLUMN COL4 ";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(tableColumnAddDDL);
            tableDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn, dataTableFullName);
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), props)) {
                long tenantViewDDLTimestamp = CreateTableIT.getLastDDLTimestamp(tenantConn,
                    tenantViewFullName);
                assertEquals(tableDDLTimestamp, tenantViewDDLTimestamp);
            }
            globalViewDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn,
                globalViewFullName);
            assertEquals(tableDDLTimestamp, globalViewDDLTimestamp);

            conn.createStatement().execute(tableColumnDropDDL);
            tableDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn, dataTableFullName);
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), props)) {
                long tenantViewDDLTimestamp = CreateTableIT.getLastDDLTimestamp(tenantConn,
                    tenantViewFullName);
                assertEquals(tableDDLTimestamp, tenantViewDDLTimestamp);
            }
            globalViewDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn,
                globalViewFullName);
            assertEquals(tableDDLTimestamp, globalViewDDLTimestamp);
        }

        //now add / drop a column from the global view and make sure it doesn't change its
        // parent (the base table) but does change the timestamp for its child (the tenant view)
        String globalViewColumnAddDDL = "ALTER VIEW " + globalViewFullName + " ADD COL5 varchar" +
            "(50) " + "NULL ";
        String globalViewColumnDropDDL = "ALTER VIEW " + globalViewFullName + " DROP COLUMN COL5 ";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement().execute(globalViewColumnAddDDL);
            globalViewDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn, globalViewFullName);
            long newTableDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn,
                dataTableFullName);
            //table DDL timestamp shouldn't have changed
            assertEquals(tableDDLTimestamp, newTableDDLTimestamp);
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), props)) {
                long tenantViewDDLTimestamp = CreateTableIT.getLastDDLTimestamp(tenantConn,
                    tenantViewFullName);
                //but tenant timestamp should have changed
                assertEquals(globalViewDDLTimestamp, tenantViewDDLTimestamp);
            }

            conn.createStatement().execute(globalViewColumnDropDDL);
            globalViewDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn, globalViewFullName);
            newTableDDLTimestamp = CreateTableIT.getLastDDLTimestamp(conn,
                dataTableFullName);
            //table DDL timestamp shouldn't have changed
            assertEquals(tableDDLTimestamp, newTableDDLTimestamp);
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), props)) {
                long tenantViewDDLTimestamp = CreateTableIT.getLastDDLTimestamp(tenantConn,
                    tenantViewFullName);
                //but tenant timestamp should have changed
                assertEquals(globalViewDDLTimestamp, tenantViewDDLTimestamp);
            }
        }

    }

    @Test
    public void testCreateViewSchemaVersion() throws Exception {
        Properties props = new Properties();
        final String schemaName = generateUniqueName();
        final String tableName = generateUniqueName();
        final String viewName = generateUniqueName();
        final String dataTableFullName = SchemaUtil.getTableName(schemaName, tableName);
        final String viewFullName = SchemaUtil.getTableName(schemaName, viewName);
        try (PhoenixConnection conn = (PhoenixConnection) DriverManager.getConnection(getUrl(),
                props)) {
            String oldVersion = "V1.0";
            CreateTableIT.testCreateTableSchemaVersionAndTopicNameHelper(conn, schemaName, tableName, oldVersion, null);
            String createViewSql = "CREATE VIEW " + viewFullName + " AS SELECT * FROM " + dataTableFullName +
                    " SCHEMA_VERSION='" + oldVersion + "'";
            conn.createStatement().execute(createViewSql);
            PTable view = conn.getTableNoCache(viewFullName);
            assertEquals(oldVersion, view.getSchemaVersion());
            assertNull(view.getStreamingTopicName());

            String newVersion = "V1.1";
            String topicName = "MyTopicName";
            String alterViewSql = "ALTER VIEW " + viewFullName + " SET SCHEMA_VERSION='"
                + newVersion + "', STREAMING_TOPIC_NAME='" + topicName + "'";
            conn.createStatement().execute(alterViewSql);
            PTable view2 = conn.getTableNoCache(viewFullName);
            assertEquals(newVersion, view2.getSchemaVersion());
            PTable baseTable = conn.getTableNoCache(dataTableFullName);
            assertEquals(oldVersion, baseTable.getSchemaVersion());
            assertNull(baseTable.getStreamingTopicName());
            assertEquals(topicName, view2.getStreamingTopicName());
        }
    }

}
