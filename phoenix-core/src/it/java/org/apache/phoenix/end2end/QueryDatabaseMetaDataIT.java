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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CHILD_LINK_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_FUNCTION_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_MUTEX_TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_SEQUENCE;
import static org.apache.phoenix.util.TestUtil.ATABLE_NAME;
import static org.apache.phoenix.util.TestUtil.CUSTOM_ENTITY_DATA_FULL_NAME;
import static org.apache.phoenix.util.TestUtil.ENTITY_HISTORY_TABLE_NAME;
import static org.apache.phoenix.util.TestUtil.PTSDB_NAME;
import static org.apache.phoenix.util.TestUtil.STABLE_NAME;
import static org.apache.phoenix.util.TestUtil.TABLE_WITH_SALTING;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.apache.phoenix.util.TestUtil.createGroupByTestTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.GroupedAggregateRegionObserver;
import org.apache.phoenix.coprocessor.ServerCachingEndpointImpl;
import org.apache.phoenix.coprocessor.UngroupedAggregateRegionObserver;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PTable.ViewType;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(ParallelStatsDisabledTest.class)
public class QueryDatabaseMetaDataIT extends ParallelStatsDisabledIT {

    private static void createMDTestTable(Connection conn, String tableName, String extraProps)
            throws SQLException {
        String ddl =
                "create table if not exists " + tableName + "   (id char(1) primary key,\n"
                        + "    a.col1 integer default 42,\n" + "    b.col2 bigint,\n" + "    b.col3 decimal,\n"
                        + "    b.col4 decimal(5),\n" + "    b.col5 decimal(6,3))\n" + "    a."
                        + HConstants.VERSIONS + "=" + 1 + "," + "a."
                        + ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING + "='" + DataBlockEncoding.NONE
                        + "'";
        if (extraProps != null && extraProps.length() > 0) {
            ddl += "," + extraProps;
        }
        conn.createStatement().execute(ddl);
    }

    @Before
    // We need to clean up phoenix metadata to ensure tests don't step on each other
    public void deleteMetadata() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String delete =
                    "DELETE FROM SYSTEM.CATALOG WHERE TABLE_SCHEM IS NULL OR TABLE_SCHEM = '' OR TABLE_SCHEM != 'SYSTEM'";
            conn.createStatement().executeUpdate(delete);
            conn.commit();
            delete = "DELETE FROM \"SYSTEM\".\"SEQUENCE\"";
            conn.createStatement().executeUpdate(delete);
            conn.commit();
            conn.unwrap(PhoenixConnection.class).getQueryServices().clearCache();
        }
    }

    @Test
    public void testMetadataTenantSpecific() throws SQLException {
    	// create multi-tenant table
    	String tableName = generateUniqueName();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
        	String baseTableDdl = "CREATE TABLE %s (K1 VARCHAR NOT NULL, K2 VARCHAR NOT NULL, V VARCHAR CONSTRAINT PK PRIMARY KEY(K1, K2)) MULTI_TENANT=true";
        	conn.createStatement().execute(String.format(baseTableDdl, tableName));
        }
    	
        // create tenant specific view and execute metdata data call with tenant specific connection
        String tenantId = generateUniqueName();
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
        	String viewName = generateUniqueName();
        	String viewDdl = "CREATE VIEW %s AS SELECT * FROM %s";
        	tenantConn.createStatement().execute(String.format(viewDdl, viewName, tableName));
        	DatabaseMetaData dbmd = tenantConn.getMetaData();
        	ResultSet rs = dbmd.getTables(tenantId, "", viewName, null);
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_NAME"), viewName);
            assertEquals(PTableType.VIEW.toString(), rs.getString("TABLE_TYPE"));
            assertFalse(rs.next());
        }
    }
    
    @Test
    public void testTableMetadataScan() throws SQLException {
        String tableAName = generateUniqueName() + "TABLE";
        String tableASchema = "";
        ensureTableCreated(getUrl(), tableAName, ATABLE_NAME, null);
        String tableS = generateUniqueName() + "TABLE";
        ensureTableCreated(getUrl(), tableS, STABLE_NAME, null);
        String tableC = generateUniqueName();
        String tableCSchema = generateUniqueName();
        ensureTableCreated(getUrl(), tableCSchema + "." + tableC, CUSTOM_ENTITY_DATA_FULL_NAME,
            null);

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, tableASchema, tableAName, null);
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_NAME"), tableAName);
            assertEquals(PTableType.TABLE.toString(), rs.getString("TABLE_TYPE"));
            assertEquals(rs.getString(3), tableAName);
            assertEquals(PTableType.TABLE.toString(), rs.getString(4));
            assertFalse(rs.next());

            rs = dbmd.getTables(null, null, null, null);
            assertTrue(rs.next());
            assertEquals(SYSTEM_CATALOG_SCHEMA, rs.getString("TABLE_SCHEM"));
            assertEquals(SYSTEM_CATALOG_TABLE, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.SYSTEM.toString(), rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertEquals(SYSTEM_CATALOG_SCHEMA, rs.getString("TABLE_SCHEM"));
            assertEquals(SYSTEM_CHILD_LINK_TABLE, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.SYSTEM.toString(), rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertEquals(SYSTEM_CATALOG_SCHEMA, rs.getString("TABLE_SCHEM"));
            assertEquals(SYSTEM_FUNCTION_TABLE, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.SYSTEM.toString(), rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertEquals(SYSTEM_CATALOG_SCHEMA, rs.getString("TABLE_SCHEM"));
            assertEquals(PhoenixDatabaseMetaData.SYSTEM_LOG_TABLE, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.SYSTEM.toString(), rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertEquals(SYSTEM_CATALOG_SCHEMA, rs.getString("TABLE_SCHEM"));
            assertEquals(SYSTEM_MUTEX_TABLE_NAME, rs.getString("TABLE_NAME"));
            assertTrue(rs.next());
            assertEquals(SYSTEM_CATALOG_SCHEMA, rs.getString("TABLE_SCHEM"));
            assertEquals(TYPE_SEQUENCE, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.SYSTEM.toString(), rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertEquals(SYSTEM_CATALOG_SCHEMA, rs.getString("TABLE_SCHEM"));
            assertEquals(PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.SYSTEM.toString(), rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertEquals(SYSTEM_CATALOG_SCHEMA, rs.getString("TABLE_SCHEM"));
            assertEquals(PhoenixDatabaseMetaData.SYSTEM_TASK_TABLE, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.SYSTEM.toString(), rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertEquals(null, rs.getString("TABLE_SCHEM"));
            assertEquals(tableAName, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.TABLE.toString(), rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertEquals(null, rs.getString("TABLE_SCHEM"));
            assertEquals(tableS, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.TABLE.toString(), rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertEquals(tableCSchema, rs.getString("TABLE_SCHEM"));
            assertEquals(tableC, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.TABLE.toString(), rs.getString("TABLE_TYPE"));
            assertEquals("false", rs.getString(PhoenixDatabaseMetaData.TRANSACTIONAL));
            assertEquals(Boolean.FALSE, rs.getBoolean(PhoenixDatabaseMetaData.IS_NAMESPACE_MAPPED));

            rs = dbmd.getTables(null, tableCSchema, tableC, null);
            assertTrue(rs.next());
            try {
                rs.getString("RANDOM_COLUMN_NAME");
                fail();
            } catch (ColumnNotFoundException e) {
                // expected
            }
            assertEquals(tableCSchema, rs.getString("TABLE_SCHEM"));
            assertEquals(tableC, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.TABLE.toString(), rs.getString("TABLE_TYPE"));
            assertFalse(rs.next());

            rs = dbmd.getTables(null, "", "%TABLE", new String[] { PTableType.TABLE.toString() });
            assertTrue(rs.next());
            assertEquals(null, rs.getString("TABLE_SCHEM"));
            assertEquals(tableAName, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.TABLE.toString(), rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertEquals(null, rs.getString("TABLE_SCHEM"));
            assertEquals(tableS, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.TABLE.toString(), rs.getString("TABLE_TYPE"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testTableTypes() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTableTypes();
            assertTrue(rs.next());
            assertEquals("INDEX", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("SEQUENCE", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("SYSTEM TABLE", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("TABLE", rs.getString(1));
            assertTrue(rs.next());
            assertEquals("VIEW", rs.getString(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSequenceMetadataScan() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String schema1 = "B" + generateUniqueName();
            String seq1 = generateUniqueName();
            String seq1FullName = schema1 + "." + seq1;
            String schema2 = generateUniqueName();
            String seq2 = generateUniqueName();
            String seq2FullName = schema2 + "." + seq2;
            String schema3 = schema1;
            String seq3 = generateUniqueName();
            String seq3FullName = schema3 + "." + seq3;
            String schema4 = generateUniqueName();
            String seq4 = seq1;
            String seq4FullName = schema4 + "." + seq4;
            conn.createStatement().execute("CREATE SEQUENCE " + seq1FullName);
            conn.createStatement().execute("CREATE SEQUENCE " + seq2FullName);
            conn.createStatement().execute("CREATE SEQUENCE " + seq3FullName);
            conn.createStatement().execute("CREATE SEQUENCE " + seq4FullName);
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, null, new String[] { "FOO" });
            assertFalse(rs.next());
            rs =
                    dbmd.getTables(null, null, null,
                        new String[] { "FOO", PhoenixDatabaseMetaData.SEQUENCE_TABLE_TYPE });
            assertTrue(rs.next());
            assertNull(rs.getString("TABLE_CAT"));
            assertEquals(schema1, rs.getString("TABLE_SCHEM"));
            assertEquals(seq1, rs.getString("TABLE_NAME"));
            assertTrue(rs.next());
            assertNull(rs.getString("TABLE_CAT"));
            assertEquals(schema3, rs.getString("TABLE_SCHEM"));
            assertEquals(seq3, rs.getString("TABLE_NAME"));
            assertTrue(rs.next());
            assertNull(rs.getString("TABLE_CAT"));
            assertEquals(schema2, rs.getString("TABLE_SCHEM"));
            assertEquals(seq2, rs.getString("TABLE_NAME"));
            assertTrue(rs.next());
            assertNull(rs.getString("TABLE_CAT"));
            assertEquals(schema4, rs.getString("TABLE_SCHEM"));
            assertEquals(seq4, rs.getString("TABLE_NAME"));
            assertFalse(rs.next());

            String foo = generateUniqueName();
            String basSchema = generateUniqueName();
            String bas = generateUniqueName();
            conn.createStatement().execute("CREATE TABLE " + foo + " (k bigint primary key)");
            conn.createStatement()
                    .execute("CREATE TABLE " + basSchema + "." + bas + " (k bigint primary key)");

            dbmd = conn.getMetaData();
            rs =
                    dbmd.getTables(null, null, null, new String[] { PTableType.TABLE.toString(),
                            PhoenixDatabaseMetaData.SEQUENCE_TABLE_TYPE });
            assertTrue(rs.next());
            assertNull(rs.getString("TABLE_CAT"));
            assertEquals(schema1, rs.getString("TABLE_SCHEM"));
            assertEquals(seq1, rs.getString("TABLE_NAME"));
            assertEquals(PhoenixDatabaseMetaData.SEQUENCE_TABLE_TYPE, rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertNull(rs.getString("TABLE_CAT"));
            assertEquals(schema3, rs.getString("TABLE_SCHEM"));
            assertEquals(seq3, rs.getString("TABLE_NAME"));
            assertEquals(PhoenixDatabaseMetaData.SEQUENCE_TABLE_TYPE, rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertNull(rs.getString("TABLE_CAT"));
            assertEquals(schema2, rs.getString("TABLE_SCHEM"));
            assertEquals(seq2, rs.getString("TABLE_NAME"));
            assertEquals(PhoenixDatabaseMetaData.SEQUENCE_TABLE_TYPE, rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertNull(rs.getString("TABLE_CAT"));
            assertEquals(schema4, rs.getString("TABLE_SCHEM"));
            assertEquals(seq4, rs.getString("TABLE_NAME"));
            assertEquals(PhoenixDatabaseMetaData.SEQUENCE_TABLE_TYPE, rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertNull(rs.getString("TABLE_CAT"));
            assertNull(rs.getString("TABLE_SCHEM"));
            assertEquals(foo, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.TABLE.toString(), rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertNull(rs.getString("TABLE_CAT"));
            assertEquals(basSchema, rs.getString("TABLE_SCHEM"));
            assertEquals(bas, rs.getString("TABLE_NAME"));
            assertEquals(PTableType.TABLE.toString(), rs.getString("TABLE_TYPE"));
            assertFalse(rs.next());

            rs =
                    dbmd.getTables(null, "B%", null,
                        new String[] { PhoenixDatabaseMetaData.SEQUENCE_TABLE_TYPE });
            assertTrue(rs.next());
            assertNull(rs.getString("TABLE_CAT"));
            assertEquals(schema1, rs.getString("TABLE_SCHEM"));
            assertEquals(seq1, rs.getString("TABLE_NAME"));
            assertEquals(PhoenixDatabaseMetaData.SEQUENCE_TABLE_TYPE, rs.getString("TABLE_TYPE"));
            assertTrue(rs.next());
            assertNull(rs.getString("TABLE_CAT"));
            assertEquals(schema3, rs.getString("TABLE_SCHEM"));
            assertEquals(seq3, rs.getString("TABLE_NAME"));
            assertEquals(PhoenixDatabaseMetaData.SEQUENCE_TABLE_TYPE, rs.getString("TABLE_TYPE"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testShowSchemas() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            ResultSet rs = conn.prepareStatement("show schemas").executeQuery();
            assertTrue(rs.next());
            assertEquals("SYSTEM", rs.getString("TABLE_SCHEM"));
            assertEquals(null, rs.getString("TABLE_CATALOG"));
            assertFalse(rs.next());
            // Create another schema and make sure it is listed.
            String schema = "showschemastest_" + generateUniqueName();
            String fullTable = schema + "." + generateUniqueName();
            ensureTableCreated(getUrl(), fullTable, ENTITY_HISTORY_TABLE_NAME, null);
            // show schemas
            rs = conn.prepareStatement("show schemas").executeQuery();
            Set<String> schemas = new HashSet<>();
            while (rs.next()) {
                schemas.add(rs.getString("TABLE_SCHEM"));
                assertEquals(null, rs.getString("TABLE_CATALOG"));
            }
            assertEquals(2, schemas.size());
            assertTrue(schemas.contains("SYSTEM"));
            assertTrue(schemas.contains(schema.toUpperCase()));
            // show schemas like 'SYST%' and only SYSTEM should show up.
            rs = conn.prepareStatement("show schemas like 'SYST%'").executeQuery();
            assertTrue(rs.next());
            assertEquals("SYSTEM", rs.getString("TABLE_SCHEM"));
            assertEquals(null, rs.getString("TABLE_CATALOG"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testShowTables() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            // List all the tables in a particular schema.
            ResultSet rs = conn.prepareStatement("show tables in SYSTEM").executeQuery();
            Set<String> tables = new HashSet<>();
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
                assertEquals("SYSTEM", rs.getString("TABLE_SCHEM"));
            }
            assertEquals(8, tables.size());
            assertTrue(tables.contains("CATALOG"));
            assertTrue(tables.contains("FUNCTION"));

            tables.clear();
            // Add a filter on the table name.
            rs = conn.prepareStatement("show tables in SYSTEM like 'FUNC%'").executeQuery();
            while (rs.next()) tables.add(rs.getString("TABLE_NAME"));
            assertEquals(1, tables.size());
            assertTrue(tables.contains("FUNCTION"));
        }
    }

    @Test
    public void testSchemaMetadataScan() throws SQLException {
        String table1 = generateUniqueName();
        String schema1 = "Z_" + generateUniqueName();
        String fullTable1 = schema1 + "." + table1;
        ensureTableCreated(getUrl(), fullTable1, CUSTOM_ENTITY_DATA_FULL_NAME, null);
        String fullTable2 = generateUniqueName();
        ensureTableCreated(getUrl(), fullTable2, PTSDB_NAME, null);
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs;
            rs = dbmd.getSchemas(null, schema1);
            assertTrue(rs.next());
            assertEquals(rs.getString(1), schema1);
            assertEquals(rs.getString(2), null);
            assertFalse(rs.next());

            rs = dbmd.getSchemas(null, "");
            assertTrue(rs.next());
            assertEquals(rs.getString(1), null);
            assertEquals(rs.getString(2), null);
            assertFalse(rs.next());

            rs = dbmd.getSchemas(null, null);
            assertTrue(rs.next());
            assertEquals(null, rs.getString("TABLE_SCHEM"));
            assertEquals(null, rs.getString("TABLE_CATALOG"));
            assertTrue(rs.next());
            assertEquals(PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA,
                rs.getString("TABLE_SCHEM"));
            assertEquals(null, rs.getString("TABLE_CATALOG"));
            assertTrue(rs.next());
            assertEquals(schema1, rs.getString("TABLE_SCHEM"));
            assertEquals(null, rs.getString("TABLE_CATALOG"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testColumnMetadataScan() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        String table = generateUniqueName();
        createMDTestTable(conn, table, "");
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs;
        rs = dbmd.getColumns(null, "", table, null);
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), null);
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(null, rs.getString("TABLE_CAT"));
        assertEquals(SchemaUtil.normalizeIdentifier("id"), rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.attributeNoNulls, rs.getShort("NULLABLE"));
        assertEquals(PChar.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
        assertEquals(1, rs.getInt("ORDINAL_POSITION"));
        assertEquals(1, rs.getInt("COLUMN_SIZE"));
        assertEquals(0, rs.getInt("DECIMAL_DIGITS"));

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), null);
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier("a"), rs.getString("COLUMN_FAMILY"));
        assertEquals(SchemaUtil.normalizeIdentifier("col1"), rs.getString("COLUMN_NAME"));
        assertEquals("42", rs.getString("COLUMN_DEF"));
        assertEquals(DatabaseMetaData.attributeNullable, rs.getShort("NULLABLE"));
        assertEquals(PInteger.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
        assertEquals(2, rs.getInt("ORDINAL_POSITION"));
        assertEquals(0, rs.getInt("COLUMN_SIZE"));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
        assertTrue(rs.wasNull());

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), null);
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier("b"), rs.getString("COLUMN_FAMILY"));
        assertEquals(SchemaUtil.normalizeIdentifier("col2"), rs.getString("COLUMN_NAME"));
        assertEquals(null, rs.getString("COLUMN_DEF"));
        assertEquals(DatabaseMetaData.attributeNullable, rs.getShort("NULLABLE"));
        assertEquals(PLong.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
        assertEquals(3, rs.getInt("ORDINAL_POSITION"));
        assertEquals(0, rs.getInt("COLUMN_SIZE"));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
        assertTrue(rs.wasNull());

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), null);
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier("b"), rs.getString("COLUMN_FAMILY"));
        assertEquals(SchemaUtil.normalizeIdentifier("col3"), rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.attributeNullable, rs.getShort("NULLABLE"));
        assertEquals(PDecimal.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
        assertEquals(4, rs.getInt("ORDINAL_POSITION"));
        assertEquals(0, rs.getInt("COLUMN_SIZE"));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
        assertTrue(rs.wasNull());

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), null);
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier("b"), rs.getString("COLUMN_FAMILY"));
        assertEquals(SchemaUtil.normalizeIdentifier("col4"), rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.attributeNullable, rs.getShort("NULLABLE"));
        assertEquals(PDecimal.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
        assertEquals(5, rs.getInt("ORDINAL_POSITION"));
        assertEquals(5, rs.getInt("COLUMN_SIZE"));
        assertEquals(0, rs.getInt("DECIMAL_DIGITS"));

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), null);
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier("b"), rs.getString("COLUMN_FAMILY"));
        assertEquals(SchemaUtil.normalizeIdentifier("col5"), rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.attributeNullable, rs.getShort("NULLABLE"));
        assertEquals(PDecimal.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
        assertEquals(6, rs.getInt("ORDINAL_POSITION"));
        assertEquals(6, rs.getInt("COLUMN_SIZE"));
        assertEquals(3, rs.getInt("DECIMAL_DIGITS"));

        assertFalse(rs.next());

        // Look up only columns in a column family
        rs = dbmd.getColumns(null, "", table, "A.");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), null);
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier("a"), rs.getString("COLUMN_FAMILY"));
        assertEquals(SchemaUtil.normalizeIdentifier("col1"), rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.attributeNullable, rs.getShort("NULLABLE"));
        assertEquals(PInteger.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
        assertEquals(2, rs.getInt("ORDINAL_POSITION"));
        assertEquals(0, rs.getInt("COLUMN_SIZE"));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
        assertTrue(rs.wasNull());

        assertFalse(rs.next());

        // Look up KV columns in a column family
        rs = dbmd.getColumns("", "", table, "%.COL%");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), null);
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier("a"), rs.getString("COLUMN_FAMILY"));
        assertEquals(SchemaUtil.normalizeIdentifier("col1"), rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.attributeNullable, rs.getShort("NULLABLE"));
        assertEquals(PInteger.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
        assertEquals(2, rs.getInt("ORDINAL_POSITION"));
        assertEquals(0, rs.getInt("COLUMN_SIZE"));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
        assertTrue(rs.wasNull());

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), null);
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier("b"), rs.getString("COLUMN_FAMILY"));
        assertEquals(SchemaUtil.normalizeIdentifier("col2"), rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.attributeNullable, rs.getShort("NULLABLE"));
        assertEquals(PLong.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
        assertEquals(3, rs.getInt("ORDINAL_POSITION"));
        assertEquals(0, rs.getInt("COLUMN_SIZE"));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
        assertTrue(rs.wasNull());

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), null);
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier("b"), rs.getString("COLUMN_FAMILY"));
        assertEquals(SchemaUtil.normalizeIdentifier("col3"), rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.attributeNullable, rs.getShort("NULLABLE"));
        assertEquals(PDecimal.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
        assertEquals(4, rs.getInt("ORDINAL_POSITION"));
        assertEquals(0, rs.getInt("COLUMN_SIZE"));
        assertTrue(rs.wasNull());
        assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
        assertTrue(rs.wasNull());

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), null);
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier("b"), rs.getString("COLUMN_FAMILY"));
        assertEquals(SchemaUtil.normalizeIdentifier("col4"), rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.attributeNullable, rs.getShort("NULLABLE"));
        assertEquals(PDecimal.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
        assertEquals(5, rs.getInt("ORDINAL_POSITION"));
        assertEquals(5, rs.getInt("COLUMN_SIZE"));
        assertEquals(0, rs.getInt("DECIMAL_DIGITS"));
        assertFalse(rs.wasNull());

        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), null);
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier("b"), rs.getString("COLUMN_FAMILY"));
        assertEquals(SchemaUtil.normalizeIdentifier("col5"), rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.attributeNullable, rs.getShort("NULLABLE"));
        assertEquals(PDecimal.INSTANCE.getSqlType(), rs.getInt("DATA_TYPE"));
        assertEquals(6, rs.getInt("ORDINAL_POSITION"));
        assertEquals(6, rs.getInt("COLUMN_SIZE"));
        assertEquals(3, rs.getInt("DECIMAL_DIGITS"));

        assertFalse(rs.next());

        // Look up KV columns in a column family
        rs = dbmd.getColumns("", "", table, "B.COL2");
        assertTrue(rs.next());
        assertEquals(rs.getString("TABLE_SCHEM"), null);
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier("b"), rs.getString("COLUMN_FAMILY"));
        assertEquals(SchemaUtil.normalizeIdentifier("col2"), rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());

        String table2 = generateUniqueName();
        ensureTableCreated(getUrl(), table2, TABLE_WITH_SALTING, null);
        rs = dbmd.getColumns("", "", table2, StringUtil.escapeLike("A_INTEGER"));
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("ORDINAL_POSITION"));
        assertFalse(rs.next());

    }

    @Test
    public void testPrimaryKeyMetadataScan() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String table1 = generateUniqueName();
            createMDTestTable(conn, table1, "");
            String schema2 = generateUniqueName();
            String table2 = generateUniqueName();
            String fullTable2 = schema2 + "." + table2;
            ensureTableCreated(getUrl(), fullTable2, CUSTOM_ENTITY_DATA_FULL_NAME, null);
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs;
            rs = dbmd.getPrimaryKeys(null, "", table1);
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_SCHEM"), null);
            assertEquals(table1, rs.getString("TABLE_NAME"));
            assertEquals(null, rs.getString("TABLE_CAT"));
            assertEquals(SchemaUtil.normalizeIdentifier("id"), rs.getString("COLUMN_NAME"));
            assertEquals(1, rs.getInt("KEY_SEQ"));
            assertEquals(null, rs.getString("PK_NAME"));
            assertFalse(rs.next());

            rs = dbmd.getPrimaryKeys(null, schema2, table2);
            assertTrue(rs.next());
            assertEquals(schema2, rs.getString("TABLE_SCHEM"));
            assertEquals(table2, rs.getString("TABLE_NAME"));
            assertEquals(null, rs.getString("TABLE_CAT"));
            assertEquals(SchemaUtil.normalizeIdentifier("custom_entity_data_id"),
                rs.getString("COLUMN_NAME"));
            assertEquals(3, rs.getInt("KEY_SEQ"));
            assertEquals(SchemaUtil.normalizeIdentifier("pk"), rs.getString("PK_NAME"));

            assertTrue(rs.next());
            assertEquals(schema2, rs.getString("TABLE_SCHEM"));
            assertEquals(table2, rs.getString("TABLE_NAME"));
            assertEquals(null, rs.getString("TABLE_CAT"));
            assertEquals(SchemaUtil.normalizeIdentifier("key_prefix"), rs.getString("COLUMN_NAME"));
            assertEquals(2, rs.getInt("KEY_SEQ"));
            assertEquals(SchemaUtil.normalizeIdentifier("pk"), rs.getString("PK_NAME"));

            assertTrue(rs.next());
            assertEquals(schema2, rs.getString("TABLE_SCHEM"));
            assertEquals(table2, rs.getString("TABLE_NAME"));
            assertEquals(null, rs.getString("TABLE_CAT"));
            assertEquals(SchemaUtil.normalizeIdentifier("organization_id"),
                rs.getString("COLUMN_NAME"));
            assertEquals(1, rs.getInt("KEY_SEQ"));
            assertEquals(SchemaUtil.normalizeIdentifier("pk"), rs.getString("PK_NAME")); // TODO:
                                                                                         // this is
                                                                                         // on the
                                                                                         // table
                                                                                         // row

            assertFalse(rs.next());

            rs = dbmd.getColumns("", schema2, table2, null);
            assertTrue(rs.next());
            assertEquals(schema2, rs.getString("TABLE_SCHEM"));
            assertEquals(table2, rs.getString("TABLE_NAME"));
            assertEquals(null, rs.getString("TABLE_CAT"));
            assertEquals(SchemaUtil.normalizeIdentifier("organization_id"),
                rs.getString("COLUMN_NAME"));
            assertEquals(rs.getInt("COLUMN_SIZE"), 15);

            assertTrue(rs.next());
            assertEquals(schema2, rs.getString("TABLE_SCHEM"));
            assertEquals(table2, rs.getString("TABLE_NAME"));
            assertEquals(null, rs.getString("TABLE_CAT"));
            assertEquals(SchemaUtil.normalizeIdentifier("key_prefix"), rs.getString("COLUMN_NAME"));
            assertEquals(rs.getInt("COLUMN_SIZE"), 3);

            assertTrue(rs.next());
            assertEquals(schema2, rs.getString("TABLE_SCHEM"));
            assertEquals(table2, rs.getString("TABLE_NAME"));
            assertEquals(null, rs.getString("TABLE_CAT"));
            assertEquals(SchemaUtil.normalizeIdentifier("custom_entity_data_id"),
                rs.getString("COLUMN_NAME"));

            // The above returns all columns, starting with the PK columns
            assertTrue(rs.next());

            rs = dbmd.getColumns("", schema2, table2, "KEY_PREFIX");
            assertTrue(rs.next());
            assertEquals(schema2, rs.getString("TABLE_SCHEM"));
            assertEquals(table2, rs.getString("TABLE_NAME"));
            assertEquals(null, rs.getString("TABLE_CAT"));
            assertEquals(SchemaUtil.normalizeIdentifier("key_prefix"), rs.getString("COLUMN_NAME"));

            rs = dbmd.getColumns("", schema2, table2, "KEY_PREFIX");
            assertTrue(rs.next());
            assertEquals(schema2, rs.getString("TABLE_SCHEM"));
            assertEquals(table2, rs.getString("TABLE_NAME"));
            assertEquals(null, rs.getString("TABLE_CAT"));
            assertEquals(SchemaUtil.normalizeIdentifier("key_prefix"), rs.getString("COLUMN_NAME"));

            assertFalse(rs.next());

            String table3 = generateUniqueName();
            conn.createStatement().execute(
                "CREATE TABLE " + table3 + " (k INTEGER PRIMARY KEY, v VARCHAR) SALT_BUCKETS=3");
            dbmd = conn.getMetaData();
            rs = dbmd.getPrimaryKeys(null, "", table3);
            assertTrue(rs.next());
            assertEquals(null, rs.getString("TABLE_SCHEM"));
            assertEquals(table3, rs.getString("TABLE_NAME"));
            assertEquals(null, rs.getString("TABLE_CAT"));
            assertEquals("K", rs.getString("COLUMN_NAME"));
            assertEquals(1, rs.getInt("KEY_SEQ"));
            assertEquals(null, rs.getString("PK_NAME"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testMultiTableColumnsMetadataScan() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String table1 = "TEST" + generateUniqueName();
            String table2 = "TEST" + generateUniqueName();
            createGroupByTestTable(conn, table1);
            createMDTestTable(conn, table2, "");
            String table3 = generateUniqueName();
            ensureTableCreated(getUrl(), table3, PTSDB_NAME, null);
            String table4 = generateUniqueName();
            ensureTableCreated(getUrl(), table4, CUSTOM_ENTITY_DATA_FULL_NAME, null);
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getColumns(null, "", "%TEST%", null);
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_SCHEM"), null);
            assertEquals(rs.getString("TABLE_NAME"), table1);
            assertEquals(null, rs.getString("COLUMN_FAMILY"));
            assertEquals(SchemaUtil.normalizeIdentifier("id"), rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_SCHEM"), null);
            assertEquals(rs.getString("TABLE_NAME"), table1);
            assertEquals(PhoenixDatabaseMetaData.TABLE_FAMILY, rs.getString("COLUMN_FAMILY"));
            assertEquals(SchemaUtil.normalizeIdentifier("uri"), rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_SCHEM"), null);
            assertEquals(rs.getString("TABLE_NAME"), table1);
            assertEquals(PhoenixDatabaseMetaData.TABLE_FAMILY, rs.getString("COLUMN_FAMILY"));
            assertEquals(SchemaUtil.normalizeIdentifier("appcpu"), rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_SCHEM"), null);
            assertEquals(rs.getString("TABLE_NAME"), table2);
            assertEquals(null, rs.getString("COLUMN_FAMILY"));
            assertEquals(SchemaUtil.normalizeIdentifier("id"), rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_SCHEM"), null);
            assertEquals(rs.getString("TABLE_NAME"), table2);
            assertEquals(SchemaUtil.normalizeIdentifier("a"), rs.getString("COLUMN_FAMILY"));
            assertEquals(SchemaUtil.normalizeIdentifier("col1"), rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_SCHEM"), null);
            assertEquals(rs.getString("TABLE_NAME"), table2);
            assertEquals(SchemaUtil.normalizeIdentifier("b"), rs.getString("COLUMN_FAMILY"));
            assertEquals(SchemaUtil.normalizeIdentifier("col2"), rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_SCHEM"), null);
            assertEquals(rs.getString("TABLE_NAME"), table2);
            assertEquals(SchemaUtil.normalizeIdentifier("b"), rs.getString("COLUMN_FAMILY"));
            assertEquals(SchemaUtil.normalizeIdentifier("col3"), rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_SCHEM"), null);
            assertEquals(rs.getString("TABLE_NAME"), table2);
            assertEquals(SchemaUtil.normalizeIdentifier("b"), rs.getString("COLUMN_FAMILY"));
            assertEquals(SchemaUtil.normalizeIdentifier("col4"), rs.getString("COLUMN_NAME"));
            assertTrue(rs.next());
            assertEquals(rs.getString("TABLE_SCHEM"), null);
            assertEquals(rs.getString("TABLE_NAME"), table2);
            assertEquals(SchemaUtil.normalizeIdentifier("b"), rs.getString("COLUMN_FAMILY"));
            assertEquals(SchemaUtil.normalizeIdentifier("col5"), rs.getString("COLUMN_NAME"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testCreateOnExistingTable() throws Exception {
        try (PhoenixConnection pconn =
                DriverManager.getConnection(getUrl()).unwrap(PhoenixConnection.class)) {
            String tableName = generateUniqueName();// MDTEST_NAME;
            String schemaName = "";// MDTEST_SCHEMA_NAME;
            byte[] cfA = Bytes.toBytes(SchemaUtil.normalizeIdentifier("a"));
            byte[] cfB = Bytes.toBytes(SchemaUtil.normalizeIdentifier("b"));
            byte[] cfC = Bytes.toBytes("c");
            byte[][] familyNames = new byte[][] { cfB, cfC };
            byte[] htableName = SchemaUtil.getTableNameAsBytes(schemaName, tableName);
            Admin admin = pconn.getQueryServices().getAdmin();
            try {
                admin.disableTable(TableName.valueOf(htableName));
                admin.deleteTable(TableName.valueOf(htableName));
                admin.enableTable(TableName.valueOf(htableName));
            } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
            }

            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(htableName));
            for (byte[] familyName : familyNames) {
                builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(familyName));
            }
            admin.createTable(builder.build());
            createMDTestTable(pconn, tableName,
                "a." + ColumnFamilyDescriptorBuilder.BLOCKSIZE+ "=" + 50000);

            TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(htableName));
            assertEquals(3, descriptor.getColumnFamilies().length);
            ColumnFamilyDescriptor cdA = descriptor.getColumnFamily(cfA);
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_KEEP_DELETED, cdA.getKeepDeletedCells());
            assertNotEquals(ColumnFamilyDescriptorBuilder.DEFAULT_BLOCKSIZE, cdA.getBlocksize());
            assertEquals(DataBlockEncoding.NONE, cdA.getDataBlockEncoding()); // Overriden using
                                                                              // WITH
            assertEquals(1, cdA.getMaxVersions());// Overriden using WITH
            ColumnFamilyDescriptor cdB = descriptor.getColumnFamily(cfB);
            // Allow KEEP_DELETED_CELLS to be false for VIEW
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_KEEP_DELETED, cdB.getKeepDeletedCells());
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_BLOCKSIZE, cdB.getBlocksize());
            assertEquals(DataBlockEncoding.NONE, cdB.getDataBlockEncoding()); // Should keep the
                                                                              // original value.
            // CF c should stay the same since it's not a Phoenix cf.
            ColumnFamilyDescriptor cdC = descriptor.getColumnFamily(cfC);
            assertNotNull("Column family not found", cdC);
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_KEEP_DELETED, cdC.getKeepDeletedCells());
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_BLOCKSIZE, cdC.getBlocksize());
            assertFalse(SchemaUtil.DEFAULT_DATA_BLOCK_ENCODING == cdC.getDataBlockEncoding());
            assertTrue(descriptor.hasCoprocessor(UngroupedAggregateRegionObserver.class.getName()));
            assertTrue(descriptor.hasCoprocessor(GroupedAggregateRegionObserver.class.getName()));
            assertTrue(descriptor.hasCoprocessor(ServerCachingEndpointImpl.class.getName()));
            admin.close();

            int rowCount = 5;
            String upsert = "UPSERT INTO " + tableName + "(id,col1,col2) VALUES(?,?,?)";
            PreparedStatement ps = pconn.prepareStatement(upsert);
            for (int i = 0; i < rowCount; i++) {
                ps.setString(1, Integer.toString(i));
                ps.setInt(2, i + 1);
                ps.setInt(3, i + 2);
                ps.execute();
            }
            pconn.commit();
            String query = "SELECT count(1) FROM " + tableName;
            ResultSet rs = pconn.createStatement().executeQuery(query);
            assertTrue(rs.next());
            assertEquals(rowCount, rs.getLong(1));

            query = "SELECT id, col1,col2 FROM " + tableName;
            rs = pconn.createStatement().executeQuery(query);
            for (int i = 0; i < rowCount; i++) {
                assertTrue(rs.next());
                assertEquals(Integer.toString(i), rs.getString(1));
                assertEquals(i + 1, rs.getInt(2));
                assertEquals(i + 2, rs.getInt(3));
            }
            assertFalse(rs.next());
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCreateViewOnExistingTable() throws Exception {
        try (PhoenixConnection pconn =
                DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES))
                        .unwrap(PhoenixConnection.class)) {
            String tableName = generateUniqueName();// MDTEST_NAME;
            String schemaName = "";// MDTEST_SCHEMA_NAME;
            byte[] cfB = Bytes.toBytes(SchemaUtil.normalizeIdentifier("b"));
            byte[] cfC = Bytes.toBytes("c");
            byte[][] familyNames = new byte[][] { cfB, cfC };
            byte[] htableName = SchemaUtil.getTableNameAsBytes(schemaName, tableName);
            try (Admin admin = pconn.getQueryServices().getAdmin()) {
                try {
                    admin.disableTable(TableName.valueOf(htableName));
                    admin.deleteTable(TableName.valueOf(htableName));
                } catch (org.apache.hadoop.hbase.TableNotFoundException e) {
                }

                TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf(htableName));
                for (byte[] familyName : familyNames) {
                    builder.addColumnFamily(ColumnFamilyDescriptorBuilder.of(familyName));
                }
                admin.createTable(builder.build());
            }
            String createStmt =
                    "create view " + generateUniqueName() + "  (id char(1) not null primary key,\n"
                            + "    a.col1 integer,\n" + "    d.col2 bigint)\n";
            try {
                pconn.createStatement().execute(createStmt);
                fail();
            } catch (TableNotFoundException e) {
                // expected to fail b/c table doesn't exist
            } catch (ReadOnlyTableException e) {
                // expected to fail b/c table doesn't exist
            }

            createStmt =
                    "create view " + tableName + "   (id char(1) not null primary key,\n"
                            + "    a.col1 integer,\n" + "    b.col2 bigint)\n";
            try {
                pconn.createStatement().execute(createStmt);
                fail();
            } catch (ReadOnlyTableException e) {
                // expected to fail b/c cf a doesn't exist
            }
            createStmt =
                    "create view " + tableName + "   (id char(1) not null primary key,\n"
                            + "    b.col1 integer,\n" + "    c.col2 bigint)\n";
            try {
                pconn.createStatement().execute(createStmt);
                fail();
            } catch (ReadOnlyTableException e) {
                // expected to fail b/c cf C doesn't exist (case issue)
            }

            createStmt =
                    "create view " + tableName + "   (id char(1) not null primary key,\n"
                            + "    b.col1 integer,\n"
                            + "    \"c\".col2 bigint) IMMUTABLE_ROWS=true \n";
            // should be ok now
            pconn.createStatement().execute(createStmt);
            ResultSet rs = pconn.getMetaData().getTables(null, null, tableName, null);
            assertTrue(rs.next());
            assertEquals(ViewType.MAPPED.name(), rs.getString(PhoenixDatabaseMetaData.VIEW_TYPE));
            assertFalse(rs.next());

            String deleteStmt = "DELETE FROM " + tableName;
            PreparedStatement ps = pconn.prepareStatement(deleteStmt);
            try {
                ps.execute();
                fail();
            } catch (ReadOnlyTableException e) {
                // expected to fail b/c table is read-only
            }

            String upsert = "UPSERT INTO " + tableName + "(id,col1,col2) VALUES(?,?,?)";
            ps = pconn.prepareStatement(upsert);
            try {
                ps.setString(1, Integer.toString(0));
                ps.setInt(2, 1);
                ps.setInt(3, 2);
                ps.execute();
                fail();
            } catch (ReadOnlyTableException e) {
                // expected to fail b/c table is read-only
            }

            Table htable =
                    pconn.getQueryServices()
                            .getTable(SchemaUtil.getTableNameAsBytes(schemaName, tableName));
            Put put = new Put(Bytes.toBytes("0"));
            put.addColumn(cfB, Bytes.toBytes("COL1"), PInteger.INSTANCE.toBytes(1));
            put.addColumn(cfC, Bytes.toBytes("COL2"), PLong.INSTANCE.toBytes(2));
            htable.put(put);

            // Should be ok b/c we've marked the view with IMMUTABLE_ROWS=true
            pconn.createStatement().execute("CREATE INDEX idx ON " + tableName + "(B.COL1)");
            String select = "SELECT col1 FROM " + tableName + " WHERE col2=?";
            ps = pconn.prepareStatement(select);
            ps.setInt(1, 2);
            rs = ps.executeQuery();
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertFalse(rs.next());

            String dropTable = "DROP TABLE " + tableName;
            ps = pconn.prepareStatement(dropTable);
            try {
                ps.execute();
                fail();
            } catch (TableNotFoundException e) {
                // expected to fail b/c it is a view
            }
            String alterView = "alter view " + tableName + " drop column \"c\".col2";
            pconn.createStatement().execute(alterView);
        }
    }

    @Test
    public void testAddKVColumnToExistingFamily() throws Throwable {
        String tenantId = getOrganizationId();
        String tableName = generateUniqueName();
        initATableValues(tableName, tenantId, getDefaultSplits(tenantId), null, null, getUrl(),
            null);
        try (Connection conn1 = DriverManager.getConnection(getUrl())) {
            conn1.createStatement()
                    .executeUpdate("ALTER TABLE " + tableName + " ADD z_integer integer");
            String query = "SELECT z_integer FROM " + tableName;
            assertTrue(conn1.prepareStatement(query).executeQuery().next());
        }
    }

    @Test
    public void testAddKVColumnToNewFamily() throws Exception {
        String tenantId = getOrganizationId();
        String tableName =
                initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(),
                    null);
        try (Connection conn1 = DriverManager.getConnection(getUrl())) {
            conn1.createStatement()
                    .executeUpdate("ALTER TABLE " + tableName + " ADD newcf.z_integer integer");
            String query = "SELECT z_integer FROM " + tableName;
            assertTrue(conn1.prepareStatement(query).executeQuery().next());
        }
    }

    @Test
    public void testAddPKColumn() throws Exception {
        String tenantId = getOrganizationId();
        String tableName =
                initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(),
                    null);
        try (Connection conn1 = DriverManager.getConnection(getUrl())) {
            try {
                conn1.createStatement().executeUpdate(
                    "ALTER TABLE " + tableName + " ADD z_string varchar not null primary key");
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.NOT_NULLABLE_COLUMN_IN_ROW_KEY.getErrorCode(), e.getErrorCode());
            }
            conn1.createStatement().executeUpdate(
                "ALTER TABLE " + tableName + " ADD z_string varchar primary key");

            String query = "SELECT z_string FROM " + tableName;
            assertTrue(conn1.prepareStatement(query).executeQuery().next());
        }
    }

    @Test
    public void testDropKVColumn() throws Exception {
        String tenantId = getOrganizationId();
        String tableName =
                initATableValues(null, tenantId, getDefaultSplits(tenantId), null, null, getUrl(),
                    null);
        try (Connection conn5 = DriverManager.getConnection(getUrl())) {
            assertTrue(conn5.createStatement()
                    .executeQuery("SELECT 1 FROM " + tableName + " WHERE b_string IS NOT NULL")
                    .next());
            conn5.createStatement()
                    .executeUpdate("ALTER TABLE " + tableName + " DROP COLUMN b_string");

            String query = "SELECT b_string FROM " + tableName;
            try {
                conn5.prepareStatement(query).executeQuery().next();
                fail();
            } catch (ColumnNotFoundException e) {
            }

            conn5.createStatement()
                    .executeUpdate("ALTER TABLE " + tableName + " ADD b_string VARCHAR");
            assertFalse(conn5.createStatement()
                    .executeQuery("SELECT 1 FROM  " + tableName + "  WHERE b_string IS NOT NULL")
                    .next());
        }

    }

    @Test
    public void testDropPKColumn() throws Exception {
        String tenantId = getOrganizationId();
        String tableName =
                initATableValues(generateUniqueName(), tenantId, getDefaultSplits(tenantId), null,
                    null, getUrl(), null);
        try (Connection conn1 = DriverManager.getConnection(getUrl())) {
            conn1.createStatement()
                    .executeUpdate("ALTER TABLE " + tableName + " DROP COLUMN entity_id");
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("ERROR 506 (42817): Primary key column may not be dropped."));
        }
    }

    @Test
    public void testDropAllKVCols() throws Exception {
        ResultSet rs;
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String tableName = generateUniqueName();
            createMDTestTable(conn, tableName, "");
            conn.createStatement().executeUpdate("UPSERT INTO " + tableName + " VALUES('a',1,1)");
            conn.createStatement().executeUpdate("UPSERT INTO " + tableName + " VALUES('b',2,2)");
            conn.commit();

            rs = conn.createStatement().executeQuery("SELECT count(1) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));

            conn.createStatement().executeUpdate("ALTER TABLE " + tableName + " DROP COLUMN col1");

            rs = conn.createStatement().executeQuery("SELECT count(1) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));

            conn.createStatement().executeUpdate("ALTER TABLE " + tableName + " DROP COLUMN col2");

            rs = conn.createStatement().executeQuery("SELECT count(1) FROM " + tableName);
            assertTrue(rs.next());
            assertEquals(2, rs.getLong(1));
        }
    }

    @Test
    public void testTableWithScemaMetadataScan() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String table1 = generateUniqueName();
            String schema1 = generateUniqueName();
            String fullTable1 = schema1 + "." + table1;
            String table2 = table1;

            conn.createStatement()
                    .execute("create table  " + fullTable1 + " (k varchar primary key)");
            conn.createStatement().execute("create table " + table2 + " (k varchar primary key)");
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet rs;

            // Tricky case that requires returning false for null AND true expression
            rs = metaData.getTables(null, schema1, table1, null);
            assertTrue(rs.next());
            assertEquals(schema1, rs.getString("TABLE_SCHEM"));
            assertEquals(table1, rs.getString("TABLE_NAME"));
            assertFalse(rs.next());

            // Tricky case that requires end key to maintain trailing nulls
            rs = metaData.getTables("", schema1, table1, null);
            assertTrue(rs.next());
            assertEquals(schema1, rs.getString("TABLE_SCHEM"));
            assertEquals(table1, rs.getString("TABLE_NAME"));
            assertFalse(rs.next());

            rs = metaData.getTables("", null, table2, null);
            assertTrue(rs.next());
            assertEquals(null, rs.getString("TABLE_SCHEM"));
            assertEquals(table2, rs.getString("TABLE_NAME"));
            assertTrue(rs.next());
            assertEquals(schema1, rs.getString("TABLE_SCHEM"));
            assertEquals(table1, rs.getString("TABLE_NAME"));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testRemarkColumn() throws SQLException {
        Connection conn = DriverManager.getConnection(getUrl());
        // Retrieve the database metadata
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getColumns(null, null, null, null);
        assertTrue(rs.next());

        // Lookup column by name, this should return null but not throw an exception
        String remarks = rs.getString("REMARKS");
        assertNull(remarks);

        // Same as above, but lookup by position
        remarks = rs.getString(12);
        assertNull(remarks);

        // Iterate through metadata columns to find 'COLUMN_NAME' == 'REMARKS'
        boolean foundRemarksColumn = false;
        while (rs.next()) {
            String colName = rs.getString("COLUMN_NAME");
            if (PhoenixDatabaseMetaData.REMARKS.equals(colName)) {
                foundRemarksColumn = true;
                break;
            }
        }
        assertTrue("Could not find REMARKS column", foundRemarksColumn);
    }
}
