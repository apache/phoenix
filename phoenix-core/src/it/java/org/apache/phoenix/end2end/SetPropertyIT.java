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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class SetPropertyIT extends ParallelStatsDisabledIT {
    private String schemaName;
    private String dataTableName;
    private String viewName;
    private String dataTableFullName;
    private String tableDDLOptions;
    private final boolean columnEncoded;

    public SetPropertyIT(boolean columnEncoded) {
        this.columnEncoded = columnEncoded;
        this.tableDDLOptions = columnEncoded ? "" : "COLUMN_ENCODED_BYTES=0";
    }
    
    @Before
    public void setupTableNames() throws Exception {
        schemaName = "";
        dataTableName = generateUniqueName();
        dataTableFullName = SchemaUtil.getTableName(schemaName, dataTableName);
        viewName = generateUniqueName();
    }

    private String generateDDLOptions(String options) {
        StringBuilder sb = new StringBuilder();
        if (!options.isEmpty()) {
            sb.append(options);
        }
        if (!tableDDLOptions.isEmpty()) {
            if (sb.length()!=0)
                sb.append(",");
            sb.append(tableDDLOptions);
        }
        return sb.toString();
    }

    @Test
    public void testSetHColumnProperties() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET REPLICATION_SCOPE=1";
        conn1.createStatement().execute(ddl);
        try (Admin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            ColumnFamilyDescriptor[] columnFamilies = admin.getDescriptor(TableName.valueOf(dataTableFullName))
                    .getColumnFamilies();
            assertEquals(1, columnFamilies.length);
            assertEquals("0", columnFamilies[0].getNameAsString());
            assertEquals(1, columnFamilies[0].getScope());
        }
    }

    @Test
    public void testSetHTableProperties() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET COMPACTION_ENABLED=FALSE";
        conn1.createStatement().execute(ddl);
        try (Admin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
            assertEquals(1, tableDesc.getColumnFamilies().length);
            assertEquals("0", tableDesc.getColumnFamilies()[0].getNameAsString());
            assertEquals(Boolean.toString(false), tableDesc.getValue(TableDescriptorBuilder.COMPACTION_ENABLED));
        }
    }

    @Test
    public void testSetHTableAndHColumnProperties() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET COMPACTION_ENABLED = FALSE, REPLICATION_SCOPE = 1";
        conn1.createStatement().execute(ddl);
        try (Admin admin = conn1.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
            ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            assertEquals(1, columnFamilies.length);
            assertEquals("0", columnFamilies[0].getNameAsString());
            assertEquals(1, columnFamilies[0].getScope());
            assertEquals(false, tableDesc.isCompactionEnabled());
        }
    }

    @Test
    public void testSetHTableHColumnAndPhoenixTableProperties() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CF1.CREATION_TIME BIGINT,\n"
                +"CF2.LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("IMMUTABLE_ROWS=true"
                + (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : ""));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, dataTableFullName, true);
        ddl = "ALTER TABLE " + dataTableFullName + " SET COMPACTION_ENABLED = FALSE, VERSIONS = 10";
        conn.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET COMPACTION_ENABLED = FALSE, CF1.MIN_VERSIONS = 1, CF2.MIN_VERSIONS = 3, " +
                "MIN_VERSIONS = 8, CF1.BLOCKSIZE = 50000, KEEP_DELETED_CELLS = false";
        conn.createStatement().execute(ddl);

        try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
            ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            assertEquals(3, columnFamilies.length);

            assertEquals("0", columnFamilies[0].getNameAsString());
            assertEquals(8, columnFamilies[0].getMinVersions());
            assertEquals(10, columnFamilies[0].getMaxVersions());
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_BLOCKSIZE, columnFamilies[0].getBlocksize());
            assertEquals(KeepDeletedCells.FALSE, columnFamilies[0].getKeepDeletedCells());

            assertEquals("CF1", columnFamilies[1].getNameAsString());
            assertEquals(1, columnFamilies[1].getMinVersions());
            assertEquals(10, columnFamilies[1].getMaxVersions());
            assertEquals(50000, columnFamilies[1].getBlocksize());
            assertEquals(KeepDeletedCells.FALSE, columnFamilies[1].getKeepDeletedCells());

            assertEquals("CF2", columnFamilies[2].getNameAsString());
            assertEquals(3, columnFamilies[2].getMinVersions());
            assertEquals(10, columnFamilies[2].getMaxVersions());
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_BLOCKSIZE, columnFamilies[2].getBlocksize());
            assertEquals(KeepDeletedCells.FALSE, columnFamilies[2].getKeepDeletedCells());

            assertEquals(Boolean.toString(false), tableDesc.getValue(TableDescriptorBuilder.COMPACTION_ENABLED));
        }
    }

    @Test
    public void testSpecifyingColumnFamilyForHTablePropertyFails() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET CF.COMPACTION_ENABLED = FALSE";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSpecifyingColumnFamilyForPhoenixTablePropertyFails() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET CF.DISABLE_WAL = TRUE";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_TABLE_PROPERTY.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSpecifyingColumnFamilyForTTLFails() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"CF.LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET CF.TTL = 86400";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_PROPERTY.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSetPropertyNeedsColumnFamilyToExist() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET CF.BLOCKSIZE = 50000";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.COLUMN_FAMILY_NOT_FOUND.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSetDefaultColumnFamilyNotAllowed() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions(" SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "ALTER TABLE " + dataTableFullName + " SET DEFAULT_COLUMN_FAMILY = 'A'";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.DEFAULT_COLUMN_FAMILY_ONLY_ON_CREATE_TABLE.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSetHColumnOrHTablePropertiesOnViewsNotAllowed() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + viewName + "  AS SELECT * FROM " + dataTableFullName + " WHERE CREATION_TIME = 1";
        conn1.createStatement().execute(ddl);
        ddl = "ALTER VIEW " + viewName + " SET REPLICATION_SCOPE = 1";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VIEW_WITH_PROPERTIES.getErrorCode(), e.getErrorCode());
        }
        ddl = "ALTER VIEW " + viewName + " SET COMPACTION_ENABLED = FALSE";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VIEW_WITH_PROPERTIES.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSetForSomePhoenixTablePropertiesOnViewsAllowed() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("SALT_BUCKETS = 8");
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        String viewFullName = SchemaUtil.getTableName(schemaName, generateUniqueName());
        ddl = "CREATE VIEW " + viewFullName + " AS SELECT * FROM " + dataTableFullName + " WHERE CREATION_TIME = 1";
        conn1.createStatement().execute(ddl);
        ddl = "ALTER VIEW " + viewFullName + " SET UPDATE_CACHE_FREQUENCY = 10";
        conn1.createStatement().execute(ddl);
        conn1.createStatement().execute("SELECT * FROM " + viewFullName);
        PhoenixConnection pconn = conn1.unwrap(PhoenixConnection.class);
        assertEquals(10, pconn.getTable(new PTableKey(pconn.getTenantId(), viewFullName)).getUpdateCacheFrequency());
        ddl = "ALTER VIEW " + viewFullName + " SET UPDATE_CACHE_FREQUENCY = 20";
        conn1.createStatement().execute(ddl);
        conn1.createStatement().execute("SELECT * FROM " + viewFullName);
        pconn = conn1.unwrap(PhoenixConnection.class);
        assertEquals(20, pconn.getTable(new PTableKey(pconn.getTenantId(), viewFullName)).getUpdateCacheFrequency());
        assertImmutableRows(conn1, viewFullName, false);
        ddl = "ALTER VIEW " + viewFullName + " SET DISABLE_WAL = TRUE";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VIEW_WITH_PROPERTIES.getErrorCode(), e.getErrorCode());
        }
        ddl = "ALTER VIEW " + viewFullName + " SET THROW_INDEX_WRITE_FAILURE = FALSE";
        try {
            conn1.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VIEW_WITH_PROPERTIES.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testSettingPropertiesWhenTableHasDefaultColFamilySpecified() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE  " + dataTableFullName + " (\n"
                +"ID1 VARCHAR(15) NOT NULL,\n"
                +"ID2 VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"CF.LAST_USED DATE,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID1, ID2)) " + generateDDLOptions("IMMUTABLE_ROWS=true, DEFAULT_COLUMN_FAMILY = 'XYZ'"
                + (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : ""));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, dataTableFullName, true);
        ddl = "ALTER TABLE  " + dataTableFullName
                + " SET COMPACTION_ENABLED = FALSE, CF.BLOCKSIZE=50000, IMMUTABLE_ROWS = TRUE, TTL=1000";
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, dataTableFullName, true);
        try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
            TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
            ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
            assertEquals(2, columnFamilies.length);
            assertEquals("CF", columnFamilies[0].getNameAsString());
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_REPLICATION_SCOPE, columnFamilies[0].getScope());
            assertEquals(1000, columnFamilies[0].getTimeToLive());
            assertEquals(50000, columnFamilies[0].getBlocksize());
            assertEquals("XYZ", columnFamilies[1].getNameAsString());
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_REPLICATION_SCOPE, columnFamilies[1].getScope());
            assertEquals(1000, columnFamilies[1].getTimeToLive());
            assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_BLOCKSIZE, columnFamilies[1].getBlocksize());
            assertEquals(Boolean.toString(false), tableDesc.getValue(TableDescriptorBuilder.COMPACTION_ENABLED));
        }
    }

    private static void assertImmutableRows(Connection conn, String fullTableName, boolean expectedValue) throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        assertEquals(expectedValue, pconn.getTable(new PTableKey(pconn.getTenantId(), fullTableName)).isImmutableRows());
    }

    @Test
    public void testSetPropertyAndAddColumnForExistingColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE " + dataTableFullName
                +
                "  (a_string varchar not null, col1 integer, CF.col2 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) " + tableDDLOptions;
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute(
                    "ALTER TABLE " + dataTableFullName + " ADD CF.col3 integer CF.IN_MEMORY=true");
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                ColumnFamilyDescriptor[] columnFamilies = admin.getDescriptor(TableName.valueOf(dataTableFullName))
                        .getColumnFamilies();
                assertEquals(2, columnFamilies.length);
                assertEquals("0", columnFamilies[0].getNameAsString());
                assertFalse(columnFamilies[0].isInMemory());
                assertEquals("CF", columnFamilies[1].getNameAsString());
                assertTrue(columnFamilies[1].isInMemory());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetPropertyAndAddColumnForNewAndExistingColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE " + dataTableFullName + " "
                +
                "  (a_string varchar not null, col1 integer, CF1.col2 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) " + tableDDLOptions;
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement()
                    .execute(
                            "ALTER TABLE "
                                    + dataTableFullName
                                    + " ADD col4 integer, CF1.col5 integer, CF2.col6 integer IN_MEMORY=true, REPLICATION_SCOPE=1, CF2.IN_MEMORY=false ");
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                ColumnFamilyDescriptor[] columnFamilies = admin.getDescriptor(TableName.valueOf(dataTableFullName))
                        .getColumnFamilies();
                assertEquals(3, columnFamilies.length);
                assertEquals("0", columnFamilies[0].getNameAsString());
                assertTrue(columnFamilies[0].isInMemory());
                assertEquals(1, columnFamilies[0].getScope());
                assertEquals("CF1", columnFamilies[1].getNameAsString());
                assertTrue(columnFamilies[1].isInMemory());
                assertEquals(1, columnFamilies[1].getScope());
                assertEquals("CF2", columnFamilies[2].getNameAsString());
                assertFalse(columnFamilies[2].isInMemory());
                assertEquals(1, columnFamilies[2].getScope());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetPropertyAndAddColumnWhenTableHasExplicitDefaultColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE " + dataTableFullName + " "
                +
                "  (a_string varchar not null, col1 integer, CF1.col2 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) " + generateDDLOptions("DEFAULT_COLUMN_FAMILY = 'XYZ'");
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement()
                    .execute(
                            "ALTER TABLE "
                                    + dataTableFullName
                                    + " ADD col4 integer, CF1.col5 integer, CF2.col6 integer IN_MEMORY=true, CF1.BLOCKSIZE=50000, "
                                    + "CF2.IN_MEMORY=false, REPLICATION_SCOPE=1 ");
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                ColumnFamilyDescriptor[] columnFamilies = admin.getDescriptor(TableName.valueOf(dataTableFullName))
                        .getColumnFamilies();
                assertEquals(3, columnFamilies.length);
                assertEquals("CF1", columnFamilies[0].getNameAsString());
                assertTrue(columnFamilies[0].isInMemory());
                assertEquals(1, columnFamilies[0].getScope());
                assertEquals(50000, columnFamilies[0].getBlocksize());
                assertEquals("CF2", columnFamilies[1].getNameAsString());
                assertFalse(columnFamilies[1].isInMemory());
                assertEquals(1, columnFamilies[1].getScope());
                assertEquals("XYZ", columnFamilies[2].getNameAsString());
                assertTrue(columnFamilies[2].isInMemory());
                assertEquals(1, columnFamilies[2].getScope());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetPropertyAndAddColumnFailsForColumnFamilyNotPresentInAddCol() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE " + dataTableFullName + " "
                +
                "  (a_string varchar not null, col1 integer, CF1.col2 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) "+ generateDDLOptions("DEFAULT_COLUMN_FAMILY = 'XYZ'");
        try {
            conn.createStatement().execute(ddl);
            try {
                conn.createStatement().execute(
                        "ALTER TABLE " + dataTableFullName
                                + " ADD col4 integer CF1.BLOCKSIZE=50000, XYZ.IN_MEMORY=true ");
                fail();
            } catch(SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_SET_PROPERTY_FOR_COLUMN_NOT_ADDED.getErrorCode(), e.getErrorCode());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetPropertyAndAddColumnForDifferentColumnFamilies() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE " + dataTableFullName
                +
                "  (a_string varchar not null, col1 integer, CF1.col2 integer, CF2.col3 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) " + generateDDLOptions("DEFAULT_COLUMN_FAMILY = 'XYZ' ");
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement()
                    .execute(
                            "ALTER TABLE "
                                    + dataTableFullName
                                    + " ADD col4 integer, CF1.col5 integer, CF2.col6 integer, CF3.col7 integer CF1.BLOCKSIZE=50000, CF1.IN_MEMORY=false, IN_MEMORY=true ");
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                ColumnFamilyDescriptor[] columnFamilies = admin.getDescriptor(TableName.valueOf(dataTableFullName))
                        .getColumnFamilies();
                assertEquals(4, columnFamilies.length);
                assertEquals("CF1", columnFamilies[0].getNameAsString());
                assertFalse(columnFamilies[0].isInMemory());
                assertEquals(0, columnFamilies[0].getScope());
                assertEquals(50000, columnFamilies[0].getBlocksize());
                assertEquals("CF2", columnFamilies[1].getNameAsString());
                assertTrue(columnFamilies[1].isInMemory());
                assertEquals(0, columnFamilies[1].getScope());
                assertEquals("CF3", columnFamilies[2].getNameAsString());
                assertTrue(columnFamilies[2].isInMemory());
                assertEquals(0, columnFamilies[2].getScope());
                assertEquals("XYZ", columnFamilies[3].getNameAsString());
                assertTrue(columnFamilies[3].isInMemory());
                assertEquals(0, columnFamilies[3].getScope());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetPropertyAndAddColumnUsingDefaultColumnFamilySpecifier() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE " + dataTableFullName
                +
                "  (a_string varchar not null, col1 integer, CF1.col2 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) " + generateDDLOptions("DEFAULT_COLUMN_FAMILY = 'XYZ'");
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute(
                    "ALTER TABLE " + dataTableFullName + " ADD col4 integer REPLICATION_SCOPE=1, XYZ.BLOCKSIZE=50000");
            conn.createStatement()
                    .execute("ALTER TABLE " + dataTableFullName + " ADD XYZ.col5 integer IN_MEMORY=true ");
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                ColumnFamilyDescriptor[] columnFamilies = admin.getDescriptor(TableName.valueOf(dataTableFullName))
                        .getColumnFamilies();
                assertEquals(2, columnFamilies.length);
                assertEquals("CF1", columnFamilies[0].getNameAsString());
                assertFalse(columnFamilies[0].isInMemory());
                assertEquals(1, columnFamilies[0].getScope());
                assertEquals(ColumnFamilyDescriptorBuilder.DEFAULT_BLOCKSIZE, columnFamilies[0].getBlocksize());
                assertEquals("XYZ", columnFamilies[1].getNameAsString());
                assertTrue(columnFamilies[1].isInMemory());
                assertEquals(1, columnFamilies[1].getScope());
                assertEquals(50000, columnFamilies[1].getBlocksize());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetPropertyAndAddColumnForDefaultColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        String ddl = "CREATE TABLE " + dataTableFullName +
                "  (a_string varchar not null, col1 integer" +
                "  CONSTRAINT pk PRIMARY KEY (a_string)) " + tableDDLOptions;
        try {
            conn.createStatement().execute(ddl);
            conn.createStatement().execute("ALTER TABLE " + dataTableFullName + " ADD col2 integer IN_MEMORY=true");
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                ColumnFamilyDescriptor[] columnFamilies = admin.getDescriptor(TableName.valueOf(dataTableFullName))
                        .getColumnFamilies();
                assertEquals(1, columnFamilies.length);
                assertEquals("0", columnFamilies[0].getNameAsString());
                assertTrue(columnFamilies[0].isInMemory());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testAddNewColumnFamilyProperties() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            conn.createStatement()
            .execute(
                            "CREATE TABLE "
                                    + dataTableFullName
                            + "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
                            + "  CONSTRAINT pk PRIMARY KEY (a_string)) " + generateDDLOptions("immutable_rows=true , SALT_BUCKETS=3 "
                            + (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : ""))); 

            String ddl = "Alter table " + dataTableFullName + " add cf3.col5 integer, cf4.col6 integer in_memory=true";
            conn.createStatement().execute(ddl);

            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
                assertTrue(tableDesc.isCompactionEnabled());
                ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
                assertEquals(5, columnFamilies.length);
                assertEquals("0", columnFamilies[0].getNameAsString());
                assertFalse(columnFamilies[0].isInMemory());
                assertEquals("CF1", columnFamilies[1].getNameAsString());
                assertFalse(columnFamilies[1].isInMemory());
                assertEquals("CF2", columnFamilies[2].getNameAsString());
                assertFalse(columnFamilies[2].isInMemory());
                assertEquals("CF3", columnFamilies[3].getNameAsString());
                assertTrue(columnFamilies[3].isInMemory());
                assertEquals("CF4", columnFamilies[4].getNameAsString());
                assertTrue(columnFamilies[4].isInMemory());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testAddProperyToExistingColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            conn.createStatement()
            .execute(
                            "CREATE TABLE "
                                    + dataTableFullName
                            + "  (a_string varchar not null, col1 integer, cf1.col2 integer, col3 integer , cf2.col4 integer "
                            + "  CONSTRAINT pk PRIMARY KEY (a_string)) " + generateDDLOptions("immutable_rows=true , SALT_BUCKETS=3 "
                            + (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : "")));    

            String ddl = "Alter table " + dataTableFullName + " add cf1.col5 integer in_memory=true";
            conn.createStatement().execute(ddl);

            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
                assertTrue(tableDesc.isCompactionEnabled());
                ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
                assertEquals(3, columnFamilies.length);
                assertEquals("0", columnFamilies[0].getNameAsString());
                assertFalse(columnFamilies[0].isInMemory());
                assertEquals("CF1", columnFamilies[1].getNameAsString());
                assertTrue(columnFamilies[1].isInMemory());
                assertEquals("CF2", columnFamilies[2].getNameAsString());
                assertFalse(columnFamilies[2].isInMemory());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testAddTTLToExistingColumnFamily() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + dataTableFullName
                    + " (pk char(2) not null primary key, col1 integer, b.col1 integer) " + tableDDLOptions + " SPLIT ON ('EA','EZ') ";
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " add b.col2 varchar ttl=30";
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSettingTTLWhenAddingColumnNotAllowed() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try {
            String ddl = "CREATE TABLE " + dataTableFullName
                    + " (pk char(2) not null primary key) " + generateDDLOptions("TTL=100") + " SPLIT ON ('EA','EZ')";
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " add col1 varchar ttl=30";
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN.getErrorCode(), e.getErrorCode());
        }
        try {
            String ddl = "ALTER TABLE " + dataTableFullName + " add col1 varchar a.ttl=30";
            conn.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_PROPERTY.getErrorCode(), e.getErrorCode());
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetTTLForTableWithOnlyPKCols() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "create table " + dataTableFullName + " ("
                    + " id char(1) NOT NULL,"
                    + " col1 integer NOT NULL,"
                    + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
                    + " ) " + generateDDLOptions("TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'");
            conn.createStatement().execute(ddl);
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
                ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
                assertEquals(1, columnFamilies.length);
                assertEquals("XYZ", columnFamilies[0].getNameAsString());
                assertEquals(86400, columnFamilies[0].getTimeToLive());
            }
            ddl = "ALTER TABLE " + dataTableFullName + " SET TTL=30";
            conn.createStatement().execute(ddl);
            conn.commit();
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
                ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
                assertEquals(1, columnFamilies.length);
                assertEquals(30, columnFamilies[0].getTimeToLive());
                assertEquals("XYZ", columnFamilies[0].getNameAsString());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetHColumnPropertyForTableWithOnlyPKCols1() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "create table " + dataTableFullName + " ("
                    + " id char(1) NOT NULL,"
                    + " col1 integer NOT NULL,"
                    + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
                    + " ) " + generateDDLOptions("TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'");
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " SET IN_MEMORY=true";
            conn.createStatement().execute(ddl);
            conn.commit();
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
                ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
                assertEquals(1, columnFamilies.length);
                assertEquals(true, columnFamilies[0].isInMemory());
                assertEquals("XYZ", columnFamilies[0].getNameAsString());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetHColumnPropertyForTableWithOnlyPKCols2() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "create table " + dataTableFullName + " ("
                    + " id char(1) NOT NULL,"
                    + " col1 integer NOT NULL,"
                    + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
                    + " ) " + generateDDLOptions("TTL=86400, SALT_BUCKETS = 4");
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " SET IN_MEMORY=true";
            conn.createStatement().execute(ddl);
            conn.commit();
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
                ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
                assertEquals(1, columnFamilies.length);
                assertEquals(true, columnFamilies[0].isInMemory());
                assertEquals("0", columnFamilies[0].getNameAsString());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetHColumnPropertyAndAddColumnForDefaultCFForTableWithOnlyPKCols() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "create table " + dataTableFullName + " ("
                    + " id char(1) NOT NULL,"
                    + " col1 integer NOT NULL,"
                    + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
                    + " ) " + generateDDLOptions("TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'");
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " ADD COL3 INTEGER IN_MEMORY=true";
            conn.createStatement().execute(ddl);
            conn.commit();
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
                ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
                assertEquals(1, columnFamilies.length);
                assertEquals(true, columnFamilies[0].isInMemory());
                assertEquals("XYZ", columnFamilies[0].getNameAsString());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSetHColumnPropertyAndAddColumnForNewCFForTableWithOnlyPKCols() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "create table " + dataTableFullName + " ("
                    + " id char(1) NOT NULL,"
                    + " col1 integer NOT NULL,"
                    + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
                    + " ) " + generateDDLOptions("TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'");
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " ADD NEWCF.COL3 INTEGER IN_MEMORY=true";
            conn.createStatement().execute(ddl);
            conn.commit();
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
                ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
                assertEquals(2, columnFamilies.length);
                assertEquals("NEWCF", columnFamilies[0].getNameAsString());
                assertEquals(true, columnFamilies[0].isInMemory());
                assertEquals("XYZ", columnFamilies[1].getNameAsString());
                assertEquals(false, columnFamilies[1].isInMemory());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testTTLAssignmentForNewEmptyCF() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            String ddl = "create table " + dataTableFullName + " ("
                    + " id char(1) NOT NULL,"
                    + " col1 integer NOT NULL,"
                    + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
                    + " ) " + generateDDLOptions("TTL=86400, SALT_BUCKETS = 4, DEFAULT_COLUMN_FAMILY='XYZ'");
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " ADD NEWCF.COL3 INTEGER IN_MEMORY=true";
            conn.createStatement().execute(ddl);
            conn.commit();
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
                ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
                assertEquals(2, columnFamilies.length);
                assertEquals("NEWCF", columnFamilies[0].getNameAsString());
                assertEquals(true, columnFamilies[0].isInMemory());
                assertEquals(86400, columnFamilies[0].getTimeToLive());
                assertEquals("XYZ", columnFamilies[1].getNameAsString());
                assertEquals(false, columnFamilies[1].isInMemory());
                assertEquals(86400, columnFamilies[1].getTimeToLive());
            }

            ddl = "ALTER TABLE " + dataTableFullName + " SET TTL=1000";
            conn.createStatement().execute(ddl);
            conn.commit();
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
                ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
                assertEquals(2, columnFamilies.length);
                assertEquals("NEWCF", columnFamilies[0].getNameAsString());
                assertEquals(true, columnFamilies[0].isInMemory());
                assertEquals(1000, columnFamilies[0].getTimeToLive());
                assertEquals("XYZ", columnFamilies[1].getNameAsString());
                assertEquals(false, columnFamilies[1].isInMemory());
                assertEquals(1000, columnFamilies[1].getTimeToLive());
            }

            // the new column will be assigned to the column family XYZ. With the a KV column getting added for XYZ,
            // the column family will start showing up in PTable.getColumnFamilies() after the column is added. Thus
            // being a new column family for the PTable, it will end up inheriting the TTL of the emptyCF (NEWCF).
            ddl = "ALTER TABLE " + dataTableFullName + " ADD COL3 INTEGER";
            conn.createStatement().execute(ddl);
            conn.commit();
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
                ColumnFamilyDescriptor[] columnFamilies = tableDesc.getColumnFamilies();
                assertEquals(2, columnFamilies.length);
                assertEquals("NEWCF", columnFamilies[0].getNameAsString());
                assertEquals(true, columnFamilies[0].isInMemory());
                assertEquals(1000, columnFamilies[0].getTimeToLive());
                assertEquals("XYZ", columnFamilies[1].getNameAsString());
                assertEquals(false, columnFamilies[1].isInMemory());
                assertEquals(1000, columnFamilies[1].getTimeToLive());
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testSettingNotHColumnNorPhoenixPropertyEndsUpAsHTableProperty() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            String ddl = "create table " + dataTableFullName + " ("
                    + " id char(1) NOT NULL,"
                    + " col1 integer NOT NULL,"
                    + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
                    + " ) " +  tableDDLOptions;
            conn.createStatement().execute(ddl);
            ddl = "ALTER TABLE " + dataTableFullName + " ADD NEWCF.COL3 INTEGER NEWCF.UNKNOWN_PROP='ABC'";
            try {
                conn.createStatement().execute(ddl);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_SET_TABLE_PROPERTY_ADD_COLUMN.getErrorCode(), e.getErrorCode());
            }
            ddl = "ALTER TABLE " + dataTableFullName + " SET UNKNOWN_PROP='ABC'";
            conn.createStatement().execute(ddl);
            try (Admin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {
                TableDescriptor tableDesc = admin.getDescriptor(TableName.valueOf(dataTableFullName));
                assertEquals("ABC", tableDesc.getValue("UNKNOWN_PROP"));
            }
        } finally {
            conn.close();
        }
    }

    @Test
    public void testAlterImmutableRowsPropertyForOneCellPerKeyValueColumnStorageScheme() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID)) " + tableDDLOptions;
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, dataTableFullName, false);
        ddl = "ALTER TABLE " + dataTableFullName + " SET IMMUTABLE_ROWS = true";
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, dataTableFullName, true);
    }
    
    @Test
    @Ignore("We changed this behaviour and mutable tables can have columnencodedbytes")
    public void testAlterImmutableRowsPropertyForOneCellPerColumnFamilyStorageScheme() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String ddl = "CREATE TABLE " + dataTableFullName + " (\n"
                +"ID VARCHAR(15) NOT NULL,\n"
                +"CREATED_DATE DATE,\n"
                +"CREATION_TIME BIGINT,\n"
                +"CONSTRAINT PK PRIMARY KEY (ID)) " + generateDDLOptions("COLUMN_ENCODED_BYTES=4, IMMUTABLE_ROWS=true"
                + (!columnEncoded ? ",IMMUTABLE_STORAGE_SCHEME=" + PTable.ImmutableStorageScheme.ONE_CELL_PER_COLUMN : ""));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        assertImmutableRows(conn, dataTableFullName, true);
        try {
            ddl = "ALTER TABLE " + dataTableFullName + " SET IMMUTABLE_ROWS = false";
            conn.createStatement().execute(ddl);
            if (columnEncoded) {
                fail();
            }
        }
        catch(SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_ALTER_IMMUTABLE_ROWS_PROPERTY.getErrorCode(), e.getErrorCode());
        }
        assertImmutableRows(conn, dataTableFullName, columnEncoded);
    }
       
}
