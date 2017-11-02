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

import static org.apache.hadoop.hbase.HColumnDescriptor.DEFAULT_REPLICATION_SCOPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos.GlobalPermissionOrBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.SchemaNotFoundException;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;


public class CreateTableIT extends ParallelStatsDisabledIT {

    @Test
    public void testStartKeyStopKey() throws SQLException {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        conn.createStatement().execute("CREATE TABLE " + tableName
                + " (pk char(2) not null primary key) SPLIT ON ('EA','EZ')");
        conn.close();

        String query = "select count(*) from  " + tableName + "  where pk >= 'EA' and pk < 'EZ'";
        conn = DriverManager.getConnection(getUrl(), props);
        Statement statement = conn.createStatement();
        statement.execute(query);
        PhoenixStatement pstatement = statement.unwrap(PhoenixStatement.class);
        List<KeyRange> splits = pstatement.getQueryPlan().getSplits();
        assertTrue(splits.size() > 0);
    }

    @Test
    public void testCreateTable() throws Exception {
        String schemaName = "TEST";
        String tableName = schemaName + generateUniqueName();
        Properties props = new Properties();

        String ddl =
                "CREATE TABLE " + tableName + "(                data.addtime VARCHAR ,\n"
                        + "                data.dir VARCHAR ,\n"
                        + "                data.end_time VARCHAR ,\n"
                        + "                data.file VARCHAR ,\n"
                        + "                data.fk_log VARCHAR ,\n"
                        + "                data.host VARCHAR ,\n"
                        + "                data.r VARCHAR ,\n"
                        + "                data.size VARCHAR ,\n"
                        + "                data.start_time VARCHAR ,\n"
                        + "                data.stat_date DATE ,\n"
                        + "                data.stat_hour VARCHAR ,\n"
                        + "                data.stat_minute VARCHAR ,\n"
                        + "                data.state VARCHAR ,\n"
                        + "                data.title VARCHAR ,\n"
                        + "                data.\"user\" VARCHAR ,\n"
                        + "                data.inrow VARCHAR ,\n"
                        + "                data.jobid VARCHAR ,\n"
                        + "                data.jobtype VARCHAR ,\n"
                        + "                data.level VARCHAR ,\n"
                        + "                data.msg VARCHAR ,\n"
                        + "                data.outrow VARCHAR ,\n"
                        + "                data.pass_time VARCHAR ,\n"
                        + "                data.type VARCHAR ,\n"
                        + "                id INTEGER not null primary key desc\n"
                        + "                ) ";
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(ddl);
        }
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
        assertNotNull(admin.getTableDescriptor(Bytes.toBytes(tableName)));
        HColumnDescriptor[] columnFamilies =
                admin.getTableDescriptor(Bytes.toBytes(tableName)).getColumnFamilies();
        assertEquals(BloomType.NONE, columnFamilies[0].getBloomFilterType());

        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(ddl);
            fail();
        } catch (TableAlreadyExistsException e) {
            // expected
        }
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute("DROP TABLE " + tableName);
        }

        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute("CREATE SCHEMA " + schemaName);
        }
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(ddl);
            assertNotEquals(null, admin.getTableDescriptor(
                SchemaUtil.getPhysicalTableName(tableName.getBytes(), true).getName()));
        } finally {
            admin.close();
        }
        props.setProperty(QueryServices.DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testCreateMultiTenantTable() throws Exception {
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE  " + tableName
                        + " (                TenantId UNSIGNED_INT NOT NULL ,\n"
                        + "                Id UNSIGNED_INT NOT NULL ,\n"
                        + "                val VARCHAR ,\n"
                        + "                CONSTRAINT pk PRIMARY KEY(TenantId, Id) \n"
                        + "                ) MULTI_TENANT=true";
        conn.createStatement().execute(ddl);
        conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (TableAlreadyExistsException e) {
            // expected
        }
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("DROP TABLE  " + tableName);
    }

    /**
     * Test that when the ddl only has PK cols, ttl is set.
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs1() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "create table IF NOT EXISTS  " + tableName + "  (" + " id char(1) NOT NULL,"
                        + " col1 integer NOT NULL," + " col2 bigint NOT NULL,"
                        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
                        + " ) TTL=86400, SALT_BUCKETS = 4";
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
        HColumnDescriptor[] columnFamilies =
                admin.getTableDescriptor(Bytes.toBytes(tableName)).getColumnFamilies();
        assertEquals(1, columnFamilies.length);
        assertEquals(86400, columnFamilies[0].getTimeToLive());
    }

    @Test
    public void testCreatingTooManyIndexesIsNotAllowed() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "CREATE TABLE " + tableName + " (\n"
            + "ID VARCHAR(15) PRIMARY KEY,\n"
            + "COL1 BIGINT,"
            + "COL2 BIGINT,"
            + "COL3 BIGINT,"
            + "COL4 BIGINT) ";
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        
        int maxIndexes = conn.unwrap(PhoenixConnection.class).getQueryServices().getProps().getInt(
        		QueryServices.MAX_INDEXES_PER_TABLE, QueryServicesOptions.DEFAULT_MAX_INDEXES_PER_TABLE);

        // Use local indexes since there's only one physical table for all of them.
        for (int i = 0; i < maxIndexes; i++) {
            conn.createStatement().execute("CREATE LOCAL INDEX I_" + i + tableName + " ON " + tableName + "(COL1) INCLUDE (COL2,COL3,COL4)");
        }
        
        // here we ensure we get a too many indexes error
        try {
            conn.createStatement().execute("CREATE LOCAL INDEX I_" + maxIndexes + tableName + " ON " + tableName + "(COL1) INCLUDE (COL2,COL3,COL4)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.TOO_MANY_INDEXES.getErrorCode(), e.getErrorCode());
        }
    }

    /**
     * Tests that when: 1) DDL has both pk as well as key value columns 2) Key value columns have
     * different column family names 3) TTL specifier doesn't have column family name. Then: 1)TTL
     * is set. 2)All column families have the same TTL.
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs2() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "create table IF NOT EXISTS  " + tableName + "  (" + " id char(1) NOT NULL,"
                        + " col1 integer NOT NULL," + " b.col2 bigint," + " c.col3 bigint, "
                        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
                        + " ) TTL=86400, SALT_BUCKETS = 4";
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
        HColumnDescriptor[] columnFamilies =
                admin.getTableDescriptor(Bytes.toBytes(tableName)).getColumnFamilies();
        assertEquals(2, columnFamilies.length);
        assertEquals(86400, columnFamilies[0].getTimeToLive());
        assertEquals("B", columnFamilies[0].getNameAsString());
        assertEquals(86400, columnFamilies[1].getTimeToLive());
        assertEquals("C", columnFamilies[1].getNameAsString());
    }

    /**
     * Tests that when: 1) DDL has both pk as well as key value columns 2) Key value columns have
     * both default and explicit column family names 3) TTL specifier doesn't have column family
     * name. Then: 1)TTL is set. 2)All column families have the same TTL.
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs3() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "create table IF NOT EXISTS  " + tableName + "  (" + " id char(1) NOT NULL,"
                        + " col1 integer NOT NULL," + " b.col2 bigint," + " col3 bigint, "
                        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
                        + " ) TTL=86400, SALT_BUCKETS = 4";
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
        HColumnDescriptor[] columnFamilies =
                admin.getTableDescriptor(Bytes.toBytes(tableName)).getColumnFamilies();
        assertEquals(2, columnFamilies.length);
        assertEquals("0", columnFamilies[0].getNameAsString());
        assertEquals(86400, columnFamilies[0].getTimeToLive());
        assertEquals("B", columnFamilies[1].getNameAsString());
        assertEquals(86400, columnFamilies[1].getTimeToLive());
    }

    /**
     * Tests that when: 1) DDL has both pk as well as key value columns 2) Key value columns have
     * both default and explicit column family names 3) Replication scope specifier has the explicit
     * column family name. Then: 1)REPLICATION_SCOPE is set. 2)The default column family has
     * DEFAULT_REPLICATION_SCOPE. 3)The explicit column family has the REPLICATION_SCOPE specified
     * in DDL.
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs4() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "create table IF NOT EXISTS  " + tableName + "  (" + " id char(1) NOT NULL,"
                        + " col1 integer NOT NULL," + " b.col2 bigint," + " col3 bigint, "
                        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
                        + " ) b.REPLICATION_SCOPE=1, SALT_BUCKETS = 4";
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
        HColumnDescriptor[] columnFamilies =
                admin.getTableDescriptor(Bytes.toBytes(tableName)).getColumnFamilies();
        assertEquals(2, columnFamilies.length);
        assertEquals("0", columnFamilies[0].getNameAsString());
        assertEquals(DEFAULT_REPLICATION_SCOPE, columnFamilies[0].getScope());
        assertEquals("B", columnFamilies[1].getNameAsString());
        assertEquals(1, columnFamilies[1].getScope());
    }

    /**
     * Tests that when: 1) DDL has both pk as well as key value columns 2) Key value columns have
     * explicit column family names 3) Different REPLICATION_SCOPE specifiers for different column
     * family names. Then: 1)REPLICATION_SCOPE is set. 2)Each explicit column family has the
     * REPLICATION_SCOPE as specified in DDL.
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs5() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "create table IF NOT EXISTS  " + tableName + "  (" + " id char(1) NOT NULL,"
                        + " col1 integer NOT NULL," + " b.col2 bigint," + " c.col3 bigint, "
                        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
                        + " ) b.REPLICATION_SCOPE=0, c.REPLICATION_SCOPE=1, SALT_BUCKETS = 4";
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
        HColumnDescriptor[] columnFamilies =
                admin.getTableDescriptor(Bytes.toBytes(tableName)).getColumnFamilies();
        assertEquals(2, columnFamilies.length);
        assertEquals("B", columnFamilies[0].getNameAsString());
        assertEquals(0, columnFamilies[0].getScope());
        assertEquals("C", columnFamilies[1].getNameAsString());
        assertEquals(1, columnFamilies[1].getScope());
    }

    /**
     * Tests that when: 1) DDL has both pk as well as key value columns 2) There is a default column
     * family specified. Then: 1)TTL is set for the specified default column family.
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs6() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "create table IF NOT EXISTS  " + tableName + "  (" + " id char(1) NOT NULL,"
                        + " col1 integer NOT NULL," + " col2 bigint," + " col3 bigint, "
                        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
                        + " ) DEFAULT_COLUMN_FAMILY='a', TTL=10000, SALT_BUCKETS = 4";
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
        HColumnDescriptor[] columnFamilies =
                admin.getTableDescriptor(Bytes.toBytes(tableName)).getColumnFamilies();
        assertEquals(1, columnFamilies.length);
        assertEquals("a", columnFamilies[0].getNameAsString());
        assertEquals(10000, columnFamilies[0].getTimeToLive());
    }

    /**
     * Tests that when: 1) DDL has only pk columns 2) There is a default column family specified.
     * Then: 1)TTL is set for the specified default column family.
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs7() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "create table IF NOT EXISTS  " + tableName + "  (" + " id char(1) NOT NULL,"
                        + " col1 integer NOT NULL," + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
                        + " ) DEFAULT_COLUMN_FAMILY='a', TTL=10000, SALT_BUCKETS = 4";
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
        HColumnDescriptor[] columnFamilies =
                admin.getTableDescriptor(Bytes.toBytes(tableName)).getColumnFamilies();
        assertEquals(1, columnFamilies.length);
        assertEquals("a", columnFamilies[0].getNameAsString());
        assertEquals(10000, columnFamilies[0].getTimeToLive());
    }

    @Test
    public void testCreateTableColumnFamilyHBaseAttribs8() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "create table IF NOT EXISTS  " + tableName + "  (" + " id char(1) NOT NULL,"
                        + " col1 integer NOT NULL," + " col2 bigint NOT NULL,"
                        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
                        + " ) BLOOMFILTER = 'ROW', SALT_BUCKETS = 4";
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
        HColumnDescriptor[] columnFamilies =
                admin.getTableDescriptor(Bytes.toBytes(tableName)).getColumnFamilies();
        assertEquals(BloomType.ROW, columnFamilies[0].getBloomFilterType());
    }

    /**
     * Test to ensure that NOT NULL constraint isn't added to a non primary key column.
     * @throws Exception
     */
    @Test
    public void testNotNullConstraintForNonPKColumn() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "CREATE TABLE IF NOT EXISTS " + tableName + " ( "
                        + " ORGANIZATION_ID CHAR(15) NOT NULL, "
                        + " EVENT_TIME DATE NOT NULL, USER_ID CHAR(15) NOT NULL, "
                        + " ENTRY_POINT_ID CHAR(15) NOT NULL, ENTRY_POINT_TYPE CHAR(2) NOT NULL , "
                        + " APEX_LIMIT_ID CHAR(15) NOT NULL,  USERNAME CHAR(80),  "
                        + " NAMESPACE_PREFIX VARCHAR, ENTRY_POINT_NAME VARCHAR  NOT NULL , "
                        + " EXECUTION_UNIT_NO VARCHAR, LIMIT_TYPE VARCHAR, "
                        + " LIMIT_VALUE DOUBLE  " + " CONSTRAINT PK PRIMARY KEY ("
                        + "     ORGANIZATION_ID, EVENT_TIME,USER_ID,ENTRY_POINT_ID, ENTRY_POINT_TYPE, APEX_LIMIT_ID "
                        + " ) ) VERSIONS=1";

        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(ddl);
            fail(" Non pk column ENTRY_POINT_NAME has a NOT NULL constraint");
        } catch (SQLException sqle) {
            assertEquals(SQLExceptionCode.INVALID_NOT_NULL_CONSTRAINT.getErrorCode(),
                sqle.getErrorCode());
        }
    }

    @Test
    public void testNotNullConstraintForWithSinglePKCol() throws Exception {
        String tableName = generateUniqueName();
        String ddl = "create table  " + tableName + " (k integer primary key, v bigint not null)";

        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(ddl);
            fail(" Non pk column V has a NOT NULL constraint");
        } catch (SQLException sqle) {
            assertEquals(SQLExceptionCode.INVALID_NOT_NULL_CONSTRAINT.getErrorCode(),
                sqle.getErrorCode());
        }
    }

    @Test
    public void testSpecifyingColumnFamilyForTTLFails() throws Exception {
        String tableName = generateUniqueName();
        String ddl =
                "create table IF NOT EXISTS  " + tableName + "  (" + " id char(1) NOT NULL,"
                        + " col1 integer NOT NULL," + " CF.col2 integer,"
                        + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
                        + " ) DEFAULT_COLUMN_FAMILY='a', CF.TTL=10000, SALT_BUCKETS = 4";
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(ddl);
        } catch (SQLException sqle) {
            assertEquals(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_TTL.getErrorCode(),
                sqle.getErrorCode());
        }
    }

    @Test
    public void testCreateTableWithoutSchema() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        String schemaName = generateUniqueName();
        String createSchemaDDL = "CREATE SCHEMA " + schemaName;
        ;
        String tableName = generateUniqueName();
        String createTableDDL =
                "CREATE TABLE " + schemaName + "." + tableName + " (pk INTEGER PRIMARY KEY)";
        String dropTableDDL = "DROP TABLE " + schemaName + "." + tableName;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            try {
                conn.createStatement().execute(createTableDDL);
                fail();
            } catch (SchemaNotFoundException snfe) {
                // expected
            }
            conn.createStatement().execute(createSchemaDDL);
        }
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(createTableDDL);
        }
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(dropTableDDL);
        }
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(false));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(createTableDDL);
        } catch (SchemaNotFoundException e) {
            fail();
        }
    }

    @Test
    public void testCreateTableIfNotExistsForEncodedColumnNames() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        String tableName = generateUniqueName();
        String createTableDDL =
                "CREATE TABLE IF NOT EXISTS " + tableName + " (pk INTEGER PRIMARY KEY)";
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(createTableDDL);
            assertColumnEncodingMetadata(QualifierEncodingScheme.TWO_BYTE_QUALIFIERS,
                ImmutableStorageScheme.ONE_CELL_PER_COLUMN, tableName, conn);
        }
        // Execute the ddl again
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(createTableDDL);
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertFalse(rs.next());
            assertColumnEncodingMetadata(QualifierEncodingScheme.TWO_BYTE_QUALIFIERS,
                ImmutableStorageScheme.ONE_CELL_PER_COLUMN, tableName, conn);
        }
        // Now execute the ddl with a different COLUMN_ENCODED_BYTES. This shouldn't change the
        // original encoded bytes setting.
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(createTableDDL + " COLUMN_ENCODED_BYTES = 1");
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertFalse(rs.next());
            assertColumnEncodingMetadata(QualifierEncodingScheme.TWO_BYTE_QUALIFIERS,
                ImmutableStorageScheme.ONE_CELL_PER_COLUMN, tableName, conn);
        }
        // Now execute the ddl where COLUMN_ENCODED_BYTES=0. This shouldn't change the original
        // encoded bytes setting.
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(createTableDDL + " COLUMN_ENCODED_BYTES = 0");
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertFalse(rs.next());
            assertColumnEncodingMetadata(QualifierEncodingScheme.TWO_BYTE_QUALIFIERS,
                ImmutableStorageScheme.ONE_CELL_PER_COLUMN, tableName, conn);
        }

    }

    private void assertColumnEncodingMetadata(QualifierEncodingScheme expectedEncodingScheme,
            ImmutableStorageScheme expectedStorageScheme, String tableName, Connection conn)
            throws Exception {
        PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
        PTable table = phxConn.getTable(new PTableKey(null, tableName));
        assertEquals(expectedEncodingScheme, table.getEncodingScheme());
        assertEquals(expectedStorageScheme, table.getImmutableStorageScheme());
    }

    @Test
    public void testMultiTenantImmutableTableMetadata() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        String nonEncodedOneCellPerColumnMultiTenantTable = generateUniqueName();
        String twoByteQualifierEncodedOneCellPerColumnMultiTenantTable = generateUniqueName();
        String oneByteQualifierEncodedOneCellPerColumnMultiTenantTable = generateUniqueName();
        String twoByteQualifierSingleCellArrayWithOffsetsMultitenantTable = generateUniqueName();
        String oneByteQualifierSingleCellArrayWithOffsetsMultitenantTable = generateUniqueName();
        String createTableDDL;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTableDDL =
                    "create IMMUTABLE TABLE " + nonEncodedOneCellPerColumnMultiTenantTable + " ("
                            + " id char(1) NOT NULL," + " col1 integer NOT NULL,"
                            + " col2 bigint NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)) MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0";
            conn.createStatement().execute(createTableDDL);
            assertColumnEncodingMetadata(QualifierEncodingScheme.NON_ENCODED_QUALIFIERS,
                ImmutableStorageScheme.ONE_CELL_PER_COLUMN,
                nonEncodedOneCellPerColumnMultiTenantTable, conn);
        }
        props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTableDDL =
                    "create IMMUTABLE table "
                            + twoByteQualifierEncodedOneCellPerColumnMultiTenantTable + " ("
                            + " id char(1) NOT NULL," + " col1 integer NOT NULL,"
                            + " col2 bigint NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)) MULTI_TENANT=true";
            conn.createStatement().execute(createTableDDL);
            assertColumnEncodingMetadata(QualifierEncodingScheme.TWO_BYTE_QUALIFIERS,
                ImmutableStorageScheme.ONE_CELL_PER_COLUMN,
                twoByteQualifierEncodedOneCellPerColumnMultiTenantTable, conn);
        }
        props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTableDDL =
                    "create IMMUTABLE table "
                            + oneByteQualifierEncodedOneCellPerColumnMultiTenantTable + " ("
                            + " id char(1) NOT NULL," + " col1 integer NOT NULL,"
                            + " col2 bigint NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)) MULTI_TENANT=true, COLUMN_ENCODED_BYTES = 1";
            conn.createStatement().execute(createTableDDL);
            assertColumnEncodingMetadata(QualifierEncodingScheme.ONE_BYTE_QUALIFIERS,
                ImmutableStorageScheme.ONE_CELL_PER_COLUMN,
                oneByteQualifierEncodedOneCellPerColumnMultiTenantTable, conn);
        }
        props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTableDDL =
                    "create IMMUTABLE table "
                            + twoByteQualifierSingleCellArrayWithOffsetsMultitenantTable + " ("
                            + " id char(1) NOT NULL," + " col1 integer NOT NULL,"
                            + " col2 bigint NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)) MULTI_TENANT=true, IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS";
            conn.createStatement().execute(createTableDDL);
            assertColumnEncodingMetadata(QualifierEncodingScheme.TWO_BYTE_QUALIFIERS,
                ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS,
                twoByteQualifierSingleCellArrayWithOffsetsMultitenantTable, conn);
        }
        props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTableDDL =
                    "create IMMUTABLE table "
                            + oneByteQualifierSingleCellArrayWithOffsetsMultitenantTable + " ("
                            + " id char(1) NOT NULL," + " col1 integer NOT NULL,"
                            + " col2 bigint NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)) MULTI_TENANT=true, IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=1";
            conn.createStatement().execute(createTableDDL);
            assertColumnEncodingMetadata(QualifierEncodingScheme.ONE_BYTE_QUALIFIERS,
                ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS,
                oneByteQualifierSingleCellArrayWithOffsetsMultitenantTable, conn);

        }
    }

    @Test
    public void testCreateTableWithUpdateCacheFrequencyAttrib() throws Exception {
        Connection connection = null;
        String tableName = generateUniqueName();
        try {
            Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
            connection = DriverManager.getConnection(getUrl(), props);

            // Assert update cache frequency to default value zero
            connection.createStatement().execute(
                "create table " + tableName + " (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
            String readSysCatQuery =
                    "select TABLE_NAME,UPDATE_CACHE_FREQUENCY from SYSTEM.CATALOG where "
                            + "TABLE_NAME = '" + tableName + "'  AND TABLE_TYPE='u'";
            ResultSet rs = connection.createStatement().executeQuery(readSysCatQuery);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getLong(2));
            connection.createStatement().execute("drop table " + tableName);
            connection.close();

            // Assert update cache frequency to configured default value 10sec
            int defaultUpdateCacheFrequency = 10000;
            props.put(QueryServices.DEFAULT_UPDATE_CACHE_FREQUENCY_ATRRIB,
                "" + defaultUpdateCacheFrequency);
            connection = DriverManager.getConnection(getUrl(), props);
            connection.createStatement().execute(
                "create table " + tableName + " (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR)");
            rs = connection.createStatement().executeQuery(readSysCatQuery);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(defaultUpdateCacheFrequency, rs.getLong(2));
            connection.createStatement().execute("drop table " + tableName);

            // Assert update cache frequency to table specific value 30sec
            int tableSpecificUpdateCacheFrequency = 30000;
            connection.createStatement()
                    .execute("create table " + tableName
                            + " (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) "
                            + "UPDATE_CACHE_FREQUENCY=" + tableSpecificUpdateCacheFrequency);
            rs = connection.createStatement().executeQuery(readSysCatQuery);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(tableSpecificUpdateCacheFrequency, rs.getLong(2));
        } finally {
            if (connection != null) {
                connection.createStatement().execute("drop table if exists " + tableName);
                connection.close();
            }
        }
    }

    @Test
    public void testCreateTableWithNamespaceMappingEnabled() throws Exception {
        final String NS = "NS_" + generateUniqueName();
        final String TBL = "TBL_" + generateUniqueName();
        final String CF = "CF";

        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());

        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute("CREATE SCHEMA " + NS);

            // test for a table that is in non-default schema
            {
                String table = NS + "." + TBL;
                conn.createStatement().execute(
                    "CREATE TABLE " + table + " (PK VARCHAR PRIMARY KEY, " + CF + ".COL VARCHAR)");

                assertTrue(QueryUtil
                        .getExplainPlan(
                            conn.createStatement().executeQuery("explain select * from " + table))
                        .contains(NS + ":" + TBL));

                conn.createStatement().execute("DROP TABLE " + table);
            }

            // test for a table whose name contains a dot (e.g. "AAA.BBB") in default schema
            {
                String table = "\"" + NS + "." + TBL + "\"";
                conn.createStatement().execute(
                    "CREATE TABLE " + table + " (PK VARCHAR PRIMARY KEY, " + CF + ".COL VARCHAR)");

                assertTrue(QueryUtil
                        .getExplainPlan(
                            conn.createStatement().executeQuery("explain select * from " + table))
                        .contains(NS + "." + TBL));

                conn.createStatement().execute("DROP TABLE " + table);
            }

            // test for a view whose name contains a dot (e.g. "AAA.BBB") in non-default schema
            {
                String table = NS + ".\"" + NS + "." + TBL + "\"";
                conn.createStatement().execute(
                    "CREATE TABLE " + table + " (PK VARCHAR PRIMARY KEY, " + CF + ".COL VARCHAR)");

                assertTrue(QueryUtil
                        .getExplainPlan(
                            conn.createStatement().executeQuery("explain select * from " + table))
                        .contains(NS + ":" + NS + "." + TBL));

                conn.createStatement().execute("DROP TABLE " + table);
            }

            conn.createStatement().execute("DROP SCHEMA " + NS);
        }
    }

    @Test
    public void testSetHTableDescriptorPropertyOnView() throws Exception {
        Properties props = new Properties();
        final String dataTableFullName = generateUniqueName();
        String ddl =
                "CREATE TABLE " + dataTableFullName + " (\n" + "ID1 VARCHAR(15) NOT NULL,\n"
                        + "ID2 VARCHAR(15) NOT NULL,\n" + "CREATED_DATE DATE,\n"
                        + "CREATION_TIME BIGINT,\n" + "LAST_USED DATE,\n"
                        + "CONSTRAINT PK PRIMARY KEY (ID1, ID2)) ";
        Connection conn1 = DriverManager.getConnection(getUrl(), props);
        conn1.createStatement().execute(ddl);
        conn1.close();
        final String viewFullName = generateUniqueName();
        Connection conn2 = DriverManager.getConnection(getUrl(), props);
        ddl =
                "CREATE VIEW " + viewFullName + " AS SELECT * FROM " + dataTableFullName
                        + " WHERE CREATION_TIME = 1 THROW_INDEX_WRITE_FAILURE = FALSE";
        try {
            conn2.createStatement().execute(ddl);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.VIEW_WITH_PROPERTIES.getErrorCode(), e.getErrorCode());
        }
        conn2.close();
    }

    @Test
    public void testSettingGuidePostWidth() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String dataTable = generateUniqueName();
            int guidePostWidth = 20;
            String ddl =
                    "CREATE TABLE " + dataTable + " (k INTEGER PRIMARY KEY, a bigint, b bigint)"
                            + " GUIDE_POSTS_WIDTH=" + guidePostWidth;
            conn.createStatement().execute(ddl);
            assertEquals(20, checkGuidePostWidth(dataTable));
            String viewName = "V_" + generateUniqueName();
            ddl =
                    "CREATE VIEW " + viewName + " AS SELECT * FROM " + dataTable
                            + " GUIDE_POSTS_WIDTH=" + guidePostWidth;
            try {
                conn.createStatement().execute(ddl);
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_SET_GUIDE_POST_WIDTH.getErrorCode(),
                    e.getErrorCode());
            }

            // let the view creation go through
            ddl = "CREATE VIEW " + viewName + " AS SELECT * FROM " + dataTable;
            conn.createStatement().execute(ddl);

            String globalIndex = "GI_" + generateUniqueName();
            ddl =
                    "CREATE INDEX " + globalIndex + " ON " + dataTable
                            + "(a) INCLUDE (b) GUIDE_POSTS_WIDTH = " + guidePostWidth;
            try {
                conn.createStatement().execute(ddl);
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_SET_GUIDE_POST_WIDTH.getErrorCode(),
                    e.getErrorCode());
            }
            String localIndex = "LI_" + generateUniqueName();
            ddl =
                    "CREATE LOCAL INDEX " + localIndex + " ON " + dataTable
                            + "(b) INCLUDE (a) GUIDE_POSTS_WIDTH = " + guidePostWidth;
            try {
                conn.createStatement().execute(ddl);
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_SET_GUIDE_POST_WIDTH.getErrorCode(),
                    e.getErrorCode());
            }
            String viewIndex = "VI_" + generateUniqueName();
            ddl =
                    "CREATE LOCAL INDEX " + viewIndex + " ON " + dataTable
                            + "(b) INCLUDE (a) GUIDE_POSTS_WIDTH = " + guidePostWidth;
            try {
                conn.createStatement().execute(ddl);
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_SET_GUIDE_POST_WIDTH.getErrorCode(),
                    e.getErrorCode());
            }
        }
    }

    private int checkGuidePostWidth(String tableName) throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String query =
                    "SELECT GUIDE_POSTS_WIDTH FROM SYSTEM.CATALOG WHERE TABLE_NAME = ? AND COLUMN_FAMILY IS NULL AND COLUMN_NAME IS NULL";
            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setString(1, tableName);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            return rs.getInt(1);
        }
    }

}
