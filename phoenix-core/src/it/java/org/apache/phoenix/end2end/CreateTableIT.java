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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.NewerTableAlreadyExistsException;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.schema.PTable.QualifierEncodingScheme;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.SchemaNotFoundException;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


public class CreateTableIT extends BaseClientManagedTimeIT {
    
    @Test
    public void testStartKeyStopKey() throws SQLException {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("CREATE TABLE start_stop_test (pk char(2) not null primary key) SPLIT ON ('EA','EZ')");
        conn.close();
        
        String query = "select count(*) from start_stop_test where pk >= 'EA' and pk < 'EZ'";
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 2));
        conn = DriverManager.getConnection(getUrl(), props);
        Statement statement = conn.createStatement();
        statement.execute(query);
        PhoenixStatement pstatement = statement.unwrap(PhoenixStatement.class);
        List<KeyRange>splits = pstatement.getQueryPlan().getSplits();
        assertTrue(splits.size() > 0);
    }
    
    @Test
    public void testCreateTable() throws Exception {
        long ts = nextTimestamp();
        String schemaName = "TEST";
        String tableName = schemaName + ".M_INTERFACE_JOB";
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        
        String ddl = "CREATE TABLE " + tableName + "(                data.addtime VARCHAR ,\n" + 
                "                data.dir VARCHAR ,\n" + 
                "                data.end_time VARCHAR ,\n" + 
                "                data.file VARCHAR ,\n" + 
                "                data.fk_log VARCHAR ,\n" + 
                "                data.host VARCHAR ,\n" + 
                "                data.r VARCHAR ,\n" + 
                "                data.size VARCHAR ,\n" + 
                "                data.start_time VARCHAR ,\n" + 
                "                data.stat_date DATE ,\n" + 
                "                data.stat_hour VARCHAR ,\n" + 
                "                data.stat_minute VARCHAR ,\n" + 
                "                data.state VARCHAR ,\n" + 
                "                data.title VARCHAR ,\n" + 
                "                data.user VARCHAR ,\n" + 
                "                data.inrow VARCHAR ,\n" + 
                "                data.jobid VARCHAR ,\n" + 
                "                data.jobtype VARCHAR ,\n" + 
                "                data.level VARCHAR ,\n" + 
                "                data.msg VARCHAR ,\n" + 
                "                data.outrow VARCHAR ,\n" + 
                "                data.pass_time VARCHAR ,\n" + 
                "                data.type VARCHAR ,\n" + 
                "                id INTEGER not null primary key desc\n" + 
                "                ) ";
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(ddl);
        }
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
        assertNotNull(admin.getTableDescriptor(Bytes.toBytes(tableName)));
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(ddl);
            fail();
        } catch (TableAlreadyExistsException e) {
            // expected
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute("DROP TABLE " + tableName);
        }

        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 30));
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute("CREATE SCHEMA " + schemaName);
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 40));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(ddl);
            assertNotEquals(null,
                    admin.getTableDescriptor(SchemaUtil.getPhysicalTableName(tableName.getBytes(), true).getName()));
        } finally {
            admin.close();
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 50));
        props.setProperty(QueryServices.DROP_METADATA_ATTRIB, Boolean.TRUE.toString());
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testCreateMultiTenantTable() throws Exception {
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        String ddl = "CREATE TABLE m_multi_tenant_test(                TenantId UNSIGNED_INT NOT NULL ,\n" +
                "                Id UNSIGNED_INT NOT NULL ,\n" +
                "                val VARCHAR ,\n" +
                "                CONSTRAINT pk PRIMARY KEY(TenantId, Id) \n" +
                "                ) MULTI_TENANT=true";
        conn.createStatement().execute(ddl);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
        conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (TableAlreadyExistsException e) {
            // expected
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 20));
        conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("DROP TABLE m_multi_tenant_test");
    }
    
    /**
     * Test that when the ddl only has PK cols, ttl is set.
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs1() throws Exception {
    	String ddl = "create table IF NOT EXISTS TEST1 ("
    		    + " id char(1) NOT NULL,"
    		    + " col1 integer NOT NULL,"
    		    + " col2 bigint NOT NULL,"
    		    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)"
    		    + " ) TTL=86400, SALT_BUCKETS = 4";
    	long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.createStatement().execute(ddl);
    	HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
    	HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("TEST1")).getColumnFamilies();
    	assertEquals(1, columnFamilies.length);
    	assertEquals(86400, columnFamilies[0].getTimeToLive());
    }
    
    /**
     * Tests that when:
     * 1) DDL has both pk as well as key value columns
     * 2) Key value columns have different column family names
     * 3) TTL specifier doesn't have column family name.
     * 
     * Then:
     * 1)TTL is set.
     * 2)All column families have the same TTL.  
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs2() throws Exception {
    	String ddl = "create table IF NOT EXISTS TEST2 ("
    			+ " id char(1) NOT NULL,"
    			+ " col1 integer NOT NULL,"
    			+ " b.col2 bigint,"
    			+ " c.col3 bigint, "
    			+ " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
    			+ " ) TTL=86400, SALT_BUCKETS = 4";
    	long ts = nextTimestamp();
    	Properties props = new Properties();
    	props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.createStatement().execute(ddl);
    	HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
    	HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("TEST2")).getColumnFamilies();
    	assertEquals(2, columnFamilies.length);
    	assertEquals(86400, columnFamilies[0].getTimeToLive());
    	assertEquals("B", columnFamilies[0].getNameAsString());
    	assertEquals(86400, columnFamilies[1].getTimeToLive());
    	assertEquals("C", columnFamilies[1].getNameAsString());
    }
    
    /**
     * Tests that when:
     * 1) DDL has both pk as well as key value columns
     * 2) Key value columns have both default and explicit column family names
     * 3) TTL specifier doesn't have column family name.
     * 
     * Then:
     * 1)TTL is set.
     * 2)All column families have the same TTL.  
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs3() throws Exception {
    	String ddl = "create table IF NOT EXISTS TEST3 ("
    			+ " id char(1) NOT NULL,"
    			+ " col1 integer NOT NULL,"
    			+ " b.col2 bigint,"
    			+ " col3 bigint, "
    			+ " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
    			+ " ) TTL=86400, SALT_BUCKETS = 4";
    	long ts = nextTimestamp();
    	Properties props = new Properties();
    	props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.createStatement().execute(ddl);
    	HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
    	HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("TEST3")).getColumnFamilies();
    	assertEquals(2, columnFamilies.length);
    	assertEquals("0", columnFamilies[0].getNameAsString());
    	assertEquals(86400, columnFamilies[0].getTimeToLive());
    	assertEquals("B", columnFamilies[1].getNameAsString());
    	assertEquals(86400, columnFamilies[1].getTimeToLive());
    }
    
    /**
     * Tests that when:
     * 1) DDL has both pk as well as key value columns
     * 2) Key value columns have both default and explicit column family names
     * 3) Replication scope specifier has the explicit column family name.
     * 
     * Then:
     * 1)REPLICATION_SCOPE is set.
     * 2)The default column family has DEFAULT_REPLICATION_SCOPE.
     * 3)The explicit column family has the REPLICATION_SCOPE specified in DDL.  
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs4() throws Exception {
    	String ddl = "create table IF NOT EXISTS TEST4 ("
    			+ " id char(1) NOT NULL,"
    			+ " col1 integer NOT NULL,"
    			+ " b.col2 bigint,"
    			+ " col3 bigint, "
    			+ " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
    			+ " ) b.REPLICATION_SCOPE=1, SALT_BUCKETS = 4";
    	long ts = nextTimestamp();
    	Properties props = new Properties();
    	props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.createStatement().execute(ddl);
    	HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
    	HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("TEST4")).getColumnFamilies();
    	assertEquals(2, columnFamilies.length);
    	assertEquals("0", columnFamilies[0].getNameAsString());
    	assertEquals(DEFAULT_REPLICATION_SCOPE, columnFamilies[0].getScope());
    	assertEquals("B", columnFamilies[1].getNameAsString());
    	assertEquals(1, columnFamilies[1].getScope());
    }
    
    /**
     * Tests that when:
     * 1) DDL has both pk as well as key value columns
     * 2) Key value columns have explicit column family names
     * 3) Different REPLICATION_SCOPE specifiers for different column family names.
     * 
     * Then:
     * 1)REPLICATION_SCOPE is set.
     * 2)Each explicit column family has the REPLICATION_SCOPE as specified in DDL.  
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs5() throws Exception {
    	String ddl = "create table IF NOT EXISTS TEST5 ("
    			+ " id char(1) NOT NULL,"
    			+ " col1 integer NOT NULL,"
    			+ " b.col2 bigint,"
    			+ " c.col3 bigint, "
    			+ " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
    			+ " ) b.REPLICATION_SCOPE=0, c.REPLICATION_SCOPE=1, SALT_BUCKETS = 4";
    	long ts = nextTimestamp();
    	Properties props = new Properties();
    	props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.createStatement().execute(ddl);
    	HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
    	HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("TEST5")).getColumnFamilies();
    	assertEquals(2, columnFamilies.length);
    	assertEquals("B", columnFamilies[0].getNameAsString());
    	assertEquals(0, columnFamilies[0].getScope());
    	assertEquals("C", columnFamilies[1].getNameAsString());
    	assertEquals(1, columnFamilies[1].getScope());
    }
    
    /**
     * Tests that when:
     * 1) DDL has both pk as well as key value columns
     * 2) There is a default column family specified.
     *  
     * Then:
     * 1)TTL is set for the specified default column family.
     * 
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs6() throws Exception {
    	String ddl = "create table IF NOT EXISTS TEST6 ("
    			+ " id char(1) NOT NULL,"
    			+ " col1 integer NOT NULL,"
    			+ " col2 bigint,"
    			+ " col3 bigint, "
    			+ " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
    			+ " ) DEFAULT_COLUMN_FAMILY='a', TTL=10000, SALT_BUCKETS = 4";
    	long ts = nextTimestamp();
    	Properties props = new Properties();
    	props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.createStatement().execute(ddl);
    	HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
    	HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("TEST6")).getColumnFamilies();
    	assertEquals(1, columnFamilies.length);
    	assertEquals("a", columnFamilies[0].getNameAsString());
    	assertEquals(10000, columnFamilies[0].getTimeToLive());
    }
    
    /**
     * Tests that when:
     * 1) DDL has only pk columns
     * 2) There is a default column family specified.
     *  
     * Then:
     * 1)TTL is set for the specified default column family.
     * 
     */
    @Test
    public void testCreateTableColumnFamilyHBaseAttribs7() throws Exception {
    	String ddl = "create table IF NOT EXISTS TEST7 ("
    			+ " id char(1) NOT NULL,"
    			+ " col1 integer NOT NULL,"
    			+ " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
    			+ " ) DEFAULT_COLUMN_FAMILY='a', TTL=10000, SALT_BUCKETS = 4";
    	long ts = nextTimestamp();
    	Properties props = new Properties();
    	props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
    	Connection conn = DriverManager.getConnection(getUrl(), props);
    	conn.createStatement().execute(ddl);
    	HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), props).getAdmin();
    	HColumnDescriptor[] columnFamilies = admin.getTableDescriptor(Bytes.toBytes("TEST7")).getColumnFamilies();
    	assertEquals(1, columnFamilies.length);
    	assertEquals("a", columnFamilies[0].getNameAsString());
    	assertEquals(10000, columnFamilies[0].getTimeToLive());
    }
    
    
    /**
     * Test to ensure that NOT NULL constraint isn't added to a non primary key column.
     * @throws Exception
     */
    @Test
    public void testNotNullConstraintForNonPKColumn() throws Exception {
        
        String ddl = "CREATE TABLE IF NOT EXISTS EVENT.APEX_LIMIT ( " +
                " ORGANIZATION_ID CHAR(15) NOT NULL, " +
                " EVENT_TIME DATE NOT NULL, USER_ID CHAR(15) NOT NULL, " +
                " ENTRY_POINT_ID CHAR(15) NOT NULL, ENTRY_POINT_TYPE CHAR(2) NOT NULL , " +
                " APEX_LIMIT_ID CHAR(15) NOT NULL,  USERNAME CHAR(80),  " +
                " NAMESPACE_PREFIX VARCHAR, ENTRY_POINT_NAME VARCHAR  NOT NULL , " +
                " EXECUTION_UNIT_NO VARCHAR, LIMIT_TYPE VARCHAR, " +
                " LIMIT_VALUE DOUBLE  " +
                " CONSTRAINT PK PRIMARY KEY (" + 
                "     ORGANIZATION_ID, EVENT_TIME,USER_ID,ENTRY_POINT_ID, ENTRY_POINT_TYPE, APEX_LIMIT_ID " +
                " ) ) VERSIONS=1";
                    
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(ddl);    
            fail(" Non pk column ENTRY_POINT_NAME has a NOT NULL constraint");
        } catch( SQLException sqle) {
            assertEquals(SQLExceptionCode.INVALID_NOT_NULL_CONSTRAINT.getErrorCode(),sqle.getErrorCode());
        }
   }

    @Test
    public void testNotNullConstraintForWithSinglePKCol() throws Exception {
        
        String ddl = "create table test.testing(k integer primary key, v bigint not null)";
                    
        Properties props = new Properties();
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(ddl);    
            fail(" Non pk column V has a NOT NULL constraint");
        } catch( SQLException sqle) {
            assertEquals(SQLExceptionCode.INVALID_NOT_NULL_CONSTRAINT.getErrorCode(),sqle.getErrorCode());
        }
   }
    
    @Test
    public void testSpecifyingColumnFamilyForTTLFails() throws Exception {
        String ddl = "create table IF NOT EXISTS TESTXYZ ("
                + " id char(1) NOT NULL,"
                + " col1 integer NOT NULL,"
                + " CF.col2 integer,"
                + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1)"
                + " ) DEFAULT_COLUMN_FAMILY='a', CF.TTL=10000, SALT_BUCKETS = 4";
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute(ddl);
        } catch (SQLException sqle) {
            assertEquals(SQLExceptionCode.COLUMN_FAMILY_NOT_ALLOWED_FOR_TTL.getErrorCode(),sqle.getErrorCode());
        }
    }
    
    @Test
    public void testAlterDeletedTable() throws Exception {
        String ddl = "create table T ("
                + " K varchar primary key,"
                + " V1 varchar)";
        long ts = nextTimestamp();
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute(ddl);
        conn.close();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+50));
        Connection connAt50 = DriverManager.getConnection(getUrl(), props);
        connAt50.createStatement().execute("DROP TABLE T");
        connAt50.close();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+20));
        Connection connAt20 = DriverManager.getConnection(getUrl(), props);
        connAt20.createStatement().execute("UPDATE STATISTICS T"); // Invalidates from cache
        try {
            connAt20.createStatement().execute("ALTER TABLE T ADD V2 VARCHAR");
            fail();
        } catch (NewerTableAlreadyExistsException e) {
            
        }
        connAt20.close();
    }

    @Test
    public void testCreateTableWithoutSchema() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(true));
        String createSchemaDDL = "CREATE SCHEMA T_SCHEMA";
        String createTableDDL = "CREATE TABLE T_SCHEMA.TEST(pk INTEGER PRIMARY KEY)";
        String dropTableDDL = "DROP TABLE T_SCHEMA.TEST";
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            try {
                conn.createStatement().execute(createTableDDL);
                fail();
            } catch (SchemaNotFoundException snfe) {
                //expected
            }
            conn.createStatement().execute(createSchemaDDL);
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+10));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(createTableDDL);
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+20));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(dropTableDDL);
        }
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts+30));
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(false));
        try (Connection conn = DriverManager.getConnection(getUrl(), props);) {
            conn.createStatement().execute(createTableDDL);
        } catch (SchemaNotFoundException e) {
            fail();
        }
    }
    
    @Test
    public void testCreateTableIfNotExistsForEncodedColumnNames() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        String tableName = "testCreateTableIfNotExistsForEncodedColumnNames".toUpperCase();
        String createTableDDL = "CREATE TABLE IF NOT EXISTS " + tableName + " (pk INTEGER PRIMARY KEY)";
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(createTableDDL);
            assertColumnEncodingMetadata(QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, ImmutableStorageScheme.ONE_CELL_PER_COLUMN, tableName, conn);
        }
        // Execute the ddl again
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(createTableDDL);
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertFalse(rs.next());
            assertColumnEncodingMetadata(QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, ImmutableStorageScheme.ONE_CELL_PER_COLUMN, tableName, conn);
        }
        // Now execute the ddl with a different COLUMN_ENCODED_BYTES. This shouldn't change the original encoded bytes setting.
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(createTableDDL + " COLUMN_ENCODED_BYTES = 1");
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertFalse(rs.next());
            assertColumnEncodingMetadata(QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, ImmutableStorageScheme.ONE_CELL_PER_COLUMN, tableName, conn);
        }
        // Now execute the ddl where COLUMN_ENCODED_BYTES=0. This shouldn't change the original encoded bytes setting.
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            conn.createStatement().execute(createTableDDL + " COLUMN_ENCODED_BYTES = 0");
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM " + tableName);
            assertFalse(rs.next());
            assertColumnEncodingMetadata(QualifierEncodingScheme.TWO_BYTE_QUALIFIERS, ImmutableStorageScheme.ONE_CELL_PER_COLUMN, tableName, conn);
        }

    }

    private void assertColumnEncodingMetadata(QualifierEncodingScheme expectedEncodingScheme,
            ImmutableStorageScheme expectedStorageScheme, String tableName,
            Connection conn) throws Exception {
        PhoenixConnection phxConn = conn.unwrap(PhoenixConnection.class);
        PTable table = phxConn.getTable(new PTableKey(null, tableName));
        assertEquals(expectedEncodingScheme, table.getEncodingScheme());
        assertEquals(expectedStorageScheme, table.getImmutableStorageScheme());
    }
    
    @Test
    public void testMultiTenantImmutableTableMetadata() throws Exception {
        long ts = nextTimestamp();
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        String nonEncodedOneCellPerColumnMultiTenantTable = "nonEncodedOneCellPerColumnMultiTenantTable".toUpperCase();
        String twoByteQualifierEncodedOneCellPerColumnMultiTenantTable = "twoByteQualifierEncodedOneCellPerColumnMultiTenantTable"
                .toUpperCase();
        String oneByteQualifierEncodedOneCellPerColumnMultiTenantTable = "oneByteQualifierEncodedOneCellPerColumnMultiTenantTable"
                .toUpperCase();
        String twoByteQualifierSingleCellArrayWithOffsetsMultitenantTable = "twoByteQualifierSingleCellArrayWithOffsetsMultitenantTable"
                .toUpperCase();
        String oneByteQualifierSingleCellArrayWithOffsetsMultitenantTable = "oneByteQualifierSingleCellArrayWithOffsetsMultitenantTable"
                .toUpperCase();
        String createTableDDL;
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTableDDL = "create IMMUTABLE TABLE " + nonEncodedOneCellPerColumnMultiTenantTable + " ("
                    + " id char(1) NOT NULL," + " col1 integer NOT NULL," + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)) MULTI_TENANT=true, COLUMN_ENCODED_BYTES=0";
            conn.createStatement().execute(createTableDDL);
            assertColumnEncodingMetadata(QualifierEncodingScheme.NON_ENCODED_QUALIFIERS,
                    ImmutableStorageScheme.ONE_CELL_PER_COLUMN, nonEncodedOneCellPerColumnMultiTenantTable, conn);
        }
        ts = nextTimestamp();
        props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTableDDL = "create IMMUTABLE table " + twoByteQualifierEncodedOneCellPerColumnMultiTenantTable + " ("
                    + " id char(1) NOT NULL," + " col1 integer NOT NULL," + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)) MULTI_TENANT=true";
            conn.createStatement().execute(createTableDDL);
            assertColumnEncodingMetadata(QualifierEncodingScheme.TWO_BYTE_QUALIFIERS,
                    ImmutableStorageScheme.ONE_CELL_PER_COLUMN,
                    twoByteQualifierEncodedOneCellPerColumnMultiTenantTable, conn);
        }
        ts = nextTimestamp();
        props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTableDDL = "create IMMUTABLE table " + oneByteQualifierEncodedOneCellPerColumnMultiTenantTable + " ("
                    + " id char(1) NOT NULL," + " col1 integer NOT NULL," + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)) MULTI_TENANT=true, COLUMN_ENCODED_BYTES = 1";
            conn.createStatement().execute(createTableDDL);
            assertColumnEncodingMetadata(QualifierEncodingScheme.ONE_BYTE_QUALIFIERS,
                    ImmutableStorageScheme.ONE_CELL_PER_COLUMN,
                    oneByteQualifierEncodedOneCellPerColumnMultiTenantTable, conn);
        }
        ts = nextTimestamp();
        props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTableDDL = "create IMMUTABLE table "
                    + twoByteQualifierSingleCellArrayWithOffsetsMultitenantTable
                    + " ("
                    + " id char(1) NOT NULL,"
                    + " col1 integer NOT NULL,"
                    + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)) MULTI_TENANT=true, IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS";
            conn.createStatement().execute(createTableDDL);
            assertColumnEncodingMetadata(QualifierEncodingScheme.TWO_BYTE_QUALIFIERS,
                    ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS,
                    twoByteQualifierSingleCellArrayWithOffsetsMultitenantTable, conn);
        }
        ts = nextTimestamp();
        props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            createTableDDL = "create IMMUTABLE table "
                    + oneByteQualifierSingleCellArrayWithOffsetsMultitenantTable
                    + " ("
                    + " id char(1) NOT NULL,"
                    + " col1 integer NOT NULL,"
                    + " col2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (id, col1, col2)) MULTI_TENANT=true, IMMUTABLE_STORAGE_SCHEME=SINGLE_CELL_ARRAY_WITH_OFFSETS, COLUMN_ENCODED_BYTES=1";
            conn.createStatement().execute(createTableDDL);
            assertColumnEncodingMetadata(QualifierEncodingScheme.ONE_BYTE_QUALIFIERS,
                    ImmutableStorageScheme.SINGLE_CELL_ARRAY_WITH_OFFSETS,
                    oneByteQualifierSingleCellArrayWithOffsetsMultitenantTable, conn);

        }
    }
}
