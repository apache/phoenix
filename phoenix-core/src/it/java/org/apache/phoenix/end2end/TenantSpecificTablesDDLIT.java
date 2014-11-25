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

import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_DEFINE_PK_FOR_VIEW;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_DROP_PK;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_MODIFY_VIEW_PK;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_MUTATE_TABLE;
import static org.apache.phoenix.exception.SQLExceptionCode.TABLE_UNDEFINED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.KEY_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TYPE_SEQUENCE;
import static org.apache.phoenix.schema.PTableType.SYSTEM;
import static org.apache.phoenix.schema.PTableType.TABLE;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;
import org.junit.Test;


public class TenantSpecificTablesDDLIT extends BaseTenantSpecificTablesIT {
    
    @Test
    public void testCreateTenantSpecificTable() throws Exception {
        // ensure we didn't create a physical HBase table for the tenant-specific table
        HBaseAdmin admin = driver.getConnectionQueryServices(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).getAdmin();
        assertEquals(0, admin.listTables(TENANT_TABLE_NAME).length);
    }
    
    @Test
    public void testCreateTenantTableTwice() throws Exception {
        try {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, TENANT_TABLE_DDL, null, nextTimestamp(), false);
        	fail();
        }
        catch (TableAlreadyExistsException expected) {}
    }
    
    @Test
    public void testCreateTenantViewFromNonMultiTenant() throws Exception {
        createTestTable(getUrl(), "CREATE TABLE NON_MULTI_TENANT_TABLE (K VARCHAR PRIMARY KEY)", null, nextTimestamp());
        try {
            // Only way to get this exception is to attempt to derive from a global, multi-type table, as we won't find
            // a tenant-specific table when we attempt to resolve the base table.
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW TENANT_TABLE2 (COL VARCHAR) AS SELECT * FROM NON_MULTI_TENANT_TABLE", null, nextTimestamp());
        }
        catch (TableNotFoundException expected) {
        }
    }

    @Test
    public void testAlterMultiTenantWithViewsToGlobal() throws Exception {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().execute("alter table " + PARENT_TABLE_NAME + " set MULTI_TENANT=false");
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testCreateTenantTableWithSameWhereClause() throws Exception {
        createTestTable(getUrl(), PARENT_TABLE_DDL.replace(PARENT_TABLE_NAME, PARENT_TABLE_NAME + "_II"), null, nextTimestamp());
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, TENANT_TABLE_DDL.replace(TENANT_TABLE_NAME, TENANT_TABLE_NAME + "2"), null, nextTimestamp());
    }
    
    @Test(expected=TableNotFoundException.class)
    public void testDeletionOfParentTableFailsOnTenantSpecificConnection() throws Exception {
        createTestTable(getUrl(), PARENT_TABLE_DDL.replace(PARENT_TABLE_NAME, "TEMP_PARENT"), null, nextTimestamp());
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, TENANT_ID); // connection is tenant-specific
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("DROP TABLE TEMP_PARENT");
        conn.close();
    }
    
    public void testCreationOfParentTableFailsOnTenantSpecificConnection() throws Exception {
        try {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE PARENT_TABLE ( \n" + 
                    "                user VARCHAR ,\n" + 
                    "                id INTEGER not null primary key desc\n" + 
                    "                ) ", null, nextTimestamp());
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_CREATE_TENANT_SPECIFIC_TABLE.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testTenantSpecificAndParentTablesMayBeInDifferentSchemas() throws SQLException {
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW DIFFSCHEMA.TENANT_TABLE ( \n" + 
                "                tenant_col VARCHAR) AS SELECT * \n" + 
                "                FROM " + PARENT_TABLE_NAME + " WHERE tenant_type_id = 'aaa'", null, nextTimestamp());
        try {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW DIFFSCHEMA.TENANT_TABLE ( \n" + 
                    "                tenant_col VARCHAR) AS SELECT *\n"+
                    "                FROM DIFFSCHEMA." + PARENT_TABLE_NAME + " WHERE tenant_type_id = 'aaa'", null, nextTimestamp());
            fail();
        }
        catch (SQLException expected) {
            assertEquals(TABLE_UNDEFINED.getErrorCode(), expected.getErrorCode());
        }
        String newDDL =
        "CREATE TABLE DIFFSCHEMA." + PARENT_TABLE_NAME + " ( \n" + 
        "                user VARCHAR ,\n" + 
        "                tenant_id VARCHAR(5) NOT NULL,\n" + 
        "                tenant_type_id VARCHAR(3) NOT NULL, \n" + 
        "                id INTEGER NOT NULL\n" + 
        "                CONSTRAINT pk PRIMARY KEY (tenant_id, tenant_type_id, id)) MULTI_TENANT=true";
        createTestTable(getUrl(), newDDL, null, nextTimestamp());
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW DIFFSCHEMA.TENANT_TABLE ( \n" + 
                "                tenant_col VARCHAR) AS SELECT *\n"+
                "                FROM DIFFSCHEMA." + PARENT_TABLE_NAME + " WHERE tenant_type_id = 'aaa'", null, nextTimestamp());
    }
    
    @Test
    public void testTenantSpecificTableCannotDeclarePK() throws SQLException {
        try {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW TENANT_TABLE2 ( \n" + 
                    "                tenant_col VARCHAR PRIMARY KEY) AS SELECT *\n" + 
                    "                FROM PARENT_TABLE", null, nextTimestamp());
            fail();
        }
        catch (SQLException expected) {
            assertEquals(CANNOT_DEFINE_PK_FOR_VIEW.getErrorCode(), expected.getErrorCode());
        }
    }
    
    @Test(expected=ColumnAlreadyExistsException.class)
    public void testTenantSpecificTableCannotOverrideParentCol() throws SQLException {
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW TENANT_TABLE2 ( \n" + 
                "                user INTEGER) AS SELECT *\n" + 
                "                FROM PARENT_TABLE", null, nextTimestamp());
    }
    
    @Test
    public void testBaseTableWrongFormatWithTenantTypeId() throws Exception {
        // only two PK columns for multi_tenant, multi_type
        try {
            createTestTable(getUrl(), "CREATE TABLE BASE_TABLE2 (TENANT_ID VARCHAR NOT NULL PRIMARY KEY, ID VARCHAR, A INTEGER) MULTI_TENANT=true", null, nextTimestamp());
            fail();
        }
        catch (SQLException expected) {
            assertEquals(SQLExceptionCode.INSUFFICIENT_MULTI_TENANT_COLUMNS.getErrorCode(), expected.getErrorCode());
        }
    }
    
    @Test
    public void testBaseTableWrongFormatWithNoTenantTypeId() throws Exception {
        // tenantId column of wrong type
        try {
            createTestTable(getUrl(), "CREATE TABLE BASE_TABLE5 (TENANT_ID INTEGER NOT NULL, ID VARCHAR, A INTEGER CONSTRAINT PK PRIMARY KEY (TENANT_ID, ID)) MULTI_TENANT=true", null, nextTimestamp());
            fail();
        }
        catch (SQLException expected) {
            assertEquals(SQLExceptionCode.INSUFFICIENT_MULTI_TENANT_COLUMNS.getErrorCode(), expected.getErrorCode());
        }
    }
    
    @Test
    public void testAddDropColumn() throws Exception {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        conn.setAutoCommit(true);
        try {
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (1, 'Viva Las Vegas')");
            
            conn.createStatement().execute("alter view " + TENANT_TABLE_NAME + " add tenant_col2 char(1) null");
            
            conn.close();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            conn.setAutoCommit(true);
            
            conn.createStatement().executeUpdate("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col2) values (2, 'a')");
            conn.close();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            conn.setAutoCommit(true);
            
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
            
            rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME + " where tenant_col2 = 'a'");
            rs.next();
            assertEquals(1, rs.getInt(1));
            
            conn.close();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            conn.setAutoCommit(true);
            
            conn.createStatement().execute("alter view " + TENANT_TABLE_NAME + " drop column tenant_col");
            
            conn.close();
            props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
            conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            conn.setAutoCommit(true);
            
            rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME + "");
            rs.next();
            assertEquals(2, rs.getInt(1));
            
            try {
                rs = conn.createStatement().executeQuery("select tenant_col from TENANT_TABLE");
                fail();
            }
            catch (ColumnNotFoundException expected) {}
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testMutationOfPKInTenantTablesNotAllowed() throws Exception {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        try {
            try {
                conn.createStatement().execute("alter table " + TENANT_TABLE_NAME + " add new_tenant_pk char(1) primary key");
                fail();
            }
            catch (SQLException expected) {
                assertEquals(CANNOT_MODIFY_VIEW_PK.getErrorCode(), expected.getErrorCode());
            }
            
            try {
                conn.createStatement().execute("alter table " + TENANT_TABLE_NAME + " drop column id");
                fail();
            }
            catch (SQLException expected) {
                assertEquals(CANNOT_DROP_PK.getErrorCode(), expected.getErrorCode());
            }
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testColumnMutationInParentTableWithExistingTenantTable() throws Exception {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // try adding a PK col
            try {
                conn.createStatement().execute("alter table " + PARENT_TABLE_NAME + " add new_pk varchar primary key");
                fail();
            }
            catch (SQLException expected) {
                assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(), expected.getErrorCode());
            }
            
            // try adding a non-PK col
            try {
                conn.createStatement().execute("alter table " + PARENT_TABLE_NAME + " add new_col char(1)");
                fail();
            }
            catch (SQLException expected) {
                assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(), expected.getErrorCode());
            }
            
            // try removing a PK col
            try {
                conn.createStatement().execute("alter table " + PARENT_TABLE_NAME + " drop column id");
                fail();
            }
            catch (SQLException expected) {
                assertEquals(CANNOT_DROP_PK.getErrorCode(), expected.getErrorCode());
            }
            
            // try removing a non-PK col
            try {
                conn.createStatement().execute("alter table " + PARENT_TABLE_NAME + " drop column user");
                fail();
            }
            catch (SQLException expected) {
                assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(), expected.getErrorCode());
            }
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testDisallowDropParentTableWithExistingTenantTable() throws Exception {
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.createStatement().executeUpdate("drop table " + PARENT_TABLE_NAME);
            fail("Should not have been allowed to drop a parent table to which tenant-specific tables still point.");
        }
        catch (SQLException expected) {
            assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(), expected.getErrorCode());
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testAllowDropParentTableWithCascadeAndSingleTenantTable() throws Exception {
	    long ts = nextTimestamp();
	    Properties props = new Properties();
	    props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
	    Connection conn = DriverManager.getConnection(getUrl(), props);
	    Connection connTenant = null;
    
		try {
			// Drop Parent Table 
			conn.createStatement().executeUpdate("DROP TABLE " + PARENT_TABLE_NAME + " CASCADE");
			conn.close();
		      
			props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
			connTenant = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
			
	        validateTenantViewIsDropped(conn);		
	    } finally {
	    	if (conn != null) {
	    		conn.close();
	    	}
	    	if (connTenant != null) {
	    		connTenant.close();
	    	}
	    }
    }
    
    
    @Test
    public void testAllDropParentTableWithCascadeWithMultipleTenantTablesAndIndexes() throws Exception {
        // Create a second tenant table
    	createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL2, TENANT_TABLE_DDL, null, nextTimestamp());
    	//TODO Create some tenant specific table indexes
        
	    long ts = nextTimestamp();
	    Properties props = new Properties();
	    props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts));
	    Connection conn = null;
	    Connection connTenant1 = null;
	    Connection connTenant2 = null;
    
		try {
			conn = DriverManager.getConnection(getUrl(), props);
	        DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getSuperTables(null, null, StringUtil.escapeLike(TENANT_TABLE_NAME) + "%");
            assertTrue(rs.next());
            assertEquals(TENANT_ID2, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertEquals(TENANT_TABLE_NAME, rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            assertEquals(PARENT_TABLE_NAME, rs.getString(PhoenixDatabaseMetaData.SUPERTABLE_NAME));
            assertTrue(rs.next());
            assertEquals(TENANT_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertEquals(TENANT_TABLE_NAME, rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            assertEquals(PARENT_TABLE_NAME, rs.getString(PhoenixDatabaseMetaData.SUPERTABLE_NAME));
            assertTrue(rs.next());
            assertEquals(TENANT_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertEquals(TENANT_TABLE_NAME_NO_TENANT_TYPE_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            assertEquals(PARENT_TABLE_NAME_NO_TENANT_TYPE_ID, rs.getString(PhoenixDatabaseMetaData.SUPERTABLE_NAME));
            assertFalse(rs.next());
            rs.close();
            conn.close();
            
			// Drop Parent Table 
			conn.createStatement().executeUpdate("DROP TABLE " + PARENT_TABLE_NAME + " CASCADE");
		  
			// Validate Tenant Views are dropped
			props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(ts + 10));
			connTenant1 = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
	        validateTenantViewIsDropped(connTenant1);
			connTenant2 = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL2, props);
	        validateTenantViewIsDropped(connTenant2);
	        
	        // Validate Tenant Metadata is gone for the Tenant Table TENANT_TABLE_NAME
			conn = DriverManager.getConnection(getUrl(), props);
	        meta = conn.getMetaData();
            rs = meta.getSuperTables(null, null, StringUtil.escapeLike(TENANT_TABLE_NAME) + "%");
            assertTrue(rs.next());
            assertEquals(TENANT_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertEquals(TENANT_TABLE_NAME_NO_TENANT_TYPE_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            assertEquals(PARENT_TABLE_NAME_NO_TENANT_TYPE_ID, rs.getString(PhoenixDatabaseMetaData.SUPERTABLE_NAME));
            assertFalse(rs.next());
            rs.close();
	        
	    } finally {
	    	if (conn != null) {
	    		conn.close();
	    	}
	    	if (connTenant1 != null) {
	    		connTenant1.close();
	    	}
	    	if (connTenant2 != null) {
	    		connTenant2.close();
	    	}
	    }
    }

	private void validateTenantViewIsDropped(Connection connTenant)	throws SQLException {
		// Try and drop tenant view, should throw TableNotFoundException
		try {
			String ddl = "DROP VIEW " + TENANT_TABLE_NAME;
		    connTenant.createStatement().execute(ddl);
		    fail("Tenant specific view " + TENANT_TABLE_NAME + " should have been dropped when parent was dropped");
		} catch (TableNotFoundException e) {
			//Expected
		}
	}
    
    @Test
    public void testTableMetadataScan() throws Exception {
        // create a tenant table with same name for a different tenant to make sure we are not picking it up in metadata scans for TENANT_ID
        String tenantId2 = "tenant2";
        String secondTenatConnectionURL = PHOENIX_JDBC_TENANT_SPECIFIC_URL.replace(TENANT_ID,  tenantId2);
        String tenantTable2 = TENANT_TABLE_NAME+"2";
        createTestTable(secondTenatConnectionURL, TENANT_TABLE_DDL.replace(TENANT_TABLE_NAME, tenantTable2), null, nextTimestamp(), false);
        
        Properties props = new Properties();
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // empty string means global tenant id
            // make sure connections w/o tenant id only see non-tenant-specific tables, both SYSTEM and USER
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getTables("", null, null, null);
            assertTrue(rs.next());
            assertTableMetaData(rs, SYSTEM_CATALOG_SCHEMA, SYSTEM_CATALOG_TABLE, SYSTEM);
            assertTrue(rs.next());
            assertTableMetaData(rs, SYSTEM_CATALOG_SCHEMA, TYPE_SEQUENCE, SYSTEM);
            assertTrue(rs.next());
            assertTableMetaData(rs, SYSTEM_CATALOG_SCHEMA, PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE, SYSTEM);
            assertTrue(rs.next());
            assertTableMetaData(rs, null, PARENT_TABLE_NAME, TABLE);
            assertTrue(rs.next());
            assertTableMetaData(rs, null, PARENT_TABLE_NAME_NO_TENANT_TYPE_ID, TABLE);
            assertFalse(rs.next());
            
            // make sure connections w/o tenant id only see non-tenant-specific columns
            rs = meta.getColumns("", null, null, null);
            while (rs.next()) {
                assertNotEquals(TENANT_TABLE_NAME, rs.getString("TABLE_NAME"));
                assertNotEquals(tenantTable2, rs.getString("TABLE_NAME"));
            }
            
            // null catalog means across all tenant_ids
            rs = meta.getSuperTables(null, null, StringUtil.escapeLike(TENANT_TABLE_NAME) + "%");
            assertTrue(rs.next());
            assertEquals(TENANT_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertEquals(TENANT_TABLE_NAME, rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            assertEquals(PARENT_TABLE_NAME, rs.getString(PhoenixDatabaseMetaData.SUPERTABLE_NAME));
            assertTrue(rs.next());
            assertEquals(TENANT_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertEquals(TENANT_TABLE_NAME_NO_TENANT_TYPE_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            assertEquals(PARENT_TABLE_NAME_NO_TENANT_TYPE_ID, rs.getString(PhoenixDatabaseMetaData.SUPERTABLE_NAME));
            assertTrue(rs.next());
            assertEquals(tenantId2, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertEquals(tenantTable2, rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            assertEquals(PARENT_TABLE_NAME, rs.getString(PhoenixDatabaseMetaData.SUPERTABLE_NAME));
            assertFalse(rs.next());
            conn.close();
            
            // Global connection sees all tenant tables
            conn = DriverManager.getConnection(getUrl(), props);
            rs = conn.getMetaData().getSuperTables(TENANT_ID, null, null);
            assertTrue(rs.next());
            assertEquals(TENANT_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertEquals(TENANT_TABLE_NAME, rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            assertEquals(PARENT_TABLE_NAME, rs.getString(PhoenixDatabaseMetaData.SUPERTABLE_NAME));
            assertTrue(rs.next());
            assertEquals(TENANT_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertEquals(TENANT_TABLE_NAME_NO_TENANT_TYPE_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            assertEquals(PARENT_TABLE_NAME_NO_TENANT_TYPE_ID, rs.getString(PhoenixDatabaseMetaData.SUPERTABLE_NAME));
            assertFalse(rs.next());
            
            rs = conn.getMetaData().getCatalogs();
            assertTrue(rs.next());
            assertEquals(TENANT_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertTrue(rs.next());
            assertEquals(tenantId2, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertFalse(rs.next());
        } finally {
            props.clear();
            conn.close();
        }
        
        props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, Long.toString(nextTimestamp()));
        conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        try {   
            // make sure tenant-specific connections only see their own tables and the global tables
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getTables(null, null, null, null);
            assertTrue(rs.next());
            assertTableMetaData(rs, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA, PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE, PTableType.SYSTEM);
            assertTrue(rs.next());
            assertTableMetaData(rs, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA, PhoenixDatabaseMetaData.TYPE_SEQUENCE, PTableType.SYSTEM);
            assertTrue(rs.next());
            assertTableMetaData(rs, SYSTEM_CATALOG_SCHEMA, PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE, PTableType.SYSTEM);
            assertTrue(rs.next());
            assertTableMetaData(rs, null, PARENT_TABLE_NAME, PTableType.TABLE);
            assertTrue(rs.next());
            assertTableMetaData(rs, null, PARENT_TABLE_NAME_NO_TENANT_TYPE_ID, PTableType.TABLE);
            assertTrue(rs.next());
            assertTableMetaData(rs, null, TENANT_TABLE_NAME, PTableType.VIEW);
            assertTrue(rs.next());
            assertTableMetaData(rs, null, TENANT_TABLE_NAME_NO_TENANT_TYPE_ID, PTableType.VIEW);
            assertFalse(rs.next());
            
            // make sure tenants see parent table's columns and their own
            rs = meta.getColumns(null, null, StringUtil.escapeLike(TENANT_TABLE_NAME) + "%", null);
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "user", 1);
            assertTrue(rs.next());
            // (tenant_id column is not visible in tenant-specific connection)
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "tenant_type_id", 2);
            assertEquals(1, rs.getInt(KEY_SEQ));
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "id", 3);
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "tenant_col", 4);
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME_NO_TENANT_TYPE_ID, "user", 1);
            assertTrue(rs.next());
            // (tenant_id column is not visible in tenant-specific connection)
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME_NO_TENANT_TYPE_ID, "id", 2);
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME_NO_TENANT_TYPE_ID, "tenant_col", 3);
            assertFalse(rs.next()); 
        }
        finally {
            conn.close();
        }
    }
    
    private void assertTableMetaData(ResultSet rs, String schema, String table, PTableType tableType) throws SQLException {
        assertEquals(schema, rs.getString("TABLE_SCHEM"));
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(tableType.toString(), rs.getString("TABLE_TYPE"));
    }
    
    private void assertColumnMetaData(ResultSet rs, String schema, String table, String column) throws SQLException {
        assertEquals(schema, rs.getString("TABLE_SCHEM"));
        assertEquals(table, rs.getString("TABLE_NAME"));
        assertEquals(SchemaUtil.normalizeIdentifier(column), rs.getString("COLUMN_NAME"));
    }

    private void assertColumnMetaData(ResultSet rs, String schema, String table, String column, int ordinalPosition)
            throws SQLException {
        assertColumnMetaData(rs, schema, table, column);
        assertEquals(ordinalPosition, rs.getInt(ORDINAL_POSITION));
    }
}