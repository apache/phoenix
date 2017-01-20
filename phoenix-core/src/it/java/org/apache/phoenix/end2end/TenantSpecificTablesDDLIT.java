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

import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_DROP_PK;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_MUTATE_TABLE;
import static org.apache.phoenix.exception.SQLExceptionCode.TABLE_UNDEFINED;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.KEY_SEQ;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.ORDINAL_POSITION;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_FUNCTION_TABLE;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
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
        Connection conn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES));
        HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        assertEquals(0, admin.listTables(TENANT_TABLE_NAME).length);
    }
    
    @Test
    public void testCreateTenantTableTwice() throws Exception {
        try {
            Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
            Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
            conn.createStatement().execute(TENANT_TABLE_DDL);
        	fail();
        }
        catch (TableAlreadyExistsException expected) {}
    }
    
    @Test
    public void testCreateTenantViewFromNonMultiTenant() throws Exception {
        String tableName = generateUniqueName();
        createTestTable(getUrl(), "CREATE TABLE " + tableName + " (K VARCHAR PRIMARY KEY)");
        try {
            String viewName = generateUniqueName();
            // Only way to get this exception is to attempt to derive from a global, multi-type table, as we won't find
            // a tenant-specific table when we attempt to resolve the base table.
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW " + viewName + " (COL VARCHAR) AS SELECT * FROM " + tableName);
        }
        catch (TableNotFoundException expected) {
        }
    }

    @Test
    public void testAlteringMultiTenancyForTableWithViewsNotAllowed() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        String multiTenantTable = "MT_" + generateUniqueName();
        String globalTable = "G_" + generateUniqueName();
        // create the two base tables
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String ddl = "CREATE TABLE " + multiTenantTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR, V3 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true "; 
            conn.createStatement().execute(ddl);
            ddl = "CREATE TABLE " + globalTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR, V3 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) ";
            conn.createStatement().execute(ddl);
        }
        String t1 = generateUniqueName();
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, t1);
        // create view on multi-tenant table
        try (Connection tenantConn = DriverManager.getConnection(getUrl(), props)) {
            String viewName = "V_" + generateUniqueName();
            String viewDDL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + multiTenantTable;
            tenantConn.createStatement().execute(viewDDL);
        }
        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        // create view on global table
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            String viewName = "V_" + generateUniqueName();
            conn.createStatement().execute("CREATE VIEW " + viewName + " AS SELECT * FROM " + globalTable);
        }
        props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        try (Connection conn = DriverManager.getConnection(getUrl(), props)) {
            try {
                conn.createStatement().execute("ALTER TABLE " + globalTable + " SET MULTI_TENANT = " + true);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                conn.createStatement().execute("ALTER TABLE " + multiTenantTable + " SET MULTI_TENANT = " + false);
                fail();
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
        }
    }
    
    @Test(expected=TableNotFoundException.class)
    public void testDeletionOfParentTableFailsOnTenantSpecificConnection() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        props.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, TENANT_ID); // connection is tenant-specific
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.createStatement().execute("DROP TABLE " + PARENT_TABLE_NAME);
        conn.close();
    }
    
    public void testCreationOfParentTableFailsOnTenantSpecificConnection() throws Exception {
        try {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE TABLE " + generateUniqueName() + "( \n" + 
                    "                \"user\" VARCHAR ,\n" + 
                    "                id INTEGER not null primary key desc\n" + 
                    "                ) ");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_CREATE_TENANT_SPECIFIC_TABLE.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testTenantSpecificAndParentTablesMayBeInDifferentSchemas() throws SQLException {
        String fullTableName = "DIFFSCHEMA." + generateUniqueName();
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW " + fullTableName + " ( \n" + 
                "                tenant_col VARCHAR) AS SELECT * \n" + 
                "                FROM " + PARENT_TABLE_NAME + " WHERE tenant_type_id = 'aaa'");
        try {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW " + fullTableName + "( \n" + 
                    "                tenant_col VARCHAR) AS SELECT *\n"+
                    "                FROM DIFFSCHEMA." + PARENT_TABLE_NAME + " WHERE tenant_type_id = 'aaa'");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(TABLE_UNDEFINED.getErrorCode(), expected.getErrorCode());
        }
        String newDDL =
        "CREATE TABLE DIFFSCHEMA." + PARENT_TABLE_NAME + " ( \n" + 
        "                \"user\" VARCHAR ,\n" + 
        "                tenant_id VARCHAR(5) NOT NULL,\n" + 
        "                tenant_type_id VARCHAR(3) NOT NULL, \n" + 
        "                id INTEGER NOT NULL\n" + 
        "                CONSTRAINT pk PRIMARY KEY (tenant_id, tenant_type_id, id)) MULTI_TENANT=true";
        createTestTable(getUrl(), newDDL);
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW " + fullTableName + "( \n" + 
                "                tenant_col VARCHAR) AS SELECT *\n"+
                "                FROM DIFFSCHEMA." + PARENT_TABLE_NAME + " WHERE tenant_type_id = 'aaa'");
    }
    
    @Test
    public void testTenantSpecificTableCanDeclarePK() throws SQLException {
            createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW " + generateUniqueName() + "( \n" + 
                    "                tenant_col VARCHAR PRIMARY KEY) AS SELECT *\n" + 
                    "                FROM " + PARENT_TABLE_NAME);
    }
    
    @Test(expected=ColumnAlreadyExistsException.class)
    public void testTenantSpecificTableCannotOverrideParentCol() throws SQLException {
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL, "CREATE VIEW " + generateUniqueName() + " ( \n" + 
                "                \"user\" INTEGER) AS SELECT *\n" + 
                "                FROM " + PARENT_TABLE_NAME);
    }
    
    @Test
    public void testBaseTableWrongFormatWithTenantTypeId() throws Exception {
        // only two PK columns for multi_tenant, multi_type
        try {
            createTestTable(getUrl(), 
                    "CREATE TABLE " + generateUniqueName() + 
                    "(TENANT_ID VARCHAR NOT NULL PRIMARY KEY, ID VARCHAR, A INTEGER) MULTI_TENANT=true");
            fail();
        }
        catch (SQLException expected) {
            assertEquals(SQLExceptionCode.INSUFFICIENT_MULTI_TENANT_COLUMNS.getErrorCode(), expected.getErrorCode());
        }
    }
    
    @Test
    public void testAddDropColumn() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        conn.setAutoCommit(true);
        try {
            conn.createStatement().execute("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col) values (1, 'Viva Las Vegas')");
            conn.createStatement().execute("alter view " + TENANT_TABLE_NAME + " add tenant_col2 char(1) null");
            conn.createStatement().execute("upsert into " + TENANT_TABLE_NAME + " (id, tenant_col2) values (2, 'a')");
            
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME);
            rs.next();
            assertEquals(2, rs.getInt(1));
            
            rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME + " where tenant_col2 = 'a'");
            rs.next();
            assertEquals(1, rs.getInt(1));
            
            conn.createStatement().execute("alter view " + TENANT_TABLE_NAME + " drop column tenant_col");
            rs = conn.createStatement().executeQuery("select count(*) from " + TENANT_TABLE_NAME + "");
            rs.next();
            assertEquals(2, rs.getInt(1));
            
            try {
                rs = conn.createStatement().executeQuery("select tenant_col from " + TENANT_TABLE_NAME);
                fail();
            }
            catch (ColumnNotFoundException expected) {}
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testDropOfPKInTenantTablesNotAllowed() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        try {
            // try removing a PK col
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
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            try {
                conn.createStatement().execute("alter table " + PARENT_TABLE_NAME + " drop column id");
                fail();
            }
            catch (SQLException expected) {
                assertEquals(CANNOT_DROP_PK.getErrorCode(), expected.getErrorCode());
            }
            
            // try removing a non-PK col, which is allowed
            try {
                conn.createStatement().execute("alter table " + PARENT_TABLE_NAME + " drop column \"user\"");
            }
            catch (SQLException expected) {
                fail("We should be able to drop a non pk base table column");
            }
        }
        finally {
            conn.close();
        }
    }
    
    @Test
    public void testDisallowDropParentTableWithExistingTenantTable() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
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
	    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
	    Connection conn = DriverManager.getConnection(getUrl(), props);
	    Connection connTenant = null;
    
		try {
			// Drop Parent Table 
			conn.createStatement().executeUpdate("DROP TABLE " + PARENT_TABLE_NAME + " CASCADE");
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
        String tenantTable2 = "V_" + generateUniqueName();
        createTestTable(PHOENIX_JDBC_TENANT_SPECIFIC_URL2, TENANT_TABLE_DDL.replace(TENANT_TABLE_NAME, tenantTable2));
    	//TODO Create some tenant specific table indexes
        
	    Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
	    Connection conn = null;
	    Connection connTenant1 = null;
	    Connection connTenant2 = null;
    
		try {
            List<String> sortedCatalogs = Arrays.asList(TENANT_ID, TENANT_ID2);
            Collections.sort(sortedCatalogs);
			conn = DriverManager.getConnection(getUrl(), props);
	        DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getTables(null, "", StringUtil.escapeLike(TENANT_TABLE_NAME), new String[] {PTableType.VIEW.getValue().getString()});
            assertTrue(rs.next());
            assertEquals(TENANT_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertTableMetaData(rs, null, TENANT_TABLE_NAME, PTableType.VIEW);
            assertFalse(rs.next());
            
            rs = meta.getTables(null, "", StringUtil.escapeLike(tenantTable2), new String[] {PTableType.VIEW.getValue().getString()});
            assertTrue(rs.next());
            assertEquals(TENANT_ID2, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertTableMetaData(rs, null, tenantTable2, PTableType.VIEW);
            assertFalse(rs.next());
            
            rs = meta.getTables(null, "", StringUtil.escapeLike(TENANT_TABLE_NAME_NO_TENANT_TYPE_ID), new String[] {PTableType.VIEW.getValue().getString()});
            assertTrue(rs.next());
            assertEquals(TENANT_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertTableMetaData(rs, null, TENANT_TABLE_NAME_NO_TENANT_TYPE_ID, PTableType.VIEW);
            assertFalse(rs.next());
            
			// Drop Parent Table 
			conn.createStatement().executeUpdate("DROP TABLE " + PARENT_TABLE_NAME + " CASCADE");
		  
			// Validate Tenant Views are dropped
			connTenant1 = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
	        validateTenantViewIsDropped(connTenant1);
			connTenant2 = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL2, props);
	        validateTenantViewIsDropped(connTenant2);
	        
	        // Validate Tenant Metadata is gone for the Tenant Table TENANT_TABLE_NAME
            rs = meta.getTables(null, "", StringUtil.escapeLike(TENANT_TABLE_NAME), new String[] {PTableType.VIEW.getValue().getString()});
            assertFalse(rs.next());
            rs = meta.getTables(null, "", StringUtil.escapeLike(tenantTable2), new String[] {PTableType.VIEW.getValue().getString()});
            assertFalse(rs.next());
            
            rs = meta.getTables(null, "", StringUtil.escapeLike(TENANT_TABLE_NAME_NO_TENANT_TYPE_ID), new String[] {PTableType.VIEW.getValue().getString()});
            assertTrue(rs.next());
            assertEquals(TENANT_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertTableMetaData(rs, null, TENANT_TABLE_NAME_NO_TENANT_TYPE_ID, PTableType.VIEW);
            assertFalse(rs.next());
	        
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
        String tenantId2 = "T_" + generateUniqueName();
        String secondTenatConnectionURL = PHOENIX_JDBC_TENANT_SPECIFIC_URL.replace(TENANT_ID,  tenantId2);
        String tenantTable2 = "V_" + generateUniqueName();
        createTestTable(secondTenatConnectionURL, TENANT_TABLE_DDL.replace(TENANT_TABLE_NAME, tenantTable2));
        
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            // empty string means global tenant id
            // make sure connections w/o tenant id only see non-tenant-specific tables, both SYSTEM and USER
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getTables("", "", StringUtil.escapeLike(PARENT_TABLE_NAME), new String[] {TABLE.getValue().getString()});
            assertTrue(rs.next());
            assertTableMetaData(rs, null, PARENT_TABLE_NAME, TABLE);
            assertFalse(rs.next());

            rs = meta.getTables("", "", StringUtil.escapeLike(PARENT_TABLE_NAME_NO_TENANT_TYPE_ID), new String[] {TABLE.getValue().getString()});
            assertTrue(rs.next());
            assertTableMetaData(rs, null, PARENT_TABLE_NAME_NO_TENANT_TYPE_ID, TABLE);
            assertFalse(rs.next());
            
            // make sure connections w/o tenant id only see non-tenant-specific columns
            rs = meta.getColumns("", null, null, null);
            while (rs.next()) {
                assertNotEquals(TENANT_TABLE_NAME, rs.getString("TABLE_NAME"));
                assertNotEquals(tenantTable2, rs.getString("TABLE_NAME"));
            }
            
            List<String> sortedTableNames = Arrays.asList(TENANT_TABLE_NAME, TENANT_TABLE_NAME_NO_TENANT_TYPE_ID);
            Collections.sort(sortedTableNames);
            List<String> sortedParentNames;
            if (sortedTableNames.get(0).equals(TENANT_TABLE_NAME)) {
                sortedParentNames = Arrays.asList(PARENT_TABLE_NAME, PARENT_TABLE_NAME_NO_TENANT_TYPE_ID);
            } else {
                sortedParentNames = Arrays.asList(PARENT_TABLE_NAME_NO_TENANT_TYPE_ID, PARENT_TABLE_NAME);
            }
            rs = meta.getSuperTables(TENANT_ID, null, null);
            assertTrue(rs.next());
            assertEquals(TENANT_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertEquals(sortedTableNames.get(0), rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            assertEquals(sortedParentNames.get(0), rs.getString(PhoenixDatabaseMetaData.SUPERTABLE_NAME));
            assertTrue(rs.next());
            assertEquals(TENANT_ID, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertEquals(sortedTableNames.get(1), rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            assertEquals(sortedParentNames.get(1), rs.getString(PhoenixDatabaseMetaData.SUPERTABLE_NAME));
            assertFalse(rs.next());
            
            rs = meta.getSuperTables(tenantId2, null, null);
            assertTrue(rs.next());
            assertEquals(tenantId2, rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            assertEquals(tenantTable2, rs.getString(PhoenixDatabaseMetaData.TABLE_NAME));
            assertEquals(PARENT_TABLE_NAME, rs.getString(PhoenixDatabaseMetaData.SUPERTABLE_NAME));
            assertFalse(rs.next());
            conn.close();
            
            Set<String> sortedCatalogs = new HashSet<>(Arrays.asList(TENANT_ID, tenantId2));
            rs = conn.getMetaData().getCatalogs();
            while (rs.next()) {
                sortedCatalogs.remove(rs.getString(PhoenixDatabaseMetaData.TABLE_CAT));
            }
            assertTrue("Should have found both tenant IDs", sortedCatalogs.isEmpty());
        } finally {
            props.clear();
            conn.close();
        }
        
        conn = DriverManager.getConnection(PHOENIX_JDBC_TENANT_SPECIFIC_URL, props);
        try {   
            // make sure tenant-specific connections only see their own tables and the global tables
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rs = meta.getTables("", SYSTEM_CATALOG_SCHEMA, null, new String[] {PTableType.SYSTEM.getValue().getString()});
            assertTrue(rs.next());
            assertTableMetaData(rs, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA, PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE, PTableType.SYSTEM);
            assertTrue(rs.next());
            assertTableMetaData(rs, SYSTEM_CATALOG_SCHEMA, SYSTEM_FUNCTION_TABLE, SYSTEM);
            assertTrue(rs.next());
            assertTableMetaData(rs, PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA, PhoenixDatabaseMetaData.TYPE_SEQUENCE, PTableType.SYSTEM);
            assertTrue(rs.next());
            assertTableMetaData(rs, SYSTEM_CATALOG_SCHEMA, PhoenixDatabaseMetaData.SYSTEM_STATS_TABLE, PTableType.SYSTEM);
            assertFalse(rs.next());
            
            rs = meta.getTables(null, "", StringUtil.escapeLike(tenantTable2), new String[] {TABLE.getValue().getString()});
            assertFalse(rs.next());

            rs = meta.getTables(null, "", StringUtil.escapeLike(PARENT_TABLE_NAME), new String[] {TABLE.getValue().getString()});
            assertTrue(rs.next());
            assertTableMetaData(rs, null, PARENT_TABLE_NAME, TABLE);
            assertFalse(rs.next());

            rs = meta.getTables(null, "", StringUtil.escapeLike(PARENT_TABLE_NAME_NO_TENANT_TYPE_ID), new String[] {TABLE.getValue().getString()});
            assertTrue(rs.next());
            assertTableMetaData(rs, null, PARENT_TABLE_NAME_NO_TENANT_TYPE_ID, TABLE);
            assertFalse(rs.next());

            rs = meta.getTables(null, "", StringUtil.escapeLike(TENANT_TABLE_NAME), new String[] {PTableType.VIEW.getValue().getString()});
            assertTrue(rs.next());
            assertTableMetaData(rs, null, TENANT_TABLE_NAME, PTableType.VIEW);
            assertFalse(rs.next());
            
            rs = meta.getTables(null, "", StringUtil.escapeLike(TENANT_TABLE_NAME_NO_TENANT_TYPE_ID), new String[] {PTableType.VIEW.getValue().getString()});
            assertTrue(rs.next());
            assertTableMetaData(rs, null, TENANT_TABLE_NAME_NO_TENANT_TYPE_ID, PTableType.VIEW);
            assertFalse(rs.next());
            
            // make sure tenants see parent table's columns and their own
            rs = meta.getColumns(null, null, StringUtil.escapeLike(TENANT_TABLE_NAME), null);
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "\"user\"", 1);
            assertTrue(rs.next());
            // (tenant_id column is not visible in tenant-specific connection)
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "tenant_type_id", 2);
            assertEquals(1, rs.getInt(KEY_SEQ));
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "id", 3);
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME, "tenant_col", 4);
            assertFalse(rs.next());
            
            rs = meta.getColumns(null, null, StringUtil.escapeLike(TENANT_TABLE_NAME_NO_TENANT_TYPE_ID), null);
            assertTrue(rs.next());
            assertColumnMetaData(rs, null, TENANT_TABLE_NAME_NO_TENANT_TYPE_ID, "\"user\"", 1);
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