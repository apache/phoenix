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

import static com.google.common.collect.Lists.newArrayListWithExpectedSize;
import static org.apache.phoenix.exception.SQLExceptionCode.CANNOT_MODIFY_VIEW_PK;
import static org.apache.phoenix.exception.SQLExceptionCode.NOT_NULLABLE_COLUMN_IN_ROW_KEY;
import static org.apache.phoenix.util.TestUtil.analyzeTable;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ColumnAlreadyExistsException;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;

public class ViewIT extends BaseViewIT {
	
    public ViewIT(boolean transactional) {
		super(transactional);
	}
    
    @BeforeClass
    public static void doSetup() throws Exception {
        NUM_SLAVES_BASE = 5;
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.STATS_GUIDEPOST_WIDTH_BYTES_ATTRIB, Long.toString(20));
        props.put(QueryServices.STATS_UPDATE_FREQ_MS_ATTRIB, Long.toString(5));
        props.put(QueryServices.USE_STATS_FOR_PARALLELIZATION, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Test
    public void testReadOnlyOnReadOnlyView() throws Exception {
        Connection earlierCon = DriverManager.getConnection(getUrl());
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueTableName();
        String fullParentViewName = generateUniqueViewName();
        String fullViewName = generateUniqueViewName();
        
        String ddl = "CREATE TABLE " + fullTableName + " (k INTEGER NOT NULL PRIMARY KEY, v1 DATE) "+ tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullParentViewName + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullParentViewName + " WHERE k < 9";
        conn.createStatement().execute(ddl);
        
        splitSystemCatalog(Lists.newArrayList(fullTableName, fullParentViewName, fullViewName));
        
        try {
            conn.createStatement().execute("UPSERT INTO " + fullParentViewName + " VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        }
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(" + i + ")");
        }
        conn.commit();
        
        analyzeTable(conn, fullParentViewName, transactional);
        
        List<KeyRange> splits = getAllSplits(conn, fullParentViewName);
        assertEquals(4, splits.size());
        
        int count = 0;
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM " + fullTableName);
        while (rs.next()) {
            assertEquals(count++, rs.getInt(1));
        }
        assertEquals(10, count);
        
        count = 0;
        rs = conn.createStatement().executeQuery("SELECT k FROM " + fullParentViewName);
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(4, count);
        count = 0;
        rs = earlierCon.createStatement().executeQuery("SELECT k FROM " + fullParentViewName);
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(4, count);
        
        try {
            conn.createStatement().execute("UPSERT INTO " + fullViewName + " VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        } finally {
            conn.close();
        }

        conn = DriverManager.getConnection(getUrl());
        count = 0;
        rs = conn.createStatement().executeQuery("SELECT k FROM " + fullViewName);
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(3, count);
    }

    @Test
    public void testNonSaltedUpdatableViewWithIndex() throws Exception {
        testUpdatableViewWithIndex(null, false);
    }
    
    @Test
    public void testNonSaltedUpdatableViewWithLocalIndex() throws Exception {
        testUpdatableViewWithIndex(null, true);
    }
    
    @Test
    public void testUpdatableOnUpdatableView() throws Exception {
        String fullTableName = generateUniqueTableName();
        String fullViewName1 = generateUniqueViewName();
        String fullViewName2 = generateUniqueViewName();
        String ddl = "CREATE VIEW " + fullViewName2 + " AS SELECT * FROM " + fullViewName1 + " WHERE k3 = 2";
        testUpdatableView(fullTableName, fullViewName1, fullViewName2, ddl, null);
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM " + fullViewName2);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(109, rs.getInt(2));
        assertEquals(2, rs.getInt(3));
        assertFalse(rs.next());
        
        conn.createStatement().execute("UPSERT INTO " + fullViewName2 + "(k2) VALUES(122)");
        conn.commit();
        rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM " + fullViewName2 + " WHERE k2 >= 120");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(122, rs.getInt(2));
        assertEquals(2, rs.getInt(3));
        assertFalse(rs.next());
        
        try {
            conn.createStatement().execute("UPSERT INTO " + fullViewName2 + "(k2,k3) VALUES(123,3)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("UPSERT INTO " + fullViewName2 + "(k2,k3) select k2, 3 from " + fullViewName1);
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testReadOnlyOnUpdatableView() throws Exception {
        String fullTableName = generateUniqueTableName();
        String fullViewName1 = generateUniqueViewName();
        String fullViewName2 = generateUniqueViewName();
        String ddl =
                "CREATE VIEW " + fullViewName2 + " AS SELECT * FROM " + fullViewName1
                        + " WHERE k3 > 1 and k3 < 50";
        testUpdatableView(fullTableName, fullViewName1, fullViewName2, ddl, null);
        Connection conn = DriverManager.getConnection(getUrl());
        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM " + fullViewName2);
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(109, rs.getInt(2));
        assertEquals(2, rs.getInt(3));
        assertFalse(rs.next());
        try {
            conn.createStatement().execute("UPSERT INTO " + fullViewName2 + " VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        }
        
        conn.createStatement().execute("UPSERT INTO " + fullTableName + "(k1, k2,k3) VALUES(1, 122, 5)");
        conn.commit();
        rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM " + fullViewName2 + " WHERE k2 >= 120");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(122, rs.getInt(2));
        assertEquals(5, rs.getInt(3));
        assertFalse(rs.next());
    }

    @Test
    public void testDisallowDropOfReferencedColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueTableName();
        String fullViewName1 = generateUniqueViewName();
        String fullViewName2 = generateUniqueViewName();
        splitSystemCatalog(Lists.newArrayList(fullTableName, fullViewName1, fullViewName2));
        
        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName1 + "(v2 VARCHAR, v3 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName2 + " AS SELECT * FROM " + fullViewName1 + " WHERE v2 != 'foo'";
        conn.createStatement().execute(ddl);
        
        try {
            conn.createStatement().execute("ALTER VIEW " + fullViewName1 + " DROP COLUMN v1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }
        
        try {
            conn.createStatement().execute("ALTER VIEW " + fullViewName2 + " DROP COLUMN v1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().execute("ALTER VIEW " + fullViewName2 + " DROP COLUMN v2");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }
        conn.createStatement().execute("ALTER VIEW " + fullViewName2 + " DROP COLUMN v3");
        
    }
    
    @Test
    public void testReadOnlyViewWithCaseSensitiveTableNames() throws Exception {
        Connection earlierCon = DriverManager.getConnection(getUrl());
        Connection conn = DriverManager.getConnection(getUrl());
        String schemaName = TestUtil.DEFAULT_SCHEMA_NAME + "_" + generateUniqueName();
        String caseSensitiveTableName = "\"t_" + generateUniqueName() + "\"" ;
        String fullTableName = SchemaUtil.getTableName(schemaName, caseSensitiveTableName);
        String caseSensitiveViewName = "\"v_" + generateUniqueName() + "\"" ;
        splitSystemCatalog(Lists.newArrayList(fullTableName, caseSensitiveViewName));

        String ddl = "CREATE TABLE " + fullTableName + " (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + caseSensitiveViewName + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        
        try {
            conn.createStatement().execute("UPSERT INTO " + caseSensitiveViewName + " VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        }
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(" + i + ")");
        }
        conn.commit();
        
        int count = 0;
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM " + caseSensitiveViewName);
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(4, count);
        count = 0;
        rs = earlierCon.createStatement().executeQuery("SELECT k FROM " + caseSensitiveViewName);
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(4, count);
    }
    
    @Test
    public void testReadOnlyViewWithCaseSensitiveColumnNames() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueTableName();
        String viewName = generateUniqueViewName();
        String ddl = "CREATE TABLE " + fullTableName + " (\"k\" INTEGER NOT NULL PRIMARY KEY, \"v1\" INTEGER, \"a\".v2 VARCHAR)" + tableDDLOptions;
        splitSystemCatalog(Lists.newArrayList(fullTableName, viewName));

        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + viewName + " (v VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE \"k\" > 5 and \"v1\" > 1";
        conn.createStatement().execute(ddl);
        
        try {
            conn.createStatement().execute("UPSERT INTO " + viewName + " VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        }
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(" + i + ", " + (i+10) + ",'A')");
        }
        conn.commit();
        
        int count = 0;
        ResultSet rs = conn.createStatement().executeQuery("SELECT \"k\", \"v1\",\"a\".v2 FROM " + viewName);
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(4, count);
    }
    
    @Test
    public void testViewWithCurrentDate() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueTableName();
        String viewName = generateUniqueViewName();
        splitSystemCatalog(Lists.newArrayList(fullTableName, viewName));

        String ddl = "CREATE TABLE " + fullTableName + " (k INTEGER NOT NULL PRIMARY KEY, v1 INTEGER, v2 DATE)" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + viewName + " (v VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE v2 > CURRENT_DATE()-5 AND v2 > DATE '2010-01-01'";
        conn.createStatement().execute(ddl);
        
        try {
            conn.createStatement().execute("UPSERT INTO " + viewName + " VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        }
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(" + i + ", " + (i+10) + ",CURRENT_DATE()-" + i + ")");
        }
        conn.commit();
        
        int count = 0;
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM " + viewName);
        while (rs.next()) {
            assertEquals(count, rs.getInt(1));
            count++;
        }
        assertEquals(5, count);
    }

    
    @Test
    public void testViewAndTableInDifferentSchemasWithNamespaceMappingEnabled() throws Exception {
        testViewAndTableInDifferentSchemas(true);
    }

    @Test
    public void testViewAndTableInDifferentSchemas() throws Exception {
        testViewAndTableInDifferentSchemas(false);

    }

    public void testViewAndTableInDifferentSchemas(boolean isNamespaceMapped) throws Exception {
        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.toString(isNamespaceMapped));
        Connection conn = DriverManager.getConnection(getUrl(),props);
        String tableName = "T_" + generateUniqueName();
        String schemaName1 = "S_" + generateUniqueName();
        String fullTableName1 = SchemaUtil.getTableName(schemaName1, tableName);
        String viewName = "V_" + generateUniqueName();
        String viewSchemaName = "S_" + generateUniqueName();
        String fullViewName1 = SchemaUtil.getTableName(viewSchemaName, viewName);
        String fullViewName2 = "V_" + generateUniqueName();
        
        splitSystemCatalog(Lists.newArrayList(fullTableName1, fullViewName1, fullViewName2));
        
        if (isNamespaceMapped) {
            conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName1);
        }
		String ddl = "CREATE TABLE " + fullTableName1 + " (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)" + tableDDLOptions;
        HBaseAdmin admin = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin();
        conn.createStatement().execute(ddl);
        assertTrue(admin.tableExists(SchemaUtil.getPhysicalTableName(SchemaUtil.normalizeIdentifier(fullTableName1),
                conn.unwrap(PhoenixConnection.class).getQueryServices().getProps())));
        
        ddl = "CREATE VIEW " + fullViewName1 + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName1 + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        
        ddl = "CREATE VIEW " + fullViewName2 + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName1 + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        
        conn.createStatement().executeQuery("SELECT * FROM " + fullViewName1);
        conn.createStatement().executeQuery("SELECT * FROM " + fullViewName2);
        ddl = "DROP VIEW " + viewName;
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (TableNotFoundException ignore) {
        }
        ddl = "DROP VIEW " + fullViewName1;
        conn.createStatement().execute(ddl);
        ddl = "DROP VIEW " + SchemaUtil.getTableName(viewSchemaName, generateUniqueName());
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (TableNotFoundException ignore) {
        }
        ddl = "DROP TABLE " + fullTableName1;
        ddl = "DROP VIEW " + fullViewName2;
        conn.createStatement().execute(ddl);
        ddl = "DROP TABLE " + fullTableName1;
        conn.createStatement().execute(ddl);
    }


    @Test
    public void testDisallowDropOfColumnOnParentTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueTableName();
        String viewName = generateUniqueViewName();
        splitSystemCatalog(Lists.newArrayList(fullTableName, viewName));

        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + viewName + "(v2 VARCHAR, v3 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);
        
        try {
            conn.createStatement().execute("ALTER TABLE " + fullTableName + " DROP COLUMN v1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }
    }
   
    @Test
    public void testViewAndTableAndDropCascade() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueTableName();
        String fullViewName1 = generateUniqueViewName();
        String fullViewName2 = generateUniqueViewName();
        splitSystemCatalog(Lists.newArrayList(fullTableName, fullViewName1, fullViewName2));
        
        String ddl = "CREATE TABLE " + fullTableName + "  (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName1 + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        ddl = "CREATE LOCAL INDEX idx on " + fullViewName1 + "(v2)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName2 + "(v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 10";
        conn.createStatement().execute(ddl);
        
        // dropping base table without cascade should fail
        try {
            conn.createStatement().execute("DROP TABLE " + fullTableName );
            fail();
        }
        catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }

        // drop table cascade should succeed
        conn.createStatement().execute("DROP TABLE " + fullTableName + " CASCADE");
        
        validateViewDoesNotExist(conn, fullViewName1);
        validateViewDoesNotExist(conn, fullViewName2);
    }
    
    @Test
    public void testViewAndTableAndDropCascadeWithIndexes() throws Exception {
    	// Setup - Tables and Views with Indexes
    	Connection conn = DriverManager.getConnection(getUrl());
		if (tableDDLOptions.length()!=0)
			tableDDLOptions+=",";
		tableDDLOptions+="IMMUTABLE_ROWS=true";
        String fullTableName = generateUniqueTableName();
        String ddl = "CREATE TABLE " + fullTableName + " (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        String fullViewName1 = generateUniqueViewName();
        String fullViewName2 = generateUniqueViewName();
        String indexName1 = "I_" + generateUniqueName();
        String indexName2 = "I_" + generateUniqueName();
        String indexName3 = "I_" + generateUniqueName();
        splitSystemCatalog(Lists.newArrayList(fullTableName, fullViewName1, fullViewName2));
        
        ddl = "CREATE INDEX " + indexName1 + " ON " + fullTableName + " (v1)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName1 + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 5";
        conn.createStatement().execute(ddl);
        ddl = "CREATE INDEX " + indexName2 + " ON " + fullViewName1 + " (v2)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName2 + " (v2 VARCHAR) AS SELECT * FROM " + fullTableName + " WHERE k > 10";
        conn.createStatement().execute(ddl);
        ddl = "CREATE INDEX " + indexName3 + " ON " + fullViewName2 + " (v2)";
        conn.createStatement().execute(ddl);
        

        // Execute DROP...CASCADE
        conn.createStatement().execute("DROP TABLE " + fullTableName + " CASCADE");
        
        // Validate Views were deleted - Try and delete child views, should throw TableNotFoundException
        validateViewDoesNotExist(conn, fullViewName1);
        validateViewDoesNotExist(conn, fullViewName2);
    }


	private void validateViewDoesNotExist(Connection conn, String fullViewName)	throws SQLException {
		try {
        	String ddl1 = "DROP VIEW " + fullViewName;
            conn.createStatement().execute(ddl1);
            fail("View " + fullViewName + " should have been deleted when parent was dropped");
        } catch (TableNotFoundException e) {
        	//Expected
        }
	}
	
    @Test
    public void testViewUsesTableGlobalIndex() throws Exception {
        testViewUsesTableIndex(false);
    }
    
    @Test
    public void testViewUsesTableLocalIndex() throws Exception {
        testViewUsesTableIndex(true);
    }

    
    private void testViewUsesTableIndex(boolean localIndex) throws Exception {
        ResultSet rs;
        // Use unique name for table with local index as otherwise we run into issues
        // when we attempt to drop the table (with the drop metadata option set to false
        String fullTableName = generateUniqueTableName() + (localIndex ? "_WITH_LI" : "_WITHOUT_LI");
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE " + fullTableName + "  (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, k3 DECIMAL, s1 VARCHAR, s2 VARCHAR CONSTRAINT pk PRIMARY KEY (k1, k2, k3))" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        String schemaName = SchemaUtil.getSchemaNameFromFullName(fullTableName);
        String indexName1 = "I_" + generateUniqueName();
        String fullIndexName1 = SchemaUtil.getTableName(schemaName, indexName1);
        conn.createStatement().execute("CREATE " + (localIndex ? "LOCAL " : "") + " INDEX " + indexName1 + " ON " + fullTableName + "(k3, k2) INCLUDE(s1, s2)");
        String indexName2 = "I_" + generateUniqueName();
        conn.createStatement().execute("CREATE INDEX " + indexName2 + " ON " + fullTableName + "(k3, k2, s2)");
        
        String fullViewName =generateUniqueViewName();
        ddl = "CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName + " WHERE s1 = 'foo'";
        conn.createStatement().execute(ddl);
        String[] s1Values = {"foo","bar"};
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO " + fullTableName + " VALUES(" + (i % 4) + "," + (i+100) + "," + (i > 5 ? 2 : 1) + ",'" + s1Values[i%2] + "','bas')");
        }
        conn.commit();
        
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM " + fullViewName);
        assertTrue(rs.next());
        assertEquals(5, rs.getLong(1));
        assertFalse(rs.next());

        String viewIndexName = "I_" + generateUniqueName();
        conn.createStatement().execute("CREATE INDEX " + viewIndexName + " on " + fullViewName + "(k2)");
        
        // move metadata to multiple regions
        splitSystemCatalog(Lists.newArrayList(fullTableName, fullViewName));
        
        String query = "SELECT k2 FROM " + fullViewName + " WHERE k2 IN (100,109) AND k3 IN (1,2) AND s2='bas'";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(100, rs.getInt(1));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        String queryPlan = QueryUtil.getExplainPlan(rs);
        // Assert that in either case (local & global) that index from physical table used for query on view.
        if (localIndex) {
            assertEquals("CLIENT PARALLEL 1-WAY SKIP SCAN ON 4 KEYS OVER " + fullTableName + " [1,1,100] - [1,2,109]\n" + 
                    "    SERVER FILTER BY (\"S2\" = 'bas' AND \"S1\" = 'foo')\n" + 
                    "CLIENT MERGE SORT", queryPlan);
        } else {
            assertEquals(
                    "CLIENT PARALLEL 1-WAY SKIP SCAN ON 4 KEYS OVER " + fullIndexName1 + " [1,100] - [2,109]\n" + 
                    "    SERVER FILTER BY (\"S2\" = 'bas' AND \"S1\" = 'foo')", queryPlan);
        }
    }    

    @Test
    public void testCreateViewDefinesPKColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueTableName();
        String fullViewName = generateUniqueViewName();
        splitSystemCatalog(Lists.newArrayList(fullTableName, fullViewName));
        
        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "(v2 VARCHAR, k3 VARCHAR PRIMARY KEY) AS SELECT * FROM " + fullTableName + " WHERE K1 = 1";
        conn.createStatement().execute(ddl);

        // assert PK metadata
        ResultSet rs =
                conn.getMetaData().getPrimaryKeys(null,
                    SchemaUtil.getSchemaNameFromFullName(fullViewName),
                    SchemaUtil.getTableNameFromFullName(fullViewName));
        assertPKs(rs, new String[] {"K3"});
        
        // sanity check upserts into base table and view
        conn.createStatement().executeUpdate("upsert into " + fullTableName + " (k1, k2, v1) values (1, 1, 1)");
        conn.createStatement().executeUpdate("upsert into " + fullViewName + " (k1, k2, k3, v2) values (1, 1, 'abc', 'def')");
        conn.commit();
        
        // expect 2 rows in the base table
        rs = conn.createStatement().executeQuery("select count(*) from " + fullTableName);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        
        // expect 2 row in the view
        rs = conn.createStatement().executeQuery("select count(*) from " + fullViewName);
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
    }
    
    @Test
    public void testCreateViewDefinesPKConstraint() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueTableName();
        String fullViewName = generateUniqueViewName();
        splitSystemCatalog(Lists.newArrayList(fullTableName, fullViewName));

        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "(v2 VARCHAR, k3 VARCHAR, k4 INTEGER NOT NULL, CONSTRAINT PKVEW PRIMARY KEY (k3, k4)) AS SELECT * FROM " + fullTableName + " WHERE K1 = 1";
        conn.createStatement().execute(ddl);
        
        // assert PK metadata
        ResultSet rs =
                conn.getMetaData().getPrimaryKeys(null,
                    SchemaUtil.getSchemaNameFromFullName(fullViewName),
                    SchemaUtil.getTableNameFromFullName(fullViewName));
        assertPKs(rs, new String[] {"K3", "K4"});
    }
    
    @Test
    public void testViewAddsPKColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueTableName();
        String schemaName = TestUtil.DEFAULT_SCHEMA_NAME + "_" + generateUniqueName();
        String viewName = "T_" + generateUniqueName();
        String fullViewName = SchemaUtil.getTableName(schemaName, viewName);
        splitSystemCatalog(Lists.newArrayList(fullTableName, fullViewName));
        
		String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "  AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);
        
        ddl = "ALTER VIEW " + fullViewName + " ADD k3 VARCHAR PRIMARY KEY, k4 VARCHAR PRIMARY KEY, v2 INTEGER";
        conn.createStatement().execute(ddl);

        // assert PK metadata
        ResultSet rs = conn.getMetaData().getPrimaryKeys(null, schemaName, viewName);
        assertPKs(rs, new String[] {"K3", "K4"});
    }
    
    @Test
    public void testViewAddsPKColumnWhoseParentsLastPKIsVarLength() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueTableName();
        String fullViewName = generateUniqueViewName();
        splitSystemCatalog(Lists.newArrayList(fullTableName, fullViewName));

        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 VARCHAR NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "  AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);
        
        ddl = "ALTER VIEW " + fullViewName + " ADD k3 VARCHAR PRIMARY KEY, k4 VARCHAR PRIMARY KEY, v2 INTEGER";
        try {
            conn.createStatement().execute(ddl);
            fail("View cannot extend PK if parent's last PK is variable length. See https://issues.apache.org/jira/browse/PHOENIX-978.");
        } catch (SQLException e) {
            assertEquals(CANNOT_MODIFY_VIEW_PK.getErrorCode(), e.getErrorCode());
        }
        String fullViewName2 = "V_" + generateUniqueName();
        ddl = "CREATE VIEW " + fullViewName2 + " (k3 VARCHAR PRIMARY KEY)  AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        try {
        	conn.createStatement().execute(ddl);
        } catch (SQLException e) {
            assertEquals(CANNOT_MODIFY_VIEW_PK.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test(expected=ColumnAlreadyExistsException.class)
    public void testViewAddsClashingPKColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueTableName();
        String fullViewName = generateUniqueViewName();
        splitSystemCatalog(Lists.newArrayList(fullTableName, fullViewName));
        
        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "  AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);
        
        ddl = "ALTER VIEW " + fullViewName + " ADD k3 VARCHAR PRIMARY KEY, k2 VARCHAR PRIMARY KEY, v2 INTEGER";
        conn.createStatement().execute(ddl);
    }

    @Test
    public void testViewAddsNotNullPKColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullViewName = generateUniqueViewName();
        String fullTableName = generateUniqueTableName();
        splitSystemCatalog(Lists.newArrayList(fullTableName, fullViewName));
        String ddl = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))" + tableDDLOptions;
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW " + fullViewName + "  AS SELECT * FROM " + fullTableName + " WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);
        
        try {
            ddl = "ALTER VIEW " + fullViewName + " ADD k3 VARCHAR NOT NULL PRIMARY KEY"; 
            conn.createStatement().execute(ddl);
            fail("can only add nullable PKs via ALTER VIEW/TABLE");
        } catch (SQLException e) {
            assertEquals(NOT_NULLABLE_COLUMN_IN_ROW_KEY.getErrorCode(), e.getErrorCode());
        }
    }
    
    @Test
    public void testQueryViewStatementOptimization() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String fullTableName = generateUniqueTableName();
        String fullViewName1 = generateUniqueViewName();
        String fullViewName2 = generateUniqueViewName();
        String sql = "CREATE TABLE " + fullTableName + " (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))" + tableDDLOptions;
        conn.createStatement().execute(sql);
        sql = "CREATE VIEW " + fullViewName1 + "  AS SELECT * FROM " + fullTableName;
        conn.createStatement().execute(sql);
        sql = "CREATE VIEW " + fullViewName2 + "  AS SELECT * FROM " + fullTableName + " WHERE k1 = 1.0";
        conn.createStatement().execute(sql);
        
        splitSystemCatalog(Lists.newArrayList(fullTableName, fullViewName1, fullViewName2));
        
        sql = "SELECT * FROM " + fullViewName1 + " order by k1, k2";
        PreparedStatement stmt = conn.prepareStatement(sql);
        QueryPlan plan = PhoenixRuntime.getOptimizedQueryPlan(stmt);
        assertEquals(0, plan.getOrderBy().getOrderByExpressions().size());
        
        sql = "SELECT * FROM " + fullViewName2 + " order by k1, k2";
        stmt = conn.prepareStatement(sql);
        plan = PhoenixRuntime.getOptimizedQueryPlan(stmt);
        assertEquals(0, plan.getOrderBy().getOrderByExpressions().size());
    }

    @Test
    public void testCreateViewWithUpdateCacheFrquency() throws Exception {
      Properties props = new Properties();
      Connection conn1 = DriverManager.getConnection(getUrl(), props);
      conn1.setAutoCommit(true);
      String tableName=generateUniqueTableName();
      String viewName=generateUniqueViewName();
      splitSystemCatalog(Lists.newArrayList(tableName, viewName));
      conn1.createStatement().execute(
        "CREATE TABLE "+tableName+" (k VARCHAR PRIMARY KEY, v1 VARCHAR, v2 VARCHAR) UPDATE_CACHE_FREQUENCY=1000000");
      conn1.createStatement().execute("upsert into "+tableName+" values ('row1', 'value1', 'key1')");
      conn1.createStatement().execute(
        "CREATE VIEW "+viewName+" (v43 VARCHAR) AS SELECT * FROM "+tableName+" WHERE v1 = 'value1'");
      
      ResultSet rs = conn1.createStatement()
          .executeQuery("SELECT * FROM "+tableName+" WHERE v1 = 'value1'");
      assertTrue(rs.next());
    }

    @Test
    public void testCreateViewMappedToExistingHbaseTableWithNamespaceMappingEnabled() throws Exception {
        final String NS = "NS_" + generateUniqueName();
        final String TBL = "TBL_" + generateUniqueName();
        final String CF = "CF";

        Properties props = new Properties();
        props.setProperty(QueryServices.IS_NAMESPACE_MAPPING_ENABLED, Boolean.TRUE.toString());

        try (Connection conn = DriverManager.getConnection(getUrl(), props);
                HBaseAdmin admin =
                        conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin()) {

            conn.createStatement().execute("CREATE SCHEMA " + NS);

            // test for a view that is in non-default schema
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(NS, TBL));
            desc.addFamily(new HColumnDescriptor(CF));
            admin.createTable(desc);

            String view1 = NS + "." + TBL;
            conn.createStatement().execute(
                "CREATE VIEW " + view1 + " (PK VARCHAR PRIMARY KEY, " + CF + ".COL VARCHAR)");

            assertTrue(QueryUtil
                    .getExplainPlan(
                        conn.createStatement().executeQuery("explain select * from " + view1))
                    .contains(NS + ":" + TBL));

            

            // test for a view whose name contains a dot (e.g. "AAA.BBB") in default schema (for
            // backward compatibility)
            desc = new HTableDescriptor(TableName.valueOf(NS + "." + TBL));
            desc.addFamily(new HColumnDescriptor(CF));
            admin.createTable(desc);

            String view2 = "\"" + NS + "." + TBL + "\"";
            conn.createStatement().execute(
                "CREATE VIEW " + view2 + " (PK VARCHAR PRIMARY KEY, " + CF + ".COL VARCHAR)");

            assertTrue(QueryUtil
                    .getExplainPlan(
                        conn.createStatement().executeQuery("explain select * from " + view2))
                    .contains(NS + "." + TBL));

            // test for a view whose name contains a dot (e.g. "AAA.BBB") in non-default schema
            desc = new HTableDescriptor(TableName.valueOf(NS, NS + "." + TBL));
            desc.addFamily(new HColumnDescriptor(CF));
            admin.createTable(desc);

            String view3 = NS + ".\"" + NS + "." + TBL + "\"";
            conn.createStatement().execute(
                "CREATE VIEW " + view3 + " (PK VARCHAR PRIMARY KEY, " + CF + ".COL VARCHAR)");

            assertTrue(QueryUtil
                    .getExplainPlan(
                        conn.createStatement().executeQuery("explain select * from " + view3))
                    .contains(NS + ":" + NS + "." + TBL));
            
            Map<String, List<String>> tenantToTableAndViewMap = Maps.newHashMapWithExpectedSize(3);
            // since all of the above views map to a single schema, just split on view1
            tenantToTableAndViewMap.put(null, Lists.newArrayList(TBL, view1));
            // move metadata to multiple regions
            splitSystemCatalog(tenantToTableAndViewMap);

            conn.createStatement().execute("DROP VIEW " + view1);
            conn.createStatement().execute("DROP VIEW " + view2);
            conn.createStatement().execute("DROP VIEW " + view3);

            conn.createStatement().execute("DROP SCHEMA " + NS);
        }
    }

    private void assertPKs(ResultSet rs, String[] expectedPKs) throws SQLException {
        List<String> pkCols = newArrayListWithExpectedSize(expectedPKs.length);
        while (rs.next()) {
            pkCols.add(rs.getString("COLUMN_NAME"));
        }
        String[] actualPKs = pkCols.toArray(new String[0]);
        assertArrayEquals(expectedPKs, actualPKs);
    }

    @Test
    public void testCompositeDescPK() throws Exception {
        Properties props = new Properties();
        try (Connection globalConn = DriverManager.getConnection(getUrl(), props)) {
            String tableName = generateUniqueTableName();
            String viewName1 = generateUniqueViewName();
            String viewName2 = generateUniqueViewName();
            String viewName3 = generateUniqueViewName();
            String viewName4 = generateUniqueViewName();
            splitSystemCatalog(
                Lists.newArrayList(tableName, viewName1, viewName2, viewName3, viewName4));

            // create global base table
            globalConn.createStatement().execute("CREATE TABLE " + tableName
                    + " (TENANT_ID CHAR(15) NOT NULL, KEY_PREFIX CHAR(3) NOT NULL, CREATED_DATE DATE, CREATED_BY CHAR(15), SYSTEM_MODSTAMP DATE CONSTRAINT PK PRIMARY KEY (TENANT_ID, KEY_PREFIX)) VERSIONS=1, MULTI_TENANT=true, IMMUTABLE_ROWS=TRUE, REPLICATION_SCOPE=1");
            
            String tenantId = "tenantId";
            Properties tenantProps = new Properties();
            tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            // create a tenant specific view
            try (Connection tenantConn = DriverManager.getConnection(getUrl(), tenantProps)) {
                // create various tenant specific views
                // view with composite PK with multiple PK values of VARCHAR values DESC
                tenantConn.createStatement()
                        .execute("CREATE VIEW " + viewName1
                                + " (pk1 VARCHAR(10) NOT NULL, pk2 VARCHAR(10) NOT NULL, col1 DATE, col3 DECIMAL CONSTRAINT PK PRIMARY KEY (pk1 DESC, pk2 DESC)) AS SELECT * FROM "
                                + tableName + " WHERE KEY_PREFIX = 'abc' ");
                // view with composite PK with single pk value DESC
                tenantConn.createStatement()
                        .execute("CREATE VIEW " + viewName2
                                + " (pk1 VARCHAR(10) NOT NULL, pk2 VARCHAR(10) NOT NULL, col1 DATE, col3 DECIMAL CONSTRAINT PK PRIMARY KEY (pk1 DESC, pk2 DESC)) AS SELECT * FROM "
                                + tableName + " WHERE KEY_PREFIX = 'abc' ");

                // view with composite PK with multiple Date PK values DESC
                tenantConn.createStatement()
                        .execute("CREATE VIEW " + viewName3
                                + " (pk1 DATE(10) NOT NULL, pk2 DATE(10) NOT NULL, col1 VARCHAR(10), col3 DECIMAL CONSTRAINT PK PRIMARY KEY (pk1 DESC, pk2 DESC)) AS SELECT * FROM "
                                + tableName + " WHERE KEY_PREFIX = 'ab3' ");
                
                tenantConn.createStatement()
                .execute("CREATE VIEW " + viewName4
                        + " (pk1 DATE(10) NOT NULL, pk2 DECIMAL NOT NULL, pk3 VARCHAR(10) NOT NULL, col3 DECIMAL CONSTRAINT PK PRIMARY KEY (pk1 DESC, pk2 DESC, pk3 DESC)) AS SELECT * FROM "
                        + tableName + " WHERE KEY_PREFIX = 'ab4' ");

                // upsert rows
                upsertRows(viewName1, tenantConn);
                upsertRows(viewName2, tenantConn);

                // run queries
                String[] whereClauses =
                        new String[] { "pk1 = 'testa'", "", "pk1 >= 'testa'", "pk1 <= 'testa'",
                                "pk1 > 'testa'", "pk1 < 'testa'" };
                long[] expectedArray = new long[] { 4, 5, 5, 4, 1, 0 };
                validate(viewName1, tenantConn, whereClauses, expectedArray);
                validate(viewName2, tenantConn, whereClauses, expectedArray);

                tenantConn.createStatement().execute("UPSERT INTO " + viewName3
                        + " (pk1, pk2, col1, col3) VALUES (TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss'), TO_DATE('2017-10-16 21:00:00', 'yyyy-MM-dd HH:mm:ss'), 'txt1', 10)");
                tenantConn.createStatement().execute("UPSERT INTO " + viewName3
                        + " (pk1, pk2, col1, col3) VALUES (TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss'), TO_DATE('2017-10-16 21:01:00', 'yyyy-MM-dd HH:mm:ss'), 'txt1', 10)");
                tenantConn.createStatement().execute("UPSERT INTO " + viewName3
                        + " (pk1, pk2, col1, col3) VALUES (TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss'), TO_DATE('2017-10-16 21:02:00', 'yyyy-MM-dd HH:mm:ss'), 'txt1', 10)");
                tenantConn.createStatement().execute("UPSERT INTO " + viewName3
                        + " (pk1, pk2, col1, col3) VALUES (TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss'), TO_DATE('2017-10-16 21:03:00', 'yyyy-MM-dd HH:mm:ss'), 'txt1', 10)");
                tenantConn.createStatement().execute("UPSERT INTO " + viewName3
                        + " (pk1, pk2, col1, col3) VALUES (TO_DATE('2017-10-16 23:00:00', 'yyyy-MM-dd HH:mm:ss'), TO_DATE('2017-10-16 21:04:00', 'yyyy-MM-dd HH:mm:ss'), 'txt1', 10)");
                tenantConn.commit();

                String[] view3WhereClauses =
                        new String[] {
                                "pk1 = TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss')", "",
                                "pk1 >= TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss')",
                                "pk1 <= TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss')",
                                "pk1 > TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss')",
                                "pk1 < TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss')" };
                validate(viewName3, tenantConn, view3WhereClauses, expectedArray);

                tenantConn.createStatement().execute("UPSERT INTO " + viewName4
                        + " (pk1, pk2, pk3, col3) VALUES (TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss'), 1, 'txt1', 10)");
                tenantConn.createStatement().execute("UPSERT INTO " + viewName4
                        + " (pk1, pk2, pk3, col3) VALUES (TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss'), 2, 'txt2', 10)");
                tenantConn.createStatement().execute("UPSERT INTO " + viewName4
                        + " (pk1, pk2, pk3, col3) VALUES (TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss'), 3, 'txt3', 10)");
                tenantConn.createStatement().execute("UPSERT INTO " + viewName4
                        + " (pk1, pk2, pk3, col3) VALUES (TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss'), 4, 'txt4', 10)");
                tenantConn.createStatement().execute("UPSERT INTO " + viewName4
                        + " (pk1, pk2, pk3, col3) VALUES (TO_DATE('2017-10-16 23:00:00', 'yyyy-MM-dd HH:mm:ss'), 1, 'txt1', 10)");
                tenantConn.commit();

                String[] view4WhereClauses =
                        new String[] {
                                "pk1 = TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss')",
                                "pk1 = TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss') AND pk2 = 2",
                                "pk1 = TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss') AND pk2 > 2",
                                "", "pk1 >= TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss')",
                                "pk1 <= TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss')",
                                "pk1 > TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss')",
                                "pk1 < TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss')" };
                long[] view4ExpectedArray = new long[] { 4, 1, 2, 5, 5, 4, 1, 0 };
                validate(viewName4, tenantConn, view4WhereClauses, view4ExpectedArray);

            }
        }
    }

    private void validate(String viewName, Connection tenantConn, String[] whereClauseArray,
            long[] expectedArray) throws SQLException {
        for (int i = 0; i < whereClauseArray.length; ++i) {
            String where = !whereClauseArray[i].isEmpty() ? (" WHERE " + whereClauseArray[i]) : "";
            ResultSet rs =
                    tenantConn.createStatement()
                            .executeQuery("SELECT count(*) FROM " + viewName + where);
            assertTrue(rs.next());
            assertEquals(expectedArray[i], rs.getLong(1));
            assertFalse(rs.next());
        }
    }

    private void upsertRows(String viewName1, Connection tenantConn) throws SQLException {
        tenantConn.createStatement().execute("UPSERT INTO " + viewName1
                + " (pk1, pk2, col1, col3) VALUES ('testa', 'testb', TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss'), 10)");
        tenantConn.createStatement().execute("UPSERT INTO " + viewName1
                + " (pk1, pk2, col1, col3) VALUES ('testa', 'testc', TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss'), 10)");
        tenantConn.createStatement().execute("UPSERT INTO " + viewName1
                + " (pk1, pk2, col1, col3) VALUES ('testa', 'testd', TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss'), 10)");
        tenantConn.createStatement().execute("UPSERT INTO " + viewName1
                + " (pk1, pk2, col1, col3) VALUES ('testa', 'teste', TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss'), 10)");
        tenantConn.createStatement().execute("UPSERT INTO " + viewName1
                + " (pk1, pk2, col1, col3) VALUES ('testb', 'testa', TO_DATE('2017-10-16 22:00:00', 'yyyy-MM-dd HH:mm:ss'), 10)");
        tenantConn.commit();
    }
    
    @Test
    public void testUpdatingPropertyOnBaseTable() throws Exception {
        String fullTableName = generateUniqueTableName();
        String fullViewName = generateUniqueViewName();
        splitSystemCatalog(Lists.newArrayList(fullTableName, fullViewName));
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            conn.createStatement()
                    .execute("create table " + fullTableName
                            + "(tenantId CHAR(15) NOT NULL, pk1 integer NOT NULL, v varchar CONSTRAINT PK PRIMARY KEY "
                            + "(tenantId, pk1)) MULTI_TENANT=true");
            conn.createStatement().execute("CREATE VIEW " + fullViewName + " AS SELECT * FROM " + fullTableName);
            
            conn.createStatement()
                    .execute("ALTER TABLE " + fullTableName + " set IMMUTABLE_ROWS = true");
            
            // fetch the latest tables
            PTable table = PhoenixRuntime.getTableNoCache(conn, fullTableName);
            PTable view = PhoenixRuntime.getTableNoCache(conn, fullViewName);
            assertEquals("IMMUTABLE_ROWS property set incorrectly", true, table.isImmutableRows());
            assertEquals("IMMUTABLE_ROWS property set incorrectly", true, view.isImmutableRows());
        }
    }
}
