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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixTransactionalProcessor;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
public class AlterTableWithViewsIT extends BaseHBaseManagedTimeIT {
    
    private final boolean isMultiTenant;
    
    private final String TENANT_SPECIFIC_URL1 = getUrl() + ';' + TENANT_ID_ATTRIB + "=tenant1";
    private final String TENANT_SPECIFIC_URL2 = getUrl() + ';' + TENANT_ID_ATTRIB + "=tenant2";
    
    public AlterTableWithViewsIT(boolean isMultiTenant) {
        this.isMultiTenant = isMultiTenant;
    }
    
    @Parameters(name="multiTenant = {0}")
    public static Collection<Boolean> data() {
        return Arrays.asList(false, true);
    }
	
	@BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
	
    private String generateDDL(String format) {
        return String.format(format, isMultiTenant ? "TENANT_ID VARCHAR NOT NULL, " : "",
            isMultiTenant ? "TENANT_ID, " : "", isMultiTenant ? "MULTI_TENANT=true" : "");
    }
    
    @Test
    public void testAddNewColumnsToBaseTableWithViews() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {       
            String ddlFormat = "CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                            + " %s ID char(1) NOT NULL,"
                            + " COL1 integer NOT NULL,"
                            + " COL2 bigint NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
                            + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            viewConn.createStatement().execute("CREATE VIEW VIEWOFTABLE ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // adding a new pk column and a new regular column
            conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD COL3 varchar(10) PRIMARY KEY, COL4 integer");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 1, 5, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2", "COL3", "COL4");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 1, 7, 5, "ID", "COL1", "COL2", "COL3", "COL4", "VIEW_COL1", "VIEW_COL2");
        } 
    }
    
    @Test
    public void testDropColumnsFromBaseTableWithView() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) { 
            String ddlFormat = "CREATE TABLE IF NOT EXISTS TABLEWITHVIEW (" + " %s ID char(1) NOT NULL,"
                            + " COL1 integer NOT NULL," + " COL2 bigint NOT NULL,"
                            + " COL3 varchar(10)," + " COL4 varchar(10)," + " COL5 varchar(10),"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)" + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 6,
                QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2", "COL3", "COL4",
                "COL5");

            viewConn.createStatement()
                    .execute(
                        "CREATE VIEW VIEWOFTABLE ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 8, 6,
                "ID", "COL1", "COL2", "COL3", "COL4", "COL5", "VIEW_COL1", "VIEW_COL2");

            // drop two columns from the base table
            conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW DROP COLUMN COL3, COL5");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 1, 4,
                QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2", "COL4");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 1, 6, 4,
                "ID", "COL1", "COL2", "COL4", "VIEW_COL1", "VIEW_COL2");
        }
    }
    
    @Test
    public void testAddExistingViewColumnToBaseTableWithViews() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {
            conn.setAutoCommit(false);
            viewConn.setAutoCommit(false);
            String ddlFormat = "CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                            + " %s ID char(10) NOT NULL,"
                            + " COL1 integer NOT NULL,"
                            + " COL2 bigint NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
                            + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            viewConn.createStatement().execute("CREATE VIEW VIEWOFTABLE ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256), VIEW_COL3 VARCHAR, VIEW_COL4 DECIMAL, VIEW_COL5 DECIMAL(10,2), VIEW_COL6 VARCHAR, CONSTRAINT pk PRIMARY KEY (VIEW_COL5, VIEW_COL6) ) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 9, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2", "VIEW_COL3", "VIEW_COL4", "VIEW_COL5", "VIEW_COL6");
            
            // upsert single row into view
            String dml = "UPSERT INTO VIEWOFTABLE VALUES(?,?,?,?,?, ?, ?, ?, ?)";
            PreparedStatement stmt = viewConn.prepareStatement(dml);
            stmt.setString(1, "view1");
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setInt(4, 14);
            stmt.setString(5, "view5");
            stmt.setString(6, "view6");
            stmt.setInt(7, 17);
            stmt.setInt(8, 18);
            stmt.setString(9, "view9");
            stmt.execute();
            viewConn.commit();
            
            try {
                // should fail because there is already a view column with same name of different type
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 char(10)");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }           
            
            try {
                // should fail because there is already a view column with same name with different scale
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,1)");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            } 
            
            try {
                // should fail because there is already a view column with same name with different length
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(9,2)");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            } 
            
            try {
                // should fail because there is already a view column with different length
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL2 VARCHAR");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            // validate that there were no columns added to the table or view
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 9, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2", "VIEW_COL3", "VIEW_COL4", "VIEW_COL5", "VIEW_COL6");
            
            // should succeed 
            conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL4 DECIMAL, VIEW_COL2 VARCHAR(256)");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 1, 5, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2", "VIEW_COL4", "VIEW_COL2");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 1, 9, 5, "ID", "COL1", "COL2", "VIEW_COL4", "VIEW_COL2", "VIEW_COL1", "VIEW_COL3", "VIEW_COL5", "VIEW_COL6");
            
            // query table
            ResultSet rs = stmt.executeQuery("SELECT * FROM TABLEWITHVIEW");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertEquals(17, rs.getInt("VIEW_COL4"));
            assertFalse(rs.next());

            // query view
            rs = stmt.executeQuery("SELECT * FROM VIEWOFTABLE");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertEquals("view6", rs.getString("VIEW_COL3"));
            assertEquals(17, rs.getInt("VIEW_COL4"));
            assertEquals(18, rs.getInt("VIEW_COL5"));
            assertEquals("view9", rs.getString("VIEW_COL6"));
            assertFalse(rs.next());
        } 
    }
    
    @Test
    public void testAddExistingViewPkColumnToBaseTableWithViews() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {      
            conn.setAutoCommit(false);
            viewConn.setAutoCommit(false);
            String ddlFormat = "CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                            + " %s ID char(10) NOT NULL,"
                            + " COL1 integer NOT NULL,"
                            + " COL2 integer NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
                            + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            viewConn.createStatement().execute("CREATE VIEW VIEWOFTABLE ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // upsert single row into view
            String dml = "UPSERT INTO VIEWOFTABLE VALUES(?,?,?,?,?)";
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
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL2 VARCHAR(256) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because there we have to add both VIEW_COL1 and VIEW_COL2  to the pk 
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because there we have to add both VIEW_COL1 and VIEW_COL2 to the pk
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because there we have to add both VIEW_COL1 and VIEW_COL2  to the pk 
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY, VIEW_COL2 VARCHAR(256)");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because there we have to add both VIEW_COL1 and VIEW_COL2 to the pk in the right order
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL2 VARCHAR(256) PRIMARY KEY, VIEW_COL1 DECIMAL(10,2) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because there we have to add both VIEW_COL1 and VIEW_COL2 with the right sort order
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY DESC, VIEW_COL2 VARCHAR(256) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            // add the pk column of the view to the base table
            conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY, VIEW_COL2 VARCHAR(256) PRIMARY KEY");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 1, 5, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 1, 5, 5, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // query table
            ResultSet rs = stmt.executeQuery("SELECT * FROM TABLEWITHVIEW");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());

            // query view
            rs = stmt.executeQuery("SELECT * FROM VIEWOFTABLE");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());
        } 
    }
    
    @Test
    public void testAddExistingViewPkColumnToBaseTableWithMultipleViews() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {              
            String ddlFormat = "CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                            + " %s ID char(10) NOT NULL,"
                            + " COL1 integer NOT NULL,"
                            + " COL2 integer NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
                            + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            viewConn.createStatement().execute("CREATE VIEW VIEWOFTABLE1 ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE1", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            viewConn.createStatement().execute("CREATE VIEW VIEWOFTABLE2 ( VIEW_COL3 VARCHAR(256), VIEW_COL4 DECIMAL(10,2) CONSTRAINT pk PRIMARY KEY (VIEW_COL3, VIEW_COL4)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE2", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL3", "VIEW_COL4");
            
            try {
                // should fail because there are two view with different pk columns
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL PRIMARY KEY, VIEW_COL2 VARCHAR PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because there are two view with different pk columns
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL3 VARCHAR PRIMARY KEY, VIEW_COL4 DECIMAL PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because slot positions of pks are different
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL PRIMARY KEY, VIEW_COL2 VARCHAR PRIMARY KEY, VIEW_COL3 VARCHAR PRIMARY KEY, VIEW_COL4 DECIMAL PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because slot positions of pks are different
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL3 VARCHAR PRIMARY KEY, VIEW_COL4 DECIMAL PRIMARY KEY, VIEW_COL1 DECIMAL PRIMARY KEY, VIEW_COL2 VARCHAR PRIMARY KEY");
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
            String ddlFormat = "CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                    + " %s ID char(10) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 integer NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
                    + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            viewConn.createStatement().execute("CREATE VIEW VIEWOFTABLE1 ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE1", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            viewConn2.createStatement().execute("CREATE VIEW VIEWOFTABLE2 ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE2", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // upsert single row into both view
            String dml = "UPSERT INTO VIEWOFTABLE1 VALUES(?,?,?,?,?)";
            PreparedStatement stmt = viewConn.prepareStatement(dml);
            stmt.setString(1, "view1");
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setInt(4, 14);
            stmt.setString(5, "view5");
            stmt.execute();
            viewConn.commit();
            dml = "UPSERT INTO VIEWOFTABLE2 VALUES(?,?,?,?,?)";
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
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because the view have two extra columns in their pk
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL2 VARCHAR(256) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            try {
                // should fail because slot positions of pks are different
                conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL2 DECIMAL(10,2) PRIMARY KEY, VIEW_COL1 VARCHAR(256) PRIMARY KEY");
                fail();
            }
            catch (SQLException e) {
                assertEquals("Unexpected exception", CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
            
            conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD VIEW_COL1 DECIMAL(10,2) PRIMARY KEY, VIEW_COL2 VARCHAR(256) PRIMARY KEY");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 1, 5, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            assertTableDefinition(conn, "VIEWOFTABLE1", PTableType.VIEW, "TABLEWITHVIEW", 1, 5, 5, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            assertTableDefinition(conn, "VIEWOFTABLE2", PTableType.VIEW, "TABLEWITHVIEW", 1, 5, 5, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // query table
            ResultSet rs = stmt.executeQuery("SELECT * FROM TABLEWITHVIEW");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());

            // query both views
            rs = viewConn.createStatement().executeQuery("SELECT * FROM VIEWOFTABLE1");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());
            rs = viewConn2.createStatement().executeQuery("SELECT * FROM VIEWOFTABLE2");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());
        }
    }
    
    public void assertTableDefinition(Connection conn, String tableName, PTableType tableType, String parentTableName, int sequenceNumber, int columnCount, int baseColumnCount, String... columnNames) throws Exception {
        int delta = isMultiTenant ? 1 : 0;
        String[] cols;
        if (isMultiTenant) {
            cols = (String[])ArrayUtils.addAll(new String[]{"TENANT_ID"}, columnNames);
        }
        else {
            cols = columnNames;
        }
        AlterMultiTenantTableWithViews.assertTableDefinition(conn, tableName, tableType, parentTableName, sequenceNumber, columnCount + delta,
            baseColumnCount==QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT ? baseColumnCount : baseColumnCount +delta, cols);
    }
    
    public static String getSystemCatalogEntriesForTable(Connection conn, String tableName, String message) throws Exception {
        StringBuilder sb = new StringBuilder(message);
        sb.append("\n\n\n");
        ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM SYSTEM.CATALOG WHERE TABLE_NAME='"+ tableName +"'");
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
        String baseTable = "testAlteringViewThatHasChildViews";
        String childView = "childView";
        String grandChildView = "grandChildView";
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {
            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + baseTable + "  ("
                    + " %s PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
                    + " CONSTRAINT NAME_PK PRIMARY KEY (%s PK2)"
                    + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));

            String childViewDDL = "CREATE VIEW " + childView + " AS SELECT * FROM " + baseTable;
            viewConn.createStatement().execute(childViewDDL);

            String addColumnToChildViewDDL =
                    "ALTER VIEW " + childView + " ADD CHILD_VIEW_COL VARCHAR";
            viewConn.createStatement().execute(addColumnToChildViewDDL);

            String grandChildViewDDL =
                    "CREATE VIEW " + grandChildView + " AS SELECT * FROM " + childView;
            viewConn.createStatement().execute(grandChildViewDDL);

            // dropping base table column from child view should succeed
            String dropColumnFromChildView = "ALTER VIEW " + childView + " DROP COLUMN V2";
            viewConn.createStatement().execute(dropColumnFromChildView);

            // dropping view specific column from child view should succeed
            dropColumnFromChildView = "ALTER VIEW " + childView + " DROP COLUMN CHILD_VIEW_COL";
            viewConn.createStatement().execute(dropColumnFromChildView);
            
            // Adding column to view that has child views is allowed
            String addColumnToChildView = "ALTER VIEW " + childView + " ADD V5 VARCHAR";
            viewConn.createStatement().execute(addColumnToChildView);
            // V5 column should be visible now for childView
            viewConn.createStatement().execute("SELECT V5 FROM " + childView);    
            
            // However, column V5 shouldn't have propagated to grandChildView. Not till PHOENIX-2054 is fixed.
            try {
                viewConn.createStatement().execute("SELECT V5 FROM " + grandChildView);
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
            }

            // dropping column from the grand child view, however, should work.
            String dropColumnFromGrandChildView =
                    "ALTER VIEW " + grandChildView + " DROP COLUMN CHILD_VIEW_COL";
            viewConn.createStatement().execute(dropColumnFromGrandChildView);

            // similarly, dropping column inherited from the base table should work.
            dropColumnFromGrandChildView = "ALTER VIEW " + grandChildView + " DROP COLUMN V2";
            viewConn.createStatement().execute(dropColumnFromGrandChildView);
        }
    }
    
    @Test
    public void testDivergedViewsStayDiverged() throws Exception {
        String baseTable = "testDivergedViewsStayDiverged";
        String view1 = baseTable + "_view1";
        String view2 = baseTable + "_view2";
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ;
                Connection viewConn2 = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL2) : conn) {
            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + baseTable + " ("
                    + " %s PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR "
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
            
            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD V3 VARCHAR";
            conn.createStatement().execute(alterBaseTable);
            
            // Column V3 shouldn't have propagated to the diverged view.
            String sql = "SELECT V3 FROM " + view1;
            try {
                viewConn.createStatement().execute(sql);
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
            }
            
            // However, column V3 should have propagated to the non-diverged view.
            sql = "SELECT V3 FROM " + view2;
            viewConn2.createStatement().execute(sql);
        } 
    }
    
    @Test
    public void testMakeBaseTableTransactional() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl());
                Connection viewConn = isMultiTenant ? DriverManager.getConnection(TENANT_SPECIFIC_URL1) : conn ) {  
            String baseTableName = "NONTXNTBL_" + (isMultiTenant ? "0":"1");
            String ddlFormat = "CREATE TABLE IF NOT EXISTS " + baseTableName + " ("
                            + " %s ID char(1) NOT NULL,"
                            + " COL1 integer NOT NULL,"
                            + " COL2 bigint NOT NULL,"
                            + " CONSTRAINT NAME_PK PRIMARY KEY (%s ID, COL1, COL2)"
                            + " ) %s";
            conn.createStatement().execute(generateDDL(ddlFormat));
            assertTableDefinition(conn, baseTableName, PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            viewConn.createStatement().execute("CREATE VIEW VIEWOFTABLE ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM "+baseTableName);
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            PName tenantId = isMultiTenant ? PNameFactory.newName("tenant1") : null;
            PhoenixConnection phoenixConn = conn.unwrap(PhoenixConnection.class);
            HTableInterface htable = phoenixConn.getQueryServices().getTable(Bytes.toBytes(baseTableName));
            assertFalse(htable.getTableDescriptor().getCoprocessors().contains(PhoenixTransactionalProcessor.class.getName()));
            assertFalse(phoenixConn.getTable(new PTableKey(null, baseTableName)).isTransactional());
            assertFalse(viewConn.unwrap(PhoenixConnection.class).getTable(new PTableKey(tenantId, "VIEWOFTABLE")).isTransactional());
            
            // make the base table transactional
            conn.createStatement().execute("ALTER TABLE " + baseTableName + " SET TRANSACTIONAL=true");
            // query the view to force the table cache to be updated
            viewConn.createStatement().execute("SELECT * FROM VIEWOFTABLE");
            htable = phoenixConn.getQueryServices().getTable(Bytes.toBytes(baseTableName));
            assertTrue(htable.getTableDescriptor().getCoprocessors().contains(PhoenixTransactionalProcessor.class.getName()));
            assertTrue(phoenixConn.getTable(new PTableKey(null, baseTableName)).isTransactional());
            assertTrue(viewConn.unwrap(PhoenixConnection.class).getTable(new PTableKey(tenantId, "VIEWOFTABLE")).isTransactional());
        } 
    }
    
}
