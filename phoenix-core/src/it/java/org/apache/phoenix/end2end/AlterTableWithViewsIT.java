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
import static org.apache.phoenix.query.QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT;
import static org.apache.phoenix.query.QueryConstants.DIVERGED_VIEW_BASE_COLUMN_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.PhoenixTransactionalProcessor;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

public class AlterTableWithViewsIT extends BaseHBaseManagedTimeIT {
	
	@BeforeClass
    @Shadower(classBeingShadowed = BaseHBaseManagedTimeIT.class)
    public static void doSetup() throws Exception {
        Map<String,String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.TRANSACTIONS_ENABLED, Boolean.toString(true));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }
    
    @Test
    public void testAddNewColumnsToBaseTableWithViews() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {       
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                    + " ID char(1) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
                    + " )");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // adding a new pk column and a new regular column
            conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW ADD COL3 varchar(10) PRIMARY KEY, COL4 integer");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 1, 5, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2", "COL3", "COL4");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 1, 7, 5, "ID", "COL1", "COL2", "COL3", "COL4", "VIEW_COL1", "VIEW_COL2");
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAddExistingViewColumnToBaseTableWithViews() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {       
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                    + " ID char(10) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
                    + " )");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256), VIEW_COL3 VARCHAR, VIEW_COL4 DECIMAL, VIEW_COL5 DECIMAL(10,2), VIEW_COL6 VARCHAR, CONSTRAINT pk PRIMARY KEY (VIEW_COL5, VIEW_COL6) ) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 9, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2", "VIEW_COL3", "VIEW_COL4", "VIEW_COL5", "VIEW_COL6");
            
            // upsert single row into view
            String dml = "UPSERT INTO VIEWOFTABLE VALUES(?,?,?,?,?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
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
            conn.commit();
            
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
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAddExistingViewPkColumnToBaseTableWithViews() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {       
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                    + " ID char(10) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 integer NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
                    + " )");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // upsert single row into view
            String dml = "UPSERT INTO VIEWOFTABLE VALUES(?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "view1");
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setInt(4, 14);
            stmt.setString(5, "view5");
            stmt.execute();
            conn.commit();
            
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
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAddExistingViewPkColumnToBaseTableWithMultipleViews() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {       
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                    + " ID char(10) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 integer NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
                    + " )");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE1 ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE1", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE2 ( VIEW_COL3 VARCHAR(256), VIEW_COL4 DECIMAL(10,2) CONSTRAINT pk PRIMARY KEY (VIEW_COL3, VIEW_COL4)) AS SELECT * FROM TABLEWITHVIEW");
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
        } finally {
            conn.close();
        }
    }
    
    @Test
    public void testAddExistingViewPkColumnToBaseTableWithMultipleViewsHavingSamePks() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        try {       
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                    + " ID char(10) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 integer NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
                    + " )");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE1 ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE1", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE2 ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR(256) CONSTRAINT pk PRIMARY KEY (VIEW_COL1, VIEW_COL2)) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE2", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            // upsert single row into both view
            String dml = "UPSERT INTO VIEWOFTABLE1 VALUES(?,?,?,?,?)";
            PreparedStatement stmt = conn.prepareStatement(dml);
            stmt.setString(1, "view1");
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setInt(4, 14);
            stmt.setString(5, "view5");
            stmt.execute();
            conn.commit();
            dml = "UPSERT INTO VIEWOFTABLE2 VALUES(?,?,?,?,?)";
            stmt = conn.prepareStatement(dml);
            stmt.setString(1, "view1");
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setInt(4, 14);
            stmt.setString(5, "view5");
            stmt.execute();
            conn.commit();
            
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
            rs = stmt.executeQuery("SELECT * FROM VIEWOFTABLE1");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());
            rs = stmt.executeQuery("SELECT * FROM VIEWOFTABLE2");
            assertTrue(rs.next());
            assertEquals("view1", rs.getString("ID"));
            assertEquals(12, rs.getInt("COL1"));
            assertEquals(13, rs.getInt("COL2"));
            assertEquals(14, rs.getInt("VIEW_COL1"));
            assertEquals("view5", rs.getString("VIEW_COL2"));
            assertFalse(rs.next());
        } finally {
            conn.close();
        }
    }
    
    private void assertTableDefinition(Connection conn, String tableName, PTableType tableType, String parentTableName, int sequenceNumber, int columnCount, int baseColumnCount, String... columnName) throws Exception {
        PreparedStatement p = conn.prepareStatement("SELECT * FROM SYSTEM.CATALOG WHERE TABLE_NAME=? AND TABLE_TYPE=?");
        p.setString(1, tableName);
        p.setString(2, tableType.getSerializedValue());
        ResultSet rs = p.executeQuery();
        assertTrue(rs.next());
        assertEquals(getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in BaseColumnCount"), baseColumnCount, rs.getInt("BASE_COLUMN_COUNT"));
        assertEquals(getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in columnCount"), columnCount, rs.getInt("COLUMN_COUNT"));
        assertEquals(getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in sequenceNumber"), sequenceNumber, rs.getInt("TABLE_SEQ_NUM"));
        rs.close();

        ResultSet parentTableColumnsRs = null; 
        if (parentTableName != null) {
            parentTableColumnsRs = conn.getMetaData().getColumns(null, null, parentTableName, null);
            parentTableColumnsRs.next();
        }
        
        ResultSet viewColumnsRs = conn.getMetaData().getColumns(null, null, tableName, null);
        for (int i = 0; i < columnName.length; i++) {
            if (columnName[i] != null) {
                assertTrue(viewColumnsRs.next());
                assertEquals(getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in columnName: i=" + i), columnName[i], viewColumnsRs.getString(PhoenixDatabaseMetaData.COLUMN_NAME));
                int viewColOrdinalPos = viewColumnsRs.getInt(PhoenixDatabaseMetaData.ORDINAL_POSITION);
                assertEquals(getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in ordinalPosition: i=" + i), i+1, viewColOrdinalPos);
                // validate that all the columns in the base table are present in the view   
                if (parentTableColumnsRs != null && !parentTableColumnsRs.isAfterLast()) {
                    ResultSetMetaData parentTableColumnsMetadata = parentTableColumnsRs.getMetaData();
                    assertEquals(parentTableColumnsMetadata.getColumnCount(), viewColumnsRs.getMetaData().getColumnCount());
                    int parentTableColOrdinalRs = parentTableColumnsRs.getInt(PhoenixDatabaseMetaData.ORDINAL_POSITION);
                    assertEquals(getSystemCatalogEntriesForTable(conn, tableName, "Mismatch in ordinalPosition of view and base table for i=" + i), parentTableColOrdinalRs, viewColOrdinalPos);
                    for (int columnIndex = 1; columnIndex < parentTableColumnsMetadata.getColumnCount(); columnIndex++) {
                        String viewColumnValue = viewColumnsRs.getString(columnIndex);
                        String parentTableColumnValue = parentTableColumnsRs.getString(columnIndex);
                        if (!Objects.equal(viewColumnValue, parentTableColumnValue)) {
                            if (parentTableColumnsMetadata.getColumnName(columnIndex).equals(PhoenixDatabaseMetaData.TABLE_NAME)) {
                                assertEquals(parentTableName, parentTableColumnValue);
                                assertEquals(tableName, viewColumnValue);
                            } 
                        }
                    }
                    parentTableColumnsRs.next();
                }
            }
        }
        assertFalse(getSystemCatalogEntriesForTable(conn, tableName, ""), viewColumnsRs.next());
    }
    
    private String getSystemCatalogEntriesForTable(Connection conn, String tableName, String message) throws Exception {
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
    public void testCacheInvalidatedAfterAddingColumnToBaseTableWithViews() throws Exception {
        String baseTable = "testCacheInvalidatedAfterAddingColumnToBaseTableWithViews";
        String viewName = baseTable + "_view";
        String tenantId = "tenantId";
        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            String tableDDL = "CREATE TABLE " + baseTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true " ;
            globalConn.createStatement().execute(tableDDL);
            Properties tenantProps = new Properties();
            tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
            // create a tenant specific view
            try (Connection tenantConn =  DriverManager.getConnection(getUrl(), tenantProps)) {
                String viewDDL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + baseTable;
                tenantConn.createStatement().execute(viewDDL);
                
                // Add a column to the base table using global connection
                globalConn.createStatement().execute("ALTER TABLE " + baseTable + " ADD NEW_COL VARCHAR");

                // Check now whether the tenant connection can see the column that was added
                tenantConn.createStatement().execute("SELECT NEW_COL FROM " + viewName);
                tenantConn.createStatement().execute("SELECT NEW_COL FROM " + baseTable);
            }
        }
    }
    
    @Test
    public void testDropColumnOnTableWithViewsNotAllowed() throws Exception {
        String baseTable = "testDropColumnOnTableWithViewsNotAllowed";
        String viewName = baseTable + "_view";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableDDL = "CREATE TABLE " + baseTable + " (PK1 VARCHAR NOT NULL PRIMARY KEY, V1 VARCHAR, V2 VARCHAR)";
            conn.createStatement().execute(tableDDL);
            
            String viewDDL = "CREATE VIEW " + viewName + " AS SELECT * FROM " + baseTable;
            conn.createStatement().execute(viewDDL);
            
            String dropColumn = "ALTER TABLE " + baseTable + " DROP COLUMN V2";
            try {
                conn.createStatement().execute(dropColumn);
                fail("Dropping column on a base table that has views is not allowed");
            } catch (SQLException e) {  
                assertEquals(CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
            }
        }
    }
    
    @Test
    public void testAlteringViewThatHasChildViews() throws Exception {
        String baseTable = "testAlteringViewThatHasChildViews";
        String childView = "childView";
        String grandChildView = "grandChildView";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL =
                    "CREATE TABLE " +  baseTable + " (TENANT_ID VARCHAR NOT NULL, PK2 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK2))";
            conn.createStatement().execute(baseTableDDL);

            String childViewDDL = "CREATE VIEW " + childView + " AS SELECT * FROM " + baseTable;
            conn.createStatement().execute(childViewDDL);

            String addColumnToChildViewDDL =
                    "ALTER VIEW " + childView + " ADD CHILD_VIEW_COL VARCHAR";
            conn.createStatement().execute(addColumnToChildViewDDL);

            String grandChildViewDDL =
                    "CREATE VIEW " + grandChildView + " AS SELECT * FROM " + childView;
            conn.createStatement().execute(grandChildViewDDL);

            // dropping base table column from child view should succeed
            String dropColumnFromChildView = "ALTER VIEW " + childView + " DROP COLUMN V2";
            conn.createStatement().execute(dropColumnFromChildView);

            // dropping view specific column from child view should succeed
            dropColumnFromChildView = "ALTER VIEW " + childView + " DROP COLUMN CHILD_VIEW_COL";
            conn.createStatement().execute(dropColumnFromChildView);
            
            // Adding column to view that has child views is allowed
            String addColumnToChildView = "ALTER VIEW " + childView + " ADD V5 VARCHAR";
            conn.createStatement().execute(addColumnToChildView);
            // V5 column should be visible now for childView
            conn.createStatement().execute("SELECT V5 FROM " + childView);    
            
            // However, column V5 shouldn't have propagated to grandChildView. Not till PHOENIX-2054 is fixed.
            try {
                conn.createStatement().execute("SELECT V5 FROM " + grandChildView);
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
            }

            // dropping column from the grand child view, however, should work.
            String dropColumnFromGrandChildView =
                    "ALTER VIEW " + grandChildView + " DROP COLUMN CHILD_VIEW_COL";
            conn.createStatement().execute(dropColumnFromGrandChildView);

            // similarly, dropping column inherited from the base table should work.
            dropColumnFromGrandChildView = "ALTER VIEW " + grandChildView + " DROP COLUMN V2";
            conn.createStatement().execute(dropColumnFromGrandChildView);
        }
    }
    
    @Test
    public void testDivergedViewsStayDiverged() throws Exception {
        String baseTable = "testDivergedViewsStayDiverged";
        String view1 = baseTable + "_view1";
        String view2 = baseTable + "_view2";
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String tableDDL = "CREATE TABLE " + baseTable + " (PK1 VARCHAR NOT NULL PRIMARY KEY, V1 VARCHAR, V2 VARCHAR)";
            conn.createStatement().execute(tableDDL);
            
            String viewDDL = "CREATE VIEW " + view1 + " AS SELECT * FROM " + baseTable;
            conn.createStatement().execute(viewDDL);
            
            viewDDL = "CREATE VIEW " + view2 + " AS SELECT * FROM " + baseTable;
            conn.createStatement().execute(viewDDL);
            
            // Drop the column inherited from base table to make it diverged
            String dropColumn = "ALTER VIEW " + view1 + " DROP COLUMN V2";
            conn.createStatement().execute(dropColumn);
            
            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD V3 VARCHAR";
            conn.createStatement().execute(alterBaseTable);
            
            // Column V3 shouldn't have propagated to the diverged view.
            String sql = "SELECT V3 FROM " + view1;
            try {
                conn.createStatement().execute(sql);
            } catch (SQLException e) {
                assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
            }
            
            // However, column V3 should have propagated to the non-diverged view.
            sql = "SELECT V3 FROM " + view2;
            conn.createStatement().execute(sql);
        } 
    }
    
    @Test
    public void testAddingColumnToBaseTablePropagatesToEntireViewHierarchy() throws Exception {
        String baseTable = "testViewHierarchy";
        String view1 = "view1";
        String view2 = "view2";
        String view3 = "view3";
        String view4 = "view4";
        /*                                     baseTable
                                 /                  |               \ 
                         view1(tenant1)    view3(tenant2)          view4(global)
                          /
                        view2(tenant1)  
        */
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE " + baseTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true ";
            conn.createStatement().execute(baseTableDDL);
            
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + view1 + " AS SELECT * FROM " + baseTable;
                tenant1Conn.createStatement().execute(view1DDL);
                
                String view2DDL = "CREATE VIEW " + view2 + " AS SELECT * FROM " + view1;
                tenant1Conn.createStatement().execute(view2DDL);
            }
            
            try (Connection tenant2Conn = getTenantConnection("tenant2")) {
                String view3DDL = "CREATE VIEW " + view3 + " AS SELECT * FROM " + baseTable;
                tenant2Conn.createStatement().execute(view3DDL);
            }
            
            String view4DDL = "CREATE VIEW " + view4 + " AS SELECT * FROM " + baseTable;
            conn.createStatement().execute(view4DDL);
            
            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD V3 VARCHAR";
            conn.createStatement().execute(alterBaseTable);
            
            // verify that the column is visible to view4
            conn.createStatement().execute("SELECT V3 FROM " + view4);
            
            // verify that the column is visible to view1 and view2
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                tenant1Conn.createStatement().execute("SELECT V3 from " + view1);
                tenant1Conn.createStatement().execute("SELECT V3 from " + view2);
            }
            
            // verify that the column is visible to view3
            try (Connection tenant2Conn = getTenantConnection("tenant2")) {
                tenant2Conn.createStatement().execute("SELECT V3 from " + view3);
            }
            
        }
           
    }
    
    @Test
    public void testChangingPKOfBaseTableChangesPKForAllViews() throws Exception {
        String baseTable = "testChangePKOfBaseTable";
        String view1 = "view1";
        String view2 = "view2";
        String view3 = "view3";
        String view4 = "view4";
        /*                                     baseTable
                                 /                  |               \ 
                         view1(tenant1)    view3(tenant2)          view4(global)
                          /
                        view2(tenant1)  
         */
        Connection tenant1Conn = null, tenant2Conn = null;
        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE "
                    + baseTable
                    + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true ";
            globalConn.createStatement().execute(baseTableDDL);

            tenant1Conn = getTenantConnection("tenant1");
            String view1DDL = "CREATE VIEW " + view1 + " AS SELECT * FROM " + baseTable;
            tenant1Conn.createStatement().execute(view1DDL);

            String view2DDL = "CREATE VIEW " + view2 + " AS SELECT * FROM " + view1;
            tenant1Conn.createStatement().execute(view2DDL);

            tenant2Conn = getTenantConnection("tenant2");
            String view3DDL = "CREATE VIEW " + view3 + " AS SELECT * FROM " + baseTable;
            tenant2Conn.createStatement().execute(view3DDL);

            String view4DDL = "CREATE VIEW " + view4 + " AS SELECT * FROM " + baseTable;
            globalConn.createStatement().execute(view4DDL);

            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD NEW_PK varchar primary key ";
            globalConn.createStatement().execute(alterBaseTable);
            
            // verify that the new column new_pk is now part of the primary key for the entire hierarchy
            
            globalConn.createStatement().execute("SELECT * FROM " + baseTable);
            assertTrue(checkColumnPartOfPk(globalConn.unwrap(PhoenixConnection.class), "NEW_PK", baseTable));
            
            tenant1Conn.createStatement().execute("SELECT * FROM " + view1);
            assertTrue(checkColumnPartOfPk(tenant1Conn.unwrap(PhoenixConnection.class), "NEW_PK", view1));
            
            tenant1Conn.createStatement().execute("SELECT * FROM " + view2);
            assertTrue(checkColumnPartOfPk(tenant1Conn.unwrap(PhoenixConnection.class), "NEW_PK", view2));
            
            tenant2Conn.createStatement().execute("SELECT * FROM " + view3);
            assertTrue(checkColumnPartOfPk(tenant2Conn.unwrap(PhoenixConnection.class), "NEW_PK", view3));
            
            globalConn.createStatement().execute("SELECT * FROM " + view4);
            assertTrue(checkColumnPartOfPk(globalConn.unwrap(PhoenixConnection.class), "NEW_PK", view4));

        } finally {
            if (tenant1Conn != null) {
                try {
                    tenant1Conn.close();
                } catch (Throwable ignore) {}
            }
            if (tenant2Conn != null) {
                try {
                    tenant2Conn.close();
                } catch (Throwable ignore) {}
            }
        }

    }
    
    private boolean checkColumnPartOfPk(PhoenixConnection conn, String columnName, String tableName) throws SQLException {
        String normalizedTableName = SchemaUtil.normalizeIdentifier(tableName);
        PTable table = conn.getTable(new PTableKey(conn.getTenantId(), normalizedTableName));
        List<PColumn> pkCols = table.getPKColumns();
        String normalizedColumnName = SchemaUtil.normalizeIdentifier(columnName);
        for (PColumn pkCol : pkCols) {
            if (pkCol.getName().getString().equals(normalizedColumnName)) {
                return true;
            }
        }
        return false;
    }
    
    private int getIndexOfPkColumn(PhoenixConnection conn, String columnName, String tableName) throws SQLException {
        String normalizedTableName = SchemaUtil.normalizeIdentifier(tableName);
        PTable table = conn.getTable(new PTableKey(conn.getTenantId(), normalizedTableName));
        List<PColumn> pkCols = table.getPKColumns();
        String normalizedColumnName = SchemaUtil.normalizeIdentifier(columnName);
        int i = 0;
        for (PColumn pkCol : pkCols) {
            if (pkCol.getName().getString().equals(normalizedColumnName)) {
                return i;
            }
            i++;
        }
        return -1;
    }
    
    private Connection getTenantConnection(String tenantId) throws Exception {
        Properties tenantProps = new Properties();
        tenantProps.setProperty(PhoenixRuntime.TENANT_ID_ATTRIB, tenantId);
        return DriverManager.getConnection(getUrl(), tenantProps);
    }
    
    @Test
    public void testAddPKColumnToBaseTableWhoseViewsHaveIndices() throws Exception {
        String baseTable = "testAddPKColumnToBaseTableWhoseViewsHaveIndices";
        String view1 = "view1";
        String view2 = "view2";
        String view3 = "view3";
        String tenant1 = "tenant1";
        String tenant2 = "tenant2";
        String view2Index = view2 + "_idx";
        String view3Index = view3 + "_idx";
        /*                          baseTable(mutli-tenant)
                                 /                           \                
                         view1(tenant1)                  view3(tenant2, index) 
                          /
                        view2(tenant1, index)  
         */
        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            // make sure that the tables are empty, but reachable
            globalConn
            .createStatement()
            .execute(
                    "CREATE TABLE "
                            + baseTable
                            + " (TENANT_ID VARCHAR NOT NULL, K1 varchar not null, V1 VARCHAR, V2 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, K1)) MULTI_TENANT = true ");

        }
        try (Connection tenantConn = getTenantConnection(tenant1)) {
            // create tenant specific view for tenant1 - view1
            tenantConn.createStatement().execute("CREATE VIEW " + view1 + " AS SELECT * FROM " + baseTable);
            PhoenixConnection phxConn = tenantConn.unwrap(PhoenixConnection.class);
            assertEquals(0, getTableSequenceNumber(phxConn, view1));
            assertEquals(2, getMaxKeySequenceNumber(phxConn, view1));

            // create a view - view2 on view - view1
            tenantConn.createStatement().execute("CREATE VIEW " + view2 + " AS SELECT * FROM " + view1);
            assertEquals(0, getTableSequenceNumber(phxConn, view2));
            assertEquals(2, getMaxKeySequenceNumber(phxConn, view2));


            // create an index on view2
            tenantConn.createStatement().execute("CREATE INDEX " + view2Index + " ON " + view2 + " (v1) include (v2)");
            assertEquals(0, getTableSequenceNumber(phxConn, view2Index));
            assertEquals(4, getMaxKeySequenceNumber(phxConn, view2Index));
        }
        try (Connection tenantConn = getTenantConnection(tenant2)) {
            // create tenant specific view for tenant2 - view3
            tenantConn.createStatement().execute("CREATE VIEW " + view3 + " AS SELECT * FROM " + baseTable);
            PhoenixConnection phxConn = tenantConn.unwrap(PhoenixConnection.class);
            assertEquals(0, getTableSequenceNumber(phxConn, view3));
            assertEquals(2, getMaxKeySequenceNumber(phxConn, view3));


            // create an index on view3
            tenantConn.createStatement().execute("CREATE INDEX " + view3Index + " ON " + view3 + " (v1) include (v2)");
            assertEquals(0, getTableSequenceNumber(phxConn, view3Index));
            assertEquals(4, getMaxKeySequenceNumber(phxConn, view3Index));


        }

        // alter the base table by adding 1 non-pk and 2 pk columns
        try (Connection globalConn = DriverManager.getConnection(getUrl())) {
            globalConn.createStatement().execute("ALTER TABLE " + baseTable + " ADD v3 VARCHAR, k2 VARCHAR PRIMARY KEY, k3 VARCHAR PRIMARY KEY");
            assertEquals(4, getMaxKeySequenceNumber(globalConn.unwrap(PhoenixConnection.class), baseTable));

            // Upsert records in the base table
            String upsert = "UPSERT INTO " + baseTable + " (TENANT_ID, K1, K2, K3, V1, V2, V3) VALUES (?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement stmt = globalConn.prepareStatement(upsert);
            stmt.setString(1, tenant1);
            stmt.setString(2, "K1");
            stmt.setString(3, "K2");
            stmt.setString(4, "K3");
            stmt.setString(5, "V1");
            stmt.setString(6, "V2");
            stmt.setString(7, "V3");
            stmt.executeUpdate();
            stmt.setString(1, tenant2);
            stmt.setString(2, "K11");
            stmt.setString(3, "K22");
            stmt.setString(4, "K33");
            stmt.setString(5, "V11");
            stmt.setString(6, "V22");
            stmt.setString(7, "V33");
            stmt.executeUpdate();
            globalConn.commit();
        }

        // Verify now that the sequence number of data table, indexes and views have changed.
        // Also verify that the newly added pk columns show up as pk columns of data table, indexes and views.
        try (Connection tenantConn = getTenantConnection(tenant1)) {

            ResultSet rs = tenantConn.createStatement().executeQuery("SELECT K2, K3, V3 FROM " + view1);
            PhoenixConnection phxConn = tenantConn.unwrap(PhoenixConnection.class);
            assertEquals(2, getIndexOfPkColumn(phxConn, "k2", view1));
            assertEquals(3, getIndexOfPkColumn(phxConn, "k3", view1));
            assertEquals(1, getTableSequenceNumber(phxConn, view1));
            assertEquals(4, getMaxKeySequenceNumber(phxConn, view1));
            verifyNewColumns(rs, "K2", "K3", "V3");


            rs = tenantConn.createStatement().executeQuery("SELECT K2, K3, V3 FROM " + view2);
            assertEquals(2, getIndexOfPkColumn(phxConn, "k2", view2));
            assertEquals(3, getIndexOfPkColumn(phxConn, "k3", view2));
            assertEquals(1, getTableSequenceNumber(phxConn, view2));
            assertEquals(4, getMaxKeySequenceNumber(phxConn, view2));
            verifyNewColumns(rs, "K2", "K3", "V3");

            assertEquals(4, getIndexOfPkColumn(phxConn, IndexUtil.getIndexColumnName(null, "k2"), view2Index));
            assertEquals(5, getIndexOfPkColumn(phxConn, IndexUtil.getIndexColumnName(null, "k3"), view2Index));
            assertEquals(1, getTableSequenceNumber(phxConn, view2Index));
            assertEquals(6, getMaxKeySequenceNumber(phxConn, view2Index));
        }
        try (Connection tenantConn = getTenantConnection(tenant2)) {
            ResultSet rs = tenantConn.createStatement().executeQuery("SELECT K2, K3, V3 FROM " + view3);
            PhoenixConnection phxConn = tenantConn.unwrap(PhoenixConnection.class);
            assertEquals(2, getIndexOfPkColumn(phxConn, "k2", view3));
            assertEquals(3, getIndexOfPkColumn(phxConn, "k3", view3));
            assertEquals(1, getTableSequenceNumber(phxConn, view3));
            verifyNewColumns(rs, "K22", "K33", "V33");

            assertEquals(4, getIndexOfPkColumn(phxConn, IndexUtil.getIndexColumnName(null, "k2"), view3Index));
            assertEquals(5, getIndexOfPkColumn(phxConn, IndexUtil.getIndexColumnName(null, "k3"), view3Index));
            assertEquals(1, getTableSequenceNumber(phxConn, view3Index));
            assertEquals(6, getMaxKeySequenceNumber(phxConn, view3Index));
        }
        // Verify that the index is actually being used when using newly added pk col
        try (Connection tenantConn = getTenantConnection(tenant1)) {
            String upsert = "UPSERT INTO " + view2 + " (K1, K2, K3, V1, V2, V3) VALUES ('key1', 'key2', 'key3', 'value1', 'value2', 'value3')";
            tenantConn.createStatement().executeUpdate(upsert);
            tenantConn.commit();
            Statement stmt = tenantConn.createStatement();
            String sql = "SELECT V2 FROM " + view2 + " WHERE V1 = 'value1' AND K3 = 'key3'";
            QueryPlan plan = stmt.unwrap(PhoenixStatement.class).optimizeQuery(sql);
            assertTrue(plan.getTableRef().getTable().getName().getString().equals(SchemaUtil.normalizeIdentifier(view2Index)));
            ResultSet rs = tenantConn.createStatement().executeQuery(sql);
            verifyNewColumns(rs, "value2");
        }

    }
    
    @Test
    public void testAddingPkAndKeyValueColumnsToBaseTableWithDivergedView() throws Exception {
        String baseTable = "testAlteringPkOfBaseTableWithDivergedView".toUpperCase();
        String view1 = "view1".toUpperCase();
        String divergedView = "divergedView".toUpperCase();
        String divergedViewIndex = divergedView + "_IDX";
        /*                                     baseTable
                                 /                  |                
                         view1(tenant1)         divergedView(tenant2)    
                            
        */
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE " + baseTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR, V3 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true ";
            conn.createStatement().execute(baseTableDDL);
            
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + view1 + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 CHAR(256)) AS SELECT * FROM " + baseTable;
                tenant1Conn.createStatement().execute(view1DDL);
            }
            
            try (Connection tenant2Conn = getTenantConnection("tenant2")) {
                String divergedViewDDL = "CREATE VIEW " + divergedView + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 CHAR(256)) AS SELECT * FROM " + baseTable;
                tenant2Conn.createStatement().execute(divergedViewDDL);
                // Drop column V2 from the view to have it diverge from the base table
                tenant2Conn.createStatement().execute("ALTER VIEW " + divergedView + " DROP COLUMN V2");
                
                // create an index on the diverged view
                String indexDDL = "CREATE INDEX " + divergedViewIndex + " ON " + divergedView + " (V1) include (V3)";
                tenant2Conn.createStatement().execute(indexDDL);
            }
            
            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD KV VARCHAR, PK2 VARCHAR PRIMARY KEY";
            conn.createStatement().execute(alterBaseTable);
            
            
            // verify that the both columns were added to view1
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                tenant1Conn.createStatement().execute("SELECT KV from " + view1);
                tenant1Conn.createStatement().execute("SELECT PK2 from " + view1);
            }
            
            // verify that only the primary key column PK2 was added to diverged view
            try (Connection tenant2Conn = getTenantConnection("tenant2")) {
                tenant2Conn.createStatement().execute("SELECT PK2 from " + divergedView);
                try {
                    tenant2Conn.createStatement().execute("SELECT KV FROM " + divergedView);
                } catch (SQLException e) {
                    assertEquals(SQLExceptionCode.COLUMN_NOT_FOUND.getErrorCode(), e.getErrorCode());
                }
            }
            
            // Upsert records in diverged view. Verify that the PK column was added to the index on it.
            String upsert = "UPSERT INTO " + divergedView + " (PK1, PK2, V1, V3) VALUES ('PK1', 'PK2', 'V1', 'V3')";
            try (Connection tenantConn = getTenantConnection("tenant2")) {
                tenantConn.createStatement().executeUpdate(upsert);
                tenantConn.commit();
                Statement stmt = tenantConn.createStatement();
                String sql = "SELECT V3 FROM " + divergedView + " WHERE V1 = 'V1' AND PK2 = 'PK2'";
                QueryPlan plan = stmt.unwrap(PhoenixStatement.class).optimizeQuery(sql);
                assertTrue(plan.getTableRef().getTable().getName().getString().equals(SchemaUtil.normalizeIdentifier(divergedViewIndex)));
                ResultSet rs = tenantConn.createStatement().executeQuery(sql);
                verifyNewColumns(rs, "V3");
            }
            
            // For non-diverged view, base table columns will be added at the same position as base table
            assertTableDefinition(conn, view1, PTableType.VIEW, baseTable, 1, 9, 7, "TENANT_ID", "PK1", "V1", "V2", "V3", "KV", "PK2", "VIEW_COL1", "VIEW_COL2");
            // For a diverged view, only base table's pk column will be added and that too at the end.
            assertTableDefinition(conn, divergedView, PTableType.VIEW, baseTable, 2, 7, DIVERGED_VIEW_BASE_COLUMN_COUNT, "TENANT_ID", "PK1", "V1", "V3", "VIEW_COL1", "VIEW_COL2", "PK2");
            
            // Add existing column VIEW_COL2 to the base table
            alterBaseTable = "ALTER TABLE " + baseTable + " ADD VIEW_COL2 CHAR(256)";
            conn.createStatement().execute(alterBaseTable);
            
            // For the non-diverged view, adding the column VIEW_COL2 will end up changing its ordinal position in the view.
            assertTableDefinition(conn, view1, PTableType.VIEW, baseTable, 2, 9, 8, "TENANT_ID", "PK1", "V1", "V2", "V3", "KV", "PK2", "VIEW_COL2", "VIEW_COL1");
            // For the diverged view, adding the column VIEW_COL2 will not change its ordinal position in the view. It also won't change the base column count or the sequence number
            assertTableDefinition(conn, divergedView, PTableType.VIEW, baseTable, 2, 7, DIVERGED_VIEW_BASE_COLUMN_COUNT, "TENANT_ID", "PK1", "V1", "V3", "VIEW_COL1", "VIEW_COL2", "PK2");
        }
    }
    
    @Test
    public void testAlterSaltedBaseTableWithViews() throws Exception {
        String baseTable = "testAlterSaltedBaseTableWithViews".toUpperCase();
        String view1 = "view1".toUpperCase();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE " + baseTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR, V3 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true ";
            conn.createStatement().execute(baseTableDDL);

            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + view1 + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 CHAR(256)) AS SELECT * FROM " + baseTable;
                tenant1Conn.createStatement().execute(view1DDL);
            }

            assertTableDefinition(conn, baseTable, PTableType.TABLE, null, 0, 5, BASE_TABLE_BASE_COLUMN_COUNT, "TENANT_ID", "PK1", "V1", "V2", "V3");
            assertTableDefinition(conn, view1, PTableType.VIEW, baseTable, 0, 7, 5, "TENANT_ID", "PK1", "V1", "V2", "V3", "VIEW_COL1", "VIEW_COL2");

            String alterBaseTable = "ALTER TABLE " + baseTable + " ADD KV VARCHAR, PK2 VARCHAR PRIMARY KEY";
            conn.createStatement().execute(alterBaseTable);

            assertTableDefinition(conn, baseTable, PTableType.TABLE, null, 1, 7, BASE_TABLE_BASE_COLUMN_COUNT, "TENANT_ID", "PK1", "V1", "V2", "V3", "KV", "PK2");
            assertTableDefinition(conn, view1, PTableType.VIEW, baseTable, 1, 9, 7, "TENANT_ID", "PK1", "V1", "V2", "V3", "KV", "PK2", "VIEW_COL1", "VIEW_COL2");

            // verify that the both columns were added to view1
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                tenant1Conn.createStatement().execute("SELECT KV from " + view1);
                tenant1Conn.createStatement().execute("SELECT PK2 from " + view1);
            }
        }
    }
    
    @Test
    public void testAlteringViewConditionallyModifiesHTableMetadata() throws Exception {
        String baseTable = "testAlteringViewConditionallyModifiesBaseTable".toUpperCase();
        String view1 = "view1".toUpperCase();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            String baseTableDDL = "CREATE TABLE " + baseTable + " (TENANT_ID VARCHAR NOT NULL, PK1 VARCHAR NOT NULL, V1 VARCHAR, V2 VARCHAR, V3 VARCHAR CONSTRAINT NAME_PK PRIMARY KEY(TENANT_ID, PK1)) MULTI_TENANT = true ";
            conn.createStatement().execute(baseTableDDL);
            HTableDescriptor tableDesc1 = conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin().getTableDescriptor(Bytes.toBytes(baseTable)); 
            try (Connection tenant1Conn = getTenantConnection("tenant1")) {
                String view1DDL = "CREATE VIEW " + view1 + " ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 CHAR(256)) AS SELECT * FROM " + baseTable;
                tenant1Conn.createStatement().execute(view1DDL);
                // This should not modify the base table
                String alterView = "ALTER VIEW " + view1 + " ADD NEWCOL1 VARCHAR";
                tenant1Conn.createStatement().execute(alterView);
                HTableDescriptor tableDesc2 = tenant1Conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin().getTableDescriptor(Bytes.toBytes(baseTable));
                assertEquals(tableDesc1, tableDesc2);
                
                // Add a new column family that doesn't already exist in the base table
                alterView = "ALTER VIEW " + view1 + " ADD CF.NEWCOL2 VARCHAR";
                tenant1Conn.createStatement().execute(alterView);
                
                // Verify that the column family now shows up in the base table descriptor
                tableDesc2 = tenant1Conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin().getTableDescriptor(Bytes.toBytes(baseTable));
                assertFalse(tableDesc2.equals(tableDesc1));
                assertNotNull(tableDesc2.getFamily(Bytes.toBytes("CF")));
                
                // Add a column with an existing column family. This shouldn't modify the base table.
                alterView = "ALTER VIEW " + view1 + " ADD CF.NEWCOL3 VARCHAR";
                tenant1Conn.createStatement().execute(alterView);
                HTableDescriptor tableDesc3 = tenant1Conn.unwrap(PhoenixConnection.class).getQueryServices().getAdmin().getTableDescriptor(Bytes.toBytes(baseTable));
                assertTrue(tableDesc3.equals(tableDesc2));
                assertNotNull(tableDesc3.getFamily(Bytes.toBytes("CF")));
            }
        }
    }
    
    @Test
    public void testMakeBaseTableTransactional() throws Exception {
        try (Connection conn = DriverManager.getConnection(getUrl())) {       
            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TABLEWITHVIEW ("
                    + " ID char(1) NOT NULL,"
                    + " COL1 integer NOT NULL,"
                    + " COL2 bigint NOT NULL,"
                    + " CONSTRAINT NAME_PK PRIMARY KEY (ID, COL1, COL2)"
                    + " )");
            assertTableDefinition(conn, "TABLEWITHVIEW", PTableType.TABLE, null, 0, 3, QueryConstants.BASE_TABLE_BASE_COLUMN_COUNT, "ID", "COL1", "COL2");
            
            conn.createStatement().execute("CREATE VIEW VIEWOFTABLE ( VIEW_COL1 DECIMAL(10,2), VIEW_COL2 VARCHAR ) AS SELECT * FROM TABLEWITHVIEW");
            assertTableDefinition(conn, "VIEWOFTABLE", PTableType.VIEW, "TABLEWITHVIEW", 0, 5, 3, "ID", "COL1", "COL2", "VIEW_COL1", "VIEW_COL2");
            
            HTableInterface htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes("TABLEWITHVIEW"));
            assertFalse(htable.getTableDescriptor().getCoprocessors().contains(PhoenixTransactionalProcessor.class.getName()));
            assertFalse(conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, "TABLEWITHVIEW")).isTransactional());
            assertFalse(conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, "VIEWOFTABLE")).isTransactional());
            
            // make the base table transactional
            conn.createStatement().execute("ALTER TABLE TABLEWITHVIEW SET TRANSACTIONAL=true");
            // query the view to force the table cache to be updated
            conn.createStatement().execute("SELECT * FROM VIEWOFTABLE");
            htable = conn.unwrap(PhoenixConnection.class).getQueryServices().getTable(Bytes.toBytes("TABLEWITHVIEW"));
            assertTrue(htable.getTableDescriptor().getCoprocessors().contains(PhoenixTransactionalProcessor.class.getName()));
            assertTrue(conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, "TABLEWITHVIEW")).isTransactional());
            assertTrue(conn.unwrap(PhoenixConnection.class).getTable(new PTableKey(null, "VIEWOFTABLE")).isTransactional());
        } 
    
    }
    
    private static long getTableSequenceNumber(PhoenixConnection conn, String tableName) throws SQLException {
        PTable table = conn.getTable(new PTableKey(conn.getTenantId(), SchemaUtil.normalizeIdentifier(tableName)));
        return table.getSequenceNumber();
    }
    
    private static short getMaxKeySequenceNumber(PhoenixConnection conn, String tableName) throws SQLException {
        PTable table = conn.getTable(new PTableKey(conn.getTenantId(), SchemaUtil.normalizeIdentifier(tableName)));
        return SchemaUtil.getMaxKeySeq(table);
    }
    
    private static void verifyNewColumns(ResultSet rs, String ... values) throws SQLException {
        assertTrue(rs.next());
        int i = 1;
        for (String value : values) {
            assertEquals(value, rs.getString(i++));
        }
        assertFalse(rs.next());
        assertEquals(values.length, i - 1);
    }
}
