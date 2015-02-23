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

import static org.apache.phoenix.util.TestUtil.analyzeTable;
import static org.apache.phoenix.util.TestUtil.getAllSplits;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.ReadOnlyTableException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.util.QueryUtil;
import org.junit.Test;


public class ViewIT extends BaseViewIT {
    
    @Test
    public void testReadOnlyView() throws Exception {
        Connection earlierCon = DriverManager.getConnection(getUrl());
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW v (v2 VARCHAR) AS SELECT * FROM t WHERE k > 5";
        conn.createStatement().execute(ddl);
        try {
            conn.createStatement().execute("UPSERT INTO v VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        }
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO t VALUES(" + i + ")");
        }
        conn.commit();
        
        analyzeTable(conn, "v");
        
        List<KeyRange> splits = getAllSplits(conn, "v");
        assertEquals(4, splits.size());
        
        int count = 0;
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM v");
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(4, count);
        count = 0;
        rs = earlierCon.createStatement().executeQuery("SELECT k FROM v");
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(4, count);
    }


    @Test
    public void testReadOnlyOnReadOnlyView() throws Exception {
        testReadOnlyView();
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE VIEW v2 AS SELECT * FROM v WHERE k < 9";
        conn.createStatement().execute(ddl);
        try {
            conn.createStatement().execute("UPSERT INTO v2 VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        }

        int count = 0;
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM v2");
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
        testUpdatableView(null);
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE VIEW v2 AS SELECT * FROM v WHERE k3 = 2";
        conn.createStatement().execute(ddl);
        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM v2");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(109, rs.getInt(2));
        assertEquals(2, rs.getInt(3));
        assertFalse(rs.next());

        conn.createStatement().execute("UPSERT INTO v2(k2) VALUES(122)");
        conn.commit();
        rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM v2 WHERE k2 >= 120");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(122, rs.getInt(2));
        assertEquals(2, rs.getInt(3));
        assertFalse(rs.next());
        
        try {
            conn.createStatement().execute("UPSERT INTO v2(k2,k3) VALUES(123,3)");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN.getErrorCode(), e.getErrorCode());
        }

        try {
            conn.createStatement().execute("UPSERT INTO v2(k2,k3) select k2, 3 from v2");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_UPDATE_VIEW_COLUMN.getErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testReadOnlyOnUpdatableView() throws Exception {
        testUpdatableView(null);
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE VIEW v2 AS SELECT * FROM v WHERE k3 > 1 and k3 < 50";
        conn.createStatement().execute(ddl);
        ResultSet rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM v2");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(109, rs.getInt(2));
        assertEquals(2, rs.getInt(3));
        assertFalse(rs.next());

        try {
            conn.createStatement().execute("UPSERT INTO v2 VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        }
        
        conn.createStatement().execute("UPSERT INTO t(k1, k2,k3) VALUES(1, 122, 5)");
        conn.commit();
        rs = conn.createStatement().executeQuery("SELECT k1, k2, k3 FROM v2 WHERE k2 >= 120");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(122, rs.getInt(2));
        assertEquals(5, rs.getInt(3));
        assertFalse(rs.next());
    }
    
    @Test
    public void testDisallowDropOfReferencedColumn() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE t (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW v1(v2 VARCHAR, v3 VARCHAR) AS SELECT * FROM t WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);
        
        try {
            conn.createStatement().execute("ALTER VIEW v1 DROP COLUMN v1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }
        
        ddl = "CREATE VIEW v2 AS SELECT * FROM v1 WHERE v2 != 'foo'";
        conn.createStatement().execute(ddl);

        try {
            conn.createStatement().execute("ALTER VIEW v2 DROP COLUMN v1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }
        try {
            conn.createStatement().execute("ALTER VIEW v2 DROP COLUMN v2");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }
        conn.createStatement().execute("ALTER VIEW v2 DROP COLUMN v3");
        
    }
    
    @Test
    public void testReadOnlyViewWithCaseSensitiveTableNames() throws Exception {
        Connection earlierCon = DriverManager.getConnection(getUrl());
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE \"case_SENSITIVE_table\" (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW \"v\" (v2 VARCHAR) AS SELECT * FROM \"case_SENSITIVE_table\" WHERE k > 5";
        conn.createStatement().execute(ddl);
        try {
            conn.createStatement().execute("UPSERT INTO \"v\" VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        }
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO \"case_SENSITIVE_table\" VALUES(" + i + ")");
        }
        conn.commit();
        
        int count = 0;
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM \"v\"");
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(4, count);
        count = 0;
        rs = earlierCon.createStatement().executeQuery("SELECT k FROM \"v\"");
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(4, count);
    }
    
    @Test
    public void testReadOnlyViewWithCaseSensitiveColumnNames() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE t (\"k\" INTEGER NOT NULL PRIMARY KEY, \"v1\" INTEGER, \"a\".v2 VARCHAR)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW v (v VARCHAR) AS SELECT * FROM t WHERE \"k\" > 5 and \"v1\" > 1";
        conn.createStatement().execute(ddl);
        try {
            conn.createStatement().execute("UPSERT INTO v VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        }
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO t VALUES(" + i + ", " + (i+10) + ",'A')");
        }
        conn.commit();
        
        int count = 0;
        ResultSet rs = conn.createStatement().executeQuery("SELECT \"k\", \"v1\",\"a\".v2 FROM v");
        while (rs.next()) {
            count++;
            assertEquals(count + 5, rs.getInt(1));
        }
        assertEquals(4, count);
    }
    
    @Test
    public void testViewWithCurrentDate() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE t (k INTEGER NOT NULL PRIMARY KEY, v1 INTEGER, v2 DATE)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW v (v VARCHAR) AS SELECT * FROM t WHERE v2 > CURRENT_DATE()-5 AND v2 > DATE '2010-01-01'";
        conn.createStatement().execute(ddl);
        try {
            conn.createStatement().execute("UPSERT INTO v VALUES(1)");
            fail();
        } catch (ReadOnlyTableException e) {
            
        }
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO t VALUES(" + i + ", " + (i+10) + ",CURRENT_DATE()-" + i + ")");
        }
        conn.commit();
        
        int count = 0;
        ResultSet rs = conn.createStatement().executeQuery("SELECT k FROM v");
        while (rs.next()) {
            assertEquals(count, rs.getInt(1));
            count++;
        }
        assertEquals(5, count);
    }

    @Test
    public void testViewAndTableInDifferentSchemas() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE s1.t (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW s2.v1 (v2 VARCHAR) AS SELECT * FROM s1.t WHERE k > 5";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW v2 (v2 VARCHAR) AS SELECT * FROM s1.t WHERE k > 5";
        conn.createStatement().execute(ddl);
        ddl = "DROP VIEW v1";
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (TableNotFoundException ignore) {
        }
        ddl = "DROP VIEW s2.v1";
        conn.createStatement().execute(ddl);
        ddl = "DROP VIEW s2.v2";
        try {
            conn.createStatement().execute(ddl);
            fail();
        } catch (TableNotFoundException ignore) {
        }
        ddl = "DROP TABLE s1.t";
        validateCannotDropTableWithChildViewsWithoutCascade(conn, "s1.t");
        ddl = "DROP VIEW v2";
        conn.createStatement().execute(ddl);
        ddl = "DROP TABLE s1.t";
        conn.createStatement().execute(ddl);
    }

    
    @Test
    public void testDisallowDropOfColumnOnParentTable() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE tp (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, v1 DECIMAL, CONSTRAINT pk PRIMARY KEY (k1, k2))";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW v1(v2 VARCHAR, v3 VARCHAR) AS SELECT * FROM tp WHERE v1 = 1.0";
        conn.createStatement().execute(ddl);
        
        try {
            conn.createStatement().execute("ALTER TABLE tp DROP COLUMN v1");
            fail();
        } catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }
    }
   
    @Test
    public void testViewAndTableAndDropCascade() throws Exception {
    	// Setup
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE s2.t (k INTEGER NOT NULL PRIMARY KEY, v1 DATE)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW s2.v1 (v2 VARCHAR) AS SELECT * FROM s2.t WHERE k > 5";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW s2.v2 (v2 VARCHAR) AS SELECT * FROM s2.t WHERE k > 10";
        conn.createStatement().execute(ddl);

        validateCannotDropTableWithChildViewsWithoutCascade(conn, "s2.t");
        
        // Execute DROP...CASCADE
        conn.createStatement().execute("DROP TABLE s2.t CASCADE");
        
        validateViewDoesNotExist(conn, "s2.v1");
        validateViewDoesNotExist(conn, "s2.v2");
    }
    
    @Test
    public void testViewAndTableAndDropCascadeWithIndexes() throws Exception {
        
    	// Setup - Tables and Views with Indexes
    	Connection conn = DriverManager.getConnection(getUrl());
    	
        String ddl = "CREATE TABLE s3.t (k INTEGER NOT NULL PRIMARY KEY, v1 DATE) IMMUTABLE_ROWS=true";
        conn.createStatement().execute(ddl);
        ddl = "CREATE INDEX IDX1 ON s3.t (v1)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW s3.v1 (v2 VARCHAR) AS SELECT * FROM s3.t WHERE k > 5";
        conn.createStatement().execute(ddl);
        ddl = "CREATE INDEX IDX2 ON s3.v1 (v2)";
        conn.createStatement().execute(ddl);
        ddl = "CREATE VIEW s3.v2 (v2 VARCHAR) AS SELECT * FROM s3.t WHERE k > 10";
        conn.createStatement().execute(ddl);
        ddl = "CREATE INDEX IDX3 ON s3.v2 (v2)";
        conn.createStatement().execute(ddl);

        validateCannotDropTableWithChildViewsWithoutCascade(conn, "s3.t");
        
        // Execute DROP...CASCADE
        conn.createStatement().execute("DROP TABLE s3.t CASCADE");
        
        // Validate Views were deleted - Try and delete child views, should throw TableNotFoundException
        validateViewDoesNotExist(conn, "s3.v1");
        validateViewDoesNotExist(conn, "s3.v2");
    }


	private void validateCannotDropTableWithChildViewsWithoutCascade(Connection conn, String tableName) throws SQLException {
		String ddl;
		try {
	        ddl = "DROP TABLE " + tableName;
	        conn.createStatement().execute(ddl);
	        fail("Should not be able to drop table " + tableName + " with child views without explictly specifying CASCADE");
        }  catch (SQLException e) {
            assertEquals(SQLExceptionCode.CANNOT_MUTATE_TABLE.getErrorCode(), e.getErrorCode());
        }
	}


	private void validateViewDoesNotExist(Connection conn, String viewName)	throws SQLException {
		try {
        	String ddl1 = "DROP VIEW " + viewName;
            conn.createStatement().execute(ddl1);
            fail("View s3.v1 should have been deleted when parent was dropped");
        } catch (TableNotFoundException e) {
        	//Expected
        }
	}
	
	@Test
    public void testViewUsesTableIndex() throws Exception {
        ResultSet rs;
        Connection conn = DriverManager.getConnection(getUrl());
        String ddl = "CREATE TABLE t (k1 INTEGER NOT NULL, k2 INTEGER NOT NULL, k3 DECIMAL, s1 VARCHAR, s2 VARCHAR CONSTRAINT pk PRIMARY KEY (k1, k2, k3))";
        conn.createStatement().execute(ddl);
        conn.createStatement().execute("CREATE INDEX i1 ON t(k3, k2) INCLUDE(s1, s2)");
        conn.createStatement().execute("CREATE INDEX i2 ON t(k3, k2, s2)");
        
        ddl = "CREATE VIEW v AS SELECT * FROM t WHERE s1 = 'foo'";
        conn.createStatement().execute(ddl);
        String[] s1Values = {"foo","bar"};
        for (int i = 0; i < 10; i++) {
            conn.createStatement().execute("UPSERT INTO t VALUES(" + (i % 4) + "," + (i+100) + "," + (i > 5 ? 2 : 1) + ",'" + s1Values[i%2] + "','bas')");
        }
        conn.commit();
        
        rs = conn.createStatement().executeQuery("SELECT count(*) FROM v");
        assertTrue(rs.next());
        assertEquals(5, rs.getLong(1));
        assertFalse(rs.next());

        conn.createStatement().execute("CREATE INDEX vi1 on v(k2)");
        
        String query = "SELECT k2 FROM v WHERE k2 IN (100,109) AND k3 IN (1,2) AND s2='bas'";
        rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        assertEquals(100, rs.getInt(1));
        assertFalse(rs.next());
        rs = conn.createStatement().executeQuery("EXPLAIN " + query);
        String queryPlan = QueryUtil.getExplainPlan(rs);
        assertEquals(
                "CLIENT PARALLEL 1-WAY SKIP SCAN ON 4 KEYS OVER I1 [1,100] - [2,109]\n" + 
                "    SERVER FILTER BY (\"S2\" = 'bas' AND \"S1\" = 'foo')", queryPlan);
    }
}
