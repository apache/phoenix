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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.ReadOnlyTableException;
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
        testUpdatableViewWithIndex(null);
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
}
