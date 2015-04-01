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

import static org.apache.phoenix.util.TestUtil.closeStmtAndConn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.phoenix.expression.function.SignFunction;
import org.junit.Before;
import org.junit.Test;

/**
 * End to end tests for {@link SignFunction}
 * @since 4.0.0
 */
public class SignFunctionEnd2EndIT extends BaseHBaseManagedTimeIT {

    private static final String KEY = "key";

    @Before
    public void initTable() throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String ddl;
            ddl = "CREATE TABLE testSigned (k VARCHAR NOT NULL PRIMARY KEY, dec DECIMAL, doub DOUBLE, fl FLOAT, inte INTEGER, lon BIGINT, smalli SMALLINT, tinyi TINYINT)";
            conn.createStatement().execute(ddl);
            ddl = "CREATE TABLE testUnsigned (k VARCHAR NOT NULL PRIMARY KEY, doub UNSIGNED_DOUBLE, fl UNSIGNED_FLOAT, inte UNSIGNED_INT, lon UNSIGNED_LONG, smalli UNSIGNED_SMALLINT, tinyi UNSIGNED_TINYINT)";
            conn.createStatement().execute(ddl);
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    private void updateSignedTable(Connection conn, double data) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO testSigned VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, KEY);
        Double d = Double.valueOf(data);
        stmt.setBigDecimal(2, BigDecimal.valueOf(data));
        stmt.setDouble(3, d.doubleValue());
        stmt.setFloat(4, d.floatValue());
        stmt.setInt(5, d.intValue());
        stmt.setLong(6, d.longValue());
        stmt.setShort(7, d.shortValue());
        stmt.setByte(8, d.byteValue());
        stmt.executeUpdate();
        conn.commit();
    }

    private void updateUnsignedTable(Connection conn, double data) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO testUnsigned VALUES (?, ?, ?, ?, ?, ?, ?)");
        stmt.setString(1, KEY);
        Double d = Double.valueOf(data);
        stmt.setDouble(2, d.doubleValue());
        stmt.setFloat(3, d.floatValue());
        stmt.setInt(4, d.intValue());
        stmt.setLong(5, d.longValue());
        stmt.setShort(6, d.shortValue());
        stmt.setByte(7, d.byteValue());
        stmt.executeUpdate();
        conn.commit();
    }

    private void testSignedNumberSpec(Connection conn, double data, int expected) throws Exception {
        updateSignedTable(conn, data);
        ResultSet rs = conn.createStatement().executeQuery("SELECT SIGN(dec),SIGN(doub),SIGN(fl),SIGN(inte),SIGN(lon),SIGN(smalli),SIGN(tinyi) FROM testSigned");
        assertTrue(rs.next());
        for (int i = 1; i <= 7; ++i) {
            assertEquals(rs.getInt(i), expected);
        }
        assertTrue(!rs.next());

        PreparedStatement stmt = conn.prepareStatement("SELECT k FROM testSigned WHERE SIGN(dec)=? AND SIGN(doub)=? AND SIGN(fl)=? AND SIGN(inte)=? AND SIGN(lon)=? AND SIGN(smalli)=? AND SIGN(tinyi)=?");
        for (int i = 1; i <= 7; ++i)
            stmt.setInt(i, expected);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(KEY, rs.getString(1));
        assertTrue(!rs.next());
    }

    private void testUnsignedNumberSpec(Connection conn, double data, int expected) throws Exception {
        updateUnsignedTable(conn, data);
        ResultSet rs = conn.createStatement().executeQuery("SELECT SIGN(doub),SIGN(fl),SIGN(inte),SIGN(lon),SIGN(smalli),SIGN(tinyi) FROM testUnsigned");
        assertTrue(rs.next());
        for (int i = 1; i <= 6; ++i) {
            assertEquals(rs.getInt(i), expected);
        }
        assertTrue(!rs.next());

        PreparedStatement stmt = conn.prepareStatement("SELECT k FROM testUnsigned WHERE SIGN(doub)=? AND SIGN(fl)=? AND SIGN(inte)=? AND SIGN(lon)=? AND SIGN(smalli)=? AND SIGN(tinyi)=?");
        for (int i = 1; i <= 6; ++i)
            stmt.setInt(i, expected);
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(KEY, rs.getString(1));
        assertTrue(!rs.next());
    }

    @Test
    public void testSignedNumber() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        testSignedNumberSpec(conn, 0.0, 0);
        testSignedNumberSpec(conn, 1.0, 1);
        testSignedNumberSpec(conn, -1.0, -1);
        testSignedNumberSpec(conn, 123.1234, 1);
        testSignedNumberSpec(conn, -123.1234, -1);
    }

    @Test
    public void testUnsignedNumber() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        testUnsignedNumberSpec(conn, 0.0, 0);
        testUnsignedNumberSpec(conn, 1.0, 1);
        testUnsignedNumberSpec(conn, 123.1234, 1);
    }
}
