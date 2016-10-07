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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.phoenix.expression.function.CbrtFunction;
import org.junit.Before;
import org.junit.Test;

/**
 * End to end tests for {@link CbrtFunction}
 */
public class CbrtFunctionEnd2EndIT extends ParallelStatsDisabledIT {

    private static final String KEY = "key";
    private static final double ZERO = 1e-8;
    private String signedTableName;
    private String unsignedTableName;

    @Before
    public void initTable() throws Exception {
        signedTableName = generateUniqueName();
        unsignedTableName = generateUniqueName();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String ddl;
            ddl = "CREATE TABLE " + signedTableName
                + " (k VARCHAR NOT NULL PRIMARY KEY, doub DOUBLE, fl FLOAT, inte INTEGER, lon BIGINT, smalli SMALLINT, tinyi TINYINT)";
            conn.createStatement().execute(ddl);
            ddl = "CREATE TABLE " + unsignedTableName
                + " (k VARCHAR NOT NULL PRIMARY KEY, doub UNSIGNED_DOUBLE, fl UNSIGNED_FLOAT, inte UNSIGNED_INT, lon UNSIGNED_LONG, smalli UNSIGNED_SMALLINT, tinyi UNSIGNED_TINYINT)";
            conn.createStatement().execute(ddl);
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    private void updateSignedTable(Connection conn, double data) throws Exception {
        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + signedTableName + " VALUES (?, ?, ?, ?, ?, ?, ?)");
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

    private void updateUnsignedTable(Connection conn, double data) throws Exception {
        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + unsignedTableName + " VALUES (?, ?, ?, ?, ?, ?, ?)");
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

    private void testSignedNumberSpec(Connection conn, double data) throws Exception {
        updateSignedTable(conn, data);
        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT CBRT(doub),CBRT(fl),CBRT(inte),CBRT(lon),CBRT(smalli),CBRT(tinyi) FROM "
                + signedTableName);
        assertTrue(rs.next());
        Double d = Double.valueOf(data);
        assertTrue(Math.abs(rs.getDouble(1) - Math.cbrt(d.doubleValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(2) - Math.cbrt(d.floatValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(3) - Math.cbrt(d.intValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(4) - Math.cbrt(d.longValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(5) - Math.cbrt(d.shortValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(6) - Math.cbrt(d.byteValue())) < ZERO);
        assertTrue(!rs.next());
        PreparedStatement stmt = conn.prepareStatement("SELECT k FROM " + signedTableName
            + " WHERE CBRT(doub)>0 AND CBRT(fl)>0 AND CBRT(inte)>0 AND CBRT(lon)>0 AND CBRT(smalli)>0 AND CBRT(tinyi)>0");
        rs = stmt.executeQuery();
        if (data > 0) {
            assertTrue(rs.next());
            assertEquals(KEY, rs.getString(1));
        }
        assertTrue(!rs.next());
    }

    private void testUnsignedNumberSpec(Connection conn, double data) throws Exception {
        updateUnsignedTable(conn, data);
        ResultSet rs = conn.createStatement().executeQuery(
            "SELECT CBRT(doub),CBRT(fl),CBRT(inte),CBRT(lon),CBRT(smalli),CBRT(tinyi) FROM "
                + unsignedTableName);
        assertTrue(rs.next());
        Double d = Double.valueOf(data);
        assertTrue(Math.abs(rs.getDouble(1) - Math.cbrt(d.doubleValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(2) - Math.cbrt(d.floatValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(3) - Math.cbrt(d.intValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(4) - Math.cbrt(d.longValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(5) - Math.cbrt(d.shortValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(6) - Math.cbrt(d.byteValue())) < ZERO);
        assertTrue(!rs.next());
        PreparedStatement stmt = conn.prepareStatement("SELECT k FROM " + unsignedTableName
            + " WHERE CBRT(doub)>0 AND CBRT(fl)>0 AND CBRT(inte)>0 AND CBRT(lon)>0 AND CBRT(smalli)>0 AND CBRT(tinyi)>0");
        rs = stmt.executeQuery();
        if (data > 0) {
            assertTrue(rs.next());
            assertEquals(KEY, rs.getString(1));
        }
        assertTrue(!rs.next());
    }

    @Test
    public void testSignedNumber() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        for (double d : new double[] { 0.0, 1.0, -1.0, 123.1234, -123.1234 }) {
            testSignedNumberSpec(conn, d);
        }
    }

    @Test
    public void testUnsignedNumber() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        for (double d : new double[] { 0.0, 1.0, 123.1234 }) {
            testUnsignedNumberSpec(conn, d);
        }
    }
}
