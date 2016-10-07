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
import static org.apache.phoenix.util.TestUtil.getTableName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.phoenix.expression.function.SqrtFunction;
import org.junit.Before;
import org.junit.Test;

/**
 * End to end tests for {@link SqrtFunction}
 */
public class SqrtFunctionEnd2EndIT extends ParallelStatsDisabledIT {

    private static final String KEY = "key";
    private static final double ZERO = 1e-8;
    String testUnsignedTable;
    String testSignedTable;
    
    @Before
    public void initTable() throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        testUnsignedTable = generateUniqueName();
        testSignedTable = generateUniqueName();
        try {
            conn = DriverManager.getConnection(getUrl());
            String ddl;
            ddl = "CREATE TABLE " + testSignedTable + " (k VARCHAR NOT NULL PRIMARY KEY, doub DOUBLE, fl FLOAT, inte INTEGER, lon BIGINT, smalli SMALLINT, tinyi TINYINT)";
            conn.createStatement().execute(ddl);
            ddl = "CREATE TABLE " + testUnsignedTable + " (k VARCHAR NOT NULL PRIMARY KEY, doub UNSIGNED_DOUBLE, fl UNSIGNED_FLOAT, inte UNSIGNED_INT, lon UNSIGNED_LONG, smalli UNSIGNED_SMALLINT, tinyi UNSIGNED_TINYINT)";
            conn.createStatement().execute(ddl);
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    private void updateSignedTable(Connection conn, double data) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + testSignedTable + " VALUES (?, ?, ?, ?, ?, ?, ?)");
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
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + testUnsignedTable + " VALUES (?, ?, ?, ?, ?, ?, ?)");
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
        ResultSet rs = conn.createStatement().executeQuery("SELECT SQRT(doub),SQRT(fl),SQRT(inte),SQRT(lon),SQRT(smalli),SQRT(tinyi) FROM " + testSignedTable );
        assertTrue(rs.next());
        Double d = Double.valueOf(data);
        assertTrue(Math.abs(rs.getDouble(1) - Math.sqrt(d.doubleValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(2) - Math.sqrt(d.floatValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(3) - Math.sqrt(d.intValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(4) - Math.sqrt(d.longValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(5) - Math.sqrt(d.shortValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(6) - Math.sqrt(d.byteValue())) < ZERO);
        assertTrue(!rs.next());
        PreparedStatement stmt = conn.prepareStatement("SELECT k FROM " + testSignedTable + " WHERE SQRT(doub)>0 AND SQRT(fl)>0 AND SQRT(inte)>0 AND SQRT(lon)>0 AND SQRT(smalli)>0 AND SQRT(tinyi)>0");
        rs = stmt.executeQuery();
        if (data > 0) {
            assertTrue(rs.next());
            assertEquals(KEY, rs.getString(1));
        }
        assertTrue(!rs.next());
    }

    private void testUnsignedNumberSpec(Connection conn, double data) throws Exception {
        updateUnsignedTable(conn, data);
        ResultSet rs = conn.createStatement().executeQuery("SELECT SQRT(doub),SQRT(fl),SQRT(inte),SQRT(lon),SQRT(smalli),SQRT(tinyi) FROM " + testUnsignedTable );
        assertTrue(rs.next());
        Double d = Double.valueOf(data);
        assertTrue(Math.abs(rs.getDouble(1) - Math.sqrt(d.doubleValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(2) - Math.sqrt(d.floatValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(3) - Math.sqrt(d.intValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(4) - Math.sqrt(d.longValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(5) - Math.sqrt(d.shortValue())) < ZERO);
        assertTrue(Math.abs(rs.getDouble(6) - Math.sqrt(d.byteValue())) < ZERO);
        assertTrue(!rs.next());
        PreparedStatement stmt = conn.prepareStatement("SELECT k FROM " + testUnsignedTable + " WHERE SQRT(doub)>0 AND SQRT(fl)>0 AND SQRT(inte)>0 AND SQRT(lon)>0 AND SQRT(smalli)>0 AND SQRT(tinyi)>0");
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
        for (double d : new double[] { 0.0, 1.0, 123.1234}) {
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
