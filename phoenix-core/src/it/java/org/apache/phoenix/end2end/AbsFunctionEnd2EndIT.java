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

import org.apache.phoenix.expression.function.AbsFunction;
import org.junit.Before;
import org.junit.Test;

/**
 * End to end tests for {@link AbsFunction}
 */
public class AbsFunctionEnd2EndIT extends ParallelStatsDisabledIT {

    private static final String TABLE_NAME = generateUniqueName();
    private static final String KEY = "key";

    @Before
    public void initTable() throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String ddl;
            ddl = "CREATE TABLE " + TABLE_NAME + " (k VARCHAR NOT NULL PRIMARY KEY, \"DEC\" DECIMAL, doub DOUBLE, fl FLOAT, inte INTEGER, lon BIGINT, smalli SMALLINT, tinyi TINYINT)";
            conn.createStatement().execute(ddl);
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    private void updateSignedTable(Connection conn, double data) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + TABLE_NAME + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
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

    private void testSignedNumberSpec(Connection conn, double data) throws Exception {
        updateSignedTable(conn, data);
        ResultSet rs = conn.createStatement() .executeQuery("SELECT ABS(\"DEC\"),ABS(doub),ABS(fl),ABS(inte),ABS(lon),ABS(smalli),ABS(tinyi) FROM " + TABLE_NAME);
        assertTrue(rs.next());
        Double d = Double.valueOf(data);
        assertEquals(rs.getBigDecimal(1).compareTo(BigDecimal.valueOf(data).abs()), 0);
        assertEquals(rs.getDouble(2), Math.abs(data), 1e-6);
        assertEquals(rs.getFloat(3), Math.abs(d.floatValue()), 1e-6);
        assertEquals(rs.getInt(4), Math.abs(d.intValue()));
        assertEquals(rs.getLong(5), Math.abs(d.longValue()));
        assertEquals(rs.getShort(6), Math.abs(d.shortValue()));
        assertEquals(rs.getByte(7), Math.abs(d.byteValue()));
        assertTrue(!rs.next());

        PreparedStatement stmt = conn.prepareStatement("SELECT k FROM " + TABLE_NAME + " WHERE ABS(\"DEC\")=? AND ABS(doub)=? AND ABS(fl)=? AND ABS(inte)=? AND ABS(lon)=? AND ABS(smalli)=? AND ABS(tinyi)=?");
        stmt.setBigDecimal(1, BigDecimal.valueOf(data).abs());
        stmt.setDouble(2, Math.abs(d.doubleValue()));
        stmt.setFloat(3, Math.abs(d.floatValue()));
        stmt.setInt(4, Math.abs(d.intValue()));
        stmt.setLong(5, Math.abs(d.longValue()));
        stmt.setShort(6, (short) Math.abs(d.shortValue()));
        stmt.setByte(7, (byte) Math.abs(d.byteValue()));
        rs = stmt.executeQuery();
        assertTrue(rs.next());
        assertEquals(KEY, rs.getString(1));
        assertTrue(!rs.next());
    }

    @Test
    public void testSignedNumber() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        testSignedNumberSpec(conn, 0.0);
        testSignedNumberSpec(conn, 1.0);
        testSignedNumberSpec(conn, -1.0);
        testSignedNumberSpec(conn, 123.1234);
        testSignedNumberSpec(conn, -123.1234);
    }
}
