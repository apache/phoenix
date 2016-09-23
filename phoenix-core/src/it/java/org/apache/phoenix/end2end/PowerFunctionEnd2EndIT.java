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
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.phoenix.expression.function.PowerFunction;
import org.junit.Before;
import org.junit.Test;

/**
 * End to end tests for {@link PowerFunction}
 */
public class PowerFunctionEnd2EndIT extends ParallelStatsDisabledIT {

    private static final String KEY = "key";
    private static final double ZERO = 1e-9;
    private String signedTableName;
    private String unsignedTableName;

    private static boolean twoDoubleEquals(double a, double b) {
        if (Double.isNaN(a) ^ Double.isNaN(b)) return false;
        if (Double.isNaN(a)) return true;
        if (Double.isInfinite(a) ^ Double.isInfinite(b)) return false;
        if (Double.isInfinite(a)) {
            if ((a > 0) ^ (b > 0)) return false;
            else return true;
        }
        if (Math.abs(a - b) <= ZERO) {
            return true;
        } else {
            return false;
        }
    }

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

    private void updateTableSpec(Connection conn, double data, String tableName) throws Exception {
        PreparedStatement stmt =
                conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?, ?, ?, ?, ?, ?)");
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

    private void testNumberSpec(Connection conn, double data, String tableName) throws Exception {
        updateTableSpec(conn, data, tableName);
        ResultSet rs =
                conn.createStatement()
                        .executeQuery(
                            "SELECT POWER(doub, 1.5),POWER(fl, 1.5),POWER(inte, 1.5),POWER(lon, 1.5),POWER(smalli, 1.5),POWER(tinyi, 1.5) FROM "
                                    + tableName);
        assertTrue(rs.next());
        Double d = Double.valueOf(data);
        assertTrue(twoDoubleEquals(rs.getDouble(1), Math.pow(d.doubleValue(), 1.5)));
        assertTrue(twoDoubleEquals(rs.getDouble(2), Math.pow(d.floatValue(), 1.5)));
        assertTrue(twoDoubleEquals(rs.getDouble(3), Math.pow(d.intValue(), 1.5)));
        assertTrue(twoDoubleEquals(rs.getDouble(4), Math.pow(d.longValue(), 1.5)));
        assertTrue(twoDoubleEquals(rs.getDouble(5), Math.pow(d.shortValue(), 1.5)));
        assertTrue(twoDoubleEquals(rs.getDouble(6), Math.pow(d.byteValue(), 1.5)));

        assertTrue(!rs.next());
        rs =
                conn.createStatement()
                        .executeQuery(
                            "SELECT POWER(doub, 2),POWER(fl, 2),POWER(inte, 2),POWER(lon, 2),POWER(smalli, 2),POWER(tinyi, 2) FROM "
                                    + tableName);
        assertTrue(rs.next());
        d = Double.valueOf(data);
        assertTrue(twoDoubleEquals(rs.getDouble(1), Math.pow(d.doubleValue(), 2)));
        assertTrue(twoDoubleEquals(rs.getDouble(2), Math.pow(d.floatValue(), 2)));
        assertTrue(twoDoubleEquals(rs.getDouble(3), Math.pow(d.intValue(), 2)));
        assertTrue(twoDoubleEquals(rs.getDouble(4), Math.pow(d.longValue(), 2)));
        assertTrue(twoDoubleEquals(rs.getDouble(5), Math.pow(d.shortValue(), 2)));
        assertTrue(twoDoubleEquals(rs.getDouble(6), Math.pow(d.byteValue(), 2)));
        assertTrue(!rs.next());

        rs =
                conn.createStatement().executeQuery(
                    "SELECT POWER(doub,3),POWER(fl,3),POWER(inte,3),POWER(lon,3),POWER(smalli,3),POWER(tinyi,3) FROM "
                            + tableName);
        assertTrue(rs.next());
        d = Double.valueOf(data);
        assertTrue(twoDoubleEquals(rs.getDouble(1), Math.pow(d.doubleValue(), 3)));
        assertTrue(twoDoubleEquals(rs.getDouble(2), Math.pow(d.floatValue(), 3)));
        assertTrue(twoDoubleEquals(rs.getDouble(3), Math.pow(d.intValue(), 3)));
        assertTrue(twoDoubleEquals(rs.getDouble(4), Math.pow(d.longValue(), 3)));
        assertTrue(twoDoubleEquals(rs.getDouble(5), Math.pow(d.shortValue(), 3)));
        assertTrue(twoDoubleEquals(rs.getDouble(6), Math.pow(d.byteValue(), 3)));
        assertTrue(!rs.next());
    }

    @Test
    public void test() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        for (double d : new double[] { 0.0, 1.0, -1.0, 123.1234, -123.1234 }) {
            testNumberSpec(conn, d, signedTableName);
            if (d >= 0) testNumberSpec(conn, d, unsignedTableName);
        }
    }
}
