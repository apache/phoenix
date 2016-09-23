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

import org.apache.phoenix.expression.function.LnFunction;
import org.apache.phoenix.expression.function.LogFunction;
import org.junit.Before;
import org.junit.Test;

/**
 * End to end tests for {@link LnFunction} and {@link LogFunction}
 */
public class LnLogFunctionEnd2EndIT extends ParallelStatsDisabledIT {

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
        Connection conn = null;
        PreparedStatement stmt = null;
        signedTableName = generateUniqueName();
        unsignedTableName = generateUniqueName();

        try {
            conn = DriverManager.getConnection(getUrl());
            String ddl;
            ddl =
                    "CREATE TABLE " + signedTableName + " (k VARCHAR NOT NULL PRIMARY KEY, doub DOUBLE, fl FLOAT, inte INTEGER, lon BIGINT, smalli SMALLINT, tinyi TINYINT)";
            conn.createStatement().execute(ddl);
            ddl =
                    "CREATE TABLE " + unsignedTableName + " (k VARCHAR NOT NULL PRIMARY KEY, doub UNSIGNED_DOUBLE, fl UNSIGNED_FLOAT, inte UNSIGNED_INT, lon UNSIGNED_LONG, smalli UNSIGNED_SMALLINT, tinyi UNSIGNED_TINYINT)";
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
                conn.createStatement().executeQuery(
                    "SELECT LN(doub),LN(fl),LN(inte),LN(lon),LN(smalli),LN(tinyi) FROM "
                            + tableName);
        assertTrue(rs.next());
        Double d = Double.valueOf(data);
        assertTrue(twoDoubleEquals(rs.getDouble(1), Math.log(d.doubleValue())));
        assertTrue(twoDoubleEquals(rs.getDouble(2), Math.log(d.floatValue())));
        assertTrue(twoDoubleEquals(rs.getDouble(3), Math.log(d.intValue())));
        assertTrue(twoDoubleEquals(rs.getDouble(4), Math.log(d.longValue())));
        assertTrue(twoDoubleEquals(rs.getDouble(5), Math.log(d.shortValue())));
        assertTrue(twoDoubleEquals(rs.getDouble(6), Math.log(d.byteValue())));

        assertTrue(!rs.next());
        rs =
                conn.createStatement().executeQuery(
                    "SELECT LOG(doub),LOG(fl),LOG(inte),LOG(lon),LOG(smalli),LOG(tinyi) FROM "
                            + tableName);
        assertTrue(rs.next());
        d = Double.valueOf(data);
        assertTrue(twoDoubleEquals(rs.getDouble(1), Math.log10(d.doubleValue())));
        assertTrue(twoDoubleEquals(rs.getDouble(2), Math.log10(d.floatValue())));
        assertTrue(twoDoubleEquals(rs.getDouble(3), Math.log10(d.intValue())));
        assertTrue(twoDoubleEquals(rs.getDouble(4), Math.log10(d.longValue())));
        assertTrue(twoDoubleEquals(rs.getDouble(5), Math.log10(d.shortValue())));
        assertTrue(twoDoubleEquals(rs.getDouble(6), Math.log10(d.byteValue())));
        assertTrue(!rs.next());

        rs =
                conn.createStatement().executeQuery(
                    "SELECT LOG(doub,3),LOG(fl,3),LOG(inte,3),LOG(lon,3),LOG(smalli,3),LOG(tinyi,3) FROM "
                            + tableName);
        assertTrue(rs.next());
        d = Double.valueOf(data);
        assertTrue(twoDoubleEquals(rs.getDouble(1), Math.log(d.doubleValue()) / Math.log(3)));
        assertTrue(twoDoubleEquals(rs.getDouble(2), Math.log(d.floatValue()) / Math.log(3)));
        assertTrue(twoDoubleEquals(rs.getDouble(3), Math.log(d.intValue()) / Math.log(3)));
        assertTrue(twoDoubleEquals(rs.getDouble(4), Math.log(d.longValue()) / Math.log(3)));
        assertTrue(twoDoubleEquals(rs.getDouble(5), Math.log(d.shortValue()) / Math.log(3)));
        assertTrue(twoDoubleEquals(rs.getDouble(6), Math.log(d.byteValue()) / Math.log(3)));
        assertTrue(!rs.next());
    }

    @Test
    public void test() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        for (double d : new double[] { 0.0, 1.0, -1.0, 123.1234, -123.1234 }) {
            testNumberSpec(conn, d, signedTableName );
            if (d >= 0) testNumberSpec(conn, d, unsignedTableName );
        }
    }
}
