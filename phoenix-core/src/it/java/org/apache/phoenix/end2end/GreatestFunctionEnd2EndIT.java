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

import org.apache.phoenix.expression.function.GreatestFunction;
import org.junit.Before;
import org.junit.Test;

/**
 * End to end tests for {@link GreatestFunction}
 */
public class GreatestFunctionEnd2EndIT extends ParallelStatsDisabledIT {

    private static final Double compareDoubleNum = 1.5;
    private static final Integer compareIntNum = 1;
    private String signedTableName;
    private String unsignedTableName;
    private static final double ZERO = 1e-9;
    private static final String KEY = "key";

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
            ddl = "CREATE TABLE " + signedTableName + " (k VARCHAR NOT NULL PRIMARY KEY, doub " +
                    "DOUBLE, fl FLOAT, inte INTEGER, lon BIGINT, smalli SMALLINT, tinyi TINYINT)";
            conn.createStatement().execute(ddl);
            ddl = "CREATE TABLE " + unsignedTableName + " (k VARCHAR NOT NULL PRIMARY KEY, doub" +
                    " UNSIGNED_DOUBLE, fl UNSIGNED_FLOAT, inte UNSIGNED_INT, lon UNSIGNED_LONG," +
                    " smalli UNSIGNED_SMALLINT, tinyi UNSIGNED_TINYINT)";
            conn.createStatement().execute(ddl);
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    private void updateTableSpec(Connection conn, double data, String tableName) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO " + tableName +
                " VALUES (?, ?, ?, ?, ?, ?, ?)");
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
        PreparedStatement stmt = conn.prepareStatement("SELECT GREATEST(doub, ?)," +
                "GREATEST(fl, ?),GREATEST(inte, ?),GREATEST(lon, ?)," +
                "GREATEST(smalli, ?),GREATEST(tinyi, ?) FROM " + tableName);
        stmt.setDouble(1, compareDoubleNum.doubleValue());
        stmt.setFloat(2, compareDoubleNum.floatValue());
        stmt.setInt(3, compareIntNum.intValue());
        stmt.setLong(4, compareIntNum.longValue());
        stmt.setShort(5, compareIntNum.shortValue());
        stmt.setByte(6, compareIntNum.byteValue());
        ResultSet rs = stmt.executeQuery();

        assertTrue(rs.next());
        Double d = Double.valueOf(data);
        assertTrue(twoDoubleEquals(rs.getDouble(1), Math.max(d.doubleValue(), compareDoubleNum)));
        assertTrue(twoDoubleEquals(rs.getDouble(2), Math.max(d.floatValue(), compareDoubleNum)));
        assertTrue(twoDoubleEquals(rs.getDouble(3), Math.max(d.intValue(), compareIntNum)));
        assertTrue(twoDoubleEquals(rs.getDouble(4), Math.max(d.longValue(), compareIntNum)));
        assertTrue(twoDoubleEquals(rs.getDouble(5), Math.max(d.shortValue(), compareIntNum)));
        assertTrue(twoDoubleEquals(rs.getDouble(6), Math.max(d.byteValue(), compareIntNum)));
        assertTrue(!rs.next());
    }

    @Test
    public void test() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        for (double d : new double[] { 0.0, 1.0, -1.0, 123.1234, -123.1234 }) {
            testNumberSpec(conn, d, signedTableName);
            if (d >= 0)
                testNumberSpec(conn, d, unsignedTableName);
        }
    }
}
