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

import org.apache.phoenix.expression.function.CosFunction;
import org.apache.phoenix.expression.function.SinFunction;
import org.apache.phoenix.expression.function.TanFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * End to end tests for
 * {@link org.apache.phoenix.expression.function.CosFunction}
 * {@link org.apache.phoenix.expression.function.SinFunction}
 * {@link org.apache.phoenix.expression.function.TanFunction}
 */

@Category(ParallelStatsDisabledTest.class)
public class MathTrigFunctionEnd2EndIT extends ParallelStatsDisabledIT {

    private static final String KEY = "key";
    private String tableName;

    @Before
    public void initTable() throws Exception {
        Connection conn = null;
        PreparedStatement stmt = null;
        tableName = generateUniqueName();

        try {
            conn = DriverManager.getConnection(getUrl());
            String ddl;
            ddl =
                    "CREATE TABLE " + tableName + " (k VARCHAR NOT NULL PRIMARY KEY, doub DOUBLE)";
            conn.createStatement().execute(ddl);
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    private void updateTableSpec(Connection conn, double data, String tableName) throws Exception {
        PreparedStatement stmt =
                conn.prepareStatement("UPSERT INTO " + tableName + " VALUES (?, ?)");
        stmt.setString(1, KEY);
        stmt.setDouble(2, data);
        stmt.executeUpdate();
        conn.commit();
    }

    private void testNumberSpec(Connection conn, double data, String tableName) throws Exception {
        updateTableSpec(conn, data, tableName);
        ResultSet rs =
                conn.createStatement().executeQuery(
                        "SELECT SIN(doub),COS(doub),TAN(doub) FROM " + tableName);
        assertTrue(rs.next());
        Double d = Double.valueOf(data);
        assertTrue(twoDoubleEquals(rs.getDouble(1), Math.sin(data)));
        assertTrue(twoDoubleEquals(rs.getDouble(2), Math.cos(data)));
        assertTrue(twoDoubleEquals(rs.getDouble(3), Math.tan(data)));

        assertTrue(!rs.next());
    }

    @Test
    public void test() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        for (double d : new double[] { 0.0, 1.0, -1.0, 123.1234, -123.1234 }) {
            testNumberSpec(conn, d, tableName);
        }
    }
}
