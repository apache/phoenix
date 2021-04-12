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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.phoenix.expression.function.GetBitFunction;
import org.apache.phoenix.expression.function.GetByteFunction;
import org.apache.phoenix.expression.function.SetBitFunction;
import org.apache.phoenix.expression.function.SetByteFunction;
import org.junit.Before;
import org.junit.Test;

/**
 * End to end tests for {@link GetByteFunction} {@link SetByteFunction} {@link GetBitFunction}
 * {@link SetBitFunction}
 */
public class GetSetByteBitFunctionEnd2EndIT extends ParallelStatsDisabledIT {

    private static final String KEY = "key";
    private String TABLE_NAME;

    @Before
    public void initTable() throws Exception {
        TABLE_NAME = generateUniqueName();
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            conn = DriverManager.getConnection(getUrl());
            String ddl;
            ddl = "CREATE TABLE " + TABLE_NAME
                + " (k VARCHAR NOT NULL PRIMARY KEY, b BINARY(4), vb VARBINARY)";
            conn.createStatement().execute(ddl);
            conn.commit();
        } finally {
            closeStmtAndConn(stmt, conn);
        }
    }

    @Test
    public void test() throws Exception {
        Connection conn = DriverManager.getConnection(getUrl());
        PreparedStatement stmt = conn.prepareStatement(
            "UPSERT INTO " + TABLE_NAME + " VALUES (?, ?, ?)");
        stmt.setString(1, KEY);
        stmt.setBytes(2, new byte[] { 1, 2, 3, 4 });
        stmt.setBytes(3, new byte[] { 1, 2, 3, 4 });
        stmt.executeUpdate();
        conn.commit();
        ResultSet rs =
                conn.createStatement()
                        .executeQuery("SELECT GET_BYTE(vb, 1), GET_BYTE(b, 1) FROM " + TABLE_NAME
                            + " WHERE GET_BYTE(vb, 1)=2 and GET_BYTE(b, 1)=2");
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertTrue(!rs.next());
        rs =
                conn.createStatement()
                        .executeQuery("SELECT GET_BIT(b, 0),GET_BIT(b, 9) FROM " + TABLE_NAME
                            + " WHERE GET_BIT(vb, 0)=1 and GET_BIT(vb, 9)=1");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertTrue(!rs.next());
        rs =
                conn.createStatement().executeQuery(
                    "SELECT SET_BYTE(vb, 1, 1), SET_BYTE(b, 1, 1) FROM " + TABLE_NAME);
        assertTrue(rs.next());
        assertArrayEquals(new byte[] { 1, 1, 3, 4 }, rs.getBytes(1));
        assertArrayEquals(new byte[] { 1, 1, 3, 4 }, rs.getBytes(2));
        assertTrue(!rs.next());
        rs =
                conn.createStatement().executeQuery(
                    "SELECT SET_BIT(vb, 8, 1), SET_BIT(b, 8, 1) FROM " + TABLE_NAME);
        assertTrue(rs.next());
        assertArrayEquals(new byte[] { 1, 3, 3, 4 }, rs.getBytes(1));
        assertArrayEquals(new byte[] { 1, 3, 3, 4 }, rs.getBytes(2));
        assertTrue(!rs.next());
    }
}
