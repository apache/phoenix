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

import org.apache.phoenix.expression.function.OctetLengthFunction;
import org.junit.Before;
import org.junit.Test;

/**
 * End to end tests for {@link OctetLengthFunction}
 */
public class OctetLengthFunctionEnd2EndIT extends ParallelStatsDisabledIT {

    private static final String KEY = "key";
    private static final String TABLE_NAME = generateUniqueName();

    @Before
    public void initTable() throws Exception {
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
                        .executeQuery("SELECT OCTET_LENGTH(vb), OCTET_LENGTH(b) FROM " + TABLE_NAME
                            + " WHERE OCTET_LENGTH(vb)=4 and OCTET_LENGTH(b)=4");
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals(4, rs.getInt(2));
        assertTrue(!rs.next());
    }
}
