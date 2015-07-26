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

import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.apache.phoenix.util.TestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End to end test for {@link org.apache.phoenix.expression.function.ToJsonFunction}.
 *
 */
public class ToJsonFunctionIT extends BaseHBaseManagedTimeIT {
    private static final String TABLE_WITH_ALL_TYPES = "TABLE_WITH_ALL_TYPES";

    @Test
    public void testToJsonWithAllTypes() throws Exception {
        // create the table
        createTableWithAllTypes(getUrl());

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);
        try {
            // populate the table with data
            PreparedStatement stmt =
                    conn.prepareStatement("UPSERT INTO "
                            + TABLE_WITH_ALL_TYPES
                            + "(pk, BOOLEAN_COL, BYTE_COl, DOUBLE_COL, FLOAT_COL, INT_COL, LONG_COL, SHORT_COL, STRING_COL)"
                            + " VALUES ('valueOne', " +
                            "true," +
                            "11, " +
                            "67.78," +
                            "12.23," +
                            "5555," +
                            "7777777," +
                            "333," +
                            "'string')");
            stmt.execute();
            conn.commit();

            stmt =
                    conn.prepareStatement("SELECT pk, " +
                            "to_json(boolean_col), " +
                            "to_json(byte_col), " +
                            "to_json(double_col), " +
                            "to_json(float_col), " +
                            "to_json(int_col), " +
                            "to_json(long_col), " +
                            "to_json(short_col)," +
                            "to_json(string_col) FROM "
                            + TABLE_WITH_ALL_TYPES
                            + " where pk = 'valueOne'");

            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());

            assertEquals("valueOne", rs.getString(1));
            assertToJson(rs, 2, "true");
            assertToJson(rs, 3, "11");
            assertToJson(rs, 4, "67.78");
            assertToJson(rs, 5, "12.23");
            assertToJson(rs, 6, "5555");
            assertToJson(rs, 7, "7777777");
            assertToJson(rs, 8, "333");
            assertToJson(rs, 9, "\"string\"");
        }finally {
            conn.close();
        }
    }


    private void assertToJson(ResultSet rs, int arrayIndex, String expectedJson)
            throws SQLException {
        assertEquals("Json data read from DB is not as expected.",expectedJson, rs.getString(arrayIndex));
    }


    private static void createTableWithAllTypes(String url) throws SQLException {
        String ddlStmt = "create table "
                + TABLE_WITH_ALL_TYPES
                + "   (pk VARCHAR NOT NULL PRIMARY KEY,\n"
                + "    boolean_col boolean,\n"
                + "    byte_col tinyint,\n"
                + "    double_col double,\n"
                + "    float_col float,\n"
                + "    int_col integer,\n"
                + "    long_col bigint,\n"
                + "    short_col smallint,\n"
                + "    string_col varchar(100) "
                + ")";
        createTestTable(url, ddlStmt);
    }
}
