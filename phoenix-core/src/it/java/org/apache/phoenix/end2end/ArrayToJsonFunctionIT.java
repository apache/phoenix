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

import static org.apache.phoenix.util.TestUtil.ROW1;
import static org.apache.phoenix.util.TestUtil.TABLE_WITH_ARRAY;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.*;
import java.util.Properties;
import org.apache.phoenix.expression.function.ArrayToJsonFunction;
import org.apache.phoenix.query.BaseTest;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;

/**
 * End to end test for {@link org.apache.phoenix.expression.function.ArrayToJsonFunction}.
 *
 */
public class ArrayToJsonFunctionIT extends BaseHBaseManagedTimeIT {
    private static final String TABLE_WITH_ALL_ARRAY_TYPES = "TABLE_WITH_ALL_ARRAY_TYPES";

    @Test
    public void testArrayToJsonWithAllArrayTypes() throws Exception {
        // create the table
        createTableWithAllArrayTypes(getUrl());

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try{
            // populate the table with data
            PreparedStatement stmt =
                    conn.prepareStatement("UPSERT INTO "
                            + TABLE_WITH_ALL_ARRAY_TYPES
                            + "(pk, BOOLEAN_ARRAY, BYTE_ARRAY, DOUBLE_ARRAY, FLOAT_ARRAY, INT_ARRAY, LONG_ARRAY, SHORT_ARRAY, STRING_ARRAY)\n"
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

            stmt.setString(1, "valueOne");

            // boolean array
            Array boolArray = conn.createArrayOf("BOOLEAN", new Boolean[] { true,false });
            int boolIndex = 2;
            stmt.setArray(boolIndex, boolArray);
            // byte array
            Array byteArray = conn.createArrayOf("TINYINT", new Byte[] { 11, 22 });
            int byteIndex = 3;
            stmt.setArray(byteIndex, byteArray);
            // double array
            Array doubleArray = conn.createArrayOf("DOUBLE", new Double[] { 67.78, 78.89 });
            int doubleIndex = 4;
            stmt.setArray(doubleIndex, doubleArray);
            // float array
            Array floatArray = conn.createArrayOf("FLOAT", new Float[] { 12.23f, 45.56f });
            int floatIndex = 5;
            stmt.setArray(floatIndex, floatArray);
            // int array
            Array intArray = conn.createArrayOf("INTEGER", new Integer[] { 5555, 6666 });
            int intIndex = 6;
            stmt.setArray(intIndex, intArray);
            // long array
            Array longArray = conn.createArrayOf("BIGINT", new Long[] { 7777777L, 8888888L });
            int longIndex = 7;
            stmt.setArray(longIndex, longArray);
            // short array
            Array shortArray = conn.createArrayOf("SMALLINT", new Short[] { 333, 444 });
            int shortIndex = 8;
            stmt.setArray(shortIndex, shortArray);
            // create character array
            Array stringArray = conn.createArrayOf("VARCHAR", new String[] { "a", "b" });
            int stringIndex = 9;
            stmt.setArray(stringIndex, stringArray);
            stmt.execute();
            conn.commit();

            stmt =
                    conn.prepareStatement("SELECT pk, " +
                            "array_to_json(boolean_array), " +
                            "array_to_json(byte_array), " +
                            "array_to_json(double_array), " +
                            "array_to_json(float_array), " +
                            "array_to_json(int_array), " +
                            "array_to_json(long_array), " +
                            "array_to_json(short_array)," +
                            "array_to_json(string_array) FROM "
                            + TABLE_WITH_ALL_ARRAY_TYPES);

            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());

            assertEquals("valueOne", rs.getString(1));
            assertArrayToJson(rs, boolIndex, "[true,false]");
            assertArrayToJson(rs, byteIndex, "[11,22]");
            assertArrayToJson(rs, doubleIndex, "[67.78,78.89]");
            assertArrayToJson(rs, floatIndex, "[12.23,45.56]");
            assertArrayToJson(rs, intIndex, "[5555,6666]");
            assertArrayToJson(rs, longIndex, "[7777777,8888888]");
            assertArrayToJson(rs, shortIndex, "[333,444]");
            assertArrayToJson(rs, stringIndex, "[\"a\",\"b\"]");

        } finally {
            conn.close();
        }

    }


    private void assertArrayToJson(ResultSet rs, int arrayIndex, String expectedJson)
            throws SQLException {
        assertEquals("Json array data is not as expected.",expectedJson, rs.getString(arrayIndex));
    }


    private static void createTableWithAllArrayTypes(String url) throws SQLException {
        String ddlStmt = "create table "
                + TABLE_WITH_ALL_ARRAY_TYPES
                + "   (pk VARCHAR NOT NULL PRIMARY KEY,\n"
                + "    boolean_array boolean array[2],\n"
                + "    byte_array tinyint[2],\n"
                + "    double_array double[2],\n"
                + "    float_array float[2],\n"
                + "    int_array integer[2],\n"
                + "    long_array bigint[5],\n"
                + "    short_array smallint[2],\n"
                + "    string_array varchar(100) array[2]"
                + ")";
        createTestTable(url, ddlStmt);
    }

}
