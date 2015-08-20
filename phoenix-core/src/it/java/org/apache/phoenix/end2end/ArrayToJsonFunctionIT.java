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
            int boolColumnIndex  = 2;
            stmt.setArray(boolColumnIndex , boolArray);
            // byte array
            Array byteArray = conn.createArrayOf("TINYINT", new Byte[] { 11, 22 });
            int byteColumnIndex = 3;
            stmt.setArray(byteColumnIndex, byteArray);
            // double array
            Array doubleArray = conn.createArrayOf("DOUBLE", new Double[] { 67.78, 78.89 });
            int doubleColumnIndex = 4;
            stmt.setArray(doubleColumnIndex, doubleArray);
            // float array
            Array floatArray = conn.createArrayOf("FLOAT", new Float[] { 12.23f, 45.56f });
            int floatColumnIndex = 5;
            stmt.setArray(floatColumnIndex, floatArray);
            // int array
            Array intArray = conn.createArrayOf("INTEGER", new Integer[] { 5555, 6666 });
            int intColumnIndex = 6;
            stmt.setArray(intColumnIndex, intArray);
            // long array
            Array longArray = conn.createArrayOf("BIGINT", new Long[] { 7777777L, 8888888L });
            int longColumnIndex = 7;
            stmt.setArray(longColumnIndex, longArray);
            // short array
            Array shortArray = conn.createArrayOf("SMALLINT", new Short[] { 333, 444 });
            int shortColumnIndex = 8;
            stmt.setArray(shortColumnIndex, shortArray);
            // create character array
            Array stringArray = conn.createArrayOf("VARCHAR", new String[] { "a", "b" });
            int stringColumnIndex = 9;
            stmt.setArray(stringColumnIndex, stringArray);
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
            assertArrayToJson(rs, boolColumnIndex , "[true,false]");
            assertArrayToJson(rs, byteColumnIndex, "[11,22]");
            assertArrayToJson(rs, doubleColumnIndex, "[67.78,78.89]");
            assertArrayToJson(rs, floatColumnIndex, "[12.23,45.56]");
            assertArrayToJson(rs, intColumnIndex, "[5555,6666]");
            assertArrayToJson(rs, longColumnIndex, "[7777777,8888888]");
            assertArrayToJson(rs, shortColumnIndex, "[333,444]");
            assertArrayToJson(rs, stringColumnIndex, "[\"a\",\"b\"]");

        } finally {
            conn.close();
        }

    }

    @Test
    public void testArrayToJsonWithNullValueArray() throws Exception {
        // create the table
        String ddlStmt = "create table "
                + "TABLE_NULL_VALUE_ARRAY"
                + "   (PK VARCHAR NOT NULL PRIMARY KEY,\n"
                + "    NULL_VALUE_ARRAY varchar(100) array[2]"
                + ")";
        createTestTable(getUrl(), ddlStmt);

        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        conn.setAutoCommit(false);

        try{
            // populate the table with data
            PreparedStatement stmt =
                    conn.prepareStatement("UPSERT INTO "
                            + "TABLE_NULL_VALUE_ARRAY"
                            + "(PK, NULL_VALUE_ARRAY)\n"
                            + "VALUES (?, ?)");

            stmt.setString(1, "valueOne");

            Array nullValueArray = conn.createArrayOf("VARCHAR", new String[] { null, null });
            int nullValueIndex = 2;
            stmt.setArray(nullValueIndex, nullValueArray);
            stmt.execute();
            conn.commit();

            stmt =
                    conn.prepareStatement("SELECT PK, " +
                            "ARRAY_TO_JSON(NULL_VALUE_ARRAY) FROM "
                            + "TABLE_NULL_VALUE_ARRAY");

            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());

            assertEquals("valueOne", rs.getString(1));
            assertArrayToJson(rs, nullValueIndex, "[null,null]");

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
