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

package org.apache.phoenix.schema.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.sql.SQLException;
import java.util.Arrays;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.schema.ConstraintViolationException;
import org.apache.phoenix.schema.EqualityNotSupportedException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link PhoenixJson}.
 */
public class PhoenixJsonTest {
    public static final String TEST_JSON_STR =
            "{\"f2\":{\"f3\":\"value\"},\"f4\":{\"f5\":99,\"f6\":[1,true,\"foo\"]},\"f7\":true}";

    @Test
    public void testParsingForJsonHavingChineseChars() throws Exception {

        String jsonWithChineseChars = "[\"'普派'\"]";
        PhoenixJson phoenixJson = PhoenixJson.getInstance(jsonWithChineseChars);
        assertNotNull(phoenixJson);
        assertEquals(jsonWithChineseChars, phoenixJson.toString());

    }

    @Test
    public void testParsingForJsonHavingControlAndQuoteChars() throws Exception {

        String jsonWithControlChars = "[\"\\n \\\"jumps \\r'普派'\"]";
        PhoenixJson phoenixJson = PhoenixJson.getInstance(jsonWithControlChars);
        assertNotNull(phoenixJson);
        assertEquals(jsonWithControlChars, phoenixJson.toString());

    }

    @Test
    public void testEmptyJsonParsing() throws Exception {

        String emptyJson = "{}";
        PhoenixJson phoenixJson = PhoenixJson.getInstance(emptyJson);
        assertNotNull(phoenixJson);
        assertEquals(emptyJson, phoenixJson.toString());

    }

    @Test
    public void testZeroLengthJsonStringForParsing() throws Exception {

        String emptyJson = "";
        try {
            PhoenixJson.getInstance(emptyJson);
        } catch (SQLException expectedException) {
            assertEquals("error code is not as expected.",
                SQLExceptionCode.INVALID_JSON_DATA.getErrorCode(), expectedException.getErrorCode());
            assertEquals("sql state is not as expected.",
                SQLExceptionCode.INVALID_JSON_DATA.getSQLState(), expectedException.getSQLState());
        }

    }

    @Test
    public void testNullJsonStringForParsing() throws Exception {

        String nullJson = null;
        try {
            PhoenixJson.getInstance(nullJson);
        } catch (SQLException expectedException) {
            assertEquals("error code is not as expected.",
                SQLExceptionCode.INVALID_JSON_DATA.getErrorCode(), expectedException.getErrorCode());
            assertEquals("sql state is not as expected.",
                SQLExceptionCode.INVALID_JSON_DATA.getSQLState(), expectedException.getSQLState());
        }

    }

    @Test
    public void testJsonArrayParsing() throws Exception {

        String jsonArrayString = "[1,2,3]";
        PhoenixJson phoenixJson = PhoenixJson.getInstance(jsonArrayString);
        assertNotNull(phoenixJson);
        assertEquals(jsonArrayString, phoenixJson.toString());
    }

    @Test
    public void testVaidJsonParsing() throws Exception {

        PhoenixJson phoenixJson = PhoenixJson.getInstance(TEST_JSON_STR);
        assertNotNull(phoenixJson);
        assertEquals(TEST_JSON_STR, phoenixJson.toString());
    }

    @Test
    public void getPhoenixJson() throws Exception {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(TEST_JSON_STR);
        PhoenixJson phoenixJson2 = phoenixJson.getPhoenixJson(new String[] { "f2", "f3" });
        assertEquals("value", phoenixJson2.serializeToString());

        String[] paths = new String[] { "f2", "f3", "f4" };
        try {
            phoenixJson.getPhoenixJson(paths);
        } catch (Exception e) {
            SQLException jsonException =
                    new SQLException("path: " + Arrays.asList(paths) + " not found.");
            assertEquals(jsonException.getMessage(), e.getMessage());
            assertEquals(jsonException.getClass(), e.getClass());
        }
    }

    @Test
    public void getNullablePhoenixJson() throws Exception {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(TEST_JSON_STR);
        PhoenixJson phoenixJson2 = phoenixJson.getPhoenixJsonOrNull(new String[] { "f2", "f3" });
        assertEquals("value", phoenixJson2.serializeToString());

        assertNull(phoenixJson.getPhoenixJsonOrNull(new String[] { "f2", "f3", "f4" }));
        assertNotNull(phoenixJson.getPhoenixJsonOrNull(new String[] { "f4", "f6", "1" }));
        assertNull(phoenixJson.getPhoenixJsonOrNull(new String[] { "f4", "f6", "3" }));
        assertNull(phoenixJson.getPhoenixJsonOrNull(new String[] { "f4", "f6", "-1" }));
    }

    @Test
    public void serializeToString() throws Exception {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(TEST_JSON_STR);
        PhoenixJson phoenixJson2 = phoenixJson.getPhoenixJson(new String[] { "f4", "f5" });
        assertEquals(new Integer(99).toString(), phoenixJson2.serializeToString());

        PhoenixJson phoenixJson3 = phoenixJson.getPhoenixJson(new String[] { "f7" });
        assertEquals(Boolean.TRUE.toString(), phoenixJson3.serializeToString());

        PhoenixJson phoenixJson4 = phoenixJson.getPhoenixJson(new String[] { "f2", "f3" });
        assertEquals("value", phoenixJson4.serializeToString());

        PhoenixJson phoenixJson5 = phoenixJson.getPhoenixJson(new String[] { "f4", "f6" });

        assertEquals("[1,true,\"foo\"]", phoenixJson5.serializeToString());
    }

    @Test
    public void serializeToStringForJsonArray() throws Exception {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(TEST_JSON_STR);
        PhoenixJson phoenixJson5 =
                phoenixJson.getPhoenixJsonOrNull(new String[] { "f4", "f6", "0" });
        assertEquals(new Integer(1).toString(), phoenixJson5.serializeToString());
        phoenixJson5 = phoenixJson.getPhoenixJsonOrNull(new String[] { "f4", "f6", "1" });
        assertEquals(Boolean.TRUE.toString(), phoenixJson5.serializeToString());
        phoenixJson5 = phoenixJson.getPhoenixJsonOrNull(new String[] { "f4", "f6", "2" });
        assertEquals("foo", phoenixJson5.serializeToString());
    }

    @Test
    public void testToString() throws Exception {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(TEST_JSON_STR);
        assertEquals(TEST_JSON_STR, phoenixJson.toString());
    }

    @Test
    public void compareTo() throws Exception {
        PhoenixJson phoenixJson1 = PhoenixJson.getInstance(TEST_JSON_STR);

       try{
        phoenixJson1.compareTo(phoenixJson1);
       }catch(EqualityNotSupportedException x){
           SQLException sqe =(SQLException)x.getCause();
           assertEquals(SQLExceptionCode.NON_EQUALITY_COMPARISON.getErrorCode(), sqe.getErrorCode());
       }
       
    }
}
