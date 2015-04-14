package org.apache.phoenix.schema.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hbase.util.Bytes;
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
        byte[] json = Bytes.toBytes(jsonWithChineseChars);
        PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0, json.length);
        assertNotNull(phoenixJson);
        assertEquals(jsonWithChineseChars, phoenixJson.toString());

    }

    @Test
    public void testParsingForJsonHavingControlAndQuoteChars() throws Exception {

        String jsonWithControlChars = "[\"\\n \\\"jumps \\r'普派'\"]";
        byte[] json = Bytes.toBytes(jsonWithControlChars);
        PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0, json.length);
        assertNotNull(phoenixJson);
        assertEquals(jsonWithControlChars, phoenixJson.toString());

    }

    @Test
    public void testEmptyJsonParsing() throws Exception {

        String emptyJson = "{}";
        byte[] json = Bytes.toBytes(emptyJson);
        PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0, json.length);
        assertNotNull(phoenixJson);
        assertEquals(emptyJson, phoenixJson.toString());

    }

    @Test
    public void testJsonArrayParsing() throws Exception {

        String jsonArrayString = "[1,2,3]";
        byte[] json = Bytes.toBytes(jsonArrayString);
        PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0, json.length);
        assertNotNull(phoenixJson);
        assertEquals(jsonArrayString, phoenixJson.toString());
    }

    @Test
    public void testVaidJsonParsing() throws Exception {

        byte[] json = TEST_JSON_STR.getBytes();
        PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0, json.length);
        assertNotNull(phoenixJson);
        assertEquals(TEST_JSON_STR, phoenixJson.toString());
    }

    @Test
    public void getPhoenixJson() throws Exception {
        byte[] json = TEST_JSON_STR.getBytes();
        PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0, json.length);
        PhoenixJson phoenixJson2 = phoenixJson.getPhoenixJson(new String[] { "f2", "f3" });
        assertEquals("value", phoenixJson2.serializeToString());

        try {

            phoenixJson.getPhoenixJson(new String[] { "f2", "f3", "f4" });
        } catch (Exception e) {
            PhoenixJsonException jsonException = new PhoenixJsonException("path: f4 not found");
            assertEquals(jsonException.getMessage(), e.getMessage());
            assertEquals(jsonException.getClass(), e.getClass());
        }

    }

    @Test
    public void getNullablePhoenixJson() throws Exception {
        byte[] json = TEST_JSON_STR.getBytes();
        PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0, json.length);
        PhoenixJson phoenixJson2 = phoenixJson.getNullablePhoenixJson(new String[] { "f2", "f3" });
        assertEquals("value", phoenixJson2.serializeToString());

        assertNull(phoenixJson.getNullablePhoenixJson(new String[] { "f2", "f3", "f4" }));
        assertNotNull(phoenixJson.getNullablePhoenixJson(new String[] { "f4", "f6", "1" }));
        assertNull(phoenixJson.getNullablePhoenixJson(new String[] { "f4", "f6", "3" }));
        assertNull(phoenixJson.getNullablePhoenixJson(new String[] { "f4", "f6", "-1" }));
    }

    @Test
    public void serializeToString() throws Exception {
        byte[] json = TEST_JSON_STR.getBytes();
        PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0, json.length);
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
        byte[] json = TEST_JSON_STR.getBytes();
        PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0, json.length);
        PhoenixJson phoenixJson5 =
                phoenixJson.getNullablePhoenixJson(new String[] { "f4", "f6", "0" });
        assertEquals(new Integer(1).toString(), phoenixJson5.serializeToString());
        phoenixJson5 = phoenixJson.getNullablePhoenixJson(new String[] { "f4", "f6", "1" });
        assertEquals(Boolean.TRUE.toString(), phoenixJson5.serializeToString());
        phoenixJson5 = phoenixJson.getNullablePhoenixJson(new String[] { "f4", "f6", "2" });
        assertEquals("foo", phoenixJson5.serializeToString());
    }

    @Test
    public void testToString() throws Exception {
        byte[] json = TEST_JSON_STR.getBytes();
        PhoenixJson phoenixJson = PhoenixJson.getPhoenixJson(json, 0, json.length);
        assertEquals(TEST_JSON_STR, phoenixJson.toString());
    }
}
