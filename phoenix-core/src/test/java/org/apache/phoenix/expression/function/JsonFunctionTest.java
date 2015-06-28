package org.apache.phoenix.expression.function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.json.PhoenixJson;
import org.apache.phoenix.schema.types.*;
import org.junit.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


public class JsonFunctionTest {
    public static final String TEST_JSON_STR =
            "{\"f2\":{\"f3\":\"value\"},\"f4\":{\"f5\":99,\"f6\":[1,true,\"foo\"]},\"f7\":true}";

    public PhoenixJson testArrayToJson (Object[] array,PDataType datatype,PArrayDataType arraydatatype) throws Exception {
        LiteralExpression arrayExpr;
        List<Expression> children;
        PhoenixArray pa =PArrayDataType.instantiatePhoenixArray( datatype,array);
        arrayExpr = LiteralExpression.newConstant(pa,arraydatatype );
        children = Arrays.<Expression>asList(arrayExpr);
        ArrayToJsonFunction e = new ArrayToJsonFunction(children);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean evaluated = e.evaluate(null, ptr);
        PhoenixJson result = (PhoenixJson)e.getDataType().toObject(ptr);
        return result;
    }

    @Test
    public void testNumberArrayToJson() throws Exception {
        Object[] testarray = new Object[]{1,12,32,432};
        PhoenixJson result = testArrayToJson(testarray, PInteger.INSTANCE, PIntegerArray.INSTANCE);
        String expected ="[1,12,32,432]";
        assertEquals(result.serializeToString(), expected);
        Object[] testarray2 = new Object[]{1.12,12.34,32.45,432.78};
        PhoenixJson result2 = testArrayToJson(testarray2, PDouble.INSTANCE, PDoubleArray.INSTANCE);
        String expected2 ="[1.12,12.34,32.45,432.78]";
        assertEquals(result2.serializeToString(), expected2);
    }
    @Test
    public void testBooleanArrayToJson() throws Exception {
        Object[] testarray = new Object[]{false,true};
        PhoenixJson result = testArrayToJson(testarray, PBoolean.INSTANCE, PBooleanArray.INSTANCE);
        String expected ="[false,true]";
        assertEquals(result.toString(), expected);
    }

    @Test
    public void testStringArrayToJson() throws Exception {
        Object[] testarray = new Object[]{"abc123","12.3","string","汉字"};
        PhoenixJson result = testArrayToJson(testarray, PVarchar.INSTANCE, PVarcharArray.INSTANCE);
        String expected ="[\"abc123\",\"12.3\",\"string\",\"汉字\"]";
        assertEquals(result.serializeToString(), expected);
    }
    @Test
    public void testDateArrayToJson() throws Exception {
        SimpleDateFormat myFormatter = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss.SSS");
        Date date1= myFormatter.parse("1990-12-01 11:01:45.0");
        Date date2 = myFormatter.parse("1989-03-12 13:01:45.0");
        Date date3 = myFormatter.parse("1974-06-06 12:01:45.0");
        Object[] testarray = new Object[]{date1,date2,date3};
        PhoenixJson result = testArrayToJson(testarray, PDate.INSTANCE,PDateArray.INSTANCE);
        String expected ="[\"1990-12-01\",\"1989-03-12\",\"1974-06-06\"]";
        assertEquals(result.serializeToString(), expected);

        Timestamp ts1 = Timestamp.valueOf("1990-12-01 11:01:45.123");
        Timestamp ts2 = Timestamp.valueOf("1989-03-12 01:01:01.0");
        Timestamp ts3 = Timestamp.valueOf("1989-03-12 23:59:59.1");
        testarray = new Object[]{ts1,ts2,ts3};
        result = testArrayToJson(testarray, PTimestamp.INSTANCE,PTimestampArray.INSTANCE);
        expected ="[\"1990-12-01 11:01:45.123\",\"1989-03-12 01:01:01.0\",\"1989-03-12 23:59:59.1\"]";
        assertEquals(result.serializeToString(), expected);

        Time t1 = new Time(date1.getTime());
        Time t2 = new Time(date2.getTime());
        Time t3 = new Time(date3.getTime());
        testarray = new Object[]{t1,t2,t3};
        result = testArrayToJson(testarray, PTime.INSTANCE,PTimeArray.INSTANCE);
        expected ="[\"11:01:45\",\"13:01:45\",\"12:01:45\"]";
        assertEquals(result.serializeToString(), expected);


    }


    public String[] JsonArrayElements (String json) throws Exception {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(json);
        LiteralExpression JsonExpr;
        List<Expression> children;
        JsonExpr = LiteralExpression.newConstant(phoenixJson,PJson.INSTANCE );
        children = Arrays.<Expression>asList(JsonExpr);
        JsonArrayElementsFunction e = new JsonArrayElementsFunction(children);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean evaluated = e.evaluate(null, ptr);
        PhoenixArray pa = (PhoenixArray)e.getDataType().toObject(ptr);
        return (String[] )pa.getArray();
    }

    @Test
    public void testJsonArrayElements() throws Exception {
        String json = "[1,true,\"string\",[2,false]]";
        Object[] expected = new Object[]{"1","true","\"string\"","[2,false]"};
        String[] result = JsonArrayElements(json);

        assertEquals(result.length, expected.length);
        for(int i = 0; i<result.length;i++){
            assertEquals(result[i], expected[i]);
        }
    }
    public String JsonPopulateRecord (Object[] types,String json) throws Exception {
        List<Expression> children;
        PhoenixArray pa =PArrayDataType.instantiatePhoenixArray( PVarchar.INSTANCE,types);
        LiteralExpression typesExpr = LiteralExpression.newConstant(pa,PVarcharArray.INSTANCE );
        PhoenixJson phoenixJson = PhoenixJson.getInstance(json);
        LiteralExpression JsonExpr;
        JsonExpr = LiteralExpression.newConstant(phoenixJson,PJson.INSTANCE );
        children = Arrays.<Expression>asList(typesExpr,JsonExpr);
        JsonPopulateRecordFunction e = new JsonPopulateRecordFunction(children);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean evaluated = e.evaluate(null, ptr);
        String record = (String)e.getDataType().toObject(ptr);
        return record;
    }

    @Test
    public void testJsonPopulateRecord() throws Exception {
        Object[] types= new Object[]{"a","b"};
        String json = "{\"a\":1,\"b\":2}";
        String expected = "1,2";
        String result = JsonPopulateRecord(types,json);
        assertEquals(result, expected);
    }

    public String[] JsonPopulateRecordSet (Object[] types,String json) throws Exception {
        List<Expression> children;
        PhoenixArray pa =PArrayDataType.instantiatePhoenixArray( PVarchar.INSTANCE,types);
        LiteralExpression typesExpr = LiteralExpression.newConstant(pa,PVarcharArray.INSTANCE );
        PhoenixJson phoenixJson = PhoenixJson.getInstance(json);
        LiteralExpression JsonExpr;
        JsonExpr = LiteralExpression.newConstant(phoenixJson,PJson.INSTANCE );
        children = Arrays.<Expression>asList(typesExpr,JsonExpr);
        JsonPopulateRecordSetFunction e = new JsonPopulateRecordSetFunction(children);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean evaluated = e.evaluate(null, ptr);
        PhoenixArray record = (PhoenixArray)e.getDataType().toObject(ptr);
        return (String[] )record.getArray();
    }


    @Test
    public void testJsonPopulateRecordSet() throws Exception {
        Object[] types= new Object[]{"a","b"};
        String json = "[{\"a\":1,\"b\":2},{\"a\":2,\"b\":3},{\"a\":4,\"b\":5}]";
        Object[] expected = new Object[]{"1,2","2,3","4,5"};
        String[] result = JsonPopulateRecordSet(types,json);
        assertEquals(result.length, expected.length);
        for(int i = 0; i<result.length;i++){
            assertEquals(result[i], expected[i]);
        }
    }

    public int JsonArrayLength (String json) throws Exception {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(json);
        LiteralExpression JsonExpr;
        List<Expression> children;
        JsonExpr = LiteralExpression.newConstant(phoenixJson,PJson.INSTANCE );
        children = Arrays.<Expression>asList(JsonExpr);
        JsonArrayLengthFunction e = new JsonArrayLengthFunction(children);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean evaluated = e.evaluate(null, ptr);
        int len = (int)e.getDataType().toObject(ptr);
        return len;
    }

    @Test
    public void testJsonArrayLength() throws Exception {
        String array1 = "[1,true,\"string\",[2,false]]";
        String array2 = "[1,2.34,[1,\"abc\"],4,true,\"string\",[2,false]]";
        assertEquals(JsonArrayLength(array1),4);
        assertEquals(JsonArrayLength(array2),7);
    }


    public PhoenixJson ToJson (Object obj,PDataType datatype) throws Exception {
        List<Expression> children;
        //LiteralExpression op =LiteralExpression.newConstant(new BigDecimal("9999.1"), PDecimal.INSTANCE);
        LiteralExpression op =LiteralExpression.newConstant(obj, datatype);
        children = Arrays.<Expression>asList(op);
        ToJsonFunction e = new ToJsonFunction(children);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean evaluated = e.evaluate(null, ptr);
        PhoenixJson result = (PhoenixJson)e.getDataType().toObject(ptr);
        return result;
    }


    @Test
    public void testToJson() throws Exception {
        assertEquals(ToJson(-256, PInteger.INSTANCE).serializeToString(),"-256");
        assertEquals(ToJson(-256, PLong.INSTANCE).serializeToString(),"-256");
        assertEquals(ToJson(-1, PSmallint.INSTANCE).serializeToString(),"-1");
        assertEquals(ToJson(-1, PTinyint.INSTANCE).serializeToString(),"-1");
        assertEquals(ToJson(12, PUnsignedInt.INSTANCE).serializeToString(),"12");
        assertEquals(ToJson(12, PUnsignedSmallint.INSTANCE).serializeToString(),"12");
        assertEquals(ToJson(12, PUnsignedLong.INSTANCE).serializeToString(),"12");
        assertEquals(ToJson(123.456, PDouble.INSTANCE).serializeToString(),"123.456");
        assertEquals(ToJson(123.456, PFloat.INSTANCE).serializeToString(),"123.456");
        assertEquals(ToJson(123.456,PUnsignedDouble.INSTANCE).serializeToString(),"123.456");
        assertEquals(ToJson(123.456, PUnsignedFloat.INSTANCE).serializeToString(),"123.456");
        assertEquals(ToJson(false, PBoolean.INSTANCE).serializeToString(),"false");
        assertEquals(ToJson(true, PBoolean.INSTANCE).serializeToString(),"true");
        assertEquals(ToJson("string_abc", PVarchar.INSTANCE).toString(),"\"string_abc\"");
        assertEquals(ToJson("string_abc", PVarchar.INSTANCE).serializeToString(),"string_abc");
    }
    public String[] JsonObjectKeys (String json) throws Exception {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(json);
        LiteralExpression JsonExpr;
        List<Expression> children;
        JsonExpr = LiteralExpression.newConstant(phoenixJson,PJson.INSTANCE );
        children = Arrays.<Expression>asList(JsonExpr);
        JsonObjectKeysFunction e = new JsonObjectKeysFunction(children);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean evaluated = e.evaluate(null, ptr);
        PhoenixArray pa = (PhoenixArray)e.getDataType().toObject(ptr);
        return (String[] )pa.getArray();
    }



    @Test
    public void testJsonObjectKeys() throws Exception {
        Object[] expected = new Object[]{"f2","f4","f7"};
        String[] result = JsonObjectKeys(TEST_JSON_STR);

        assertEquals(result.length, expected.length);
        for(int i = 0; i<result.length;i++){
            assertEquals(result[i], expected[i]);
        }
    }

    public String[] JsonEach (String json) throws Exception {
        PhoenixJson phoenixJson = PhoenixJson.getInstance(json);
        LiteralExpression JsonExpr;
        List<Expression> children;
        JsonExpr = LiteralExpression.newConstant(phoenixJson,PJson.INSTANCE );
        children = Arrays.<Expression>asList(JsonExpr);
        JsonEachFunction e = new JsonEachFunction(children);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean evaluated = e.evaluate(null, ptr);
        PhoenixArray pa = (PhoenixArray)e.getDataType().toObject(ptr);
        return (String[] )pa.getArray();
    }
    @Test
    public void testJsonEach() throws Exception {
        Object[] expected = new Object[]{"f2,{\"f3\":\"value\"}","f4,{\"f5\":99,\"f6\":[1,true,\"foo\"]}","f7,true"};
        String[] result = JsonEach(TEST_JSON_STR);

        assertEquals(result.length, expected.length);
        for(int i = 0; i<result.length;i++){
            assertEquals(result[i], expected[i]);
        }
    }

}
