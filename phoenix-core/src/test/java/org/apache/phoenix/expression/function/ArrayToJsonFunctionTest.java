package org.apache.phoenix.expression.function;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.types.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;


/**
 * Created by WangLei on 2015/3/20.
 */
public class ArrayToJsonFunctionTest {

    public String testIntArrayToJson (Object[] array) throws Exception {
        LiteralExpression arrayExpr;
        List<Expression> children;
        PhoenixArray pa =new PhoenixArray.PrimitiveIntPhoenixArray( PInteger.INSTANCE,array);
        arrayExpr = LiteralExpression.newConstant(pa,PIntegerArray.INSTANCE );
        children = Arrays.<Expression>asList(arrayExpr);
        ArrayToJsonFunction e = new ArrayToJsonFunction(children);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        boolean evaluated = e.evaluate(null, ptr);
        String result = (String)e.getDataType().toObject(ptr);
        return result;
    }
    @Test
    public void testArrayToJson() throws Exception {
        Object[] testarray = new Object[]{1,12,32};
        String result = testIntArrayToJson(testarray);
        String expected ="[1,12,32]";
        assertEquals(result, expected);
    }
}
