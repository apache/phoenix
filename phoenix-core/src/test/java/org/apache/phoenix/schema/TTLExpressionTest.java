package org.apache.phoenix.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.junit.Test;

public class TTLExpressionTest {

    @Test
    public void testLiteralExpression() {
        int ttl = 100;
        TTLLiteralExpression literal = new TTLLiteralExpression(ttl);
        assertEquals(literal, TTLExpression.create(ttl));
        assertEquals(literal, TTLExpression.create(String.valueOf(ttl)));
    }

    @Test
    public void testForever() {
        assertEquals(TTLExpression.TTL_EXPRESSION_FORVER,
                TTLExpression.create(PhoenixDatabaseMetaData.FOREVER_TTL));
        assertEquals(TTLExpression.TTL_EXPRESSION_FORVER,
                TTLExpression.create(HConstants.FOREVER));
    }

    @Test
    public void testNone() {
        assertEquals(TTLExpression.TTL_EXPRESSION_NOT_DEFINED,
                TTLExpression.create(PhoenixDatabaseMetaData.NONE_TTL));
        assertEquals(TTLExpression.TTL_EXPRESSION_NOT_DEFINED,
                TTLExpression.create(PhoenixDatabaseMetaData.TTL_NOT_DEFINED));
        assertNull(TTLExpression.TTL_EXPRESSION_NOT_DEFINED.getTTLForScanAttribute());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidLiteral() {
        TTLExpression.create(-1);
    }

    @Test
    public void testConditionalExpression() {
        String ttl = "PK1 = 5 AND COL1 > 'abc'";
        TTLConditionExpression expected = new TTLConditionExpression(ttl);
        TTLExpression actual = TTLExpression.create(ttl);
        assertEquals(expected, actual);
        assertEquals(ttl, expected.getTTLExpression());
        assertNull(actual.getTTLForScanAttribute());
    }
}
