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
package org.apache.phoenix.expression;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PBaseColumn;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OrExpressionTest {

    private OrExpression createOr(Expression lhs, Expression rhs) {
        return new OrExpression(Arrays.asList(lhs, rhs));
    }

    private OrExpression createOr(Boolean x, Boolean y) {
        return createOr(LiteralExpression.newConstant(x), LiteralExpression.newConstant(y));
    }

    private void testImmediateSingle(Boolean expected, Boolean lhs, Boolean rhs) {
        OrExpression or = createOr(lhs, rhs);
        ImmutableBytesWritable out = new ImmutableBytesWritable();
        MultiKeyValueTuple tuple = new MultiKeyValueTuple();
        boolean success = or.evaluate(tuple, out);
        assertTrue(success);
        assertEquals(expected, PBoolean.INSTANCE.toObject(out));
    }

    // Evaluating OR when values of both sides are known should immediately succeed
    // and return the same result regardless of order.
    private void testImmediate(Boolean expected, Boolean a, Boolean b) {
        testImmediateSingle(expected, a, b);
        testImmediateSingle(expected, b, a);
    }

    private PColumn pcolumn(final String name) {
        return new PBaseColumn() {
            @Override public PName getName() {
                return PNameFactory.newName(name);
            }

            @Override public PDataType getDataType() {
                return PBoolean.INSTANCE;
            }

            @Override public PName getFamilyName() {
                return PNameFactory.newName(QueryConstants.DEFAULT_COLUMN_FAMILY);
            }

            @Override public int getPosition() {
                return 0;
            }

            @Override public Integer getArraySize() {
                return null;
            }

            @Override public byte[] getViewConstant() {
                return new byte[0];
            }

            @Override public boolean isViewReferenced() {
                return false;
            }

            @Override public String getExpressionStr() {
                return null;
            }

            @Override public boolean isRowTimestamp() {
                return false;
            }

            @Override public boolean isDynamic() {
                return false;
            }

            @Override public byte[] getColumnQualifierBytes() {
                return null;
            }

            @Override public long getTimestamp() {
                return 0;
            }

            @Override public boolean isDerived() {
                return false;
            }

            @Override public boolean isExcluded() {
                return false;
            }

            @Override public SortOrder getSortOrder() {
                return null;
            }
        };
    }

    private KeyValueColumnExpression kvExpr(final String name) {
        return new KeyValueColumnExpression(pcolumn(name));
    }

    private Cell createCell(String name, Boolean value) {
        byte[] valueBytes = value == null ? null : value ? PBoolean.TRUE_BYTES : PBoolean.FALSE_BYTES;
        return CellUtil.createCell(
                Bytes.toBytes("row"),
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                Bytes.toBytes(name),
                1,
                KeyValue.Type.Put.getCode(),
                valueBytes);
    }

    private void testPartialOneSideFirst(Boolean expected, Boolean lhs, Boolean rhs, boolean leftFirst) {
        KeyValueColumnExpression lhsExpr = kvExpr("LHS");
        KeyValueColumnExpression rhsExpr = kvExpr("RHS");
        OrExpression or = createOr(lhsExpr, rhsExpr);
        MultiKeyValueTuple tuple = new MultiKeyValueTuple(Collections.<Cell>emptyList());
        ImmutableBytesWritable out = new ImmutableBytesWritable();

        // with no data available, should fail
        boolean success = or.evaluate(tuple, out);
        assertFalse(success);

        // with 1 datum available, should fail
        if (leftFirst) {
            tuple.setKeyValues(Collections.singletonList(createCell("LHS", lhs)));
        } else {
            tuple.setKeyValues(Collections.singletonList(createCell("RHS", rhs)));
        }
        success = or.evaluate(tuple, out);
        assertFalse(success);

        // with 2 data available, should succeed
        tuple.setKeyValues(Arrays.asList(createCell("LHS", lhs), createCell("RHS", rhs)));
        success = or.evaluate(tuple, out);
        assertTrue(success);
        assertEquals(expected, PBoolean.INSTANCE.toObject(out));
    }

    private void testPartialEvaluation(Boolean expected, Boolean x, Boolean y, boolean xFirst) {
        testPartialOneSideFirst(expected, x, y, xFirst);
        testPartialOneSideFirst(expected, y, x, !xFirst);
    }

    private void testShortCircuitOneSideFirst(Boolean expected, Boolean lhs, Boolean rhs, boolean leftFirst) {
        KeyValueColumnExpression lhsExpr = kvExpr("LHS");
        KeyValueColumnExpression rhsExpr = kvExpr("RHS");
        OrExpression or = createOr(lhsExpr, rhsExpr);
        MultiKeyValueTuple tuple = new MultiKeyValueTuple(Collections.<Cell>emptyList());
        ImmutableBytesWritable out = new ImmutableBytesWritable();

        // with no data available, should fail
        boolean success = or.evaluate(tuple, out);
        assertFalse(success);

        // with 1 datum available, should succeed
        if (leftFirst) {
            tuple.setKeyValues(Collections.singletonList(createCell("LHS", lhs)));
        } else {
            tuple.setKeyValues(Collections.singletonList(createCell("RHS", rhs)));
        }
        success = or.evaluate(tuple, out);
        assertTrue(success);
        assertEquals(expected, PBoolean.INSTANCE.toObject(out));
    }


    private void testShortCircuit(Boolean expected, Boolean x, Boolean y, boolean xFirst) {
        testShortCircuitOneSideFirst(expected, x, y, xFirst);
        testShortCircuitOneSideFirst(expected, y, x, !xFirst);
    }

    @Test
    public void testImmediateCertainty() {
        testImmediate(true, true, true);
        testImmediate(true, false, true);
        testImmediate(false, false, false);
    }

    @Test
    public void testImmediateUncertainty() {
        testImmediate(true, true, null);
        testImmediate(null, false, null);
        testImmediate(null, null, null);
    }

    @Test
    public void testPartialCertainty() {
        // must evaluate both sides if FALSE is evaluated first

        // F OR F = F
        testPartialEvaluation(false, false, false, true);
        testPartialEvaluation(false, false, false, false);

        // T OR F = T
        testPartialEvaluation(true, false, true, true);
        testPartialEvaluation(true, true, false, false);
    }

    @Test
    public void testPartialUncertainty() {
        // T OR NULL = NULL
        testPartialEvaluation(true, null, true, true);
        testPartialEvaluation(true, true, null, false);

        // must evaluate both sides if NULL is evaluated first

        // F OR NULL = NULL
        testPartialEvaluation(null, null, false, true);
        testPartialEvaluation(null, false, null, false);

        // NULL OR NULL = NULL
        testPartialEvaluation(null, null, null, true);
        testPartialEvaluation(null, null, null, false);
    }

    @Test
    public void testShortCircuitCertainty() {
        // need only to evaluate one side if TRUE is evaluated first

        // T OR T = T
        testShortCircuit(true, true, true, true);
        testShortCircuit(true, true, true, false);


        // T OR F = F
        testShortCircuit(true, true, false, true);
        testShortCircuit(true, false, true, false);
    }

    @Test
    public void testShortCircuitUncertainty() {
        // need only to evaluate one side if TRUE is evaluated first
        testShortCircuit(true, true, null, true);
        testShortCircuit(true, null, true, false);
    }

    @Test
    public void testTruthTable() {
        // See: https://en.wikipedia.org/wiki/Null_(SQL)#Comparisons_with_NULL_and_the_three-valued_logic_(3VL)
        Boolean[][] testCases = new Boolean[][] {
            //              should short circuit?
            //    X,     Y, if X first, if Y first, X OR Y,
            {  true,  true,       true,       true,   true, },
            {  true, false,       true,      false,   true, },
            { false, false,      false,      false,  false, },
            {  true,  null,       true,      false,   true, },
            { false,  null,      false,      false,   null, },
            {  null,  null,      false,      false,   null, },
        };

        for (Boolean[] testCase : testCases) {
            Boolean x = testCase[0];
            Boolean y = testCase[1];
            boolean shouldShortCircuitWhenXEvaluatedFirst = testCase[2];
            boolean shouldShortCircuitWhenYEvaluatedFirst = testCase[3];
            Boolean expected = testCase[4];

            // test both directions
            testImmediate(expected, x, y);

            if (shouldShortCircuitWhenXEvaluatedFirst) {
                testShortCircuit(expected, x, y, true);
            } else {
                testPartialEvaluation(expected, x, y, true);
            }

            if (shouldShortCircuitWhenYEvaluatedFirst) {
                testShortCircuit(expected, x, y, false);
            } else {
                testPartialEvaluation(expected, x, y, false);
            }
        }
    }
}
