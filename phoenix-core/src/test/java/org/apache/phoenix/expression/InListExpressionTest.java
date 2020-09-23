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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


import org.apache.phoenix.schema.types.PInteger;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;


public class InListExpressionTest {

    @Test
    public void testHashCode() throws Exception {
        int valuesNumber = 500000;
        List<ImmutableBytesPtr> values = new ArrayList<>(valuesNumber);
        for (int i = 0; i < valuesNumber; i++) {
            values.add(new ImmutableBytesPtr(Bytes.toBytes(i)));
        }
        InListExpression exp = new InListExpression(values);

        // first time
        long startTs = System.currentTimeMillis();
        int firstHashCode = exp.hashCode();
        long firstTimeCost = System.currentTimeMillis() - startTs;

        // the rest access
        int restAccessNumber = 3;
        startTs = System.currentTimeMillis();
        List<Integer> hashCodes = Lists.newArrayListWithExpectedSize(restAccessNumber);
        for (int i = 0; i < restAccessNumber; i++) {
            hashCodes.add(exp.hashCode());
        }

        // check time cost
        long restTimeCost = System.currentTimeMillis() - startTs;
        assertTrue("first time: " + firstTimeCost + " <= rest time: " + restTimeCost,
                firstTimeCost > restTimeCost);

        // check hash code
        for (int hashCode : hashCodes) {
            assertEquals("hash code not equal, firstHashCode: " + firstHashCode + ", restHashCode: "
                    + hashCode, firstHashCode, hashCode);
        }
    }

    @Test
    public void testGetSortedInListColumnKeyValuePairWithNoPkOrder() {
        testGetSortedInListColumnKeyValuePair(false);
    }

    @Test
    public void testGetSortedInListColumnKeyValuePairWithPkOrder() {
        testGetSortedInListColumnKeyValuePair(true);
    }

    private void testGetSortedInListColumnKeyValuePair(boolean isPkOrder) {
        // mock literal
        List<Expression> expressionList = new ArrayList<>();
        LiteralExpression literalChild1 = Mockito.mock(LiteralExpression.class);
        List<Expression> literalExpressions = new ArrayList<>();
        when(literalChild1.getDataType()).thenReturn(PInteger.INSTANCE);
        when(literalChild1.getBytes()).thenReturn(null);
        when(literalChild1.getDeterminism()).thenReturn(Determinism.ALWAYS);
        literalExpressions.add(literalChild1);
        literalExpressions.add(literalChild1);

        // mock row key column
        List<Expression> expressionChildren = new ArrayList<>();
        RowKeyColumnExpression rowKeyColumnExpressionMock1 = Mockito.mock(RowKeyColumnExpression.class);
        RowKeyColumnExpression rowKeyColumnExpressionMock2 = Mockito.mock(RowKeyColumnExpression.class);

        when(rowKeyColumnExpressionMock1.getPosition()).thenReturn(1);
        when(rowKeyColumnExpressionMock1.getDeterminism()).thenReturn(Determinism.ALWAYS);
        when(rowKeyColumnExpressionMock2.getPosition()).thenReturn(2);
        when(rowKeyColumnExpressionMock2.getDeterminism()).thenReturn(Determinism.ALWAYS);
        when(rowKeyColumnExpressionMock1.getChildren()).thenReturn(expressionChildren);
        when(rowKeyColumnExpressionMock2.getChildren()).thenReturn(literalExpressions);

        // mock row key column PK order position
        if (isPkOrder) {
            expressionChildren.add(rowKeyColumnExpressionMock1);
            expressionChildren.add(rowKeyColumnExpressionMock2);

        } else {
            expressionChildren.add(rowKeyColumnExpressionMock2);
            expressionChildren.add(rowKeyColumnExpressionMock1);

        }

        RowValueConstructorExpression rvc1 = new RowValueConstructorExpression(expressionChildren, true);
        RowValueConstructorExpression rvc2 = new RowValueConstructorExpression(literalExpressions, true);
        expressionList.add(rvc1);
        expressionList.add(rvc2);

        if (isPkOrder) {
            assertEquals(1, ((RowKeyColumnExpression)expressionList.get(0).getChildren().get(0)).getPosition());
            assertEquals(2, ((RowKeyColumnExpression)expressionList.get(0).getChildren().get(1)).getPosition());
        } else {
            assertEquals(2, ((RowKeyColumnExpression)expressionList.get(0).getChildren().get(0)).getPosition());
            assertEquals(1, ((RowKeyColumnExpression)expressionList.get(0).getChildren().get(1)).getPosition());
        }

        List<InListExpression.InListColumnKeyValuePair> inListColumnKeyValuePairList =
                InListExpression.getSortedInListColumnKeyValuePair(expressionList);

        assertEquals(1, inListColumnKeyValuePairList.get(0).getRowKeyColumnExpression().getPosition());
        assertEquals(2, inListColumnKeyValuePairList.get(1).getRowKeyColumnExpression().getPosition());
    }

    @Test
    public void testGetSortedInListColumnKeyValuePairWithLessValueThanPkColumns() {
        List<Expression> expressionList = new ArrayList<>();
        LiteralExpression literalChild1 = Mockito.mock(LiteralExpression.class);
        List<Expression> literalExpressions = new ArrayList<>();
        when(literalChild1.getDataType()).thenReturn(PInteger.INSTANCE);
        when(literalChild1.getBytes()).thenReturn(null);
        when(literalChild1.getDeterminism()).thenReturn(Determinism.ALWAYS);
        literalExpressions.add(literalChild1);
        literalExpressions.add(literalChild1);

        // mock row key column
        List<Expression> expressionChildren = new ArrayList<>();
        RowKeyColumnExpression rowKeyColumnExpressionMock1 = Mockito.mock(RowKeyColumnExpression.class);

        when(rowKeyColumnExpressionMock1.getPosition()).thenReturn(1);
        when(rowKeyColumnExpressionMock1.getDeterminism()).thenReturn(Determinism.ALWAYS);
        when(rowKeyColumnExpressionMock1.getChildren()).thenReturn(expressionChildren);

        expressionChildren.add(rowKeyColumnExpressionMock1);

        RowValueConstructorExpression rvc1 = new RowValueConstructorExpression(expressionChildren, true);
        RowValueConstructorExpression rvc2 = new RowValueConstructorExpression(literalExpressions, true);
        expressionList.add(rvc1);
        expressionList.add(rvc2);

        List<InListExpression.InListColumnKeyValuePair> inListColumnKeyValuePairList =
                InListExpression.getSortedInListColumnKeyValuePair(expressionList);

        assertEquals(null, inListColumnKeyValuePairList);
    }

    @Test
    public void testGetSortedInListColumnKeyValuePairWithMoreValueThanPkColumn() {
        List<Expression> expressionList = new ArrayList<>();
        LiteralExpression literalChild1 = Mockito.mock(LiteralExpression.class);
        List<Expression> literalExpressions = new ArrayList<>();
        when(literalChild1.getDataType()).thenReturn(PInteger.INSTANCE);
        when(literalChild1.getBytes()).thenReturn(null);
        when(literalChild1.getDeterminism()).thenReturn(Determinism.ALWAYS);
        literalExpressions.add(literalChild1);

        // mock row key column
        List<Expression> expressionChildren = new ArrayList<>();
        RowKeyColumnExpression rowKeyColumnExpressionMock1 = Mockito.mock(RowKeyColumnExpression.class);
        when(rowKeyColumnExpressionMock1.getPosition()).thenReturn(1);
        when(rowKeyColumnExpressionMock1.getDeterminism()).thenReturn(Determinism.ALWAYS);
        when(rowKeyColumnExpressionMock1.getChildren()).thenReturn(expressionChildren);

        expressionChildren.add(rowKeyColumnExpressionMock1);
        expressionChildren.add(rowKeyColumnExpressionMock1);

        RowValueConstructorExpression rvc1 = new RowValueConstructorExpression(expressionChildren, true);
        RowValueConstructorExpression rvc2 = new RowValueConstructorExpression(literalExpressions, true);
        expressionList.add(rvc1);
        expressionList.add(rvc2);

        List<InListExpression.InListColumnKeyValuePair> inListColumnKeyValuePairList =
                InListExpression.getSortedInListColumnKeyValuePair(expressionList);

        assertEquals(null, inListColumnKeyValuePairList);
    }

    @Test
    public void testInListColumnKeyValuePairClass() {
        RowKeyColumnExpression rowKeyColumnExpression = Mockito.mock(RowKeyColumnExpression.class);
        LiteralExpression literalChild = Mockito.mock(LiteralExpression.class);

        InListExpression.InListColumnKeyValuePair inListColumnKeyValuePair =
                new InListExpression.InListColumnKeyValuePair(rowKeyColumnExpression);
        inListColumnKeyValuePair.addToLiteralExpressionList(literalChild);

        assertEquals(rowKeyColumnExpression, inListColumnKeyValuePair.getRowKeyColumnExpression());
        assertEquals(literalChild, inListColumnKeyValuePair.getLiteralExpressionList().get(0));
    }

    @Test
    public void testGetSortedRowValueConstructorExpressionList() {
        byte[] bytesValueOne = ByteBuffer.allocate(4).putInt(1).array();
        byte[] bytesValueTwo = ByteBuffer.allocate(4).putInt(1).array();
        // mock literal
        List<Expression> literalExpressions = new ArrayList<>();
        LiteralExpression literalChild1 = Mockito.mock(LiteralExpression.class);
        when(literalChild1.getDataType()).thenReturn(PInteger.INSTANCE);
        when(literalChild1.getBytes()).thenReturn(bytesValueOne);
        when(literalChild1.getDeterminism()).thenReturn(Determinism.ALWAYS);
        literalExpressions.add(literalChild1);

        LiteralExpression literalChild2 = Mockito.mock(LiteralExpression.class);
        when(literalChild2.getDataType()).thenReturn(PInteger.INSTANCE);
        when(literalChild2.getBytes()).thenReturn(bytesValueTwo);
        when(literalChild2.getDeterminism()).thenReturn(Determinism.ALWAYS);
        literalExpressions.add(literalChild2);

        List<Expression> expressionChildren = new ArrayList<>();
        RowKeyColumnExpression rowKeyColumnExpressionMock1 = Mockito.mock(RowKeyColumnExpression.class);
        RowKeyColumnExpression rowKeyColumnExpressionMock2 = Mockito.mock(RowKeyColumnExpression.class);
        expressionChildren.add(rowKeyColumnExpressionMock1);
        expressionChildren.add(rowKeyColumnExpressionMock2);

        when(rowKeyColumnExpressionMock1.getPosition()).thenReturn(1);
        when(rowKeyColumnExpressionMock1.getDeterminism()).thenReturn(Determinism.ALWAYS);
        when(rowKeyColumnExpressionMock2.getPosition()).thenReturn(2);
        when(rowKeyColumnExpressionMock2.getDeterminism()).thenReturn(Determinism.ALWAYS);
        when(rowKeyColumnExpressionMock1.getChildren()).thenReturn(expressionChildren);
        when(rowKeyColumnExpressionMock2.getChildren()).thenReturn(literalExpressions);

        //construct sorted InListColumnKeyValuePair list
        List<InListExpression.InListColumnKeyValuePair> children = new ArrayList<>();
        InListExpression.InListColumnKeyValuePair rvc1 =
                new InListExpression.InListColumnKeyValuePair(rowKeyColumnExpressionMock1);
        rvc1.addToLiteralExpressionList(literalChild1);
        children.add(rvc1);
        InListExpression.InListColumnKeyValuePair rvc2 =
                new InListExpression.InListColumnKeyValuePair(rowKeyColumnExpressionMock2);
        rvc2.addToLiteralExpressionList(literalChild2);
        children.add(rvc2);

        List<Expression> result = InListExpression.getSortedRowValueConstructorExpressionList(
                children,true, 1);

        assertTrue(result.get(0).getChildren().get(0) instanceof RowKeyColumnExpression);
        assertTrue(result.get(0).getChildren().get(1) instanceof RowKeyColumnExpression);
        assertEquals(1, ((RowKeyColumnExpression)result.get(0).getChildren().get(0)).getPosition());
        assertEquals(2, ((RowKeyColumnExpression)result.get(0).getChildren().get(1)).getPosition());

        assertTrue(result.get(1).getChildren().get(0) instanceof LiteralExpression);
        assertTrue(result.get(1).getChildren().get(1) instanceof LiteralExpression);
        assertEquals(bytesValueOne, ((LiteralExpression)result.get(1).getChildren().get(0)).getBytes());
        assertEquals(bytesValueTwo, ((LiteralExpression)result.get(1).getChildren().get(1)).getBytes());
    }
}
