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
package org.apache.phoenix.compile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.expression.AndExpression;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.ComparisonExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.IsNullExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.RowValueConstructorParseNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDecimal;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

public class RVCOffsetCompilerTest {

    private static TableName TABLE_NAME = TableName.create(null,"TABLE1");


    RVCOffsetCompiler offsetCompiler;

    @Before
    public void init(){
        offsetCompiler = RVCOffsetCompiler.getInstance();
    }

    @Test
    public void buildListOfColumnParseNodesTest() throws Exception {
        List<ParseNode> children = new ArrayList<>();
        ColumnParseNode col1 = new ColumnParseNode(TABLE_NAME,"col1");
        ColumnParseNode col2 = new ColumnParseNode(TABLE_NAME,"col2");

        children.add(col1);
        children.add(col2);
        RowValueConstructorParseNode rvc = new RowValueConstructorParseNode(children);

        List<ColumnParseNode>
                result =
                offsetCompiler.buildListOfColumnParseNodes(rvc, true);

        assertEquals(2,result.size());
        assertEquals(col1,result.get(0));
        assertEquals(col2,result.get(1));
    }

    @Test
    public void buildListOfColumnParseNodesTestIndex() throws Exception {
        List<ParseNode> children = new ArrayList<>();
        ColumnParseNode col1 = new ColumnParseNode(TABLE_NAME,"col1");
        ColumnParseNode col2 = new ColumnParseNode(TABLE_NAME,"col2");

        ParseNodeFactory factory = new ParseNodeFactory();

        children.add(factory.cast(col1, PDecimal.INSTANCE, null, null,false));
        children.add(factory.cast(col2, PDecimal.INSTANCE, null, null,false));

        RowValueConstructorParseNode rvc = new RowValueConstructorParseNode(children);

        List<ColumnParseNode>
                result =
                offsetCompiler.buildListOfColumnParseNodes(rvc, true);

        assertEquals(2,result.size());
        assertEquals(col1,result.get(0));
        assertEquals(col2,result.get(1));
    }


    @Test
    public void buildListOfRowKeyColumnExpressionsTest() throws Exception {
        List<Expression> expressions = new ArrayList<>();

        RowKeyColumnExpression rvc1 = new RowKeyColumnExpression();
        RowKeyColumnExpression rvc2 = new RowKeyColumnExpression();

        ComparisonExpression expression1 = mock(ComparisonExpression.class);
        ComparisonExpression expression2 = mock(ComparisonExpression.class);

        Mockito.when(expression1.getChildren()).thenReturn(Lists.<Expression>newArrayList(rvc1));
        Mockito.when(expression2.getChildren()).thenReturn(Lists.<Expression>newArrayList(rvc2));

        expressions.add(expression1);
        expressions.add(expression2);

        AndExpression expression = mock(AndExpression.class);
        Mockito.when(expression.getChildren()).thenReturn(expressions);

        RVCOffsetCompiler.RowKeyColumnExpressionOutput
                output = offsetCompiler.buildListOfRowKeyColumnExpressions(expression, false);
        List<RowKeyColumnExpression>
                result = output.getRowKeyColumnExpressions();

        assertEquals(2,result.size());
        assertEquals(rvc1,result.get(0));

        assertEquals(rvc2,result.get(1));
    }

    @Test
    public void buildListOfRowKeyColumnExpressionsIndexTest() throws Exception {
        List<Expression> expressions = new ArrayList<>();

        PColumn
                column = new PColumnImpl(PName.EMPTY_COLUMN_NAME, PName.EMPTY_NAME, PDecimal.INSTANCE, 10, 1,
                true, 1, SortOrder.getDefault(), 0, null, false, null, false, false, null, HConstants.LATEST_TIMESTAMP);


        RowKeyColumnExpression rvc1 = new RowKeyColumnExpression(column,null);
        RowKeyColumnExpression rvc2 = new RowKeyColumnExpression(column, null);

        Expression coerce1 = CoerceExpression.create(rvc1,PDecimal.INSTANCE);
        Expression coerce2 = CoerceExpression.create(rvc2,PDecimal.INSTANCE);

        ComparisonExpression expression1 = mock(ComparisonExpression.class);
        ComparisonExpression expression2 = mock(ComparisonExpression.class);

        Mockito.when(expression1.getChildren()).thenReturn(Lists.newArrayList(coerce1));
        Mockito.when(expression2.getChildren()).thenReturn(Lists.newArrayList(coerce2));

        expressions.add(expression1);
        expressions.add(expression2);

        AndExpression expression = mock(AndExpression.class);
        Mockito.when(expression.getChildren()).thenReturn(expressions);

        RVCOffsetCompiler.RowKeyColumnExpressionOutput
                output = offsetCompiler.buildListOfRowKeyColumnExpressions(expression, true);
        List<RowKeyColumnExpression>
                result = output.getRowKeyColumnExpressions();

        assertEquals(2,result.size());
        assertEquals(rvc1,result.get(0));
        assertEquals(rvc2,result.get(1));
    }

    @Test
    public void buildListOfRowKeyColumnExpressionsSingleNodeComparisonTest() throws Exception {
        List<Expression> expressions = new ArrayList<>();

        RowKeyColumnExpression rvc = new RowKeyColumnExpression();

        ComparisonExpression expression = mock(ComparisonExpression.class);

        Mockito.when(expression.getChildren()).thenReturn(Lists.<Expression>newArrayList(rvc));

        RVCOffsetCompiler.RowKeyColumnExpressionOutput
                output = offsetCompiler.buildListOfRowKeyColumnExpressions(expression, false);
        List<RowKeyColumnExpression>
                result = output.getRowKeyColumnExpressions();

        assertEquals(1,result.size());
        assertEquals(rvc,result.get(0));
    }

    @Test
    public void buildListOfRowKeyColumnExpressionsSingleNodeIsNullTest() throws Exception {
        List<Expression> expressions = new ArrayList<>();

        RowKeyColumnExpression rvc = new RowKeyColumnExpression();

        IsNullExpression expression = mock(IsNullExpression.class);

        Mockito.when(expression.getChildren()).thenReturn(Lists.<Expression>newArrayList(rvc));

        RVCOffsetCompiler.RowKeyColumnExpressionOutput output = offsetCompiler.buildListOfRowKeyColumnExpressions(expression, false);

        List<RowKeyColumnExpression> result = output.getRowKeyColumnExpressions();

        assertEquals(1,result.size());
        assertEquals(rvc,result.get(0));

        assertTrue(output.isTrailingNull());
    }

    @Test
    public void buildListOfRowKeyColumnExpressionsIsNullTest() throws Exception {
        List<Expression> expressions = new ArrayList<>();

        RowKeyColumnExpression rvc1 = new RowKeyColumnExpression();
        RowKeyColumnExpression rvc2 = new RowKeyColumnExpression();

        IsNullExpression expression1 = mock(IsNullExpression.class);
        IsNullExpression expression2 = mock(IsNullExpression.class);

        Mockito.when(expression1.getChildren()).thenReturn(Lists.<Expression>newArrayList(rvc1));
        Mockito.when(expression2.getChildren()).thenReturn(Lists.<Expression>newArrayList(rvc2));

        expressions.add(expression1);
        expressions.add(expression2);

        AndExpression expression = mock(AndExpression.class);
        Mockito.when(expression.getChildren()).thenReturn(expressions);

        RVCOffsetCompiler.RowKeyColumnExpressionOutput output = offsetCompiler.buildListOfRowKeyColumnExpressions(expression, false);

        List<RowKeyColumnExpression> result = output.getRowKeyColumnExpressions();

        assertEquals(2,result.size());
        assertEquals(rvc1,result.get(0));
        assertEquals(rvc2,result.get(1));

        assertTrue(output.isTrailingNull());
    }
}
