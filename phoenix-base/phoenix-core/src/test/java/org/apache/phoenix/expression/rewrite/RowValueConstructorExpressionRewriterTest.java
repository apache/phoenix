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

package org.apache.phoenix.expression.rewrite;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.RowValueConstructorExpression;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PFloat;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RowValueConstructorExpressionRewriterTest {
    @Test
    public void testRewriteAllChildrenAsc() throws SQLException {


        Expression ascChild = Mockito.mock(Expression.class);
        Mockito.when(ascChild.getSortOrder()).thenReturn(SortOrder.ASC);
        Mockito.when(ascChild.getDataType()).thenReturn(PFloat.INSTANCE);
        Mockito.when(ascChild.getDeterminism()).thenReturn(Determinism.ALWAYS);
        Mockito.when(ascChild.requiresFinalEvaluation()).thenReturn(true);

        Expression descChild = Mockito.mock(Expression.class);
        Mockito.when(descChild.getSortOrder()).thenReturn(SortOrder.DESC);
        Mockito.when(descChild.getDataType()).thenReturn(PFloat.INSTANCE);
        Mockito.when(descChild.getDeterminism()).thenReturn(Determinism.ALWAYS);
        Mockito.when(descChild.requiresFinalEvaluation()).thenReturn(true);

        List<Expression> children = ImmutableList.of(ascChild,descChild);
        RowValueConstructorExpression expression =
                new RowValueConstructorExpression(children,false);


        RowValueConstructorExpressionRewriter
                rewriter =
                RowValueConstructorExpressionRewriter.getSingleton();

        RowValueConstructorExpression result = rewriter.rewriteAllChildrenAsc(expression);

        assertEquals(2,result.getChildren().size());

        Expression child1 = result.getChildren().get(0);
        Expression child2 = result.getChildren().get(1);

        assertEquals(SortOrder.ASC, child1.getSortOrder());
        assertEquals(SortOrder.ASC, child2.getSortOrder());

        assertEquals(ascChild, child1);
        assertTrue(child2 instanceof CoerceExpression);
        assertEquals(descChild, ((CoerceExpression)child2).getChild());

    }
}
