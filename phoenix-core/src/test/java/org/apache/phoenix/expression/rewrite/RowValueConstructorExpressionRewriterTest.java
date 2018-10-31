package org.apache.phoenix.expression.rewrite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
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
        RowValueConstructorExpression expression = new RowValueConstructorExpression(children,false);


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
