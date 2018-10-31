package org.apache.phoenix.expression.rewrite;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.*;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.util.ExpressionUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RowValueConstructorExpressionRewriter {

    static RowValueConstructorExpressionRewriter singleton = null;

    public static RowValueConstructorExpressionRewriter getSingleton() {
        if (singleton == null) {
            singleton = new RowValueConstructorExpressionRewriter();
        }
        return singleton;
    }

    public RowValueConstructorExpression rewriteAllChildrenAsc(
            RowValueConstructorExpression rvcExpression) throws SQLException {
        List<Expression> replacementChildren = new ArrayList<>(rvcExpression.getChildren().size());
        for (int i = 0; i < rvcExpression.getChildren().size(); i++) {
            Expression child = rvcExpression.getChildren().get(i);
            if (child.getSortOrder() == SortOrder.DESC) {
                //As The KeySlot visitor has not been setup for InvertFunction need to Use Coerce
                child = CoerceExpression.create(child, child.getDataType(), SortOrder.ASC, null);
            }
            replacementChildren.add(child);
        }
        return rvcExpression.clone(replacementChildren);
    }
}
