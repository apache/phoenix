package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Util;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.schema.SortOrder;

import com.google.common.collect.Lists;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Sort}
 * relational expression in Phoenix.
 *
 * <p>Like {@code Sort}, it also supports LIMIT and OFFSET.
 */
abstract public class PhoenixAbstractSort extends Sort implements PhoenixQueryRel {
    
    protected PhoenixAbstractSort(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation) {
        super(cluster, traits, child, collation, null, null);
        assert !getCollation().getFieldCollations().isEmpty();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
            RelMetadataQuery mq) {
        double rowCount = mq.getRowCount(this);
        double bytesPerRow = mq.getAverageRowSize(this);
        return planner.getCostFactory().makeCost(
                Util.nLogN(rowCount) * bytesPerRow, rowCount, 0);
    }

    protected static OrderBy getOrderBy(RelCollation collation, PhoenixRelImplementor implementor, TupleProjector tupleProjector) {
        List<OrderByExpression> orderByExpressions = Lists.newArrayList();
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
            Expression expr = tupleProjector == null ? 
                      implementor.newColumnExpression(fieldCollation.getFieldIndex()) 
                    : tupleProjector.getExpressions()[fieldCollation.getFieldIndex()];
            boolean isAscending = fieldCollation.getDirection() == Direction.ASCENDING;
            if (expr.getSortOrder() == SortOrder.DESC) {
                isAscending = !isAscending;
            }
            orderByExpressions.add(new OrderByExpression(expr, fieldCollation.nullDirection == NullDirection.LAST, isAscending));
        }
        
        return new OrderBy(orderByExpressions);
    }
}
