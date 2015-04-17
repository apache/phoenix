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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.apache.phoenix.calcite.CalciteUtils;
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
abstract public class PhoenixAbstractSort extends Sort implements PhoenixRel {
    protected static final double CLIENT_MERGE_FACTOR = 0.5;
    
    private final Integer statelessFetch;
    
    protected PhoenixAbstractSort(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traits, child, collation, offset, fetch);
        Object value = fetch == null ? null : CalciteUtils.evaluateStatelessExpression(fetch);
        this.statelessFetch = value == null ? null : ((Number) value).intValue();        
        assert getConvention() == PhoenixRel.CONVENTION;
        assert !getCollation().getFieldCollations().isEmpty();
    }

    @Override 
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        // Fix rowCount for super class's computeSelfCost() with input's row count.
        double rowCount = RelMetadataQuery.getRowCount(getInput());
        double bytesPerRow = getRowType().getFieldCount() * 4;
        return planner.getCostFactory().makeCost(
                Util.nLogN(rowCount) * bytesPerRow, rowCount, 0);
    }

    @Override 
    public double getRows() {
        double rows = super.getRows();        
        if (this.statelessFetch == null)
            return rows;

        return Math.min(this.statelessFetch, rows);
    }
    
    protected OrderBy getOrderBy(Implementor implementor, TupleProjector tupleProjector) {
        List<OrderByExpression> orderByExpressions = Lists.newArrayList();
        for (RelFieldCollation fieldCollation : getCollation().getFieldCollations()) {
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
    
    protected Integer getLimit(Implementor implementor) {
        if (this.fetch == null)
            return null;
        
        if (this.statelessFetch == null)
            throw new UnsupportedOperationException("Stateful limit expression not supported");

        return this.statelessFetch;
    }
}
