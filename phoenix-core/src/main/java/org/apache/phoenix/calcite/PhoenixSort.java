package org.apache.phoenix.calcite;

import java.sql.SQLException;
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
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.BaseQueryPlan;
import org.apache.phoenix.execute.ClientScanPlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;

import com.google.common.collect.Lists;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Sort}
 * relational expression in Phoenix.
 *
 * <p>Like {@code Sort}, it also supports LIMIT and OFFSET.
 */
public class PhoenixSort extends Sort implements PhoenixRel {
    private static final double CLIENT_MERGE_FACTOR = 0.5;
    
    public PhoenixSort(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traits, child, collation, offset, fetch);
    }

    @Override
    public PhoenixSort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new PhoenixSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }
    
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        RelOptCost cost = super.computeSelfCost(planner);
        if (isServerSortDoable()) {
            cost = cost.multiplyBy(SERVER_FACTOR);
        } else if (isClientSortMergable()) {
            cost = cost.multiplyBy(CLIENT_MERGE_FACTOR);
        }
        return cost.multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        assert getConvention() == getInput().getConvention();
        if (this.fetch != null || this.offset != null)
            throw new UnsupportedOperationException();
            
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        TableRef tableRef = implementor.getTableRef();
        BaseQueryPlan basePlan = null;
        if (plan instanceof BaseQueryPlan) {
            basePlan = (BaseQueryPlan) plan;
        } else if (plan instanceof HashJoinPlan) {
            QueryPlan delegate = ((HashJoinPlan) plan).getDelegate();
            if (delegate instanceof BaseQueryPlan) {
                basePlan = (BaseQueryPlan) delegate;
            }
        }
        // We can not merge with the base plan that has a limit already.
        // But if there is order-by without a limit, we can simply ignore the order-by.
        if (plan.getLimit() != null) {
            basePlan = null;
        }
        PhoenixStatement stmt = plan.getContext().getStatement();
        StatementContext context;
        try {
            context = basePlan == null ? new StatementContext(stmt, FromCompiler.getResolver(tableRef), new Scan(), new SequenceManager(stmt)) : basePlan.getContext();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        List<OrderByExpression> orderByExpressions = Lists.newArrayList();
        for (RelFieldCollation fieldCollation : getCollation().getFieldCollations()) {
            Expression expr = implementor.newColumnExpression(fieldCollation.getFieldIndex());
            boolean isAscending = fieldCollation.getDirection() == Direction.ASCENDING;
            if (expr.getSortOrder() == SortOrder.DESC) {
                isAscending = !isAscending;
            }
            orderByExpressions.add(new OrderByExpression(expr, fieldCollation.nullDirection == NullDirection.LAST, isAscending));
        }
        OrderBy orderBy = new OrderBy(orderByExpressions);
        
        SelectStatement select = SelectStatement.SELECT_STAR;
        if (basePlan == null) {
            return new ClientScanPlan(context, select, tableRef, RowProjector.EMPTY_PROJECTOR, null, null, orderBy, plan);
        }
        
        QueryPlan newPlan;
        try {
            if (basePlan instanceof ScanPlan) {
                newPlan = ScanPlan.create((ScanPlan) basePlan, orderBy);
            } else {
                newPlan = AggregatePlan.create((AggregatePlan) basePlan, orderBy);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        if (plan instanceof HashJoinPlan) {        
            HashJoinPlan hashJoinPlan = (HashJoinPlan) plan;
            newPlan = HashJoinPlan.create(select, newPlan, hashJoinPlan.getJoinInfo(), hashJoinPlan.getSubPlans());
        }
        return newPlan;
    }
    
    private boolean isServerSortDoable() {
        RelNode rel = CalciteUtils.getBestRel(getInput());
        return rel instanceof PhoenixRel 
                && ((PhoenixRel) rel).getPlanType() != PlanType.CLIENT_SERVER;
    }
    
    private boolean isClientSortMergable() {
        RelNode rel = CalciteUtils.getBestRel(getInput());        
        return rel instanceof PhoenixAggregate;        
    }

    @Override
    public PlanType getPlanType() {
        return PlanType.CLIENT_SERVER;
    }
}
