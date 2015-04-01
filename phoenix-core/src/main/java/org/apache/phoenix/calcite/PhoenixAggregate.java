package org.apache.phoenix.calcite;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ExpressionProjector;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.coprocessor.BaseScannerRegionObserver;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.ClientAggregatePlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.expression.CoerceExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.expression.aggregator.ClientAggregators;
import org.apache.phoenix.expression.aggregator.ServerAggregators;
import org.apache.phoenix.expression.function.AggregateFunction;
import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PVarchar;

import com.google.common.collect.Lists;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Aggregate}
 * relational expression in Phoenix.
 */
public class PhoenixAggregate extends Aggregate implements PhoenixRel {
    public PhoenixAggregate(RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) throws InvalidRelException {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
        assert getConvention() == PhoenixRel.CONVENTION;

        for (AggregateCall aggCall : aggCalls) {
            if (aggCall.isDistinct()) {
                throw new InvalidRelException( "distinct aggregation not supported");
            }
        }
        switch (getGroupType()) {
            case SIMPLE:
                break;
            default:
                throw new InvalidRelException("unsupported group type: " + getGroupType());
        }
    }
    
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public PhoenixAggregate copy(RelTraitSet traits, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls) {
        try {
            return new PhoenixAggregate(getCluster(), traits, input, indicator, groupSet, groupSets, aggregateCalls);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
        }
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        assert getConvention() == getInput().getConvention();
        if (groupSets.size() > 1) {
            throw new UnsupportedOperationException();
        }
        
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        TableRef tableRef = implementor.getTableRef();
        ScanPlan basePlan = null;
        if (plan instanceof ScanPlan) {
            basePlan = (ScanPlan) plan;
        } else if (plan instanceof HashJoinPlan) {
            QueryPlan delegate = ((HashJoinPlan) plan).getDelegate();
            if (delegate instanceof ScanPlan) {
                basePlan = (ScanPlan) delegate;
            }
        }
        // TopN, we can not merge with the base plan.
        if (!plan.getOrderBy().getOrderByExpressions().isEmpty() && plan.getLimit() != null) {
            basePlan = null;
        }
        PhoenixStatement stmt = plan.getContext().getStatement();
        StatementContext context;
        try {
            context = basePlan == null ? new StatementContext(stmt, FromCompiler.getResolver(tableRef), new Scan(), new SequenceManager(stmt)) : basePlan.getContext();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }        
        
        List<Integer> ordinals = groupSet.asList();
        // TODO check order-preserving
        String groupExprAttribName = BaseScannerRegionObserver.UNORDERED_GROUP_BY_EXPRESSIONS;
        // TODO sort group by keys. not sure if there is a way to avoid this sorting,
        //      otherwise we would have add an extra projection.
        List<Expression> exprs = Lists.newArrayListWithExpectedSize(ordinals.size());
        List<Expression> keyExprs = exprs;
        for (int i = 0; i < ordinals.size(); i++) {
            Expression expr = implementor.newColumnExpression(ordinals.get(i));
            exprs.add(expr);
            PDataType keyType = getKeyType(expr);
            if (keyType == expr.getDataType()) {
                continue;
            }
            if (keyExprs == exprs) {
                keyExprs = Lists.newArrayList(exprs);
            }
            try {
                keyExprs.set(i, CoerceExpression.create(expr, keyType));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        GroupBy groupBy = new GroupBy.GroupByBuilder().setScanAttribName(groupExprAttribName).setExpressions(exprs).setKeyExpressions(keyExprs).build();
        
        // TODO sort aggFuncs. same problem with group by key sorting.
        List<SingleAggregateFunction> aggFuncs = Lists.newArrayList();
        for (AggregateCall call : aggCalls) {
            AggregateFunction aggFunc = CalciteUtils.toAggregateFunction(call.getAggregation(), call.getArgList(), implementor);
            if (!(aggFunc instanceof SingleAggregateFunction)) {
                throw new UnsupportedOperationException();
            }
            aggFuncs.add((SingleAggregateFunction) aggFunc);
        }
        int minNullableIndex = getMinNullableIndex(aggFuncs,groupBy.isEmpty());
        context.getScan().setAttribute(BaseScannerRegionObserver.AGGREGATORS, ServerAggregators.serialize(aggFuncs, minNullableIndex));
        ClientAggregators clientAggregators = new ClientAggregators(aggFuncs, minNullableIndex);
        context.getAggregationManager().setAggregators(clientAggregators);
        
        SelectStatement select = SelectStatement.SELECT_STAR;
        RowProjector rowProjector = createRowProjector(keyExprs, aggFuncs);
        if (basePlan == null) {
            return new ClientAggregatePlan(context, select, tableRef, rowProjector, null, null, OrderBy.EMPTY_ORDER_BY, groupBy, null, plan);
        }
        
        QueryPlan aggPlan = new AggregatePlan(context, select, basePlan.getTableRef(), rowProjector, null, OrderBy.EMPTY_ORDER_BY, null, groupBy, null);
        if (plan instanceof ScanPlan)
            return aggPlan;
        
        HashJoinPlan hashJoinPlan = (HashJoinPlan) plan;
        return HashJoinPlan.create(select, aggPlan, hashJoinPlan.getJoinInfo(), hashJoinPlan.getSubPlans());
    }
    
    private static RowProjector createRowProjector(List<Expression> keyExprs, List<SingleAggregateFunction> aggFuncs) {
        List<ColumnProjector> columnProjectors = Lists.<ColumnProjector>newArrayList();
        for (int i = 0; i < keyExprs.size(); i++) {
            Expression keyExpr = keyExprs.get(i);
            RowKeyValueAccessor accessor = new RowKeyValueAccessor(keyExprs, i);
            Expression expr = new RowKeyColumnExpression(keyExpr, accessor, keyExpr.getDataType());
            columnProjectors.add(new ExpressionProjector(expr.toString(), "", expr, false));
        }
        for (SingleAggregateFunction aggFunc : aggFuncs) {
            columnProjectors.add(new ExpressionProjector(aggFunc.toString(), "", aggFunc, false));
        }
        return new RowProjector(columnProjectors, 0, false);                
    }
    
    private static PDataType getKeyType(Expression expression) {
        PDataType type = expression.getDataType();
        if (!expression.isNullable() || !type.isFixedWidth()) {
            return type;
        }
        if (type.isCastableTo(PDecimal.INSTANCE)) {
            return PDecimal.INSTANCE;
        }
        if (type.isCastableTo(PVarchar.INSTANCE)) {
            return PVarchar.INSTANCE;
        }
        // This might happen if someone tries to group by an array
        throw new IllegalStateException("Multiple occurrences of type " + type + " may not occur in a GROUP BY clause");
    }
    
    private static int getMinNullableIndex(List<SingleAggregateFunction> aggFuncs, boolean isUngroupedAggregation) {
        int minNullableIndex = aggFuncs.size();
        for (int i = 0; i < aggFuncs.size(); i++) {
            SingleAggregateFunction aggFunc = aggFuncs.get(i);
            if (isUngroupedAggregation ? aggFunc.getAggregator().isNullable() : aggFunc.getAggregatorExpression().isNullable()) {
                minNullableIndex = i;
                break;
            }
        }
        return minNullableIndex;
    }
    
}
