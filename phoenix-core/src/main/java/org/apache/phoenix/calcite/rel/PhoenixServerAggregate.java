package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.expression.Expression;

public class PhoenixServerAggregate extends PhoenixAbstractAggregate {
    
    public static PhoenixServerAggregate create(RelNode input, boolean indicator, 
            ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, 
            List<AggregateCall> aggCalls) {
        RelOptCluster cluster = input.getCluster();
        RelTraitSet traits = cluster.traitSetOf(PhoenixConvention.SERVERAGG);
        return new PhoenixServerAggregate(cluster, traits, input, indicator, 
                groupSet, groupSets, aggCalls);
    }

    private PhoenixServerAggregate(RelOptCluster cluster, RelTraitSet traits,
            RelNode child, boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
    }

    @Override
    public PhoenixServerAggregate copy(RelTraitSet traits, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls) {
        return create(input, indicator, groupSet, groupSets, aggregateCalls);
    }
    
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getInput().getConvention().satisfies(PhoenixConvention.SERVER)
                && !getInput().getConvention().satisfies(PhoenixConvention.SERVERJOIN))
            return planner.getCostFactory().makeInfiniteCost();
        
        return super.computeSelfCost(planner, mq)
                .multiplyBy(SERVER_FACTOR)
                .multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(PhoenixRelImplementor implementor) {
        implementor.pushContext(implementor.getCurrentContext().withColumnRefList(getColumnRefList()));
        QueryPlan plan = implementor.visitInput(0, (PhoenixQueryRel) getInput());
        implementor.popContext();
        
        assert (plan instanceof ScanPlan 
                    || plan instanceof HashJoinPlan)
                && plan.getLimit() == null;
        
        ScanPlan basePlan;
        HashJoinPlan hashJoinPlan = null;
        if (plan instanceof ScanPlan) {
            basePlan = (ScanPlan) plan;
        } else {
            hashJoinPlan = (HashJoinPlan) plan;
            QueryPlan delegate = hashJoinPlan.getDelegate();
            assert delegate instanceof ScanPlan;
            basePlan = (ScanPlan) delegate;
        }
        
        StatementContext context = basePlan.getContext();        
        GroupBy groupBy = super.getGroupBy(implementor);       
        List<Expression> funcs = super.serializeAggregators(implementor, context, groupBy.isEmpty());
        
        QueryPlan aggPlan = new AggregatePlan(context, basePlan.getStatement(), basePlan.getTableRef(), basePlan.getSourceRefs().iterator().next(), RowProjector.EMPTY_PROJECTOR, null, null, OrderBy.EMPTY_ORDER_BY, null, groupBy, null, basePlan.getDynamicFilter());
        if (hashJoinPlan != null) {        
            aggPlan = HashJoinPlan.create(hashJoinPlan.getStatement(), aggPlan, hashJoinPlan.getJoinInfo(), hashJoinPlan.getSubPlans());
        }
        
        return PhoenixAbstractAggregate.wrapWithProject(implementor, aggPlan, groupBy.getKeyExpressions(), funcs);
    }

}
