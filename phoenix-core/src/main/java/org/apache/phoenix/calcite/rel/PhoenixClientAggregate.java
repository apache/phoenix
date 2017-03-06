package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.ClientAggregatePlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.TableRef;

public class PhoenixClientAggregate extends PhoenixAbstractAggregate {
    
    public static PhoenixClientAggregate create(RelNode input, boolean indicator, 
            ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, 
            List<AggregateCall> aggCalls) {
        RelOptCluster cluster = input.getCluster();
        RelTraitSet traits = cluster.traitSetOf(PhoenixConvention.CLIENT);
        return new PhoenixClientAggregate(cluster, traits, input, indicator, 
                groupSet, groupSets, aggCalls);
    }

    private PhoenixClientAggregate(RelOptCluster cluster, RelTraitSet traits,
            RelNode child, boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
    }

    @Override
    public PhoenixClientAggregate copy(RelTraitSet traits, RelNode input,
            boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls) {
        return create(input, indicator, groupSet, groupSets, aggregateCalls);
    }
    
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getInput().getConvention().satisfies(PhoenixConvention.CLIENT))
            return planner.getCostFactory().makeInfiniteCost();
        
        return super.computeSelfCost(planner, mq)
                .multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(PhoenixRelImplementor implementor) {
        implementor.pushContext(implementor.getCurrentContext().withColumnRefList(getColumnRefList()));
        QueryPlan plan = implementor.visitInput(0, (PhoenixQueryRel) getInput());
        implementor.popContext();
        
        TableRef tableRef = implementor.getTableMapping().getTableRef();
        PhoenixStatement stmt = plan.getContext().getStatement();
        StatementContext context;
        try {
            context = new StatementContext(stmt, FromCompiler.getResolver(tableRef), new Scan(), new SequenceManager(stmt));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }        
        GroupBy groupBy = super.getGroupBy(implementor);       
        List<Expression> funcs = super.serializeAggregators(implementor, context, groupBy.isEmpty());
        
        QueryPlan aggPlan = new ClientAggregatePlan(context, plan.getStatement(), tableRef, RowProjector.EMPTY_PROJECTOR, null, null, null, OrderBy.EMPTY_ORDER_BY, groupBy, null, plan);
        
        return PhoenixAbstractAggregate.wrapWithProject(implementor, aggPlan, groupBy.getKeyExpressions(), funcs);
    }

}
