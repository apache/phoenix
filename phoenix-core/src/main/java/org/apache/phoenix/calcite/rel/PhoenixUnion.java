package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.UnionPlan;
import org.apache.phoenix.parse.SelectStatement;
import com.google.common.collect.Lists;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Union}
 * relational expression in Phoenix.
 */
public class PhoenixUnion extends Union implements PhoenixRel {
    
    public static PhoenixUnion create(List<RelNode> inputs, boolean all) {
        RelOptCluster cluster = inputs.get(0).getCluster();
        RelTraitSet traits = cluster.traitSetOf(PhoenixConvention.CLIENT);
        return new PhoenixUnion(cluster, traits, inputs, all);
    }
    
    private PhoenixUnion(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
        super(cluster, traits, inputs, all);
    }

    @Override
    public PhoenixUnion copy(RelTraitSet traits, List<RelNode> inputs, boolean all) {
        return create(inputs, all);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        for (RelNode input : getInputs()) {
            if (!input.getConvention().satisfies(PhoenixConvention.GENERIC)) {
                return planner.getCostFactory().makeInfiniteCost();
            }
        }
        
        double rowCount = mq.getRowCount(this);
        return planner.getCostFactory().makeCost(0, rowCount, 0);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        List<QueryPlan> subPlans = Lists.newArrayListWithExpectedSize(inputs.size());
        for (Ord<RelNode> input : Ord.zip(inputs)) {
            subPlans.add(implementor.visitInput(input.i, (PhoenixRel) input.e));
        }
        
        return new UnionPlan(subPlans.get(0).getContext(), SelectStatement.SELECT_ONE, subPlans.get(0).getTableRef(), RowProjector.EMPTY_PROJECTOR,
                null, OrderBy.EMPTY_ORDER_BY, GroupBy.EMPTY_GROUP_BY, subPlans, null);
    }
}
