package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.UnionPlan;
import org.apache.phoenix.parse.SelectStatement;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class PhoenixMergeSortUnion extends Union implements PhoenixRel {
	public final RelCollation collation;
    
    public static PhoenixMergeSortUnion create(final List<RelNode> inputs,
            final boolean all, final RelCollation collation) {
        RelOptCluster cluster = inputs.get(0).getCluster();
        RelTraitSet traits = 
        		cluster.traitSetOf(PhoenixConvention.CLIENT)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return ImmutableList.<RelCollation> of(collation);
                    }
                });
        return new PhoenixMergeSortUnion(cluster, traits, inputs, all, collation);
    }
    
    private PhoenixMergeSortUnion(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all, RelCollation collation) {
        super(cluster, traits, inputs, all);
        this.collation = collation;
    }

    @Override
    public PhoenixMergeSortUnion copy(RelTraitSet traits, List<RelNode> inputs, boolean all) {
        return create(inputs, all, collation);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        for (RelNode input : getInputs()) {
            if (!input.getConvention().satisfies(PhoenixConvention.GENERIC)
                    || !mq.collations(input).contains(collation)) {
                return planner.getCostFactory().makeInfiniteCost();
            }
        }
        
        double mergeSortFactor = 1.1;
        return super.computeSelfCost(planner, mq)
                .multiplyBy(PHOENIX_FACTOR).multiplyBy(mergeSortFactor);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        List<QueryPlan> subPlans = Lists.newArrayListWithExpectedSize(inputs.size());
        for (Ord<RelNode> input : Ord.zip(inputs)) {
            subPlans.add(implementor.visitInput(input.i, (PhoenixRel) input.e));
        }
        
        final OrderBy orderBy = PhoenixAbstractSort.getOrderBy(collation, implementor, null);
        return new UnionPlan(subPlans.get(0).getContext(), SelectStatement.SELECT_ONE, subPlans.get(0).getTableRef(), RowProjector.EMPTY_PROJECTOR,
                null, null, orderBy, GroupBy.EMPTY_GROUP_BY, subPlans, null);
    }
}
