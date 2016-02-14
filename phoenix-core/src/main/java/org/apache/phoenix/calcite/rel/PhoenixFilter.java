package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.ClientScanPlan;
import org.apache.phoenix.expression.Expression;

import com.google.common.base.Supplier;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Filter}
 * relational expression in Phoenix.
 */
public class PhoenixFilter extends Filter implements PhoenixRel {
    
    public static PhoenixFilter create(final RelNode input, final RexNode condition) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        final RelTraitSet traits =
                cluster.traitSet().replace(PhoenixConvention.CLIENT)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return RelMdCollation.filter(mq, input);
                    }
                });
        return new PhoenixFilter(cluster, traits, input, condition);
    }
    
    private PhoenixFilter(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode condition) {
        super(cluster, traits, input, condition);
    }

    public PhoenixFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return create(input, condition);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getInput().getConvention().satisfies(PhoenixConvention.GENERIC))
            return planner.getCostFactory().makeInfiniteCost();
        
        double rows = mq.getRowCount(this);
        double inputRows = mq.getRowCount(getInput());
        return planner.getCostFactory().makeCost(0, rows + inputRows, 0);
    }

    public QueryPlan implement(Implementor implementor) {
        ImmutableIntList columnRefList = implementor.getCurrentContext().columnRefList;
        ImmutableBitSet bitSet = InputFinder.analyze(condition).inputBitSet.addAll(columnRefList).build();
        columnRefList = ImmutableIntList.copyOf(bitSet.asList());
        implementor.pushContext(implementor.getCurrentContext().withColumnRefList(columnRefList));
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        implementor.popContext();
        Expression expr = CalciteUtils.toExpression(condition, implementor);
        return new ClientScanPlan(plan.getContext(), plan.getStatement(), plan.getTableRef(),
                plan.getProjector(), null, expr, OrderBy.EMPTY_ORDER_BY, plan);
    }
}
