package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.ClientScanPlan;

import com.google.common.base.Supplier;

public class PhoenixLimit extends SingleRel implements PhoenixRel {
    public final RexNode offset;
    public final RexNode fetch;
    public final Integer statelessFetch;
    
    public static PhoenixLimit create(final RelNode input, RexNode offset, RexNode fetch) {
        RelOptCluster cluster = input.getCluster();
        final RelTraitSet traits =
                cluster.traitSet().replace(PhoenixRel.CLIENT_CONVENTION)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return RelMdCollation.limit(input);
                    }
                });
        return new PhoenixLimit(cluster, traits, input, offset, fetch);
    }

    private PhoenixLimit(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode offset, RexNode fetch) {
        super(cluster, traits, input);
        this.offset = offset;
        this.fetch = fetch;
        Object value = fetch == null ? null : CalciteUtils.evaluateStatelessExpression(fetch);
        this.statelessFetch = value == null ? null : ((Number) value).intValue();        
    }
    
    @Override
    public PhoenixLimit copy(
            RelTraitSet traitSet,
            List<RelNode> newInputs) {
        return create(
                sole(newInputs),
                offset,
                fetch);
    }

    @Override 
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("offset", offset, offset != null)
                .itemIf("fetch", fetch, fetch != null);
    }

    @Override 
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        double rowCount = RelMetadataQuery.getRowCount(this);
        return planner.getCostFactory()
                .makeCost(rowCount, 0, 0)
                .multiplyBy(PHOENIX_FACTOR);
    }
    
    @Override 
    public double getRows() {
        double rows = super.getRows();        
        // TODO Should we apply a factor to ensure that a limit can be propagated to
        // lower nodes as much as possible?
        if (this.statelessFetch == null)
            return rows;

        return Math.min(this.statelessFetch, rows);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        assert getConvention() == getInput().getConvention();
        
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        // TODO only wrap with ClientScanPlan 
        // if (plan.getLimit() != null);
        // otherwise add limit to "plan"
        return new ClientScanPlan(plan.getContext(), plan.getStatement(), 
                implementor.getTableRef(), RowProjector.EMPTY_PROJECTOR, 
                statelessFetch, null, OrderBy.EMPTY_ORDER_BY, plan);
    }
}
