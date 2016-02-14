package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.TupleProjector;

import com.google.common.base.Supplier;

public class PhoenixServerProject extends PhoenixAbstractProject {
    
    public static PhoenixServerProject create(final RelNode input, 
            final List<? extends RexNode> projects, RelDataType rowType) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        final RelTraitSet traits =
                cluster.traitSet().replace(PhoenixConvention.SERVER)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return RelMdCollation.project(mq, input, projects);
                    }
                });
        return new PhoenixServerProject(cluster, traits, input, projects, rowType);
    }

    private PhoenixServerProject(RelOptCluster cluster, RelTraitSet traits,
            RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public PhoenixServerProject copy(RelTraitSet traits, RelNode input,
            List<RexNode> projects, RelDataType rowType) {
        return create(input, projects, rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getInput().getConvention().satisfies(PhoenixConvention.SERVER))
            return planner.getCostFactory().makeInfiniteCost();
        
        return super.computeSelfCost(planner, mq)
                .multiplyBy(SERVER_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        implementor.pushContext(new ImplementorContext(implementor.getCurrentContext().retainPKColumns, false, getColumnRefList()));
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        implementor.popContext();
        
        assert (plan instanceof ScanPlan);
        
        TupleProjector tupleProjector = super.project(implementor);
        TupleProjector.serializeProjectorIntoScan(plan.getContext().getScan(), tupleProjector);
        return plan;
    }
}
