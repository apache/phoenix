package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.phoenix.compile.QueryPlan;

public class PhoenixToClientConverter extends SingleRel implements
        PhoenixRel {
    
    public static PhoenixToClientConverter create(RelNode input) {
        return new PhoenixToClientConverter(
                input.getCluster(), 
                input.getTraitSet().replace(PhoenixRel.CLIENT_CONVENTION), 
                input);
    }

    private PhoenixToClientConverter(RelOptCluster cluster,
            RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }
    
    @Override
    public PhoenixToClientConverter copy(
            RelTraitSet traitSet,
            List<RelNode> newInputs) {
        return create(sole(newInputs));
    }

    @Override 
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return planner.getCostFactory().makeCost(0, 0, 0);
    }
    
    @Override
    public QueryPlan implement(Implementor implementor) {
        return implementor.visitInput(0, (PhoenixRel) getInput());
    }

}
