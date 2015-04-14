package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.phoenix.compile.QueryPlan;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Union}
 * relational expression in Phoenix.
 */
public class PhoenixUnion extends Union implements PhoenixRel {
    public PhoenixUnion(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
        super(cluster, traits, inputs, all);
        assert getConvention() == PhoenixRel.CONVENTION;
    }

    @Override
    public PhoenixUnion copy(RelTraitSet traits, List<RelNode> inputs, boolean all) {
        return new PhoenixUnion(getCluster(), traits, inputs, all);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        for (RelNode input : getInputs()) {
            assert getConvention() == input.getConvention();
        }
        for (Ord<RelNode> input : Ord.zip(inputs)) {
            implementor.visitInput(input.i, (PhoenixRel) input.e);
        }
        throw new UnsupportedOperationException();
    }
}
