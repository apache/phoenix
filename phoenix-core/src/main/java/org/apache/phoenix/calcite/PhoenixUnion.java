package org.apache.phoenix.calcite;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.phoenix.jdbc.PhoenixConnection;

import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Union}
 * relational expression in Phoenix.
 */
public class PhoenixUnion extends Union implements PhoenixRel {
    protected PhoenixUnion(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
        super(cluster, traits, inputs, all);
        assert getConvention() == PhoenixRel.CONVENTION;

        for (RelNode input : inputs) {
            assert getConvention() == input.getConvention();
        }
    }

    @Override
    public PhoenixUnion copy(RelTraitSet traits, List<RelNode> inputs, boolean all) {
        return new PhoenixUnion(getCluster(), traits, inputs, all);
    }

    @Override
    public void implement(Implementor implementor, PhoenixConnection conn) {
        for (Ord<RelNode> input : Ord.zip(inputs)) {
            implementor.visitInput(input.i, (PhoenixRel) input.e);
        }
        throw new UnsupportedOperationException();
    }
}
