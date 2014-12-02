package org.apache.phoenix.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.phoenix.jdbc.PhoenixConnection;

import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Values}
 * relational expression in Phoenix.
 */
public class PhoenixValues extends Values implements PhoenixRel {
    public PhoenixValues(RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traits) {
        super(cluster, rowType, tuples, traits);
        assert getConvention() == PhoenixRel.CONVENTION;
    }

    @Override
    public PhoenixValues copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        assert inputs.isEmpty();
        return new PhoenixValues(getCluster(), rowType, tuples, traitSet);
    }

    @Override
    public void implement(Implementor implementor, PhoenixConnection conn) {
        throw new UnsupportedOperationException();
    }
}
