package org.apache.phoenix.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.phoenix.jdbc.PhoenixConnection;

import java.util.List;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Aggregate}
 * relational expression in Phoenix.
 */
public class PhoenixAggregate extends Aggregate implements PhoenixRel {
    public PhoenixAggregate(RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) throws InvalidRelException {
        super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
        assert getConvention() == PhoenixRel.CONVENTION;
        assert getConvention() == child.getConvention();

        for (AggregateCall aggCall : aggCalls) {
            if (aggCall.isDistinct()) {
                throw new InvalidRelException( "distinct aggregation not supported");
            }
        }
        switch (getGroupType()) {
            case SIMPLE:
                break;
            default:
                throw new InvalidRelException("unsupported group type: " + getGroupType());
        }
    }

    @Override
    public PhoenixAggregate copy(RelTraitSet traits, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls) {
        try {
            return new PhoenixAggregate(getCluster(), traits, input, indicator, groupSet, groupSets, aggregateCalls);
        } catch (InvalidRelException e) {
            // Semantic error not possible. Must be a bug. Convert to
            // internal error.
            throw new AssertionError(e);
        }
    }

    @Override
    public void implement(Implementor implementor, PhoenixConnection conn) {
        implementor.visitInput(0, (PhoenixRel) getInput());
        throw new UnsupportedOperationException();
    }
}
