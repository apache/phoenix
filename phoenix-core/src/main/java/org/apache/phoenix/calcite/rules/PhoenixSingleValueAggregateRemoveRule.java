package org.apache.phoenix.calcite.rules;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.phoenix.calcite.rel.PhoenixAbstractAggregate;
import com.google.common.base.Predicate;

public class PhoenixSingleValueAggregateRemoveRule extends RelOptRule {

    /** Predicate that returns true if SINGLE_VALUE is the only aggregate call in the Aggregate. */
    private static final Predicate<PhoenixAbstractAggregate> SINGLE_VALUE_FUNC_ONLY =
            new Predicate<PhoenixAbstractAggregate>() {
        @Override
        public boolean apply(PhoenixAbstractAggregate phoenixAggregate) {
            List<AggregateCall> aggCalls = phoenixAggregate.getAggCallList();
            return aggCalls.size() == 1 
                    && aggCalls.get(0).getAggregation().getName().equals("SINGLE_VALUE");
        }
    };
    
    public static PhoenixSingleValueAggregateRemoveRule INSTANCE = new PhoenixSingleValueAggregateRemoveRule();

    private PhoenixSingleValueAggregateRemoveRule() {
        super(
            operand(
                    PhoenixAbstractAggregate.class, null, SINGLE_VALUE_FUNC_ONLY,
                    operand(
                            // TODO check returns single value?
                            PhoenixAbstractAggregate.class, any())), 
            "PhoenixSingleValueAggregateRemoveRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixAbstractAggregate aggregate = call.rel(0);
        PhoenixAbstractAggregate innerAggregate = call.rel(1);
        int groupCount = aggregate.getGroupCount();
        int innerGroupCount = innerAggregate.getGroupCount();
        if (groupCount != innerGroupCount)
            return;
        
        List<Integer> ordinals = aggregate.getGroupSet().asList();
        for (int i = 0; i < ordinals.size(); i++) {
            if (ordinals.get(i) != i) {
                return;
            }
        }
        
        call.transformTo(innerAggregate);
    }

}
