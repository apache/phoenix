package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.phoenix.calcite.rel.PhoenixAbstractAggregate;
import org.apache.phoenix.calcite.rel.PhoenixAbstractJoin;
import org.apache.phoenix.calcite.rel.PhoenixRel;

import com.google.common.base.Predicate;

public class PhoenixJoinSingleValueAggregateMergeRule extends RelOptRule {

    /** Predicate that returns true if the Aggregate is solely SINGLE_VALUE check. */
    private static final Predicate<PhoenixAbstractAggregate> IS_SINGLE_VALUE_CHECK_AGGREGATE =
            new Predicate<PhoenixAbstractAggregate>() {
        @Override
        public boolean apply(PhoenixAbstractAggregate phoenixAggregate) {
            return PhoenixAbstractAggregate.isSingleValueCheckAggregate(phoenixAggregate);
        }
    };
    
    public static PhoenixJoinSingleValueAggregateMergeRule INSTANCE = new PhoenixJoinSingleValueAggregateMergeRule();

    private PhoenixJoinSingleValueAggregateMergeRule() {
        super(
            operand(
                    PhoenixAbstractJoin.class,
                    operand(
                            PhoenixRel.class, any()),
                    operand(
                            PhoenixAbstractAggregate.class, null, IS_SINGLE_VALUE_CHECK_AGGREGATE, any())), 
            "PhoenixJoinSingleValueAggregateMergeRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixAbstractJoin join = call.rel(0);
        PhoenixRel left = call.rel(1);
        PhoenixAbstractAggregate right = call.rel(2);
        int groupCount = right.getGroupCount();
        for (Integer key : join.joinInfo.rightKeys) {
            if (key >= groupCount) {
                return;
            }
        }
        
        call.transformTo(join.copy(join.getTraitSet(), join.getCondition(), 
                left, right.getInput(), join.getJoinType(), false, true));
    }

}
