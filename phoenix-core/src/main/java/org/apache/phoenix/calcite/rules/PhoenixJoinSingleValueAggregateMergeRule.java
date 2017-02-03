package org.apache.phoenix.calcite.rules;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.phoenix.calcite.rel.PhoenixAbstractAggregate;
import org.apache.phoenix.calcite.rel.PhoenixAbstractJoin;
import org.apache.phoenix.calcite.rel.PhoenixConvention;
import org.apache.phoenix.calcite.rel.PhoenixRel;

import com.google.common.base.Predicate;

public class PhoenixJoinSingleValueAggregateMergeRule extends RelOptRule {

    /** Predicate that returns true if the Aggregate is solely SINGLE_VALUE check. */
    private static final Predicate<LogicalAggregate> IS_SINGLE_VALUE_CHECK_AGGREGATE =
            new Predicate<LogicalAggregate>() {
        @Override
        public boolean apply(LogicalAggregate phoenixAggregate) {
            if (!PhoenixAbstractAggregate.isSingleValueCheckAggregate(phoenixAggregate))
                return false;
            
            List<Integer> groupSet = phoenixAggregate.getGroupSet().asList();
            if (groupSet.size() + 1 != phoenixAggregate.getInput().getRowType().getFieldCount())
                return false;
            
            for (int i = 0; i < groupSet.size(); i++) {
                if (groupSet.get(i) != i)
                    return false;
            }
            
            List<Integer> argList = phoenixAggregate.getAggCallList().get(0).getArgList();
            return (argList.size() == 1 && argList.get(0) == groupSet.size());
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
                            LogicalAggregate.class, null, IS_SINGLE_VALUE_CHECK_AGGREGATE, any())),
            "PhoenixJoinSingleValueAggregateMergeRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixAbstractJoin join = call.rel(0);
        PhoenixRel left = call.rel(1);
        LogicalAggregate right = call.rel(2);
        int groupCount = right.getGroupCount();
        for (Integer key : join.joinInfo.rightKeys) {
            if (key >= groupCount) {
                return;
            }
        }
        
        call.transformTo(join.copy(join.getTraitSet(), join.getCondition(), 
                left, convert(right.getInput(), right.getInput().getTraitSet().replace(PhoenixConvention.GENERIC)), join.getJoinType(), false, true));
    }

}
