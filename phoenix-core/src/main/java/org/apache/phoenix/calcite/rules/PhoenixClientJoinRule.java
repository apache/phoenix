package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.phoenix.calcite.rel.PhoenixClientJoin;
import org.apache.phoenix.calcite.rel.PhoenixJoin;

public class PhoenixClientJoinRule extends RelOptRule {
    
    public static PhoenixClientJoinRule INSTANCE = new PhoenixClientJoinRule();

    public PhoenixClientJoinRule() {
        super(operand(PhoenixJoin.class, any()), "PhoenixClientJoinRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixJoin join = call.rel(0);
        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        call.transformTo(PhoenixClientJoin.create(
                left, right, join.getCondition(), 
                join.getJoinType(), join.getVariablesStopped()));
    }

}
