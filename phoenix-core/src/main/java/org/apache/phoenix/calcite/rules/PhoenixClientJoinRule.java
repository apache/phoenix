package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.phoenix.calcite.rel.PhoenixClientJoin;
import org.apache.phoenix.calcite.rel.PhoenixJoin;

import com.google.common.base.Predicate;

public class PhoenixClientJoinRule extends RelOptRule {
    
    /** Predicate that returns true if a join type is not right. */
    private static final Predicate<PhoenixJoin> NO_RIGHT_JOIN =
        new Predicate<PhoenixJoin>() {
            @Override
            public boolean apply(PhoenixJoin phoenixJoin) {
                return phoenixJoin.getJoinType() != JoinRelType.RIGHT;
            }
        };
    
    public static PhoenixClientJoinRule INSTANCE = new PhoenixClientJoinRule();

    public PhoenixClientJoinRule() {
        super(operand(PhoenixJoin.class, null, NO_RIGHT_JOIN, any()), "PhoenixClientJoinRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixJoin join = call.rel(0);
        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        call.transformTo(PhoenixClientJoin.create(
                left, right, join.getCondition(), 
                join.getJoinType(), join.getVariablesStopped(), false));
    }

}
