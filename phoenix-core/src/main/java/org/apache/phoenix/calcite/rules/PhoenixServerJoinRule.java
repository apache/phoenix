package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.phoenix.calcite.rel.PhoenixJoin;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.calcite.rel.PhoenixServerJoin;
import org.apache.phoenix.calcite.rel.PhoenixServerProject;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;

import com.google.common.base.Predicate;

public class PhoenixServerJoinRule extends RelOptRule {
    
    /** Predicate that returns true if a join type is not right or full. */
    private static final Predicate<PhoenixJoin> NO_RIGHT_OR_FULL_JOIN =
        new Predicate<PhoenixJoin>() {
            @Override
            public boolean apply(PhoenixJoin phoenixJoin) {
                return phoenixJoin.getJoinType() != JoinRelType.RIGHT
                        && phoenixJoin.getJoinType() != JoinRelType.FULL;
            }
        };
   
    public static final PhoenixServerJoinRule JOIN_SCAN =
            new PhoenixServerJoinRule("PhoenixServerJoinRule:join_scan", 
                    operand(PhoenixTableScan.class, any()));
    
    public static final PhoenixServerJoinRule JOIN_SERVERPROJECT_SCAN =
            new PhoenixServerJoinRule("PhoenixServerJoinRule:join_serverproject_scan", 
                    operand(PhoenixServerProject.class, 
                            operand(PhoenixTableScan.class, any())));

    public PhoenixServerJoinRule(String description, RelOptRuleOperand left) {
        super(
            operand(PhoenixJoin.class, null, NO_RIGHT_OR_FULL_JOIN,
                    left, 
                    operand(PhoenixRel.class, any())),
            description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixJoin join = call.rel(0);
        PhoenixRel left = call.rel(1);
        PhoenixRel right = call.rel(call.getRelList().size() - 1);
        call.transformTo(PhoenixServerJoin.create(
                left, right, join.getCondition(), 
                join.getJoinType(), join.getVariablesStopped(), false));
    }

}
