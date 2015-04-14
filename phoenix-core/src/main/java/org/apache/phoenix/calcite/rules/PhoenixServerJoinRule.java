package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.phoenix.calcite.PhoenixClientJoin;
import org.apache.phoenix.calcite.PhoenixRel;
import org.apache.phoenix.calcite.PhoenixServerJoin;
import org.apache.phoenix.calcite.PhoenixServerProject;
import org.apache.phoenix.calcite.PhoenixTableScan;

import com.google.common.base.Predicate;

public class PhoenixServerJoinRule extends RelOptRule {
    
    /** Predicate that returns true if a join type is not right or full. */
    private static final Predicate<PhoenixClientJoin> NO_RIGHT_OR_FULL =
        new Predicate<PhoenixClientJoin>() {
            @Override
            public boolean apply(PhoenixClientJoin phoenixClientJoin) {
                return phoenixClientJoin.getJoinType() != JoinRelType.RIGHT
                        && phoenixClientJoin.getJoinType() != JoinRelType.FULL;
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
            operand(PhoenixClientJoin.class, null, NO_RIGHT_OR_FULL,
                    left, 
                    operand(PhoenixRel.class, any())),
            description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixClientJoin join = call.rel(0);
        PhoenixRel left = call.rel(1);
        PhoenixRel right = call.rel(call.getRelList().size() - 1);
        call.transformTo(new PhoenixServerJoin(join.getCluster(),
                join.getTraitSet(), left, right, join.getCondition(), 
                join.getJoinType(), join.getVariablesStopped()));
    }

}
