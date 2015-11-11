package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.phoenix.calcite.rel.PhoenixClientSort;
import org.apache.phoenix.calcite.rel.PhoenixCompactClientSort;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.calcite.rel.PhoenixServerAggregate;

public class PhoenixCompactClientSortRule extends RelOptRule {
    
    public static final PhoenixCompactClientSortRule INSTANCE = 
            new PhoenixCompactClientSortRule();

    public PhoenixCompactClientSortRule() {
        super(
            operand(PhoenixClientSort.class, 
                    operand(PhoenixServerAggregate.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixClientSort sort = call.rel(0);
        PhoenixRel input = call.rel(1);
        call.transformTo(PhoenixCompactClientSort.create(
                input, sort.getCollation()));
    }

}
