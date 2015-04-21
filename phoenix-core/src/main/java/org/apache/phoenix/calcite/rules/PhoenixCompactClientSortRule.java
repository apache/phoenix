package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.phoenix.calcite.rel.PhoenixClientSort;
import org.apache.phoenix.calcite.rel.PhoenixCompactClientSort;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.calcite.rel.PhoenixServerAggregate;

public class PhoenixCompactClientSortRule extends RelOptRule {
    
    public static final PhoenixCompactClientSortRule SORT_SERVERAGGREGATE = 
            new PhoenixCompactClientSortRule("PhoenixCompactClientSortRule:sort_serveraggregate", PhoenixServerAggregate.class);

    public PhoenixCompactClientSortRule(String description, Class<? extends PhoenixRel> clazz) {
        super(
            operand(PhoenixClientSort.class, 
                    operand(clazz, any())),
            description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixClientSort sort = call.rel(0);
        PhoenixRel input = call.rel(1);
        call.transformTo(PhoenixCompactClientSort.create(
                input, sort.getCollation()));
    }

}
