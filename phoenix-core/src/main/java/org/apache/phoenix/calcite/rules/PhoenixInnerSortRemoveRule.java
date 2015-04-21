package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.phoenix.calcite.rel.PhoenixAbstractSort;

public class PhoenixInnerSortRemoveRule extends RelOptRule {
    
    public static PhoenixInnerSortRemoveRule INSTANCE = new PhoenixInnerSortRemoveRule();

    private PhoenixInnerSortRemoveRule() {
        super(
            operand(
                    PhoenixAbstractSort.class, 
                    operand(
                            PhoenixAbstractSort.class, any())), 
            "PhoenixInnerSortRemoveRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixAbstractSort sort = call.rel(0);
        PhoenixAbstractSort innerSort = call.rel(1);
        call.transformTo(sort.copy(sort.getTraitSet(), 
                innerSort.getInput(), sort.getCollation(), 
                sort.offset, sort.fetch));
    }

}
