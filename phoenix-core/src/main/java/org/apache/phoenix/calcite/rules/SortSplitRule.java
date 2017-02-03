package org.apache.phoenix.calcite.rules;

import com.google.common.base.Predicate;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.phoenix.calcite.rel.Limit;
import org.apache.phoenix.calcite.rel.LogicalLimit;

public class SortSplitRule extends RelOptRule {

    /** Predicate that returns true if a table scan has no filter and is not reverse scan. */
    private static final Predicate<Sort> APPLICABLE_SORT =
            new Predicate<Sort>() {
        @Override
        public boolean apply(Sort input) {
            return input.offset != null || input.fetch != null;
        }
    };

    public static final SortSplitRule INSTANCE = new SortSplitRule();

    private SortSplitRule() {
        super(operand(LogicalSort.class, null, APPLICABLE_SORT, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        RelNode input = sort.getCollation().getFieldCollations().isEmpty()
                ? sort.getInput()
                : LogicalSort.create(
                        sort.getInput(), sort.getCollation(), null, null);
        Limit limit = LogicalLimit.create(input, sort.offset, sort.fetch);
        call.transformTo(limit);
    }
}
