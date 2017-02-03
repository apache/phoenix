package org.apache.phoenix.calcite.rules;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.phoenix.calcite.rel.PhoenixAbstractAggregate;
import org.apache.phoenix.calcite.rel.PhoenixConvention;
import org.apache.phoenix.calcite.rel.PhoenixTemporarySort;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;

/**
 * Phoenix rule that transforms an unordered Aggregate into an ordered Aggregate.
 * 
 * An Aggregate can turn into an ordered Aggregate (stream Aggregate) if its child
 * is ordered on the fields of the groupSet, and the collation fields can come in
 * as an arbitrary permutation of the groupSet fields with either ascending or
 * descending direction. That said, there could be many possible collations of a
 * child that will qualify an ordered Aggregate, but in order to keep the search
 * space reasonable, we will pick the most promising one(s) in this rule.
 */
public class PhoenixOrderedAggregateRule extends RelOptRule {
    
    private static Predicate<PhoenixAbstractAggregate> UNORDERED_GROUPBY =
            new Predicate<PhoenixAbstractAggregate>() {
                @Override
                public boolean apply(PhoenixAbstractAggregate input) {
                    return !input.isOrderedGroupBy;
                }
    };
    
    public static final PhoenixOrderedAggregateRule INSTANCE = new PhoenixOrderedAggregateRule();
    
    public PhoenixOrderedAggregateRule() {
        super(operand(PhoenixAbstractAggregate.class, null, UNORDERED_GROUPBY, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixAbstractAggregate agg = call.rel(0);
        List<RelFieldCollation> fieldCollations = Lists.newArrayList();
        for (Integer ordinal : agg.getGroupSet().asList()) {
            fieldCollations.add(new RelFieldCollation(ordinal));
        }
        RelCollation collation = RelCollations.of(fieldCollations);
        RelNode input = agg.getInput();
        RelNode newInput = input.getConvention() == PhoenixConvention.SERVER
                ? PhoenixTemporarySort.create(input, collation)
                : convert(LogicalSort.create(input, collation, null, null), input.getConvention());
        call.transformTo(agg.copy(agg.getTraitSet(), newInput, agg.indicator, agg.getGroupSet(), agg.groupSets, agg.getAggCallList()));
    }

}
