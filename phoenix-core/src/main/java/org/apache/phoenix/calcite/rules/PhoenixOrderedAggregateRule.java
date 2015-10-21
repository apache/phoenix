package org.apache.phoenix.calcite.rules;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.phoenix.calcite.rel.PhoenixAbstractAggregate;
import org.apache.phoenix.calcite.rel.PhoenixRel;

import com.google.common.base.Predicate;

/**
 * Phoenix rule that transforms an unordered Aggregate into an ordered Aggregate.
 * 
 * The Aggregate's child could have a collation that matches the groupSet and thus
 * makes the Aggregate ordered, but the Aggregate wouldn't know this matching 
 * collation if its child resides in a RelSubset with an empty collation.
 * An option would be to use conversion rules that create a subset of a specific
 * collation. But since there are so many potential collations that can match the
 * groupSet and most of them are meaningless for the actual child expression, we
 * do not want to make this rule a ConvertRule.
 * Instead, we surface the matching child expression by using RelOptRule and
 * reconstruct a new Aggregate with this child.
 */
public class PhoenixOrderedAggregateRule extends RelOptRule {
    
    private static Predicate<PhoenixAbstractAggregate> UNORDERED_GROUPBY =
            new Predicate<PhoenixAbstractAggregate>() {
                @Override
                public boolean apply(PhoenixAbstractAggregate input) {
                    return !input.isOrderedGroupBy;
                }
    };
    
    private static Predicate<PhoenixRel> NON_EMPTY_COLLATION =
            new Predicate<PhoenixRel>() {
                @Override
                public boolean apply(PhoenixRel input) {
                    if (input.getConvention() != PhoenixRel.SERVER_CONVENTION
                            && input.getConvention() != PhoenixRel.SERVERJOIN_CONVENTION)
                        return false;
                    
                    List<RelCollation> collations = input.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
                    for (RelCollation collation : collations) {
                        if (!collation.getFieldCollations().isEmpty()) {
                            return true;
                        }
                    }
                    return false;
                }
    };
    
    public static final PhoenixOrderedAggregateRule INSTANCE = new PhoenixOrderedAggregateRule();
    
    public PhoenixOrderedAggregateRule() {
        super(operand(PhoenixAbstractAggregate.class, null, UNORDERED_GROUPBY,
                operand(PhoenixRel.class, null, NON_EMPTY_COLLATION, any())));
    }
    
    @Override
    public boolean matches(RelOptRuleCall call) {
        PhoenixAbstractAggregate agg = call.rel(0);
        RelNode child = call.rel(1);
        return PhoenixAbstractAggregate.isOrderedGroupSet(agg.getGroupSet(), child);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixAbstractAggregate agg = call.rel(0);
        RelNode child = call.rel(1);
        call.transformTo(agg.copy(agg.getTraitSet(), child, agg.indicator, agg.getGroupSet(), agg.groupSets, agg.getAggCallList()));
    }

}
