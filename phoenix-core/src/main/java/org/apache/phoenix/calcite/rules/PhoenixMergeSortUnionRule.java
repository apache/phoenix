package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.phoenix.calcite.rel.PhoenixClientSort;
import org.apache.phoenix.calcite.rel.PhoenixMergeSortUnion;
import org.apache.phoenix.calcite.rel.PhoenixUnion;

import com.google.common.base.Predicate;

/**
 * Rule that converts a {@link PhoenixClientSort} over a {@link PhoenixUnion}
 * into a {@link PhoenixMergeSortUnion}.
 */
public class PhoenixMergeSortUnionRule extends RelOptRule {
    private static final Predicate<PhoenixUnion> IS_UNION_ALL =
            new Predicate<PhoenixUnion>() {
        @Override
        public boolean apply(PhoenixUnion input) {
            return input.all;
        }
    };
    
    public static final PhoenixMergeSortUnionRule INSTANCE =
            new PhoenixMergeSortUnionRule();

    public PhoenixMergeSortUnionRule() {
        super(operand(PhoenixClientSort.class, 
                operand(PhoenixUnion.class, null, IS_UNION_ALL, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final PhoenixClientSort sort = call.rel(0);
        final PhoenixUnion union = call.rel(1);        
        assert union.all;
        final RelCollation collation = sort.getCollation();
        call.transformTo(
                PhoenixMergeSortUnion.create(
                        convertList(union.getInputs(), collation),
                        union.all, collation));
    }

}
