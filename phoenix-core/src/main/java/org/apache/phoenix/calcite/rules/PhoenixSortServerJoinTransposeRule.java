package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.phoenix.calcite.rel.PhoenixConvention;
import org.apache.phoenix.calcite.rel.PhoenixServerJoin;
import org.apache.phoenix.calcite.rel.PhoenixTemporarySort;

import com.google.common.base.Predicate;

public class PhoenixSortServerJoinTransposeRule extends RelOptRule {
    private static final Predicate<Join> INNER_OR_LEFT = new Predicate<Join>() {
        @Override
        public boolean apply(Join input) {
            return input.getJoinType() == JoinRelType.INNER
                    || input.getJoinType() == JoinRelType.LEFT;
        }        
    };

    public PhoenixSortServerJoinTransposeRule(
            Class<? extends Sort> sortClass, String description) {
        super(
                operand(sortClass,
                        operand(PhoenixServerJoin.class, null, INNER_OR_LEFT, any())),
                description);
    }

    @Override public boolean matches(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final PhoenixServerJoin join = call.rel(1);
        if (!join.getLeft().getConvention().satisfies(PhoenixConvention.SERVER)) {
            return false;
        }
        for (RelFieldCollation relFieldCollation
                : sort.getCollation().getFieldCollations()) {
            if (relFieldCollation.getFieldIndex()
                    >= join.getLeft().getRowType().getFieldCount()) {
                return false;
            }
        }
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        if (RelMdUtil.checkInputForCollationAndLimit(mq,
                join.getLeft(), sort.getCollation(), null, null)) {
            return false;
        }

        return true;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Sort sort = call.rel(0);
      final PhoenixServerJoin join = call.rel(1);
      final RelNode newLeftInput = PhoenixTemporarySort.create(
              join.getLeft(), sort.getCollation());
      final RelNode newRel = join.copy(join.getTraitSet(), join.getCondition(),
              newLeftInput, join.getRight(), join.getJoinType(), join.isSemiJoinDone());

      call.transformTo(newRel);
    }
}
