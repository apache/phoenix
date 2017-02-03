package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;
import org.apache.phoenix.calcite.rel.PhoenixTableScan.ScanOrder;

import com.google.common.base.Predicate;

public class PhoenixReverseTableScanRule extends RelOptRule {    
    private static final Predicate<Sort> NON_EMPTY_COLLATION =
            new Predicate<Sort>() {
        @Override
        public boolean apply(Sort input) {
            return !input.getCollation().getFieldCollations().isEmpty()
                    && input.offset == null && input.fetch == null;
        }
    };
    
    private static final Predicate<PhoenixTableScan> APPLICABLE_TABLE_SCAN =
            new Predicate<PhoenixTableScan>() {
        @Override
        public boolean apply(PhoenixTableScan input) {
            return input.scanOrder != ScanOrder.REVERSE
                    && input.isReverseScanEnabled()
                    && (input.scanRanges == null
                        || !input.scanRanges.useSkipScanFilter());
        }
    };

    public PhoenixReverseTableScanRule(Class<? extends Sort> sortClass) {
        super(operand(sortClass, null, NON_EMPTY_COLLATION,
                operand(PhoenixTableScan.class, null, APPLICABLE_TABLE_SCAN, any())),
            PhoenixReverseTableScanRule.class.getName() + ":" + sortClass.getName());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final PhoenixTableScan scan = call.rel(1);
        final RelCollation collation = sort.getCollation();
        assert !collation.getFieldCollations().isEmpty();
        for (RelCollation candidate : scan.getTable().getCollationList()) {
            if (CalciteUtils.reverseCollation(candidate).satisfies(collation)) {
                RelNode newRel = PhoenixTableScan.create(
                        scan.getCluster(), scan.getTable(), scan.filter,
                        ScanOrder.REVERSE, scan.extendedColumnRef);
                call.transformTo(newRel);
                break;
            }
        }
    }

}
