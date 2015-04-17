package org.apache.phoenix.calcite.rules;

import com.google.common.base.Predicate;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.phoenix.calcite.rel.PhoenixFilter;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;

public class PhoenixFilterScanMergeRule extends RelOptRule {

    /** Predicate that returns true if a table scan has no filter. */
    private static final Predicate<PhoenixTableScan> NO_FILTER =
        new Predicate<PhoenixTableScan>() {
            @Override
            public boolean apply(PhoenixTableScan phoenixTableScan) {
                return phoenixTableScan.filter == null;
            }
        };

    public static final PhoenixFilterScanMergeRule INSTANCE = new PhoenixFilterScanMergeRule();

    private PhoenixFilterScanMergeRule() {
        super(
            operand(PhoenixFilter.class,
                operand(PhoenixTableScan.class, null, NO_FILTER, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixFilter filter = call.rel(0);
        PhoenixTableScan scan = call.rel(1);
        assert scan.filter == null : "predicate should have ensured no filter";
        call.transformTo(PhoenixTableScan.create(scan.getCluster(),
                scan.getTable(), filter.getCondition(), scan.statelessFetch));
    }
}
