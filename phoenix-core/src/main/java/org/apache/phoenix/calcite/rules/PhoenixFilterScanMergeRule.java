package org.apache.phoenix.calcite.rules;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.phoenix.calcite.PhoenixTable;
import org.apache.phoenix.calcite.TableMapping;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;
import org.apache.phoenix.calcite.rel.PhoenixTableScan.ScanOrder;

public class PhoenixFilterScanMergeRule extends RelOptRule {

    /** Predicate that returns true if a table scan has no filter and is not reverse scan. */
    private static final Predicate<PhoenixTableScan> APPLICABLE_TABLE_SCAN =
            new Predicate<PhoenixTableScan>() {
        @Override
        public boolean apply(PhoenixTableScan phoenixTableScan) {
            return phoenixTableScan.filter == null
                    && phoenixTableScan.scanOrder != ScanOrder.REVERSE;
        }
    };

    /** Predicate that returns true if a filter is Phoenix implementable. */
    private static Predicate<Filter> IS_CONVERTIBLE = 
            new Predicate<Filter>() {
        @Override
        public boolean apply(Filter input) {
            return PhoenixConverterRules.isConvertible(input);
        }            
    };

    public static final PhoenixFilterScanMergeRule INSTANCE = new PhoenixFilterScanMergeRule();

    private PhoenixFilterScanMergeRule() {
        super(
            operand(LogicalFilter.class, null, IS_CONVERTIBLE,
                operand(PhoenixTableScan.class, null, APPLICABLE_TABLE_SCAN, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        PhoenixTableScan scan = call.rel(1);
        RexNode condition = filter.getCondition();
        TableMapping tableMapping =
                scan.getTable().unwrap(PhoenixTable.class).tableMapping;
        ImmutableBitSet columnRef = 
                scan.extendedColumnRef.union(
                        tableMapping.getExtendedColumnRef(
                                ImmutableList.of(condition)));
        call.transformTo(PhoenixTableScan.create(
                scan.getCluster(), scan.getTable(),
                condition, scan.scanOrder, columnRef));
    }
}
