package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.phoenix.calcite.PhoenixTable;
import org.apache.phoenix.calcite.TableMapping;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

public class PhoenixTableScanColumnRefRule extends RelOptRule {

    /** Predicate that returns true if a table scan has extended columns. */
    private static final Predicate<PhoenixTableScan> APPLICABLE_TABLE_SCAN =
            new Predicate<PhoenixTableScan>() {
        @Override
        public boolean apply(PhoenixTableScan phoenixTableScan) {
            return phoenixTableScan.getTable()
                    .unwrap(PhoenixTable.class).tableMapping.hasExtendedColumns();
        }
    };

    public static final PhoenixTableScanColumnRefRule INSTANCE = new PhoenixTableScanColumnRefRule();

    private PhoenixTableScanColumnRefRule() {
        super(
            operand(Project.class,
                operand(PhoenixTableScan.class, null, APPLICABLE_TABLE_SCAN, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        PhoenixTableScan scan = call.rel(1);
        TableMapping tableMapping =
                scan.getTable().unwrap(PhoenixTable.class).tableMapping;
        ImmutableBitSet bitSet =
                tableMapping.getExtendedColumnRef(project.getProjects());
        if (scan.filter != null) {
            bitSet = bitSet.union(
                    tableMapping.getExtendedColumnRef(
                            ImmutableList.of(scan.filter)));
        }
        if (bitSet.contains(scan.extendedColumnRef)) {
            return;
        }
        
        call.transformTo(
                project.copy(
                        project.getTraitSet(),
                        PhoenixTableScan.create(
                                scan.getCluster(), scan.getTable(),
                                scan.filter, scan.scanOrder, bitSet),
                        project.getProjects(),
                        project.getRowType()));
    }
}
