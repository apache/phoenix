package org.apache.phoenix.calcite;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;

import com.google.common.base.Predicate;

public class PhoenixProjectScanMergeRule extends RelOptRule {

    /** Predicate that returns true if a table scan has no project. */
    private static final Predicate<PhoenixTableScan> NO_PROJECT =
        new Predicate<PhoenixTableScan>() {
            @Override
            public boolean apply(PhoenixTableScan phoenixTableScan) {
                return phoenixTableScan.projects == null;
            }
        };

    public static final PhoenixProjectScanMergeRule INSTANCE = new PhoenixProjectScanMergeRule();

    private PhoenixProjectScanMergeRule() {
        super(
            operand(Project.class,
                operand(PhoenixTableScan.class, null, NO_PROJECT, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Project project = call.rel(0);
        PhoenixTableScan scan = call.rel(1);
        assert scan.projects == null : "predicate should have ensured no project";
        call.transformTo(new PhoenixTableScan(scan.getCluster(),
                scan.getTraitSet(), scan.getTable(),
                scan.filter, project.getProjects(), project.getRowType()));
    }
}
