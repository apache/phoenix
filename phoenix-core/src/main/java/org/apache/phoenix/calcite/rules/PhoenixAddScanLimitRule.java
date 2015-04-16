package org.apache.phoenix.calcite.rules;

import java.util.Collections;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.phoenix.calcite.rel.PhoenixLimit;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.calcite.rel.PhoenixServerProject;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;

import com.google.common.base.Predicate;

public class PhoenixAddScanLimitRule extends RelOptRule {

    /** Predicate that returns true if a limit's fetch is stateless. */
    private static final Predicate<PhoenixLimit> IS_FETCH_STATELESS =
        new Predicate<PhoenixLimit>() {
            @Override
            public boolean apply(PhoenixLimit phoenixLimit) {
                return phoenixLimit.statelessFetch != null;
            }
        };

    /** Predicate that returns true if a table scan has no stateless fetch. */
    private static final Predicate<PhoenixTableScan> NO_STATELESSFETCH =
        new Predicate<PhoenixTableScan>() {
            @Override
            public boolean apply(PhoenixTableScan phoenixTableScan) {
                return phoenixTableScan.statelessFetch == null;
            }
        };

    public static final PhoenixAddScanLimitRule LIMIT_SCAN = 
            new PhoenixAddScanLimitRule(
                    "PhoenixAddScanLimitRule:limit_scan", 
                    operand(PhoenixTableScan.class, null, NO_STATELESSFETCH, any()));

    public static final PhoenixAddScanLimitRule LIMIT_SERVERPROJECT_SCAN = 
            new PhoenixAddScanLimitRule(
                    "PhoenixAddScanLimitRule:limit_serverproject_scan", 
                    operand(PhoenixServerProject.class, 
                            operand(PhoenixTableScan.class, null, NO_STATELESSFETCH, any())));

    private PhoenixAddScanLimitRule(String description, RelOptRuleOperand input) {
        super(
            operand(PhoenixLimit.class, null, IS_FETCH_STATELESS, input), description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        int relCount = call.getRelList().size();
        PhoenixLimit limit = call.rel(0);
        PhoenixServerProject project = null;
        if (relCount > 2) {
            project = call.rel(1);
        }
        PhoenixTableScan scan = call.rel(relCount - 1);
        assert limit.statelessFetch != null : "predicate should have ensured fetch is stateless";
        assert scan.statelessFetch == null : "predicate should have ensured table scan has no stateless fetch";
        PhoenixTableScan newScan = new PhoenixTableScan(
                scan.getCluster(), scan.getTraitSet(), scan.getTable(),
                scan.filter, limit.statelessFetch);
        PhoenixRel newInput = project == null ? 
                  newScan 
                : project.copy(project.getTraitSet(), newScan, 
                        project.getProjects(), project.getRowType());
        call.transformTo(limit.copy(limit.getTraitSet(), 
                Collections.<RelNode>singletonList(newInput)));
    }

}
