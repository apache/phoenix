package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.phoenix.calcite.rel.PhoenixClientProject;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.calcite.rel.PhoenixServerJoin;
import org.apache.phoenix.calcite.rel.PhoenixServerProject;
import org.apache.phoenix.calcite.rel.PhoenixTableScan;

public class PhoenixServerProjectRule extends RelOptRule {
    
    public static final PhoenixServerProjectRule PROJECT_SCAN = 
            new PhoenixServerProjectRule("PhoenixServerProjectRule:project_scan", PhoenixTableScan.class);
    
    public static final PhoenixServerProjectRule PROJECT_SERVERJOIN = 
            new PhoenixServerProjectRule("PhoenixServerProjectRule:project_serverjoin", PhoenixServerJoin.class);

    public PhoenixServerProjectRule(String description, Class<? extends PhoenixRel> clazz) {
        super(
            operand(PhoenixClientProject.class,
                    operand(clazz, any())),
            description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PhoenixClientProject project = call.rel(0);
        PhoenixRel input = call.rel(1);
        call.transformTo(new PhoenixServerProject(project.getCluster(),
                project.getTraitSet(), input, project.getProjects(), project.getRowType()));
    }

}
