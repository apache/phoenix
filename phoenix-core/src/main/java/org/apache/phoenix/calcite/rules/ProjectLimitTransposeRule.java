package org.apache.phoenix.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.phoenix.calcite.rel.Limit;
import org.apache.phoenix.calcite.rel.LogicalLimit;

import com.google.common.collect.ImmutableList;

public class ProjectLimitTransposeRule extends RelOptRule {

    public static final ProjectLimitTransposeRule INSTANCE =
            new ProjectLimitTransposeRule();

    private ProjectLimitTransposeRule() {
        super(
                operand(LogicalProject.class,
                        operand(LogicalLimit.class, any())));
    }

    public void onMatch(RelOptRuleCall call) {
        final Project project = call.rel(0);
        final Limit limit = call.rel(1);
        RelNode newProject =
                project.copy(
                        project.getTraitSet(),
                        ImmutableList.of(limit.getInput()));
        final Limit newLimit =
                limit.copy(
                        limit.getTraitSet(),
                        ImmutableList.of(newProject));
        call.transformTo(newLimit);
    }
}
