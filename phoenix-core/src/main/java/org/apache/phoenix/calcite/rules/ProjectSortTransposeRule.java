package org.apache.phoenix.calcite.rules;

import java.util.Map;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

public class ProjectSortTransposeRule extends RelOptRule {

    public static final ProjectSortTransposeRule INSTANCE =
            new ProjectSortTransposeRule();

    private ProjectSortTransposeRule() {
        super(
                operand(LogicalProject.class,
                        operand(LogicalSort.class, any())));
    }

    public void onMatch(RelOptRuleCall call) {
        final Project project = call.rel(0);
        final Sort sort = call.rel(1);
        final RelNode input = sort.getInput();

        Map<Integer, Integer> targets = Maps.newHashMap();
        for (Ord<? extends RexNode> exp : Ord.zip(project.getProjects())) {
            if (exp.e instanceof RexInputRef) {
              targets.putIfAbsent(((RexInputRef) exp.e).getIndex(), exp.i);
            }
        }
        ImmutableList.Builder<RelFieldCollation> builder = ImmutableList.builder();
        for (RelFieldCollation field : sort.getCollation().getFieldCollations()) {
            int index = field.getFieldIndex();
            Integer newIndex = targets.get(index);
            if (newIndex == null) {
                return;
            }
            builder.add(new RelFieldCollation(newIndex, field.direction, field.nullDirection));
        }
        RelCollation newCollation =
                RelCollationTraitDef.INSTANCE.canonize(
                        RelCollations.of(builder.build()));
        RelNode newProject =
                project.copy(
                        input.getTraitSet().replace(project.getConvention()),
                        ImmutableList.of(input));
        final Sort newSort =
                sort.copy(
                        sort.getTraitSet().replace(newCollation),
                        newProject,
                        newCollation,
                        sort.offset,
                        sort.fetch);
        call.transformTo(newSort);
    }
}
