package org.apache.phoenix.calcite;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCorrelateRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.MaterializedViewFilterScanRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.phoenix.calcite.rel.PhoenixServerProject;
import org.apache.phoenix.calcite.rel.PhoenixServerSort;
import org.apache.phoenix.calcite.rel.PhoenixTemporarySort;
import org.apache.phoenix.calcite.rules.PhoenixFilterScanMergeRule;
import org.apache.phoenix.calcite.rules.PhoenixForwardTableScanRule;
import org.apache.phoenix.calcite.rules.PhoenixJoinSingleValueAggregateMergeRule;
import org.apache.phoenix.calcite.rules.PhoenixMergeSortUnionRule;
import org.apache.phoenix.calcite.rules.PhoenixOrderedAggregateRule;
import org.apache.phoenix.calcite.rules.PhoenixReverseTableScanRule;
import org.apache.phoenix.calcite.rules.PhoenixSortServerJoinTransposeRule;
import org.apache.phoenix.calcite.rules.PhoenixTableScanColumnRefRule;
import org.apache.phoenix.calcite.rules.ProjectLimitTransposeRule;
import org.apache.phoenix.calcite.rules.ProjectSortTransposeRule;
import org.apache.phoenix.calcite.rules.SortSplitRule;

import com.google.common.collect.ImmutableList;

public class PhoenixPrograms {

    public static final RelOptRule[] EXCLUDED_VOLCANO_RULES =
            new RelOptRule[] {
                    AggregateExpandDistinctAggregatesRule.INSTANCE,
                    AggregateJoinTransposeRule.INSTANCE,
                    AggregateReduceFunctionsRule.INSTANCE, // disable
                    //AggregateProjectMergeRule.INSTANCE,
                    FilterAggregateTransposeRule.INSTANCE,
                    FilterCorrelateRule.INSTANCE,
                    FilterJoinRule.FILTER_ON_JOIN,
                    FilterJoinRule.JOIN,
                    FilterMergeRule.INSTANCE,
                    FilterProjectTransposeRule.INSTANCE,
                    JoinCommuteRule.INSTANCE,
                    JoinPushExpressionsRule.INSTANCE,
                    ProjectRemoveRule.INSTANCE,
                    SemiJoinRule.PROJECT,
                    SemiJoinRule.JOIN,
                    SortJoinTransposeRule.INSTANCE,
                    SortUnionTransposeRule.INSTANCE,
                    UnionToDistinctRule.INSTANCE,
                    MaterializedViewFilterScanRule.INSTANCE,
                    EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
            };

    public static final RelOptRule[] EXCLUDED_ENUMERABLE_RULES =
            new RelOptRule[] {
                    EnumerableRules.ENUMERABLE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_SEMI_JOIN_RULE,
                    EnumerableRules.ENUMERABLE_CORRELATE_RULE,
                    EnumerableRules.ENUMERABLE_PROJECT_RULE,
                    EnumerableRules.ENUMERABLE_FILTER_RULE,
                    EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
                    EnumerableRules.ENUMERABLE_SORT_RULE,
                    EnumerableRules.ENUMERABLE_LIMIT_RULE,
                    EnumerableRules.ENUMERABLE_UNION_RULE,
                    EnumerableRules.ENUMERABLE_INTERSECT_RULE,
                    EnumerableRules.ENUMERABLE_MINUS_RULE,
                    EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
                    EnumerableRules.ENUMERABLE_VALUES_RULE,
                    EnumerableRules.ENUMERABLE_WINDOW_RULE,
            };

    public static final RelOptRule[] ADDITIONAL_VOLCANO_RULES =
            new RelOptRule[] {
                    JoinCommuteRule.SWAP_OUTER,
                    new SortProjectTransposeRule(
                            PhoenixTemporarySort.class,
                            PhoenixServerProject.class,
                            "PhoenixTemporarySortProjectTransposeRule"),
                    new SortProjectTransposeRule(
                            PhoenixServerSort.class,
                            PhoenixServerProject.class,
                            "PhoenixServerSortProjectTransposeRule"),
                    PhoenixMergeSortUnionRule.INSTANCE,
                    PhoenixOrderedAggregateRule.INSTANCE,
                    new PhoenixSortServerJoinTransposeRule(
                            PhoenixTemporarySort.class,
                            "PhoenixTemporarySortServerJoinTransposeRule"),
                    new PhoenixSortServerJoinTransposeRule(
                            PhoenixServerSort.class,
                            "PhoenixServerSortServerJoinTransposeRule"),
                    new PhoenixForwardTableScanRule(PhoenixTemporarySort.class),
                    new PhoenixReverseTableScanRule(PhoenixTemporarySort.class),
                    new PhoenixForwardTableScanRule(PhoenixServerSort.class),
                    new PhoenixReverseTableScanRule(PhoenixServerSort.class),
            };

    public static Program standard(
            final RelMetadataProvider metadataProvider,
            final boolean forceDecorrelate) {
        final Program volcanoProgram = new Program() {
            public RelNode run(RelOptPlanner planner, RelNode rel,
                    RelTraitSet requiredOutputTraits,
                    List<RelOptMaterialization> materializations,
                    List<RelOptLattice> lattices) {
                final List<Pair<RelNode, List<RelOptMaterialization>>> materializationUses;
                final Set<RelOptMaterialization> applicableMaterializations;
                final CalciteConnectionConfig config =
                        planner.getContext().unwrap(CalciteConnectionConfig.class);
                if (config != null && config.materializationsEnabled()) {
                    // Transform rels using materialized views.
                    materializationUses =
                            RelOptMaterializations.useMaterializedViews(rel, materializations);

                    // Add not used but potentially useful materialized views to the planner.
                    applicableMaterializations = new HashSet<>(
                            RelOptMaterializations.getApplicableMaterializations(
                                    rel, materializations));
                    for (Pair<RelNode, List<RelOptMaterialization>> use : materializationUses) {
                        applicableMaterializations.removeAll(use.right);
                    }
                } else {
                    materializationUses = Collections.emptyList();
                    applicableMaterializations = Collections.emptySet();
                }

                final List<RelOptRule> rules = planner.getRules();
                // Plan for the original root rel.
                Pair<RelNode, RelOptCost> res = planFor(
                        planner, rel, requiredOutputTraits,
                        rules, applicableMaterializations);
                RelNode bestRel = res.left;
                RelOptCost bestCost = res.right;
                // Plan for transformed rels using materialized views and update the
                // best cost as needed.
                for (Pair<RelNode, List<RelOptMaterialization>> m : materializationUses) {
                    try {
                        final RelNode rel2 = m.left;
                        Hook.SUB.run(rel2);
                        res = planFor(planner, rel2, requiredOutputTraits,
                                rules, applicableMaterializations);
                        if (res.right.isLt(bestCost)) {
                            bestRel = res.left;
                            bestCost = res.right;
                        }
                    } catch (RelOptPlanner.CannotPlanException e) {
                        // Ignore
                    }
                }
                return bestRel;
            }

            private Pair<RelNode, RelOptCost> planFor(
                    RelOptPlanner planner, RelNode rel,
                    RelTraitSet requiredOutputTraits,
                    List<RelOptRule> rules,
                    Collection<RelOptMaterialization> materializations) {
                final Program preProgram = pre(
                        metadataProvider, forceDecorrelate);
                rel = preProgram.run(planner, rel, requiredOutputTraits,
                        Collections.<RelOptMaterialization>emptyList(),
                        Collections.<RelOptLattice>emptyList());
                planner.clear();
                for (RelOptRule rule : rules) {
                    planner.addRule(rule);
                }
                for (RelOptMaterialization m : materializations) {
                    planner.addMaterialization(m);
                }

                final RelNode rootRel2 =
                        rel.getTraitSet().equals(requiredOutputTraits)
                        ? rel
                                : planner.changeTraits(rel, requiredOutputTraits);
                assert rootRel2 != null;

                planner.setRoot(rootRel2);
                final RelOptPlanner planner2 = planner.chooseDelegate();
                final RelNode rootRel3 = planner2.findBestExp();
                final RelMetadataQuery mq = RelMetadataQuery.instance();
                final RelOptCost cost = planner2.getCost(rootRel3, mq);

                return Pair.of(rootRel3, cost);
            }
        };

        return Programs.sequence(
                volcanoProgram,
                // Second planner pass to do physical "tweaks". This the first time that
                // EnumerableCalcRel is introduced.
                Programs.calc(metadataProvider));
    }

    public static Program pre(
            RelMetadataProvider metadataProvider, boolean forceDecorrelate) {
        return Programs.sequence(
                subquery(metadataProvider),
                decorrelate(forceDecorrelate),
                trimFields(),
                prePlanning(metadataProvider));        
    }
    
    protected static Program subquery(RelMetadataProvider metadataProvider) {
        return Programs.hep(
                ImmutableList.of((RelOptRule) SubQueryRemoveRule.FILTER,
                        SubQueryRemoveRule.PROJECT,
                        SubQueryRemoveRule.JOIN), true, metadataProvider);
    }

    protected static Program decorrelate(final boolean forceDecorrelate) {
        return new Program() {
            public RelNode run(RelOptPlanner planner, RelNode rel,
                    RelTraitSet requiredOutputTraits,
                    List<RelOptMaterialization> materializations,
                    List<RelOptLattice> lattices) {
                if (forceDecorrelate) {
                    return RelDecorrelator.decorrelateQuery(rel);
                }
                return rel;
            }
        };
    }

    protected static Program trimFields() {
        return new Program() {
            public RelNode run(RelOptPlanner planner, RelNode rel,
                    RelTraitSet requiredOutputTraits,
                    List<RelOptMaterialization> materializations,
                    List<RelOptLattice> lattices) {
                final RelBuilder relBuilder =
                        RelFactories.LOGICAL_BUILDER.create(
                                rel.getCluster(), null);
                return new RelFieldTrimmer(null, relBuilder).trim(rel);
            }
        };
    }

    protected static Program prePlanning(RelMetadataProvider metadataProvider) {
        final Program filterPushdown = repeat(
                Programs.hep(
                        ImmutableList.of(
                                FilterCorrelateRule.INSTANCE,
                                FilterJoinRule.FILTER_ON_JOIN,
                                FilterJoinRule.JOIN,
                                FilterProjectTransposeRule.INSTANCE,
                                FilterAggregateTransposeRule.INSTANCE,
                                FilterMergeRule.INSTANCE),
                        true, metadataProvider));
        final Program filterMerge = Programs.hep(
                ImmutableList.of(PhoenixFilterScanMergeRule.INSTANCE),
                true, metadataProvider);
        final Program sortPushdown = Programs.sequence(
                    repeat(
                            Programs.hep(
                                    ImmutableList.of(
                                            SortProjectTransposeRule.INSTANCE,
                                            SortJoinTransposeRule.INSTANCE,
                                            SortUnionTransposeRule.MATCH_NULL_FETCH),
                                    true, metadataProvider)),
                    Programs.hep(
                            ImmutableList.of(
                                    SortSplitRule.INSTANCE),
                            true, metadataProvider),
                    // SortUnionTransposeRule does not work with offset != null, so
                    // we should give it one more try after SortSplitRule.
                    repeat(
                            Programs.hep(
                                    ImmutableList.of(
                                            SortUnionTransposeRule.MATCH_NULL_FETCH,
                                            SortProjectTransposeRule.INSTANCE),
                                    true, metadataProvider)));
        final Program sortMerge = Programs.hep(
                ImmutableList.of(
                        new PhoenixForwardTableScanRule(LogicalSort.class),
                        new PhoenixReverseTableScanRule(LogicalSort.class)),
                true, metadataProvider);
        final Program projectPushdown = repeat(
                Programs.hep(
                        ImmutableList.of(
                                ProjectLimitTransposeRule.INSTANCE,
                                ProjectSortTransposeRule.INSTANCE),
                        true, metadataProvider));
        final Program projectMerge = Programs.hep(
                ImmutableList.of(
                        ProjectMergeRule.INSTANCE,
                        PhoenixTableScanColumnRefRule.INSTANCE,
                        ProjectRemoveRule.INSTANCE,
                        AggregateProjectMergeRule.INSTANCE),
                true, metadataProvider);
        final Program misc1 = Programs.hep(
                ImmutableList.of(
                        ProjectRemoveRule.INSTANCE,
                        PhoenixJoinSingleValueAggregateMergeRule.INSTANCE,
                        AggregateJoinTransposeRule.INSTANCE,
                        JoinPushExpressionsRule.INSTANCE,
                        //PhoenixJoinCommuteRule.INSTANCE,
                        UnionToDistinctRule.INSTANCE),
                true, metadataProvider);
        final Program misc2 = Programs.hep(
                ImmutableList.of(
                        SemiJoinRule.PROJECT,
                        SemiJoinRule.JOIN),
                true, metadataProvider);
        return Programs.sequence(
                misc1,
                filterPushdown,
                filterMerge,
                sortPushdown,
                sortMerge,
                projectPushdown,
                projectMerge,
                misc2);
    }

    private static Program repeat(final Program program) {
        return new Program() {
            @Override
            public RelNode run(RelOptPlanner planner, RelNode rel,
                    RelTraitSet requiredOutputTraits,
                    List<RelOptMaterialization> materializations,
                    List<RelOptLattice> lattices) {
                RelNode oldRel = rel;
                rel = program.run(planner, oldRel,
                        requiredOutputTraits, materializations, lattices);
                while (!RelOptUtil.toString(oldRel).equals(RelOptUtil.toString(rel))) {
                    oldRel = rel;
                    rel = program.run(planner, oldRel,
                            requiredOutputTraits, materializations, lattices);
                }
                return rel;
            }            
        };
    }
}
