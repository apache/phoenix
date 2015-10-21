package org.apache.phoenix.calcite.rules;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.trace.CalciteTrace;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.metadata.PhoenixRelMdCollation;
import org.apache.phoenix.calcite.rel.PhoenixAbstractAggregate;
import org.apache.phoenix.calcite.rel.PhoenixClientAggregate;
import org.apache.phoenix.calcite.rel.PhoenixClientJoin;
import org.apache.phoenix.calcite.rel.PhoenixClientProject;
import org.apache.phoenix.calcite.rel.PhoenixClientSemiJoin;
import org.apache.phoenix.calcite.rel.PhoenixClientSort;
import org.apache.phoenix.calcite.rel.PhoenixCorrelate;
import org.apache.phoenix.calcite.rel.PhoenixFilter;
import org.apache.phoenix.calcite.rel.PhoenixLimit;
import org.apache.phoenix.calcite.rel.PhoenixMergeSortUnion;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.calcite.rel.PhoenixServerAggregate;
import org.apache.phoenix.calcite.rel.PhoenixServerJoin;
import org.apache.phoenix.calcite.rel.PhoenixServerProject;
import org.apache.phoenix.calcite.rel.PhoenixServerSemiJoin;
import org.apache.phoenix.calcite.rel.PhoenixServerSort;
import org.apache.phoenix.calcite.rel.PhoenixToClientConverter;
import org.apache.phoenix.calcite.rel.PhoenixToEnumerableConverter;
import org.apache.phoenix.calcite.rel.PhoenixUncollect;
import org.apache.phoenix.calcite.rel.PhoenixUnion;
import org.apache.phoenix.calcite.rel.PhoenixValues;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

/**
 * Rules and relational operators for
 * {@link PhoenixRel#CONVENTION PHOENIX}
 * calling convention.
 */
public class PhoenixConverterRules {
    private PhoenixConverterRules() {}

    protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

    public static final RelOptRule[] RULES = {
        PhoenixToEnumerableConverterRule.INSTANCE,
        PhoenixServerToClientConverterRule.INSTANCE,
        PhoenixProjectableToClientConverterRule.INSTANCE,
        PhoenixClientSortRule.INSTANCE,
        PhoenixServerSortRule.SERVER,
        PhoenixServerSortRule.SERVERJOIN,
        PhoenixLimitRule.INSTANCE,
        PhoenixFilterRule.INSTANCE,
        PhoenixClientProjectRule.INSTANCE,
        PhoenixServerProjectRule.INSTANCE,
        PhoenixClientAggregateRule.INSTANCE,
        PhoenixServerAggregateRule.SERVER,
        PhoenixServerAggregateRule.SERVERJOIN,
        PhoenixUnionRule.INSTANCE,
        PhoenixMergeSortUnionRule.INSTANCE,
        PhoenixClientJoinRule.INSTANCE,
        PhoenixServerJoinRule.INSTANCE,
        PhoenixClientSemiJoinRule.INSTANCE,
        PhoenixServerSemiJoinRule.INSTANCE,
        PhoenixValuesRule.INSTANCE,
        PhoenixUncollectRule.INSTANCE,
        PhoenixCorrelateRule.INSTANCE,
    };

    public static final RelOptRule[] CONVERTIBLE_RULES = {
        PhoenixToEnumerableConverterRule.INSTANCE,
        PhoenixServerToClientConverterRule.INSTANCE,
        PhoenixProjectableToClientConverterRule.INSTANCE,
        PhoenixClientSortRule.CONVERTIBLE,
        PhoenixServerSortRule.CONVERTIBLE_SERVER,
        PhoenixServerSortRule.CONVERTIBLE_SERVERJOIN,
        PhoenixLimitRule.CONVERTIBLE,
        PhoenixFilterRule.CONVERTIBLE,
        PhoenixClientProjectRule.CONVERTIBLE,
        PhoenixServerProjectRule.CONVERTIBLE,
        PhoenixClientAggregateRule.CONVERTIBLE,
        PhoenixServerAggregateRule.CONVERTIBLE_SERVER,
        PhoenixServerAggregateRule.CONVERTIBLE_SERVERJOIN,
        PhoenixUnionRule.CONVERTIBLE,
        PhoenixMergeSortUnionRule.CONVERTIBLE,
        PhoenixClientJoinRule.CONVERTIBLE,
        PhoenixServerJoinRule.CONVERTIBLE,
        PhoenixClientSemiJoinRule.INSTANCE,
        PhoenixServerSemiJoinRule.INSTANCE,
        PhoenixValuesRule.INSTANCE,
        PhoenixUncollectRule.INSTANCE,
        PhoenixCorrelateRule.INSTANCE,
    };

    /** Base class for planner rules that convert a relational expression to
     * Phoenix calling convention. */
    abstract static class PhoenixConverterRule extends ConverterRule {
        protected final Convention out;
        public PhoenixConverterRule(
                Class<? extends RelNode> clazz,
                RelTrait in,
                Convention out,
                String description) {
            super(clazz, in, out, description);
            this.out = out;
        }
        
        public <R extends RelNode> PhoenixConverterRule(
                Class<R> clazz,
                Predicate<? super R> predicate,
                RelTrait in,
                Convention out,
                String description) {
            super(clazz, predicate, in, out, description);
            this.out = out;
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
     * {@link PhoenixClientSort}.
     */
    public static class PhoenixClientSortRule extends PhoenixConverterRule {
        
        private static Predicate<LogicalSort> IS_CONVERTIBLE = new Predicate<LogicalSort>() {
            @Override
            public boolean apply(LogicalSort input) {
                return isConvertible(input);
            }            
        };
        
        private static Predicate<LogicalSort> SORT_ONLY = new Predicate<LogicalSort>() {
            @Override
            public boolean apply(LogicalSort input) {
                return !input.getCollation().getFieldCollations().isEmpty()
                        && input.offset == null
                        && input.fetch == null;
            }            
        };
        
        public static final PhoenixClientSortRule INSTANCE = new PhoenixClientSortRule(SORT_ONLY);
        public static final PhoenixClientSortRule CONVERTIBLE = new PhoenixClientSortRule(Predicates.and(Arrays.asList(SORT_ONLY, IS_CONVERTIBLE)));

        private PhoenixClientSortRule(Predicate<LogicalSort> predicate) {
            super(LogicalSort.class, 
                    predicate, 
                    Convention.NONE, PhoenixRel.CLIENT_CONVENTION, "PhoenixClientSortRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalSort sort = (LogicalSort) rel;
            return PhoenixClientSort.create(
                convert(
                        sort.getInput(), 
                        sort.getInput().getTraitSet().replace(PhoenixRel.CLIENT_CONVENTION)),
                sort.getCollation());
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
     * {@link PhoenixServerSort}.
     */
    public static class PhoenixServerSortRule extends PhoenixConverterRule {
        
        private static Predicate<LogicalSort> IS_CONVERTIBLE = new Predicate<LogicalSort>() {
            @Override
            public boolean apply(LogicalSort input) {
                return isConvertible(input);
            }            
        };
        
        private static Predicate<LogicalSort> SORT_ONLY = new Predicate<LogicalSort>() {
            @Override
            public boolean apply(LogicalSort input) {
                return !input.getCollation().getFieldCollations().isEmpty()
                        && input.offset == null
                        && input.fetch == null;
            }            
        };
        
        public static final PhoenixServerSortRule SERVER = new PhoenixServerSortRule(SORT_ONLY, PhoenixRel.SERVER_CONVENTION);
        public static final PhoenixServerSortRule SERVERJOIN = new PhoenixServerSortRule(SORT_ONLY, PhoenixRel.SERVERJOIN_CONVENTION);

        public static final PhoenixServerSortRule CONVERTIBLE_SERVER = new PhoenixServerSortRule(Predicates.and(Arrays.asList(SORT_ONLY, IS_CONVERTIBLE)), PhoenixRel.SERVER_CONVENTION);
        public static final PhoenixServerSortRule CONVERTIBLE_SERVERJOIN = new PhoenixServerSortRule(Predicates.and(Arrays.asList(SORT_ONLY, IS_CONVERTIBLE)), PhoenixRel.SERVERJOIN_CONVENTION);

        private final Convention inputConvention;

        private PhoenixServerSortRule(Predicate<LogicalSort> predicate, Convention inputConvention) {
            super(LogicalSort.class, 
                    predicate, 
                    Convention.NONE, PhoenixRel.CLIENT_CONVENTION, "PhoenixServerSortRule:" + inputConvention.getName());
            this.inputConvention = inputConvention;
        }

        public RelNode convert(RelNode rel) {
            final LogicalSort sort = (LogicalSort) rel;
            return PhoenixServerSort.create(
                convert(
                        sort.getInput(), 
                        sort.getInput().getTraitSet().replace(inputConvention)),
                sort.getCollation());
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
     * {@link PhoenixLimit}.
     */
    public static class PhoenixLimitRule extends PhoenixConverterRule {
        private static Predicate<LogicalSort> IS_CONVERTIBLE = new Predicate<LogicalSort>() {
            @Override
            public boolean apply(LogicalSort input) {
                return isConvertible(input);
            }            
        };
        private static Predicate<LogicalSort> OFFSET_OR_FETCH = new Predicate<LogicalSort>() {
            @Override
            public boolean apply(LogicalSort input) {
                return input.offset != null 
                        || input.fetch != null;
            }            
        };
        
        public static final PhoenixLimitRule INSTANCE = new PhoenixLimitRule(OFFSET_OR_FETCH);

        public static final PhoenixLimitRule CONVERTIBLE = new PhoenixLimitRule(Predicates.and(Arrays.asList(OFFSET_OR_FETCH, IS_CONVERTIBLE)));

        private PhoenixLimitRule(Predicate<LogicalSort> predicate) {
            super(LogicalSort.class, 
                    predicate, 
                    Convention.NONE, PhoenixRel.CLIENT_CONVENTION, "PhoenixLimitRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalSort sort = (LogicalSort) rel;
            RelNode input = sort.getInput();
            if (!sort.getCollation().getFieldCollations().isEmpty()) {
                input = sort.copy(
                            sort.getTraitSet(), 
                            sort.getInput(), 
                            sort.getCollation(), 
                            null, null);
            }
            return PhoenixLimit.create(
                convert(
                        input, 
                        input.getTraitSet().replace(out)),
                sort.offset, 
                sort.fetch);
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to a
     * {@link PhoenixFilter}.
     */
    public static class PhoenixFilterRule extends PhoenixConverterRule {
        protected static Predicate<LogicalFilter> IS_CONVERTIBLE = new Predicate<LogicalFilter>() {
            @Override
            public boolean apply(LogicalFilter input) {
                return isConvertible(input);
            }            
        };
        
        private static final PhoenixFilterRule INSTANCE = new PhoenixFilterRule(Predicates.<LogicalFilter>alwaysTrue());

        private static final PhoenixFilterRule CONVERTIBLE = new PhoenixFilterRule(IS_CONVERTIBLE);

        private PhoenixFilterRule(Predicate<LogicalFilter> predicate) {
            super(LogicalFilter.class, predicate, Convention.NONE, 
                    PhoenixRel.CLIENT_CONVENTION, "PhoenixFilterRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalFilter filter = (LogicalFilter) rel;
            return PhoenixFilter.create(
                convert(
                        filter.getInput(), 
                        filter.getInput().getTraitSet().replace(out)),
                filter.getCondition());
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
     * to a {@link PhoenixClientProject}.
     */
    public static class PhoenixClientProjectRule extends PhoenixConverterRule {
        
        private static Predicate<LogicalProject> IS_CONVERTIBLE = new Predicate<LogicalProject>() {
            @Override
            public boolean apply(LogicalProject input) {
                return isConvertible(input);
            }            
        };
        
        private static final PhoenixClientProjectRule INSTANCE = new PhoenixClientProjectRule(Predicates.<LogicalProject>alwaysTrue());

        private static final PhoenixClientProjectRule CONVERTIBLE = new PhoenixClientProjectRule(IS_CONVERTIBLE);

        private PhoenixClientProjectRule(Predicate<LogicalProject> predicate) {
            super(LogicalProject.class, predicate, Convention.NONE, 
                    PhoenixRel.CLIENT_CONVENTION, "PhoenixClientProjectRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalProject project = (LogicalProject) rel;
            return PhoenixClientProject.create(
                convert(
                        project.getInput(), 
                        project.getInput().getTraitSet().replace(PhoenixRel.CLIENT_CONVENTION)), 
                project.getProjects(),
                project.getRowType());
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
     * to a {@link PhoenixServerProject}.
     */
    public static class PhoenixServerProjectRule extends PhoenixConverterRule {
        
        private static Predicate<LogicalProject> IS_CONVERTIBLE = new Predicate<LogicalProject>() {
            @Override
            public boolean apply(LogicalProject input) {
                return isConvertible(input);
            }            
        };
        
        private static final PhoenixServerProjectRule INSTANCE = new PhoenixServerProjectRule(Predicates.<LogicalProject>alwaysTrue());

        private static final PhoenixServerProjectRule CONVERTIBLE = new PhoenixServerProjectRule(IS_CONVERTIBLE);

        private PhoenixServerProjectRule(Predicate<LogicalProject> predicate) {
            super(LogicalProject.class, predicate, Convention.NONE, 
                    PhoenixRel.SERVER_CONVENTION, "PhoenixServerProjectRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalProject project = (LogicalProject) rel;
            return PhoenixServerProject.create(
                convert(
                        project.getInput(), 
                        project.getInput().getTraitSet().replace(PhoenixRel.SERVER_CONVENTION)), 
                project.getProjects(),
                project.getRowType());
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalAggregate}
     * to an {@link PhoenixClientAggregate}.
     */
    public static class PhoenixClientAggregateRule extends PhoenixConverterRule {
        
        private static Predicate<LogicalAggregate> IS_CONVERTIBLE = new Predicate<LogicalAggregate>() {
            @Override
            public boolean apply(LogicalAggregate input) {
                return isConvertible(input);
            }            
        };
        
        public static final RelOptRule INSTANCE = new PhoenixClientAggregateRule(Predicates.<LogicalAggregate>alwaysTrue());

        public static final RelOptRule CONVERTIBLE = new PhoenixClientAggregateRule(IS_CONVERTIBLE);

        private PhoenixClientAggregateRule(Predicate<LogicalAggregate> predicate) {
            super(LogicalAggregate.class, predicate, Convention.NONE, 
                    PhoenixRel.CLIENT_CONVENTION, "PhoenixClientAggregateRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalAggregate agg = (LogicalAggregate) rel;
            return PhoenixClientAggregate.create(
                    convert(
                            agg.getInput(), 
                            agg.getInput().getTraitSet().replace(PhoenixRel.CLIENT_CONVENTION)),
                    agg.indicator,
                    agg.getGroupSet(),
                    agg.getGroupSets(),
                    agg.getAggCallList());
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalAggregate}
     * to an {@link PhoenixServerAggregate}.
     */
    public static class PhoenixServerAggregateRule extends PhoenixConverterRule {
        
        private static Predicate<LogicalAggregate> IS_CONVERTIBLE = new Predicate<LogicalAggregate>() {
            @Override
            public boolean apply(LogicalAggregate input) {
                return isConvertible(input);
            }            
        };
        
        public static final RelOptRule SERVER = new PhoenixServerAggregateRule(Predicates.<LogicalAggregate>alwaysTrue(), PhoenixRel.SERVER_CONVENTION);
        public static final RelOptRule SERVERJOIN = new PhoenixServerAggregateRule(Predicates.<LogicalAggregate>alwaysTrue(), PhoenixRel.SERVERJOIN_CONVENTION);

        public static final RelOptRule CONVERTIBLE_SERVER = new PhoenixServerAggregateRule(IS_CONVERTIBLE, PhoenixRel.SERVER_CONVENTION);
        public static final RelOptRule CONVERTIBLE_SERVERJOIN = new PhoenixServerAggregateRule(IS_CONVERTIBLE, PhoenixRel.SERVERJOIN_CONVENTION);
        
        private final Convention inputConvention;

        private PhoenixServerAggregateRule(Predicate<LogicalAggregate> predicate, Convention inputConvention) {
            super(LogicalAggregate.class, predicate, Convention.NONE, 
                    PhoenixRel.CLIENT_CONVENTION, "PhoenixServerAggregateRule:" + inputConvention.getName());
            this.inputConvention = inputConvention;
        }

        public RelNode convert(RelNode rel) {
            final LogicalAggregate agg = (LogicalAggregate) rel;
            return PhoenixServerAggregate.create(
                    convert(
                            agg.getInput(), 
                            agg.getInput().getTraitSet().replace(inputConvention)),
                    agg.indicator,
                    agg.getGroupSet(),
                    agg.getGroupSets(),
                    agg.getAggCallList());
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Union} to a
     * {@link PhoenixUnion}.
     */
    public static class PhoenixUnionRule extends PhoenixConverterRule {
        private static Predicate<LogicalUnion> IS_CONVERTIBLE = new Predicate<LogicalUnion>() {
            @Override
            public boolean apply(LogicalUnion input) {
                return isConvertible(input);
            }            
        };
        
        public static final PhoenixUnionRule INSTANCE = new PhoenixUnionRule(Predicates.<LogicalUnion>alwaysTrue());
        
        public static final PhoenixUnionRule CONVERTIBLE = new PhoenixUnionRule(IS_CONVERTIBLE);

        private PhoenixUnionRule(Predicate<LogicalUnion> predicate) {
            super(LogicalUnion.class, predicate, Convention.NONE, 
                    PhoenixRel.CLIENT_CONVENTION, "PhoenixUnionRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalUnion union = (LogicalUnion) rel;
            return PhoenixUnion.create(
                    convertList(union.getInputs(), out),
                    union.all);
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Union} to a
     * {@link PhoenixMergeSortUnion}.
     */
    public static class PhoenixMergeSortUnionRule extends PhoenixConverterRule {
        private static Predicate<LogicalUnion> IS_CONVERTIBLE = new Predicate<LogicalUnion>() {
            @Override
            public boolean apply(LogicalUnion input) {
                return isConvertible(input);
            }            
        };
        
        private static Predicate<LogicalUnion> NON_EMPTY_COLLATION = new Predicate<LogicalUnion>() {
			@Override
			public boolean apply(LogicalUnion input) {
				List<RelCollation> collations = PhoenixRelMdCollation.mergeSortUnion(input.getInputs(), input.all);
				return collations.size() == 1 && !collations.get(0).getFieldCollations().isEmpty();
			}
        };
        
        public static final PhoenixMergeSortUnionRule INSTANCE = new PhoenixMergeSortUnionRule(NON_EMPTY_COLLATION);
        
        public static final PhoenixMergeSortUnionRule CONVERTIBLE = new PhoenixMergeSortUnionRule(Predicates.and(IS_CONVERTIBLE, NON_EMPTY_COLLATION));

        private PhoenixMergeSortUnionRule(Predicate<LogicalUnion> predicate) {
            super(LogicalUnion.class, predicate, Convention.NONE, 
                    PhoenixRel.CLIENT_CONVENTION, "PhoenixMergeSortUnionRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalUnion union = (LogicalUnion) rel;
            return PhoenixMergeSortUnion.create(
                    convertList(union.getInputs(), out),
                    union.all);
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Join} to a
     * {@link PhoenixClientJoin}.
     */
    public static class PhoenixClientJoinRule extends PhoenixConverterRule {
        
        private static Predicate<LogicalJoin> IS_CONVERTIBLE = new Predicate<LogicalJoin>() {
            @Override
            public boolean apply(LogicalJoin input) {
                return isConvertible(input);
            }            
        };

        private static final Predicate<LogicalJoin> NO_RIGHT_JOIN = new Predicate<LogicalJoin>() {
            @Override
            public boolean apply(LogicalJoin input) {
                return input.getJoinType() != JoinRelType.RIGHT;
            }
        };
        
        public static final PhoenixClientJoinRule INSTANCE = new PhoenixClientJoinRule(NO_RIGHT_JOIN);

        public static final PhoenixClientJoinRule CONVERTIBLE = new PhoenixClientJoinRule(Predicates.and(Arrays.asList(IS_CONVERTIBLE, NO_RIGHT_JOIN)));

        private PhoenixClientJoinRule(Predicate<LogicalJoin> predicate) {
            super(LogicalJoin.class, predicate, Convention.NONE, 
                    PhoenixRel.CLIENT_CONVENTION, "PhoenixClientJoinRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalJoin join = (LogicalJoin) rel;
            RelNode left = join.getLeft();
            RelNode right = join.getRight();
            
            JoinInfo joinInfo = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());
            if (!joinInfo.leftKeys.isEmpty()) {
                List<RelFieldCollation> leftFieldCollations = Lists.newArrayList();
                for (Iterator<Integer> iter = joinInfo.leftKeys.iterator(); iter.hasNext();) {
                    leftFieldCollations.add(new RelFieldCollation(iter.next(), Direction.ASCENDING));
                }
                RelCollation leftCollation = RelCollations.of(leftFieldCollations);
                left = LogicalSort.create(left, leftCollation, null, null);
                
                List<RelFieldCollation> rightFieldCollations = Lists.newArrayList();
                for (Iterator<Integer> iter = joinInfo.rightKeys.iterator(); iter.hasNext();) {
                    rightFieldCollations.add(new RelFieldCollation(iter.next(), Direction.ASCENDING));
                }
                RelCollation rightCollation = RelCollations.of(rightFieldCollations);
                right = LogicalSort.create(right, rightCollation, null, null);
            }
            
            return PhoenixClientJoin.create(
                    convert(
                            left, 
                            left.getTraitSet().replace(PhoenixRel.CLIENT_CONVENTION)),
                    convert(
                            right, 
                            right.getTraitSet().replace(PhoenixRel.CLIENT_CONVENTION)),
                    join.getCondition(),
                    join.getJoinType(),
                    join.getVariablesStopped(),
                    false);
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Join} to a
     * {@link PhoenixServerJoin}.
     */
    public static class PhoenixServerJoinRule extends PhoenixConverterRule {
        
        private static Predicate<LogicalJoin> IS_CONVERTIBLE = new Predicate<LogicalJoin>() {
            @Override
            public boolean apply(LogicalJoin input) {
                return isConvertible(input);
            }            
        };

        private static final Predicate<LogicalJoin> NO_RIGHT_OR_FULL_JOIN = new Predicate<LogicalJoin>() {
            @Override
            public boolean apply(LogicalJoin input) {
                return input.getJoinType() != JoinRelType.RIGHT && input.getJoinType() != JoinRelType.FULL;
            }
        };
        
        public static final PhoenixServerJoinRule INSTANCE = new PhoenixServerJoinRule(NO_RIGHT_OR_FULL_JOIN);

        public static final PhoenixServerJoinRule CONVERTIBLE = new PhoenixServerJoinRule(Predicates.and(Arrays.asList(IS_CONVERTIBLE, NO_RIGHT_OR_FULL_JOIN)));

        private PhoenixServerJoinRule(Predicate<LogicalJoin> predicate) {
            super(LogicalJoin.class, predicate, Convention.NONE, 
                    PhoenixRel.SERVERJOIN_CONVENTION, "PhoenixServerJoinRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalJoin join = (LogicalJoin) rel;
            return PhoenixServerJoin.create(
                    convert(
                            join.getLeft(), 
                            join.getLeft().getTraitSet().replace(PhoenixRel.SERVER_CONVENTION)),
                    convert(
                            join.getRight(), 
                            join.getRight().getTraitSet().replace(PhoenixRel.CLIENT_CONVENTION)),
                    join.getCondition(),
                    join.getJoinType(),
                    join.getVariablesStopped(),
                    false);
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.SemiJoin} to a
     * {@link PhoenixClientSemiJoin}.
     */
    public static class PhoenixClientSemiJoinRule extends PhoenixConverterRule {
        
        public static final PhoenixClientSemiJoinRule INSTANCE = new PhoenixClientSemiJoinRule();

        private PhoenixClientSemiJoinRule() {
            super(SemiJoin.class, Convention.NONE, 
                    PhoenixRel.CLIENT_CONVENTION, "PhoenixClientSemiJoinRule");
        }

        public RelNode convert(RelNode rel) {
            final SemiJoin join = (SemiJoin) rel;
            RelNode left = join.getLeft();
            RelNode right = join.getRight();
            
            JoinInfo joinInfo = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());
            if (!joinInfo.leftKeys.isEmpty()) {
                List<RelFieldCollation> leftFieldCollations = Lists.newArrayList();
                for (Iterator<Integer> iter = joinInfo.leftKeys.iterator(); iter.hasNext();) {
                    leftFieldCollations.add(new RelFieldCollation(iter.next(), Direction.ASCENDING));
                }
                RelCollation leftCollation = RelCollations.of(leftFieldCollations);
                left = LogicalSort.create(left, leftCollation, null, null);
                
                List<RelFieldCollation> rightFieldCollations = Lists.newArrayList();
                for (Iterator<Integer> iter = joinInfo.rightKeys.iterator(); iter.hasNext();) {
                    rightFieldCollations.add(new RelFieldCollation(iter.next(), Direction.ASCENDING));
                }
                RelCollation rightCollation = RelCollations.of(rightFieldCollations);
                right = LogicalSort.create(right, rightCollation, null, null);
            }
            
            return PhoenixClientSemiJoin.create(
                    convert(
                            left, 
                            left.getTraitSet().replace(PhoenixRel.CLIENT_CONVENTION)),
                    convert(
                            right, 
                            right.getTraitSet().replace(PhoenixRel.CLIENT_CONVENTION)),
                    join.getCondition());
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.SemiJoin} to a
     * {@link PhoenixServerSemiJoin}.
     */
    public static class PhoenixServerSemiJoinRule extends PhoenixConverterRule {
        
        public static final PhoenixServerSemiJoinRule INSTANCE = new PhoenixServerSemiJoinRule();

        private PhoenixServerSemiJoinRule() {
            super(SemiJoin.class, Convention.NONE, 
                    PhoenixRel.SERVERJOIN_CONVENTION, "PhoenixServerSemiJoinRule");
        }

        public RelNode convert(RelNode rel) {
            final SemiJoin join = (SemiJoin) rel;
            return PhoenixServerSemiJoin.create(
                    convert(
                            join.getLeft(), 
                            join.getLeft().getTraitSet().replace(PhoenixRel.SERVER_CONVENTION)),
                    convert(
                            join.getRight(), 
                            join.getRight().getTraitSet().replace(PhoenixRel.CLIENT_CONVENTION)),
                    join.getCondition());
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Values} to a
     * {@link PhoenixValues}.
     */
    public static class PhoenixValuesRule extends PhoenixConverterRule {
        public static PhoenixValuesRule INSTANCE = new PhoenixValuesRule();
        
        private PhoenixValuesRule() {
            super(LogicalValues.class, Convention.NONE, PhoenixRel.CLIENT_CONVENTION, "PhoenixValuesRule");
        }

        @Override public RelNode convert(RelNode rel) {
            LogicalValues valuesRel = (LogicalValues) rel;
            return PhoenixValues.create(
                    valuesRel.getCluster(),
                    valuesRel.getRowType(),
                    valuesRel.getTuples());
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Uncollect} to a
     * {@link PhoenixUncollect}.
     */
    public static class PhoenixUncollectRule extends PhoenixConverterRule {
        
        private static final PhoenixUncollectRule INSTANCE = new PhoenixUncollectRule();

        private PhoenixUncollectRule() {
            super(Uncollect.class, Convention.NONE, 
                    PhoenixRel.CLIENT_CONVENTION, "PhoenixUncollectRule");
        }

        public RelNode convert(RelNode rel) {
            final Uncollect uncollect = (Uncollect) rel;
            return PhoenixUncollect.create(
                convert(
                        uncollect.getInput(), 
                        uncollect.getInput().getTraitSet().replace(out)));
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalCorrelate} to a
     * {@link PhoenixCorrelate}.
     */
    public static class PhoenixCorrelateRule extends PhoenixConverterRule {
        
        private static final PhoenixCorrelateRule INSTANCE = new PhoenixCorrelateRule();

        private PhoenixCorrelateRule() {
            super(LogicalCorrelate.class, Convention.NONE, 
                    PhoenixRel.CLIENT_CONVENTION, "PhoenixCorrelateRule");
        }

        public RelNode convert(RelNode rel) {
            final Correlate correlate = (Correlate) rel;
            return PhoenixCorrelate.create(
                convert(correlate.getLeft(), 
                        correlate.getLeft().getTraitSet().replace(out)),
                convert(correlate.getRight(), 
                        correlate.getRight().getTraitSet().replace(out)),
                correlate.getCorrelationId(),
                correlate.getRequiredColumns(),
                correlate.getJoinType());
        }
    }

    /**
     * Rule to convert an {@link org.apache.calcite.rel.logical.LogicalIntersect}
     * to an {@link PhoenixIntersectRel}.
     o/
     private static class PhoenixIntersectRule
     extends PhoenixConverterRule {
     private PhoenixIntersectRule(PhoenixConvention out) {
     super(
     LogicalIntersect.class,
     Convention.NONE,
     out,
     "PhoenixIntersectRule");
     }

     public RelNode convert(RelNode rel) {
     final LogicalIntersect intersect = (LogicalIntersect) rel;
     if (intersect.all) {
     return null; // INTERSECT ALL not implemented
     }
     final RelTraitSet traitSet =
     intersect.getTraitSet().replace(out);
     return new PhoenixIntersectRel(
     rel.getCluster(),
     traitSet,
     convertList(intersect.getInputs(), traitSet),
     intersect.all);
     }
     }

     public static class PhoenixIntersectRel
     extends Intersect
     implements PhoenixRel {
     public PhoenixIntersectRel(
     RelOptCluster cluster,
     RelTraitSet traitSet,
     List<RelNode> inputs,
     boolean all) {
     super(cluster, traitSet, inputs, all);
     assert !all;
     }

     public PhoenixIntersectRel copy(
     RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
     return new PhoenixIntersectRel(getCluster(), traitSet, inputs, all);
     }

     public SqlString implement(PhoenixImplementor implementor) {
     return setOpSql(this, implementor, " intersect ");
     }
     }

     /**
     * Rule to convert an {@link org.apache.calcite.rel.logical.LogicalMinus}
     * to an {@link PhoenixMinusRel}.
     o/
     private static class PhoenixMinusRule
     extends PhoenixConverterRule {
     private PhoenixMinusRule(PhoenixConvention out) {
     super(
     LogicalMinus.class,
     Convention.NONE,
     out,
     "PhoenixMinusRule");
     }

     public RelNode convert(RelNode rel) {
     final LogicalMinus minus = (LogicalMinus) rel;
     if (minus.all) {
     return null; // EXCEPT ALL not implemented
     }
     final RelTraitSet traitSet =
     rel.getTraitSet().replace(out);
     return new PhoenixMinusRel(
     rel.getCluster(),
     traitSet,
     convertList(minus.getInputs(), traitSet),
     minus.all);
     }
     }

     public static class PhoenixMinusRel
     extends Minus
     implements PhoenixRel {
     public PhoenixMinusRel(
     RelOptCluster cluster,
     RelTraitSet traitSet,
     List<RelNode> inputs,
     boolean all) {
     super(cluster, traitSet, inputs, all);
     assert !all;
     }

     public PhoenixMinusRel copy(
     RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
     return new PhoenixMinusRel(getCluster(), traitSet, inputs, all);
     }

     public SqlString implement(PhoenixImplementor implementor) {
     return setOpSql(this, implementor, " minus ");
     }
     }
     */

    /**
     * Rule to convert a relational expression from
     * {@link org.apache.phoenix.calcite.rel.PhoenixRel#SERVER_CONVENTION} to
     * {@link org.apache.phoenix.calcite.rel.PhoenixRel#CLIENT_CONVENTION}.
     */
    public static class PhoenixServerToClientConverterRule extends ConverterRule {
        public static final ConverterRule INSTANCE =
            new PhoenixServerToClientConverterRule();

        private PhoenixServerToClientConverterRule() {
            super(RelNode.class, PhoenixRel.SERVER_CONVENTION, PhoenixRel.CLIENT_CONVENTION,
                "PhoenixServerToClientConverterRule");
        }

        @Override public RelNode convert(RelNode rel) {
            return PhoenixToClientConverter.create(rel);
        }
    }

    /**
     * Rule to convert a relational expression from
     * {@link org.apache.phoenix.calcite.rel.PhoenixRel#SERVERJOIN_CONVENTION} to
     * {@link org.apache.phoenix.calcite.rel.PhoenixRel#CLIENT_CONVENTION}.
     */
    public static class PhoenixProjectableToClientConverterRule extends ConverterRule {
        public static final ConverterRule INSTANCE =
            new PhoenixProjectableToClientConverterRule();

        private PhoenixProjectableToClientConverterRule() {
            super(RelNode.class, PhoenixRel.SERVERJOIN_CONVENTION, PhoenixRel.CLIENT_CONVENTION,
                "PhoenixProjectableToClientConverterRule");
        }

        @Override public RelNode convert(RelNode rel) {
            return PhoenixToClientConverter.create(rel);
        }
    }

    /**
     * Rule to convert a relational expression from
     * {@link org.apache.phoenix.calcite.rel.PhoenixRel#CONVENTION} to
     * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention}.
     */
    public static class PhoenixToEnumerableConverterRule extends ConverterRule {
        public static final ConverterRule INSTANCE =
            new PhoenixToEnumerableConverterRule();

        private PhoenixToEnumerableConverterRule() {
            super(RelNode.class, PhoenixRel.CLIENT_CONVENTION, EnumerableConvention.INSTANCE,
                "PhoenixToEnumerableConverterRule");
        }

        @Override public RelNode convert(RelNode rel) {
            return PhoenixToEnumerableConverter.create(rel);
        }
    }
    
    
    //-------------------------------------------------------------------
    // Helper functions that check if a RelNode would be implementable by
    // its corresponding PhoenixRel.
    
    public static boolean isConvertible(Aggregate input) {
        if (PhoenixAbstractAggregate.isSingleValueCheckAggregate(input))
            return true;
        
        if (input.getGroupSets().size() > 1)
            return false;
        
        if (input.containsDistinctCall())
            return false;
        
        if (input.getGroupType() != Group.SIMPLE)
            return false;
        
        for (AggregateCall aggCall : input.getAggCallList()) {
            if (!CalciteUtils.isAggregateFunctionSupported(aggCall.getAggregation())) {
                return false;
            }
        }        
        
        return true;
    }
    
    public static boolean isConvertible(Filter input) {
        return CalciteUtils.isExpressionSupported(input.getCondition());
    }
    
    public static boolean isConvertible(Join input) {
        return CalciteUtils.isExpressionSupported(input.getCondition());
    }
    
    public static boolean isConvertible(Project input) {
        for (RexNode project : input.getProjects()) {
            if (!CalciteUtils.isExpressionSupported(project)) {
                return false;
            }
        }
        
        return true;
    }
    
    public static boolean isConvertible(Sort sort) {
        if (sort.offset != null)
            return false;
        
        if (sort.fetch != null 
                && CalciteUtils.evaluateStatelessExpression(sort.fetch) == null)
            return false;
        
        return true;
    }
    
    public static boolean isConvertible(Union union) {
        return union.all;
    }
}

// End PhoenixRules.java
