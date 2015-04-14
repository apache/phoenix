package org.apache.phoenix.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.util.trace.CalciteTrace;

import java.util.logging.Logger;

/**
 * Rules and relational operators for
 * {@link PhoenixRel#CONVENTION PHOENIX}
 * calling convention.
 */
public class PhoenixRules {
    private PhoenixRules() {}

    protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();

    public static final RelOptRule[] RULES = {
        PhoenixToEnumerableConverterRule.INSTANCE,
        PhoenixSortRule.INSTANCE,
        PhoenixFilterRule.INSTANCE,
        PhoenixProjectRule.INSTANCE,
        PhoenixAggregateRule.INSTANCE,
        PhoenixUnionRule.INSTANCE,
        PhoenixJoinRule.INSTANCE,
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
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
     * {@link PhoenixSort}.
     */
    private static class PhoenixSortRule extends PhoenixConverterRule {
        public static final PhoenixSortRule INSTANCE = new PhoenixSortRule();

        private PhoenixSortRule() {
            super(LogicalSort.class, Convention.NONE, PhoenixRel.CONVENTION,
                "PhoenixSortRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalSort sort = (LogicalSort) rel;
            final RelTraitSet traitSet =
                sort.getTraitSet().replace(out)
                    .replace(sort.getCollation());
            return new PhoenixClientSort(rel.getCluster(), traitSet,
                convert(sort.getInput(), sort.getInput().getTraitSet().replace(out)),
                sort.getCollation(), sort.offset, sort.fetch);
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to a
     * {@link PhoenixFilter}.
     */
    private static class PhoenixFilterRule extends PhoenixConverterRule {
        private static final PhoenixFilterRule INSTANCE = new PhoenixFilterRule();

        private PhoenixFilterRule() {
            super(LogicalFilter.class, Convention.NONE, PhoenixRel.CONVENTION,
                "PhoenixFilterRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalFilter filter = (LogicalFilter) rel;
            final RelTraitSet traitSet = filter.getTraitSet().replace(out);
            return new PhoenixFilter(
                rel.getCluster(),
                traitSet,
                convert(filter.getInput(), filter.getInput().getTraitSet().replace(out)),
                filter.getCondition());
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject}
     * to a {@link PhoenixProject}.
     */
    private static class PhoenixProjectRule extends PhoenixConverterRule {
        private static final PhoenixProjectRule INSTANCE = new PhoenixProjectRule();

        private PhoenixProjectRule() {
            super(LogicalProject.class, Convention.NONE, PhoenixRel.CONVENTION,
                "PhoenixProjectRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalProject project = (LogicalProject) rel;
            final RelTraitSet traitSet = project.getTraitSet().replace(out);
            return new PhoenixClientProject(project.getCluster(), traitSet,
                convert(project.getInput(), project.getInput().getTraitSet().replace(out)), project.getProjects(),
                project.getRowType());
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalAggregate}
     * to an {@link PhoenixAggregate}.
     */
    private static class PhoenixAggregateRule extends PhoenixConverterRule {
        public static final RelOptRule INSTANCE = new PhoenixAggregateRule();

        private PhoenixAggregateRule() {
            super(LogicalAggregate.class, Convention.NONE, PhoenixRel.CONVENTION,
                "PhoenixAggregateRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalAggregate agg = (LogicalAggregate) rel;
            final RelTraitSet traitSet =
                agg.getTraitSet().replace(out);
            return new PhoenixClientAggregate(
                    rel.getCluster(),
                    traitSet,
                    convert(agg.getInput(), agg.getInput().getTraitSet().replace(out)),
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
    private static class PhoenixUnionRule extends PhoenixConverterRule {
        public static final PhoenixUnionRule INSTANCE = new PhoenixUnionRule();

        private PhoenixUnionRule() {
            super(LogicalUnion.class, Convention.NONE, PhoenixRel.CONVENTION,
                "PhoenixUnionRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalUnion union = (LogicalUnion) rel;
            final RelTraitSet traitSet = union.getTraitSet().replace(out);
            return new PhoenixUnion(rel.getCluster(), traitSet, convertList(union.getInputs(), out),
                union.all);
        }
    }

    /**
     * Rule to convert a {@link org.apache.calcite.rel.core.Sort} to a
     * {@link PhoenixSort}.
     */
    private static class PhoenixJoinRule extends PhoenixConverterRule {
        public static final PhoenixJoinRule INSTANCE = new PhoenixJoinRule();

        private PhoenixJoinRule() {
            super(LogicalJoin.class, Convention.NONE, PhoenixRel.CONVENTION,
                "PhoenixJoinRule");
        }

        public RelNode convert(RelNode rel) {
            final LogicalJoin join = (LogicalJoin) rel;
            final RelTraitSet traitSet =
                join.getTraitSet().replace(out);
            return new PhoenixClientJoin(rel.getCluster(), traitSet,
                convert(join.getLeft(), join.getLeft().getTraitSet().replace(out)),
                convert(join.getRight(), join.getRight().getTraitSet().replace(out)),
                join.getCondition(),
                join.getJoinType(),
                join.getVariablesStopped());
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

  /*
  public static class PhoenixValuesRule extends PhoenixConverterRule {
    private PhoenixValuesRule() {
      super(Values.class, Convention.NONE, PhoenixRel.CONVENTION, "PhoenixValuesRule");
    }

    @Override public RelNode convert(RelNode rel) {
      Values valuesRel = (Values) rel;
      return new PhoenixValuesRel(
          valuesRel.getCluster(),
          valuesRel.getRowType(),
          valuesRel.getTuples(),
          valuesRel.getTraitSet().plus(out));
    }
  }

  public static class PhoenixValuesRel
      extends Values
      implements PhoenixRel {
    PhoenixValuesRel(
        RelOptCluster cluster,
        RelDataType rowType,
        List<List<RexLiteral>> tuples,
        RelTraitSet traitSet) {
      super(cluster, rowType, tuples, traitSet);
    }

    @Override public RelNode copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.isEmpty();
      return new PhoenixValuesRel(
          getCluster(), rowType, tuples, traitSet);
    }

    public SqlString implement(PhoenixImplementor implementor) {
      throw new AssertionError(); // TODO:
    }
  }
*/

    /**
     * Rule to convert a relational expression from
     * {@link org.apache.phoenix.calcite.PhoenixRel#CONVENTION} to
     * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention}.
     */
    public static class PhoenixToEnumerableConverterRule extends ConverterRule {
        public static final ConverterRule INSTANCE =
            new PhoenixToEnumerableConverterRule();

        private PhoenixToEnumerableConverterRule() {
            super(RelNode.class, PhoenixRel.CONVENTION, EnumerableConvention.INSTANCE,
                "PhoenixToEnumerableConverterRule");
        }

        @Override public RelNode convert(RelNode rel) {
            RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
            return new PhoenixToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
        }
    }
}

// End PhoenixRules.java
