package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.calcite.BuiltInMethod;
import org.apache.phoenix.calcite.rel.PhoenixRelImplementor.ImplementorContext;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.StatementPlan;
import org.apache.phoenix.execute.DelegateQueryPlan;
import org.apache.phoenix.execute.RuntimeContext;
import org.apache.phoenix.execute.RuntimeContextImpl;
import org.apache.phoenix.iterate.DefaultParallelScanGrouper;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;

/**
 * Scan of a Phoenix table.
 */
public class PhoenixToEnumerableConverter extends ConverterImpl implements EnumerableRel {
    private final StatementContext context;

    public static PhoenixToEnumerableConverter create(
            RelNode input, StatementContext context) {
        RelOptCluster cluster = input.getCluster();
        RelTraitSet traits = input.getTraitSet().replace(EnumerableConvention.INSTANCE);
        return new PhoenixToEnumerableConverter(cluster, traits, input, context);
    }

    private PhoenixToEnumerableConverter(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input,
        StatementContext context) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
        this.context = context;
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return create(sole(inputs), context);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq)
                .multiplyBy(.1)
                .multiplyBy(PhoenixRel.PHOENIX_FACTOR)
                .multiplyBy(PhoenixRel.SERVER_FACTOR);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        // Generates code that instantiates a result iterator, then converts it
        // to an enumerable.
        //
        //   ResultIterator iterator = root.get("x");
        //   return CalciteRuntime.toEnumerable(iterator);
        final BlockBuilder list = new BlockBuilder();
        StatementPlan plan = makePlan((PhoenixRel)getInput());
        Expression var = stash(implementor, plan, StatementPlan.class);
        final RelDataType rowType = getRowType();
        final PhysType physType =
            PhysTypeImpl.of(
                implementor.getTypeFactory(), rowType,
                pref.prefer(JavaRowFormat.ARRAY));
        final Expression iterator_ =
            list.append("iterator", var);
        final Expression enumerable_ =
            list.append("enumerable",
                Expressions.call(BuiltInMethod.TO_ENUMERABLE.method,
                    iterator_));
        list.add(Expressions.return_(null, enumerable_));
        return implementor.result(physType, list.toBlock());
    }
    
    StatementPlan makePlan(PhoenixRel rel) {
        RuntimeContext runtimeContext = new RuntimeContextImpl();
        RuntimeContext.THREAD_LOCAL.get().add(runtimeContext);
        final PhoenixRelImplementor phoenixImplementor =
                new PhoenixRelImplementorImpl(context, runtimeContext);
        phoenixImplementor.pushContext(new ImplementorContext(true, false, ImmutableIntList.identity(rel.getRowType().getFieldCount())));
        final StatementPlan plan = rel.implement(phoenixImplementor);
        if (!(plan instanceof QueryPlan)) {
            return plan;
        }
            
        return new DelegateQueryPlan((QueryPlan) plan) {
            @Override
            public ResultIterator iterator() throws SQLException {
                return iterator(DefaultParallelScanGrouper.getInstance());
            }
            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                return delegate.getExplainPlan();
            }
            @Override
            public RowProjector getProjector() {
                try {
                    return phoenixImplementor.getTableMapping().createRowProjector(null);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            @Override
            public ResultIterator iterator(ParallelScanGrouper scanGrouper)
                    throws SQLException {
                return delegate.iterator(scanGrouper);
            }
            @Override
            public QueryPlan limit(Integer limit, Integer offset) {
                return delegate.limit(limit, offset);
            }
            @Override
            public ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
                return delegate.iterator(scanGrouper, scan);
            }
        };
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static Expression stash(EnumerableRelImplementor implementor, Object o, Class clazz) {
        ParameterExpression x = (ParameterExpression) implementor.stash(o, clazz);
        MethodCallExpression e =
            Expressions.call(implementor.getRootExpression(),
                org.apache.calcite.util.BuiltInMethod.DATA_CONTEXT_GET.method,
                Expressions.constant(x.name));
        return Expressions.convert_(e, clazz);
    }
}
