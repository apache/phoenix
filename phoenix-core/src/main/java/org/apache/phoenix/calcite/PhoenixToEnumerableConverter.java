package org.apache.phoenix.calcite;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.phoenix.compile.QueryPlan;

import java.util.List;

/**
 * Scan of a Phoenix table.
 */
public class PhoenixToEnumerableConverter extends ConverterImpl implements EnumerableRel {
    protected PhoenixToEnumerableConverter(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PhoenixToEnumerableConverter(getCluster(), traitSet, sole(inputs));
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(.1);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        // Generates code that instantiates a result iterator, then converts it
        // to an enumerable.
        //
        //   ResultIterator iterator = root.get("x");
        //   return CalciteRuntime.toEnumerable(iterator);
        final BlockBuilder list = new BlockBuilder();
        QueryPlan plan = makePlan((PhoenixRel)getInput());
        Expression var = stash(implementor, plan, QueryPlan.class);
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
    
    static QueryPlan makePlan(PhoenixRel rel) {
        final PhoenixRel.Implementor phoenixImplementor = new PhoenixRelImplementorImpl();
        phoenixImplementor.visitInput(0, rel);
        return phoenixImplementor.makePlan();
    }

    static Expression stash(EnumerableRelImplementor implementor, Object o, Class clazz) {
        ParameterExpression x = implementor.register(o, clazz);
        MethodCallExpression e =
            Expressions.call(implementor.getRootExpression(),
                org.apache.calcite.util.BuiltInMethod.DATA_CONTEXT_GET.method,
                Expressions.constant(x.name));
        return Expressions.convert_(e, clazz);
    }
}
