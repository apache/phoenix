package org.apache.phoenix.calcite.rel;

import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.execute.LiteralResultIterationQueryPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Values}
 * relational expression in Phoenix.
 */
public class PhoenixValues extends Values implements PhoenixRel {
    
    public static PhoenixValues create(RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples) {
        RelTraitSet traits = cluster.traitSetOf(PhoenixRel.CLIENT_CONVENTION);
        return new PhoenixValues(cluster, rowType, tuples, traits);
    }
    
    private PhoenixValues(RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traits) {
        super(cluster, rowType, tuples, traits);
    }

    @Override
    public PhoenixValues copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return create(getCluster(), rowType, tuples);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        List<Tuple> literalResult = Lists.newArrayList();
        Iterator<ImmutableList<RexLiteral>> iter = getTuples().iterator();
        Tuple baseTuple = new SingleKeyValueTuple(KeyValue.LOWESTKEY);
        if (iter.hasNext()) {
            ImmutableList<RexLiteral> row = iter.next();
            List<Expression> exprs = Lists.newArrayListWithExpectedSize(row.size());
            for (RexLiteral rexLiteral : row) {
                exprs.add(CalciteUtils.toExpression(rexLiteral, implementor));
            }
            TupleProjector projector = implementor.project(exprs);
            literalResult.add(projector.projectResults(baseTuple));
        }
        
        return new LiteralResultIterationQueryPlan(literalResult.iterator(), null, SelectStatement.SELECT_ONE, TableRef.EMPTY_TABLE_REF, RowProjector.EMPTY_PROJECTOR, null, OrderBy.EMPTY_ORDER_BY, null);
    }
}
