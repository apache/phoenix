package org.apache.phoenix.calcite.rel;

import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;

abstract public class PhoenixAbstractSemiJoin extends SemiJoin implements PhoenixRel {

    protected PhoenixAbstractSemiJoin(RelOptCluster cluster, RelTraitSet traitSet,
            RelNode left, RelNode right, RexNode condition,
            ImmutableIntList leftKeys, ImmutableIntList rightKeys) {
        super(cluster, traitSet, left, right, condition, leftKeys, rightKeys);
    }
    
    protected QueryPlan implementInput(Implementor implementor, int index, List<Expression> conditionExprs) {
        assert index <= 1;
        
        PhoenixRel input = index == 0 ? (PhoenixRel) left : (PhoenixRel) right;
        ImmutableIntList keys = index == 0 ? leftKeys : rightKeys;
        QueryPlan plan = implementor.visitInput(0, input);
        for (Iterator<Integer> iter = keys.iterator(); iter.hasNext();) {
            Integer i = iter.next();
            conditionExprs.add(implementor.newColumnExpression(i));
        }
        if (conditionExprs.isEmpty()) {
            conditionExprs.add(LiteralExpression.newConstant(0));
        }

        return plan;
    }

}
