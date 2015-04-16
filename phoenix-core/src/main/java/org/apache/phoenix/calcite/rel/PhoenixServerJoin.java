package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.compile.JoinCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;

import com.google.common.collect.Lists;

public class PhoenixServerJoin extends PhoenixAbstractJoin {

    public PhoenixServerJoin(RelOptCluster cluster, RelTraitSet traits,
            RelNode left, RelNode right, RexNode condition,
            JoinRelType joinType, Set<String> variablesStopped) {
        super(cluster, traits, left, right, condition, joinType,
                variablesStopped);
    }

    @Override
    public PhoenixServerJoin copy(RelTraitSet traits, RexNode condition, RelNode left,
            RelNode right, JoinRelType joinRelType, boolean semiJoinDone) {
        return new PhoenixServerJoin(getCluster(), traits, left, right, condition, joinRelType, variablesStopped);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        //TODO return infinite cost if RHS size exceeds memory limit.
        
        double rowCount = RelMetadataQuery.getRowCount(this);
        
        for (RelNode input : getInputs()) {
            double inputRowCount = input.getRows();
            if (Double.isInfinite(inputRowCount)) {
                rowCount = inputRowCount;
            } else if (input == getLeft()) {
                rowCount += inputRowCount;
            } else {
                rowCount += Util.nLogN(inputRowCount);
            }
        }
        RelOptCost cost = planner.getCostFactory().makeCost(rowCount, 0, 0);

        return cost.multiplyBy(SERVER_FACTOR).multiplyBy(PHOENIX_FACTOR);
    }
    
    @Override
    public QueryPlan implement(Implementor implementor) {
        assert getLeft().getConvention() == PhoenixRel.CONVENTION;
        assert getRight().getConvention() == PhoenixRel.CONVENTION;
        PhoenixRel left = (PhoenixRel) getLeft();
        PhoenixRel right = (PhoenixRel) getRight();
        
        JoinInfo joinInfo = JoinInfo.of(left, right, getCondition());
        List<Expression> leftExprs = Lists.<Expression> newArrayList();
        List<Expression> rightExprs = Lists.<Expression> newArrayList();
        implementor.pushContext(new ImplementorContext(implementor.getCurrentContext().isRetainPKColumns(), true));
        QueryPlan leftPlan = implementor.visitInput(0, left);
        PTable leftTable = implementor.getTableRef().getTable();
        for (Iterator<Integer> iter = joinInfo.leftKeys.iterator(); iter.hasNext();) {
            Integer index = iter.next();
            leftExprs.add(implementor.newColumnExpression(index));
        }
        if (leftExprs.isEmpty()) {
            leftExprs.add(LiteralExpression.newConstant(0));
        }
        implementor.popContext();
        implementor.pushContext(new ImplementorContext(false, true));
        QueryPlan rightPlan = implementor.visitInput(1, right);
        PTable rightTable = implementor.getTableRef().getTable();
        for (Iterator<Integer> iter = joinInfo.rightKeys.iterator(); iter.hasNext();) {
            Integer index = iter.next();
            rightExprs.add(implementor.newColumnExpression(index));
        }
        if (rightExprs.isEmpty()) {
            rightExprs.add(LiteralExpression.newConstant(0));
        }
        implementor.popContext();
        
        JoinType type = convertJoinType(getJoinType());
        PTable joinedTable;
        try {
            joinedTable = JoinCompiler.joinProjectedTables(leftTable, rightTable, type);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        implementor.setTableRef(new TableRef(joinedTable));
        RexNode postFilter = joinInfo.getRemaining(getCluster().getRexBuilder());
        Expression postFilterExpr = postFilter.isAlwaysTrue() ? null : CalciteUtils.toExpression(postFilter, implementor);
        @SuppressWarnings("unchecked")
        HashJoinInfo hashJoinInfo = new HashJoinInfo(
                joinedTable, new ImmutableBytesPtr[] {new ImmutableBytesPtr()}, 
                (List<Expression>[]) (new List[] {leftExprs}), 
                new JoinType[] {type}, new boolean[] {true}, 
                new PTable[] {rightTable},
                new int[] {leftTable.getColumns().size() - leftTable.getPKColumns().size()}, 
                postFilterExpr, null);
        
        return HashJoinPlan.create(SelectStatement.SELECT_STAR, leftPlan, hashJoinInfo, new HashJoinPlan.HashSubPlan[] {new HashJoinPlan.HashSubPlan(0, rightPlan, rightExprs, false, null, null)});
    }

}
