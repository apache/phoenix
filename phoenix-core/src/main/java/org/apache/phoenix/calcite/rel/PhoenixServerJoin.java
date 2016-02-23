package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.metadata.PhoenixRelMdCollation;
import org.apache.phoenix.compile.JoinCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

public class PhoenixServerJoin extends PhoenixAbstractJoin {
    
    public static PhoenixServerJoin create(final RelNode left, final RelNode right, 
            RexNode condition, Set<CorrelationId> variablesSet,
            final JoinRelType joinType, boolean isSingleValueRhs) {
        final RelOptCluster cluster = left.getCluster();
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        final RelTraitSet traits =
                cluster.traitSet().replace(PhoenixConvention.SERVERJOIN)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return PhoenixRelMdCollation.hashJoin(mq, left, right, joinType);
                    }
                });
        return new PhoenixServerJoin(cluster, traits, left, right, condition, variablesSet, joinType, isSingleValueRhs);
    }

    private PhoenixServerJoin(RelOptCluster cluster, RelTraitSet traits,
            RelNode left, RelNode right, RexNode condition,
            Set<CorrelationId> variablesSet, JoinRelType joinType,
            boolean isSingleValueRhs) {
        super(cluster, traits, left, right, condition, variablesSet,
                joinType, isSingleValueRhs);
    }

    @Override
    public PhoenixServerJoin copy(RelTraitSet traits, RexNode condition, RelNode left,
            RelNode right, JoinRelType joinRelType, boolean semiJoinDone) {
        return copy(traits, condition, left, right, joinRelType, semiJoinDone, isSingleValueRhs);
    }

    @Override
    public PhoenixServerJoin copy(RelTraitSet traits, RexNode condition, RelNode left,
            RelNode right, JoinRelType joinRelType, boolean semiJoinDone, boolean isSingleValueRhs) {
        return create(left, right, condition, variablesSet, joinRelType, isSingleValueRhs);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getLeft().getConvention().satisfies(PhoenixConvention.SERVER)
                || !getRight().getConvention().satisfies(PhoenixConvention.GENERIC)
                || !variablesSet.isEmpty())
            return planner.getCostFactory().makeInfiniteCost();            
        
        if (joinType == JoinRelType.FULL || joinType == JoinRelType.RIGHT)
            return planner.getCostFactory().makeInfiniteCost();
        
        //TODO return infinite cost if RHS size exceeds memory limit.
        
        double rowCount = mq.getRowCount(this);

        double leftRowCount = mq.getRowCount(getLeft());
        if (Double.isInfinite(leftRowCount)) {
            rowCount = leftRowCount;
        } else {
            rowCount += leftRowCount;
            double rightRowCount = mq.getRowCount(getRight());
            if (Double.isInfinite(rightRowCount)) {
                rowCount = rightRowCount;
            } else {
                double rightRowSize = mq.getAverageRowSize(getRight());
                rowCount += (rightRowCount + rightRowCount * rightRowSize);
            }
        }            
        
        RelOptCost cost = planner.getCostFactory().makeCost(rowCount, 0, 0);

        return cost.multiplyBy(SERVER_FACTOR).multiplyBy(PHOENIX_FACTOR);
    }
    
    @Override
    public QueryPlan implement(Implementor implementor) {
        List<Expression> leftExprs = Lists.<Expression> newArrayList();
        List<Expression> rightExprs = Lists.<Expression> newArrayList();

        implementor.pushContext(new ImplementorContext(implementor.getCurrentContext().retainPKColumns, true, getColumnRefList(0)));
        QueryPlan leftPlan = implementInput(implementor, 0, null);
        PTable leftTable = implementor.getTableRef().getTable();
        implementor.popContext();

        implementor.pushContext(new ImplementorContext(false, true, getColumnRefList(1)));
        QueryPlan rightPlan = implementInput(implementor, 1, rightExprs);
        PTable rightTable = implementor.getTableRef().getTable();
        implementor.popContext();
        
        JoinType type = CalciteUtils.convertJoinType(getJoinType());
        PTable joinedTable;
        try {
            joinedTable = JoinCompiler.joinProjectedTables(leftTable, rightTable, type);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        implementor.setTableRef(new TableRef(joinedTable));
        
        // Compile left conditions against the joined table due to implementation of HashJoinRegionScanner.
        for (Iterator<Integer> iter = joinInfo.leftKeys.iterator(); iter.hasNext();) {
            Integer i = iter.next();
            leftExprs.add(implementor.newColumnExpression(i));
        }
        if (leftExprs.isEmpty()) {
            leftExprs.add(LiteralExpression.newConstant(0));
        }

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
        
        return HashJoinPlan.create((SelectStatement) (leftPlan.getStatement()), leftPlan, hashJoinInfo, new HashJoinPlan.HashSubPlan[] {new HashJoinPlan.HashSubPlan(0, rightPlan, rightExprs, isSingleValueRhs, null, null)});
    }

}
