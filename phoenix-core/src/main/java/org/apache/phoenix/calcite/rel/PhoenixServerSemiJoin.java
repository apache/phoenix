package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.phoenix.calcite.TableMapping;
import org.apache.phoenix.calcite.metadata.PhoenixRelMdCollation;
import org.apache.phoenix.calcite.rel.PhoenixRelImplementor.ImplementorContext;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.join.HashJoinInfo;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.schema.PTable;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

public class PhoenixServerSemiJoin extends PhoenixAbstractSemiJoin {
    
    public static PhoenixServerSemiJoin create(
            final RelNode left, final RelNode right, RexNode condition) {
        final RelOptCluster cluster = left.getCluster();
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        final RelTraitSet traits =
                cluster.traitSet().replace(PhoenixConvention.SERVERJOIN)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return PhoenixRelMdCollation.hashJoin(mq, left, right, JoinRelType.INNER);
                    }
                });
        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        assert joinInfo.isEqui();
        return new PhoenixServerSemiJoin(cluster, traits, left, right, condition, 
                joinInfo.leftKeys, joinInfo.rightKeys);
    }

    private PhoenixServerSemiJoin(RelOptCluster cluster, RelTraitSet traitSet,
            RelNode left, RelNode right, RexNode condition,
            ImmutableIntList leftKeys, ImmutableIntList rightKeys) {
        super(cluster, traitSet, left, right, condition, leftKeys, rightKeys);
    }
    
    @Override
    public SemiJoin copy(RelTraitSet traitSet, RexNode condition,
            RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        assert joinType == JoinRelType.INNER;
        return create(left, right, condition);
    }    

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getLeft().getConvention().satisfies(PhoenixConvention.SERVER) 
                || !getRight().getConvention().satisfies(PhoenixConvention.GENERIC))
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
    public QueryPlan implement(PhoenixRelImplementor implementor) {
        List<Expression> leftExprs = Lists.<Expression> newArrayList();
        List<Expression> rightExprs = Lists.<Expression> newArrayList();

        implementor.pushContext(new ImplementorContext(implementor.getCurrentContext().retainPKColumns, true, getColumnRefList(0)));
        QueryPlan leftPlan = implementInput(implementor, 0, leftExprs);
        TableMapping tableMapping = implementor.getTableMapping();
        implementor.popContext();

        implementor.pushContext(new ImplementorContext(false, true, getColumnRefList(1)));
        QueryPlan rightPlan = implementInput(implementor, 1, rightExprs);
        implementor.popContext();
        
        JoinType type = JoinType.Semi;
        implementor.setTableMapping(tableMapping);
        @SuppressWarnings("unchecked")
        HashJoinInfo hashJoinInfo = new HashJoinInfo(
                tableMapping.getPTable(), 
                new ImmutableBytesPtr[] {new ImmutableBytesPtr()}, 
                (List<Expression>[]) (new List[] {leftExprs}), 
                new JoinType[] {type}, new boolean[] {true}, 
                new PTable[] {null}, new int[] {0}, null, null);
        
        return HashJoinPlan.create((SelectStatement) (leftPlan.getStatement()), leftPlan, hashJoinInfo, new HashJoinPlan.HashSubPlan[] {new HashJoinPlan.HashSubPlan(0, rightPlan, rightExprs, false, null, null)});
    }

}
