package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.metadata.PhoenixRelMdCollation;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.JoinCompiler;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.ClientScanPlan;
import org.apache.phoenix.execute.SortMergeJoinPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.JoinTableNode.JoinType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

public class PhoenixClientJoin extends PhoenixAbstractJoin {
    
    public static PhoenixClientJoin create(final RelNode left, final RelNode right, 
            RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType,
            boolean isSingleValueRhs) {
        final RelOptCluster cluster = left.getCluster();
        final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        final RelTraitSet traits =
                cluster.traitSet().replace(PhoenixConvention.CLIENT)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return PhoenixRelMdCollation.mergeJoin(mq, left, right, joinInfo.leftKeys, joinInfo.rightKeys);
                    }
                });
        return new PhoenixClientJoin(cluster, traits, left, right, condition, variablesSet, joinType, isSingleValueRhs);
    }

    private PhoenixClientJoin(RelOptCluster cluster, RelTraitSet traits,
            RelNode left, RelNode right, RexNode condition,
             Set<CorrelationId> variablesSet,JoinRelType joinType, boolean isSingleValueRhs) {
        super(cluster, traits, left, right, condition, variablesSet,
                joinType, isSingleValueRhs);
    }

    @Override
    public PhoenixClientJoin copy(RelTraitSet traits, RexNode condition, RelNode left,
            RelNode right, JoinRelType joinRelType, boolean semiJoinDone) {
        return copy(traits, condition, left, right, joinRelType, semiJoinDone, isSingleValueRhs);
    }

    @Override
    public PhoenixClientJoin copy(RelTraitSet traits, RexNode condition, RelNode left,
            RelNode right, JoinRelType joinRelType, boolean semiJoinDone, boolean isSingleValueRhs) {
        return create(left, right, condition, variablesSet, joinRelType, isSingleValueRhs);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getLeft().getConvention().satisfies(PhoenixConvention.GENERIC) 
                || !getRight().getConvention().satisfies(PhoenixConvention.GENERIC)
                || !variablesSet.isEmpty())
            return planner.getCostFactory().makeInfiniteCost();            
        
        if (joinType == JoinRelType.RIGHT
                 || (!joinInfo.leftKeys.isEmpty() && !RelCollations.contains(mq.collations(getLeft()), joinInfo.leftKeys))
                 || (!joinInfo.rightKeys.isEmpty() && !RelCollations.contains(mq.collations(getRight()), joinInfo.rightKeys)))
            return planner.getCostFactory().makeInfiniteCost();
        
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
                rowCount += rightRowCount;
            }
        }            
        RelOptCost cost = planner.getCostFactory().makeCost(0, rowCount, 0);

        return cost.multiplyBy(SERVER_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        List<Expression> leftExprs = Lists.<Expression> newArrayList();
        List<Expression> rightExprs = Lists.<Expression> newArrayList();

        implementor.pushContext(new ImplementorContext(implementor.getCurrentContext().retainPKColumns && getJoinType() != JoinRelType.FULL, true, getColumnRefList(0)));
        QueryPlan leftPlan = implementInput(implementor, 0, leftExprs);
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
        TableRef tableRef = new TableRef(joinedTable);
        implementor.setTableRef(tableRef);
        ColumnResolver resolver;
        try {
            resolver = FromCompiler.getResolver(tableRef);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        PhoenixStatement stmt = leftPlan.getContext().getStatement();
        StatementContext context = new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));

        QueryPlan plan = new SortMergeJoinPlan(context, leftPlan.getStatement(), 
                tableRef, type, leftPlan, rightPlan, leftExprs, rightExprs, 
                joinedTable, leftTable, rightTable, 
                leftTable.getColumns().size() - leftTable.getPKColumns().size(), 
                isSingleValueRhs);
        
        RexNode postFilter = joinInfo.getRemaining(getCluster().getRexBuilder());
        Expression postFilterExpr = postFilter.isAlwaysTrue() ? null : CalciteUtils.toExpression(postFilter, implementor);
        if (postFilter != null) {
            plan = new ClientScanPlan(context, plan.getStatement(), tableRef, 
                    RowProjector.EMPTY_PROJECTOR, null, postFilterExpr, 
                    OrderBy.EMPTY_ORDER_BY, plan);
        }
        
        return plan;
    }

}
