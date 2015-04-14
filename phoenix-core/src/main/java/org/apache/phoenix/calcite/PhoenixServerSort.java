package org.apache.phoenix.calcite;

import java.sql.SQLException;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.parse.SelectStatement;

public class PhoenixServerSort extends PhoenixSort {

    public PhoenixServerSort(RelOptCluster cluster, RelTraitSet traits,
            RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traits, child, collation, offset, fetch);
    }

    @Override
    public PhoenixServerSort copy(RelTraitSet traitSet, RelNode newInput,
            RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new PhoenixServerSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }
    
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner)
                .multiplyBy(SERVER_FACTOR)
                .multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        assert getConvention() == getInput().getConvention();
        if (this.offset != null)
            throw new UnsupportedOperationException();
            
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        assert (plan instanceof ScanPlan || plan instanceof HashJoinPlan) 
                && plan.getLimit() == null;
        
        ScanPlan basePlan;
        if (plan instanceof ScanPlan) {
            basePlan = (ScanPlan) plan;
        } else {
            QueryPlan delegate = ((HashJoinPlan) plan).getDelegate();
            assert delegate instanceof ScanPlan;
            basePlan = (ScanPlan) delegate;
        }
        
        OrderBy orderBy = super.getOrderBy(implementor);
        Integer limit = super.getLimit(implementor);
        QueryPlan newPlan;
        try {
            newPlan = ScanPlan.create((ScanPlan) basePlan, orderBy, limit);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        if (plan instanceof HashJoinPlan) {        
            HashJoinPlan hashJoinPlan = (HashJoinPlan) plan;
            newPlan = HashJoinPlan.create((SelectStatement) (plan.getStatement()), newPlan, hashJoinPlan.getJoinInfo(), hashJoinPlan.getSubPlans());
        }
        return newPlan;
    }

}
