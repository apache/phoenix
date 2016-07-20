package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.HashJoinPlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.TupleProjectionPlan;
import org.apache.phoenix.execute.TupleProjector;

public class PhoenixServerSort extends PhoenixAbstractSort {
    
    public static PhoenixServerSort create(RelNode input, RelCollation collation) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        RelTraitSet traits =
            input.getTraitSet().replace(PhoenixConvention.CLIENT).replace(collation);
        return new PhoenixServerSort(cluster, traits, input, collation);
    }

    private PhoenixServerSort(RelOptCluster cluster, RelTraitSet traits,
            RelNode child, RelCollation collation) {
        super(cluster, traits, child, collation);
    }

    @Override
    public PhoenixServerSort copy(RelTraitSet traitSet, RelNode newInput,
            RelCollation newCollation, RexNode offset, RexNode fetch) {
        return create(newInput, newCollation);
    }
    
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getInput().getConvention().satisfies(PhoenixConvention.SERVER)
                && !getInput().getConvention().satisfies(PhoenixConvention.SERVERJOIN)
                && !getInput().getConvention().satisfies(PhoenixConvention.SERVERAGG))
            return planner.getCostFactory().makeInfiniteCost();
        
        return super.computeSelfCost(planner, mq)
                .multiplyBy(SERVER_FACTOR)
                .multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(PhoenixRelImplementor implementor) {
        if (this.offset != null)
            throw new UnsupportedOperationException();
            
        QueryPlan plan = implementor.visitInput(0, (PhoenixQueryRel) getInput());
        
        if (getInput().getConvention().satisfies(PhoenixConvention.SERVERAGG)) {
            return sortOverAgg(implementor, plan);
        }
        
        return sortOverScan(implementor, plan);
    }
    
    private QueryPlan sortOverScan(PhoenixRelImplementor implementor, QueryPlan plan) {
        assert (plan instanceof ScanPlan 
                || plan instanceof HashJoinPlan) 
        && plan.getLimit() == null;

        ScanPlan basePlan;
        HashJoinPlan hashJoinPlan = null;
        if (plan instanceof ScanPlan) {
            basePlan = (ScanPlan) plan;
        } else {
            hashJoinPlan = (HashJoinPlan) plan;
            QueryPlan delegate = hashJoinPlan.getDelegate();
            assert delegate instanceof ScanPlan;
            basePlan = (ScanPlan) delegate;
        }

        OrderBy orderBy = super.getOrderBy(getCollation(), implementor, null);
        QueryPlan newPlan;
        try {
            newPlan = ScanPlan.create((ScanPlan) basePlan, orderBy);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if (hashJoinPlan != null) {        
            newPlan = HashJoinPlan.create(hashJoinPlan.getStatement(), newPlan, hashJoinPlan.getJoinInfo(), hashJoinPlan.getSubPlans());
        }
        return newPlan;
    }
    
    private QueryPlan sortOverAgg(PhoenixRelImplementor implementor, QueryPlan plan) {
        assert plan instanceof TupleProjectionPlan;
        
        // PhoenixServerAggregate wraps the AggregatePlan with a TupleProjectionPlan,
        // so we need to unwrap the TupleProjectionPlan.
        TupleProjectionPlan tupleProjectionPlan = (TupleProjectionPlan) plan;
        assert tupleProjectionPlan.getPostFilter() == null;
        QueryPlan innerPlan = tupleProjectionPlan.getDelegate();
        TupleProjector tupleProjector = tupleProjectionPlan.getTupleProjector();
        assert (innerPlan instanceof AggregatePlan 
                    || innerPlan instanceof HashJoinPlan)
                && innerPlan.getLimit() == null; 
        
        AggregatePlan basePlan;
        HashJoinPlan hashJoinPlan = null;
        if (innerPlan instanceof AggregatePlan) {
            basePlan = (AggregatePlan) innerPlan;
        } else {
            hashJoinPlan = (HashJoinPlan) innerPlan;
            QueryPlan delegate = hashJoinPlan.getDelegate();
            assert delegate instanceof AggregatePlan;
            basePlan = (AggregatePlan) delegate;
        }
        
        OrderBy orderBy = super.getOrderBy(getCollation(), implementor, tupleProjector);
        QueryPlan newPlan = AggregatePlan.create((AggregatePlan) basePlan, orderBy);
        
        if (hashJoinPlan != null) {        
            newPlan = HashJoinPlan.create(hashJoinPlan.getStatement(), newPlan, hashJoinPlan.getJoinInfo(), hashJoinPlan.getSubPlans());
        }
        // Recover the wrapping of TupleProjectionPlan
        newPlan = new TupleProjectionPlan(newPlan, tupleProjector, null);
        return newPlan;        
    }
}
