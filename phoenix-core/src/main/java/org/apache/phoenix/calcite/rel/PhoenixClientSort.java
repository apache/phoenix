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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.ClientScanPlan;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.TableRef;

public class PhoenixClientSort extends PhoenixAbstractSort {
    
    public static PhoenixClientSort create(RelNode input, RelCollation collation) {
        RelOptCluster cluster = input.getCluster();
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        RelTraitSet traits =
            input.getTraitSet().replace(PhoenixConvention.CLIENT).replace(collation);
        return new PhoenixClientSort(cluster, traits, input, collation);
    }

    private PhoenixClientSort(RelOptCluster cluster, RelTraitSet traits,
            RelNode child, RelCollation collation) {
        super(cluster, traits, child, collation);
    }

    @Override
    public PhoenixClientSort copy(RelTraitSet traitSet, RelNode newInput,
            RelCollation newCollation, RexNode offset, RexNode fetch) {
        return create(newInput, newCollation);
    }
    
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getInput().getConvention().satisfies(PhoenixConvention.CLIENT))
            return planner.getCostFactory().makeInfiniteCost();
        
        return super.computeSelfCost(planner, mq)
                .multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        if (this.offset != null)
            throw new UnsupportedOperationException();
            
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        
        TableRef tableRef = implementor.getTableRef();
        PhoenixStatement stmt = plan.getContext().getStatement();
        StatementContext context;
        try {
            context = new StatementContext(stmt, FromCompiler.getResolver(tableRef), new Scan(), new SequenceManager(stmt));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        OrderBy orderBy = super.getOrderBy(getCollation(), implementor, null);
        
        return new ClientScanPlan(context, plan.getStatement(), tableRef, RowProjector.EMPTY_PROJECTOR, null, null, orderBy, plan);
    }

}
