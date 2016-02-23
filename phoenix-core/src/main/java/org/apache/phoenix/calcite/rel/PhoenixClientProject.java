package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.PhoenixSequence;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.ClientScanPlan;
import org.apache.phoenix.execute.TupleProjectionPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.schema.Sequence;

import com.google.common.base.Supplier;

public class PhoenixClientProject extends PhoenixAbstractProject {
    
    public static PhoenixClientProject create(final RelNode input, 
            final List<? extends RexNode> projects, RelDataType rowType) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = RelMetadataQuery.instance();
        final RelTraitSet traits =
                cluster.traitSet().replace(PhoenixConvention.CLIENT)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        new Supplier<List<RelCollation>>() {
                    public List<RelCollation> get() {
                        return RelMdCollation.project(mq, input, projects);
                    }
                });
        return new PhoenixClientProject(cluster, traits, input, projects, rowType);
    }

    private PhoenixClientProject(RelOptCluster cluster, RelTraitSet traits,
            RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public PhoenixClientProject copy(RelTraitSet traits, RelNode input,
            List<RexNode> projects, RelDataType rowType) {
        return create(input, projects, rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getInput().getConvention().satisfies(PhoenixConvention.GENERIC))
            return planner.getCostFactory().makeInfiniteCost();
        
        return super.computeSelfCost(planner, mq)
                .multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        implementor.pushContext(implementor.getCurrentContext().withColumnRefList(getColumnRefList()));
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        implementor.popContext();
        
        
        PhoenixSequence sequence = CalciteUtils.findSequence(this);
        final SequenceManager seqManager = sequence == null ?
                null : new SequenceManager(new PhoenixStatement(sequence.pc));
        implementor.setSequenceManager(seqManager);
        TupleProjector tupleProjector = project(implementor);
        if (seqManager != null) {
            try {
                seqManager.validateSequences(Sequence.ValueOp.VALIDATE_SEQUENCE);
                StatementContext context = new StatementContext(
                        plan.getContext().getStatement(),
                        plan.getContext().getResolver(),
                        new Scan(), seqManager);
                plan = new ClientScanPlan(
                        context, plan.getStatement(), plan.getTableRef(), 
                        RowProjector.EMPTY_PROJECTOR, null, null, 
                        OrderBy.EMPTY_ORDER_BY, plan);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        
        return new TupleProjectionPlan(plan, tupleProjector, null);
    }

}
