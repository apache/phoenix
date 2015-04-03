package org.apache.phoenix.calcite;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.TupleProjectionPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;

import com.google.common.collect.Lists;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Project}
 * relational expression in Phoenix.
 */
public class PhoenixProject extends Project implements PhoenixRel {
    public PhoenixProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
        assert getConvention() == PhoenixRel.CONVENTION;
    }

    @Override
    public PhoenixProject copy(RelTraitSet traits, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new PhoenixProject(getCluster(), traits, input, projects, rowType);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        RelOptCost cost = super.computeSelfCost(planner);
        if (getPlanType() != PlanType.CLIENT_SERVER) {
            cost = cost.multiplyBy(SERVER_FACTOR);
        }
        return cost.multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        assert getConvention() == getInput().getConvention();
        implementor.pushContext(new ImplementorContext(implementor.getCurrentContext().isRetainPKColumns(), false));
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        implementor.popContext();
        
        List<Expression> exprs = Lists.newArrayList();
        for (RexNode project : getProjects()) {
            exprs.add(CalciteUtils.toExpression(project, implementor));
        }
        TupleProjector tupleProjector = implementor.project(exprs);
        PTable projectedTable = implementor.createProjectedTable();
        implementor.setTableRef(new TableRef(projectedTable));
        
        boolean isScan = plan instanceof ScanPlan;
        if (getPlanType() == PlanType.CLIENT_SERVER 
                || TupleProjector.hasProjector(plan.getContext().getScan(), isScan))        
            return new TupleProjectionPlan(plan, tupleProjector, null);
        
        TupleProjector.serializeProjectorIntoScan(plan.getContext().getScan(), tupleProjector, isScan);
        return plan;
    }

    @Override
    public PlanType getPlanType() {
        RelNode rel = getInput();
        if (rel instanceof RelSubset) {
            rel = ((RelSubset) rel).getBest();
        }
        // TODO this is based on the assumption that there is no two Project 
        // in a row and Project can be pushed down to the input node if it is 
        // a server plan.
        return !(rel instanceof PhoenixRel) ? PlanType.CLIENT_SERVER : ((PhoenixRel) rel).getPlanType();
    }
}
