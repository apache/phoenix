package org.apache.phoenix.calcite.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.phoenix.calcite.CalciteUtils;
import org.apache.phoenix.calcite.ImplicitNullLiteral;
import org.apache.phoenix.calcite.TableMapping;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.PTable;

import com.google.common.collect.Lists;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Project}
 * relational expression in Phoenix.
 */
abstract public class PhoenixAbstractProject extends Project implements PhoenixQueryRel {
    protected PhoenixAbstractProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // This is to minimize the weight of cost of Project so that it
        // does not affect more important decisions like join algorithms.
        double rowCount = mq.getRowCount(getInput());
        double rows = 2 * rowCount / (rowCount + 1);
        return planner.getCostFactory().makeCost(rows, 0, 0);
    }
    
    protected ImmutableIntList getColumnRefList() {
        ImmutableBitSet bitSet = ImmutableBitSet.of();
        for (RexNode node : getProjects()) {
            InputFinder inputFinder = InputFinder.analyze(node);
            bitSet = bitSet.union(inputFinder.inputBitSet.build());
        }
        return ImmutableIntList.copyOf(bitSet.asList());
    }
    
    protected TupleProjector project(PhoenixRelImplementor implementor) {        
        List<Expression> exprs = Lists.newArrayList();
        List<Integer> unspecifiedColumnPositions = Lists.newArrayList();
        List<RexNode> projects = getProjects();
        boolean bindVariablesPresent = false;
        for(RexNode project: projects) {
            if(project instanceof RexDynamicParam) {
                bindVariablesPresent = true;
                break;
            }
        }
        for (int i = 0; i < projects.size(); i++) {
            RexNode project = projects.get(i);
            if ((bindVariablesPresent && RexLiteral.isNullLiteral(project))
                    || project instanceof ImplicitNullLiteral
                    || (project instanceof RexCall && !((RexCall) project).getOperands().isEmpty() && (((RexCall) project)
                            .getOperands().get(0) instanceof ImplicitNullLiteral))) {
                unspecifiedColumnPositions.add(new Integer(i));
                continue;
            }
            exprs.add(CalciteUtils.toExpression(project, implementor));
        }
        TupleProjector tupleProjector = implementor.project(exprs);
        PTable projectedTable = implementor.getTableMapping().createProjectedTable(implementor.getCurrentContext().retainPKColumns);
        implementor.setTableMapping(new TableMapping(projectedTable));
        implementor.setUnspecifiedColumnPositions(unspecifiedColumnPositions);

        return tupleProjector;
    }
}
