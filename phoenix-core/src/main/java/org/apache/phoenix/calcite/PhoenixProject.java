package org.apache.phoenix.calcite;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExpressionProjector;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.execute.DelegateQueryPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.iterate.ResultIterator;

import com.google.common.collect.Lists;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Project}
 * relational expression in Phoenix.
 */
public class PhoenixProject extends Project implements PhoenixRel {
    public PhoenixProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
        super(cluster, traits, input, projects, rowType, Flags.BOXED);
        assert getConvention() == PhoenixRel.CONVENTION;
        assert getConvention() == input.getConvention();
    }

    @Override
    public PhoenixProject copy(RelTraitSet traits, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new PhoenixProject(getCluster(), traits, input, projects, rowType);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner) {
        return super.computeSelfCost(planner).multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        
        List<RexNode> projects = getProjects();
        List<ColumnProjector> columnProjectors = Lists.<ColumnProjector>newArrayList();
        for (int i = 0; i < projects.size(); i++) {
            String name = projects.get(i).toString();
            Expression expr = CalciteUtils.toExpression(projects.get(i), implementor);
            columnProjectors.add(new ExpressionProjector(name, "", expr, false));
        }
        final RowProjector rowProjector = new RowProjector(columnProjectors, plan.getProjector().getEstimatedRowByteSize(), plan.getProjector().isProjectEmptyKeyValue());

        return new DelegateQueryPlan(plan) {
            
            @Override
            public RowProjector getProjector() {
                return rowProjector;
            }

            @Override
            public ResultIterator iterator() throws SQLException {
                return delegate.iterator();
            }

            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                return delegate.getExplainPlan();
            }
            
        };
    }
}
