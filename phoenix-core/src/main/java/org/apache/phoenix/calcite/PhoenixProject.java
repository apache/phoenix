package org.apache.phoenix.calcite;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.execute.TupleProjectionPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
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
        return super.computeSelfCost(planner).multiplyBy(PHOENIX_FACTOR);
    }

    @Override
    public QueryPlan implement(Implementor implementor) {
        assert getConvention() == getInput().getConvention();
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        
        List<Expression> exprs = Lists.newArrayList();
        for (RexNode project : getProjects()) {
            exprs.add(CalciteUtils.toExpression(project, implementor));
        }
        TupleProjector tupleProjector = implementor.project(exprs);
        PTable projectedTable = implementor.createProjectedTable();
        implementor.setTableRef(new TableRef(projectedTable));
        return new TupleProjectionPlan(plan, tupleProjector, null, implementor.createRowProjector());
    }
}
