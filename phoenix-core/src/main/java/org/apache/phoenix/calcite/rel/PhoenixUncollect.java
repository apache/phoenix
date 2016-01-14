package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.Arrays;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.UnnestArrayPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.types.PDataType;

public class PhoenixUncollect extends Uncollect implements PhoenixRel {
    
    public static PhoenixUncollect create(RelNode input) {
        RelOptCluster cluster = input.getCluster();
        RelTraitSet traits = cluster.traitSetOf(PhoenixConvention.CLIENT);
        return new PhoenixUncollect(cluster, traits, input);
    }

    private PhoenixUncollect(RelOptCluster cluster, RelTraitSet traitSet,
            RelNode child) {
        super(cluster, traitSet, child);
    }

    @Override
    public PhoenixUncollect copy(RelTraitSet traitSet,
        RelNode newInput) {
        return create(newInput);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getInput().getConvention().satisfies(PhoenixConvention.GENERIC))
            return planner.getCostFactory().makeInfiniteCost();
        
        return super.computeSelfCost(planner, mq).multiplyBy(PHOENIX_FACTOR);
    }
    
    @Override
    public QueryPlan implement(Implementor implementor) {
        QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
        Expression arrayExpression = implementor.newColumnExpression(0);
        @SuppressWarnings("rawtypes")
        PDataType baseType = PDataType.fromTypeId(arrayExpression.getDataType().getSqlType() - PDataType.ARRAY_TYPE_BASE);
        try {
            implementor.project(Arrays.<Expression> asList(LiteralExpression.newConstant(null, baseType)));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        PTable projectedTable = implementor.createProjectedTable();
        implementor.setTableRef(new TableRef(projectedTable));
        return new UnnestArrayPlan(plan, arrayExpression, false);
    }

}
