package org.apache.phoenix.calcite.rel;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.phoenix.calcite.TableMapping;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.execute.UnnestArrayPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;

import com.google.common.collect.Lists;

public class PhoenixUncollect extends Uncollect implements PhoenixQueryRel {
    
    public static PhoenixUncollect create(RelNode input, boolean withOrdinality) {
        RelOptCluster cluster = input.getCluster();
        RelTraitSet traits = cluster.traitSetOf(PhoenixConvention.CLIENT);
        return new PhoenixUncollect(cluster, traits, input, withOrdinality);
    }

    private PhoenixUncollect(RelOptCluster cluster, RelTraitSet traitSet,
            RelNode child, boolean withOrdinality) {
        super(cluster, traitSet, child, withOrdinality);
    }

    @Override
    public PhoenixUncollect copy(RelTraitSet traitSet,
        RelNode newInput) {
        return create(newInput, withOrdinality);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (!getInput().getConvention().satisfies(PhoenixConvention.GENERIC))
            return planner.getCostFactory().makeInfiniteCost();
        
        return super.computeSelfCost(planner, mq).multiplyBy(PHOENIX_FACTOR);
    }
    
    @Override
    public QueryPlan implement(PhoenixRelImplementor implementor) {
        QueryPlan plan = implementor.visitInput(0, (PhoenixQueryRel) getInput());
        Expression arrayExpression = implementor.newColumnExpression(0);
        @SuppressWarnings("rawtypes")
        PDataType baseType = PDataType.fromTypeId(arrayExpression.getDataType().getSqlType() - PDataType.ARRAY_TYPE_BASE);
        try {
            List<Expression> fields = Lists.newArrayList();
            fields.add(LiteralExpression.newConstant(null, baseType));
            if (withOrdinality) {
                fields.add(LiteralExpression.newConstant(null, PInteger.INSTANCE));
            }
            implementor.project(fields);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        PTable projectedTable = implementor.getTableMapping().createProjectedTable(implementor.getCurrentContext().retainPKColumns);
        implementor.setTableMapping(new TableMapping(projectedTable));
        return new UnnestArrayPlan(plan, arrayExpression, withOrdinality);
    }

}
