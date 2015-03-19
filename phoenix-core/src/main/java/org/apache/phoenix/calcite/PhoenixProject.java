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
        
        TupleProjector tupleProjector = project(implementor, getProjects());
        PTable projectedTable = implementor.createProjectedTable();
        implementor.setTableRef(new TableRef(projectedTable));
        return new TupleProjectionPlan(plan, tupleProjector, null, implementor.createRowProjector());
    }
    
    protected static TupleProjector project(Implementor implementor, List<RexNode> projects) {
        KeyValueSchema.KeyValueSchemaBuilder builder = new KeyValueSchema.KeyValueSchemaBuilder(0);
        Expression[] exprs = new Expression[projects.size()];
        List<PColumn> columns = Lists.<PColumn>newArrayList();
        for (int i = 0; i < projects.size(); i++) {
            String name = projects.get(i).toString();
            Expression expr = CalciteUtils.toExpression(projects.get(i), implementor);
            builder.addField(expr);
            exprs[i] = expr;
            columns.add(new PColumnImpl(PNameFactory.newName(name), PNameFactory.newName(TupleProjector.VALUE_COLUMN_FAMILY),
                    expr.getDataType(), expr.getMaxLength(), expr.getScale(), expr.isNullable(),
                    i, expr.getSortOrder(), null, null, false, name));
        }
        try {
            PTable pTable = PTableImpl.makePTable(null, PName.EMPTY_NAME, PName.EMPTY_NAME,
                    PTableType.SUBQUERY, null, MetaDataProtocol.MIN_TABLE_TIMESTAMP, PTable.INITIAL_SEQ_NUM,
                    null, null, columns, null, null, Collections.<PTable>emptyList(),
                    false, Collections.<PName>emptyList(), null, null, false, false, false, null,
                    null, null);
            implementor.setTableRef(new TableRef(CalciteUtils.createTempAlias(), pTable, HConstants.LATEST_TIMESTAMP, false));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        
        return new TupleProjector(builder.build(), exprs);        
    }
}
