package org.apache.phoenix.calcite;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.ProjectionCompiler;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.compile.WhereOptimizer;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.TupleProjectionPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.AliasedNode;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.TableWildcardParseNode;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

class PhoenixRelImplementorImpl implements PhoenixRel.Implementor {
    private TableRef tableRef;
    private PhoenixConnection conn;
    private StatementContext context;
    private RowProjector projector;
    private SelectStatement select;
    private List<? extends RexNode> projects;
    private List<Expression> projectExpressions;
    
    @Override
    public void visitInput(int i, PhoenixRel input) {
        input.implement(this, conn);
    }

    @Override
    public ColumnExpression newColumnExpression(int index) {
        ColumnRef colRef = new ColumnRef(tableRef, index);
        return colRef.newColumnExpression();
    }


    @Override
    public void setContext(PhoenixConnection conn, PTable table, RexNode filter) {
        this.conn = conn;
        this.tableRef = new TableRef(table);
        PhoenixStatement stmt = new PhoenixStatement(conn);
        ColumnResolver resolver;
        try {
            resolver = FromCompiler.getResolver(
                    NamedTableNode.create(
                        null,
                        TableName.create(tableRef.getTable().getSchemaName().getString(), tableRef.getTable().getTableName().getString()),
                        ImmutableList.<ColumnDef>of()), conn);
            this.context = new StatementContext(stmt, resolver, new Scan(), new SequenceManager(stmt));
            if (filter != null) {
                Expression filterExpr = CalciteUtils.toExpression(filter, this);
                filterExpr = WhereOptimizer.pushKeyExpressionsToScan(context, select, filterExpr);
                WhereCompiler.setScanFilter(context, select, filterExpr, true, false);
            }
            this.projectExpressions = Lists.<Expression> newArrayListWithExpectedSize(projects.size());
            for (RexNode p : projects) {
                this.projectExpressions.add(CalciteUtils.toExpression(p, this));
            }
            PTable projectedTable = createProjectedTable(table);
            this.context.setResolver(FromCompiler.getResolver(new TableRef(projectedTable)));
            this.select = SelectStatement.create(SelectStatement.SELECT_STAR, Collections.<AliasedNode>singletonList(new AliasedNode(null, TableWildcardParseNode.create(TableName.create(table.getSchemaName().getString(), table.getName().getString()), false))));
            this.projector = ProjectionCompiler.compile(context, select, GroupBy.EMPTY_GROUP_BY, Collections.<PDatum>emptyList());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setProjects(List<? extends RexNode> projects) {
        this.projects = projects;
    }

    @Override
    public QueryPlan makePlan() {
        try {
            Integer limit = null;
            OrderBy orderBy = OrderBy.EMPTY_ORDER_BY;
            ParallelIteratorFactory iteratorFactory = null;
            QueryPlan plan = new ScanPlan(context, select, tableRef, projector, limit, orderBy, iteratorFactory, true);
            TupleProjector tupleProjector = createTupleProjector(tableRef);
            return new TupleProjectionPlan(plan, tupleProjector, null);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    private PTable createProjectedTable(PTable table) throws SQLException {
        if (this.projects == null) {
            return PTableImpl.makePTable(table, PTableType.PROJECTED, table.getColumns());
        }
        
        List<PColumn> projectedColumns = new ArrayList<PColumn>();
        for (int i = 0; i < this.projects.size(); i++) {
            Expression e = this.projectExpressions.get(i);
            PColumnImpl projectedColumn = new PColumnImpl(PNameFactory.newName(
                    this.projects.get(i).toString()), PNameFactory.newName(TupleProjector.VALUE_COLUMN_FAMILY), 
                    e.getDataType(), e.getMaxLength(), e.getScale(), e.isNullable(), 
                    projectedColumns.size(), e.getSortOrder(), null, null, false);                
            projectedColumns.add(projectedColumn);
        }
        return PTableImpl.makePTable(table, PTableType.PROJECTED, projectedColumns);        
    }
    
    private TupleProjector createTupleProjector(TableRef tableRef) {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        List<Expression> exprs = this.projectExpressions;
        if (exprs == null) {
            exprs = Lists.<Expression> newArrayList();
            for (PColumn column : tableRef.getTable().getColumns()) {
                if (!SchemaUtil.isPKColumn(column)) {
                    exprs.add(newColumnExpression(column.getPosition()));
                }
            }
        }
        for (Expression e : exprs) {
            builder.addField(e);                
        }
        
        return new TupleProjector(builder.build(), exprs.toArray(new Expression[exprs.size()]));
    }

}
