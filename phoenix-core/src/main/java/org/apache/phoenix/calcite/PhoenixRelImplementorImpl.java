package org.apache.phoenix.calcite;

import java.sql.SQLException;
import java.util.List;

import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExpressionProjector;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.compile.WhereOptimizer;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ProjectedColumnExpression;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.SchemaUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

class PhoenixRelImplementorImpl implements PhoenixRel.Implementor {
	private TableRef tableRef;
	private PhoenixConnection conn;
	private StatementContext context;
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
	        this.select = SelectStatement.SELECT_STAR;
	        if (filter != null) {
	        	Expression filterExpr = CalciteUtils.toExpression(filter, this);
	        	filterExpr = WhereOptimizer.pushKeyExpressionsToScan(context, select, filterExpr);
	        	WhereCompiler.setScanFilter(context, select, filterExpr, true, false);
	        }
            this.projectExpressions = Lists.<Expression> newArrayListWithExpectedSize(projects.size());
            for (RexNode p : projects) {
                this.projectExpressions.add(CalciteUtils.toExpression(p, this));
            }
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
	        projectAllColumnFamilies(context.getScan());
	        TupleProjector tupleProjector = createTupleProjector();
	        TupleProjector.serializeProjectorIntoScan(context.getScan(), tupleProjector);
	        Integer limit = null;
	        OrderBy orderBy = OrderBy.EMPTY_ORDER_BY;
	        ParallelIteratorFactory iteratorFactory = null;
	        return new ScanPlan(context, select, tableRef, createRowProjector(tupleProjector), limit, orderBy, iteratorFactory, true);
	    } catch (SQLException e) {
	        throw new RuntimeException(e);
	    }
	}
    
    private TupleProjector createTupleProjector() {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(0);
        List<Expression> exprs = this.projectExpressions;
        if (this.projects == null) {
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
    
    private RowProjector createRowProjector(TupleProjector tupleProjector) {
        List<ColumnProjector> columnProjectors = Lists.<ColumnProjector>newArrayList();
        if (this.projects == null) {
            for (PColumn column : tableRef.getTable().getPKColumns()) {
                columnProjectors.add(new ExpressionProjector("dummy", "dummy", new ColumnRef(tableRef, column.getPosition()).newColumnExpression(), false));
            }
        }
        
        for (int i = 0; i < tupleProjector.getSchema().getFieldCount(); i++) {
            columnProjectors.add(new ExpressionProjector("dummy", "dummy", new ProjectedColumnExpression(tupleProjector.getSchema(), i, "dummy"), false));
        }
        
        return new RowProjector(columnProjectors, 0, false);
    }
    
    private void projectAllColumnFamilies(Scan scan) {
        scan.getFamilyMap().clear();
        for (PColumnFamily family : tableRef.getTable().getColumnFamilies()) {
            scan.addFamily(family.getName().getBytes());
        }
    }

}
