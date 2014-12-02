package org.apache.phoenix.calcite;

import java.sql.SQLException;
import java.util.Collections;

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
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.ScanPlan;
import org.apache.phoenix.expression.ColumnExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.iterate.ParallelIteratorFactory;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ColumnDef;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.NamedTableNode;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.TableRef;

import com.google.common.collect.ImmutableList;

class PhoenixRelImplementorImpl implements PhoenixRel.Implementor {
	private TableRef tableRef;
	private PhoenixConnection conn;
	private StatementContext context;
	private RowProjector projector;
	private SelectStatement select;
	
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
	        // TODO: real projection
	        this.select = SelectStatement.SELECT_STAR;
	        this.projector = ProjectionCompiler.compile(context, select, GroupBy.EMPTY_GROUP_BY, Collections.<PDatum>emptyList());
	        if (filter != null) {
	        	Expression filterExpr = CalciteUtils.toExpression(filter, this);
	        	filterExpr = WhereOptimizer.pushKeyExpressionsToScan(context, select, filterExpr);
	        	WhereCompiler.setScanFilter(context, select, filterExpr, true, false);
	        }
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public QueryPlan makePlan() {
		Integer limit = null;
		OrderBy orderBy = OrderBy.EMPTY_ORDER_BY;
		ParallelIteratorFactory iteratorFactory = null;
        return new ScanPlan(context, select, tableRef, projector, limit, orderBy, iteratorFactory, true);
	}

}
