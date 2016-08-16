package org.apache.phoenix.execute;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.iterate.CursorResultIterator;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixStatement.Operation;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.TableRef;

public class CursorFetchPlan extends DelegateQueryPlan {

	//QueryPlan cursorQueryPlan;
	private CursorResultIterator resultIterator;
	private int fetchSize;

	public CursorFetchPlan(QueryPlan cursorQueryPlan) {
		super(cursorQueryPlan);
	}


	@Override
	public ResultIterator iterator() throws SQLException {
		// TODO Auto-generated method stub
		StatementContext context = delegate.getContext();
		if (resultIterator != null) {
			return resultIterator;
		} else {
			context.getOverallQueryMetrics().startQuery();
			resultIterator = (CursorResultIterator) delegate.iterator();
			return resultIterator;
		}
	}

	@Override
	public ResultIterator iterator(ParallelScanGrouper scanGrouper) throws SQLException {
		// TODO Auto-generated method stub
		StatementContext context = delegate.getContext();
		if (resultIterator != null) {
			return resultIterator;
		} else {
			context.getOverallQueryMetrics().startQuery();
			resultIterator = (CursorResultIterator) delegate.iterator(scanGrouper);
			return resultIterator;
		}
	}

	@Override
	public ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
		// TODO Auto-generated method stub
		StatementContext context = delegate.getContext();
		if (resultIterator != null) {
			return resultIterator;
		} else {
			context.getOverallQueryMetrics().startQuery();
			resultIterator = (CursorResultIterator) delegate.iterator(scanGrouper, scan);
			return resultIterator;
		}
	}


	@Override
	public ExplainPlan getExplainPlan() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void setFetchSize(int fetchSize){
	    this.fetchSize = fetchSize;	
	}

	public int getFetchSize() {
		return fetchSize;
	}
}
