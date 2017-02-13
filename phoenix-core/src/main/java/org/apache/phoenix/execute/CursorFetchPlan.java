package org.apache.phoenix.execute;

import java.sql.SQLException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.iterate.CursorResultIterator;
import org.apache.phoenix.iterate.LookAheadResultIterator;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;

public class CursorFetchPlan extends DelegateQueryPlan {

    private CursorResultIterator resultIterator;
    private int fetchSize;
    private boolean isAggregate;
    private String cursorName;

	public CursorFetchPlan(QueryPlan cursorQueryPlan,String cursorName) {
		super(cursorQueryPlan);
        this.isAggregate = delegate.getStatement().isAggregate() || delegate.getStatement().isDistinct();
        this.cursorName = cursorName;
	}

	@Override
	public ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
		StatementContext context = delegate.getContext();
		if (resultIterator == null) {
			context.getOverallQueryMetrics().startQuery();
			resultIterator = new CursorResultIterator(LookAheadResultIterator.wrap(delegate.iterator(scanGrouper, scan)),cursorName);
		}
	    return resultIterator;
	}


	@Override
	public ExplainPlan getExplainPlan() throws SQLException {
		return delegate.getExplainPlan();
	}
	
	public void setFetchSize(int fetchSize){
	    this.fetchSize = fetchSize;	
	}

	public int getFetchSize() {
		return fetchSize;
	}

        public boolean isAggregate(){
            return this.isAggregate;
        }
}
