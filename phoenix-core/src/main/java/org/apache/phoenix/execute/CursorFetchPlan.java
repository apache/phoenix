/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
