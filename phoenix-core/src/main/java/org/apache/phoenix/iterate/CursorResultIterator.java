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
package org.apache.phoenix.iterate;

import org.apache.phoenix.compile.ExplainPlanAttributes
    .ExplainPlanAttributesBuilder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.CursorUtil;

import java.sql.SQLException;
import java.util.List;

public class CursorResultIterator implements ResultIterator {
    private String cursorName;
    private PeekingResultIterator delegate;
    //TODO Configure fetch size from FETCH call
    private int fetchSize = 0;
    private int rowsRead = 0;
    public CursorResultIterator(PeekingResultIterator delegate, String cursorName) {
        this.delegate = delegate;
        this.cursorName = cursorName;
    }

    @Override
    public Tuple next() throws SQLException {
    	if(!CursorUtil.moreValues(cursorName)){
    	    return null;
        } else if (fetchSize == rowsRead) {
            return null;
    	}

        Tuple next = delegate.next();
        CursorUtil.updateCursor(cursorName,next, delegate.peek());
        rowsRead++;
        return next;
    }
    
    @Override
    public void explain(List<String> planSteps) {
        delegate.explain(planSteps);
        planSteps.add("CLIENT CURSOR " + cursorName);
    }

    @Override
    public void explain(List<String> planSteps,
            ExplainPlanAttributesBuilder explainPlanAttributesBuilder) {
        delegate.explain(planSteps, explainPlanAttributesBuilder);
        explainPlanAttributesBuilder.setClientCursorName(cursorName);
        planSteps.add("CLIENT CURSOR " + cursorName);
    }

    @Override
    public String toString() {
        return "CursorResultIterator [cursor=" + cursorName + "]";
    }

    @Override
    public void close() throws SQLException {
        //NOP
    }

    public void closeCursor() throws SQLException {
        delegate.close();
    }

    public void setFetchSize(int fetchSize){
        this.fetchSize = fetchSize;
        this.rowsRead = 0;
    }
}
