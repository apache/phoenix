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

import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.schema.tuple.ResultTuple;
import org.apache.phoenix.schema.tuple.Tuple;


abstract public class LookAheadResultIterator implements PeekingResultIterator {
    public static PeekingResultIterator wrap(final ResultIterator iterator) {
        if (iterator instanceof PeekingResultIterator) {
            return (PeekingResultIterator) iterator;
        }

        return new LookAheadResultIterator() {

            @Override
            public void explain(List<String> planSteps) {
                iterator.explain(planSteps);
            }

            @Override
            public void close() throws SQLException {
                iterator.close();
            }

            @Override
            protected Tuple advance() throws SQLException {
                return iterator.next();
            }
            
        };
    }
    
    private final static Tuple UNINITIALIZED = new ResultTuple();
    private Tuple next = UNINITIALIZED;
    private ResultIterator iterator;
    private RowProjector rowProjector;
    private boolean isUnionQuery = false;

    abstract protected Tuple advance() throws SQLException;
    
    private void init() throws SQLException {
        if (next == UNINITIALIZED) {
            next = advance();
        }
    }
    
    @Override
    public Tuple next() throws SQLException {
        init();
        Tuple next = this.next;
        this.next = advance();
        return next;
    }
    
    @Override
    public Tuple peek() throws SQLException {
        init();
        return next;
    }

    public ResultIterator getIterator() {
        return iterator;
    }

    public void setIterator(ResultIterator iterator) {
        this.iterator = iterator;
    }

    public RowProjector getRowProjector() {
        return rowProjector;
    }

    public void setRowProjector(RowProjector rowProjector) {
        this.rowProjector = rowProjector;
    }

    public boolean isUnionQuery() {
        return isUnionQuery;
    }

    public void setUnionQuery(boolean isUnionQuery) {
        this.isUnionQuery = isUnionQuery;
    }
   
}
 