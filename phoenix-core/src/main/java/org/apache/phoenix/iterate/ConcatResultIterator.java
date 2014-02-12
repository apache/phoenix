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

import org.apache.phoenix.schema.tuple.Tuple;


/**
 * 
 * Result iterator that concatenates a list of other iterators.
 *
 * 
 * @since 0.1
 */
public class ConcatResultIterator implements PeekingResultIterator {
    private final ResultIterators resultIterators;
    private List<PeekingResultIterator> iterators;
    private int index;
    
    public ConcatResultIterator(ResultIterators iterators) {
        this.resultIterators = iterators;
    }
    
    private List<PeekingResultIterator> getIterators() throws SQLException {
        if (iterators == null) {
            iterators = resultIterators.getIterators();
        }
        return iterators;
    }
    
    @Override
    public void close() throws SQLException {
        if (iterators != null) {
            for (;index < iterators.size(); index++) {
                PeekingResultIterator iterator = iterators.get(index);
                iterator.close();
            }
        }
    }


    @Override
    public void explain(List<String> planSteps) {
        resultIterators.explain(planSteps);
    }

    private PeekingResultIterator currentIterator() throws SQLException {
        List<PeekingResultIterator> iterators = getIterators();
        while (index < iterators.size()) {
            PeekingResultIterator iterator = iterators.get(index);
            Tuple r = iterator.peek();
            if (r != null) {
                return iterator;
            }
            iterator.close();
            index++;
        }
        return EMPTY_ITERATOR;
    }
    
    @Override
    public Tuple peek() throws SQLException {
        return currentIterator().peek();
    }

    @Override
    public Tuple next() throws SQLException {
        return currentIterator().next();
    }

}
