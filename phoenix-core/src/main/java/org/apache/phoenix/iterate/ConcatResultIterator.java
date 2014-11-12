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
import org.apache.phoenix.util.ServerUtil;


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
    
    private ConcatResultIterator(List<PeekingResultIterator> iterators) {
        this.resultIterators = null;
        this.iterators = iterators;
    }
    
    private List<PeekingResultIterator> getIterators() throws SQLException {
        if (iterators == null && resultIterators != null) {
            iterators = resultIterators.getIterators();
        }
        return iterators;
    }
    
    @Override
    public void close() throws SQLException {
        SQLException toThrow = null;
        try {
            if (resultIterators != null) {
                resultIterators.close();
            }
        } catch (Exception e) {
           toThrow = ServerUtil.parseServerException(e);
        } finally {
            try {
                if (iterators != null) {
                    for (;index < iterators.size(); index++) {
                        PeekingResultIterator iterator = iterators.get(index);
                        try {
                            iterator.close();
                        } catch (Exception e) {
                            if (toThrow == null) {
                                toThrow = ServerUtil.parseServerException(e);
                            } else {
                                toThrow.setNextException(ServerUtil.parseServerException(e));
                            }
                        }
                    }
                }
            } finally {
                if (toThrow != null) {
                    throw toThrow;
                }
            }
        }
    }


    @Override
    public void explain(List<String> planSteps) {
        if (resultIterators != null) {
            resultIterators.explain(planSteps);
        }
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
        Tuple next = currentIterator().next();
        if (next == null) {
            close(); // Close underlying ResultIterators to free resources sooner rather than later
        }
        return next;
    }

	@Override
	public String toString() {
		return "ConcatResultIterator [" + resultIterators == null ? ("iterators=" + iterators) : ("resultIterators=" + resultIterators) 
				+ ", index=" + index + "]";
	}

    public static PeekingResultIterator newIterator(final List<PeekingResultIterator> concatIterators) {
        if (concatIterators.isEmpty()) {
            return PeekingResultIterator.EMPTY_ITERATOR;
        } 
        
        if (concatIterators.size() == 1) {
            return concatIterators.get(0);
        }
        return new ConcatResultIterator(concatIterators);
    }
}
