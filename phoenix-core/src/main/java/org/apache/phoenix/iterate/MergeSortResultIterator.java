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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.ServerUtil;


/**
 * 
 * Base class for a ResultIterator that does a merge sort on the list of iterators
 * provided.
 *
 * 
 * @since 1.2
 */
public abstract class MergeSortResultIterator implements PeekingResultIterator {
    protected final ResultIterators resultIterators;
    protected final ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();
    private List<PeekingResultIterator> iterators;
    
    public MergeSortResultIterator(ResultIterators iterators) {
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
                    SQLCloseables.closeAll(iterators);
                }
            } catch (Exception e) {
                if (toThrow == null) {
                    toThrow = ServerUtil.parseServerException(e);
                } else {
                    toThrow.setNextException(ServerUtil.parseServerException(e));
                }
            } finally {
                if (toThrow != null) {
                    throw toThrow;
                }
            }
        }
    }

    abstract protected int compare(Tuple t1, Tuple t2);
    
    private PeekingResultIterator minIterator() throws SQLException {
        List<PeekingResultIterator> iterators = getIterators();
        Tuple minResult = null;
        PeekingResultIterator minIterator = EMPTY_ITERATOR;
        for (int i = iterators.size()-1; i >= 0; i--) {
            PeekingResultIterator iterator = iterators.get(i);
            Tuple r = iterator.peek();
            if (r != null) {
                if (minResult == null || compare(r, minResult) < 0) {
                    minResult = r;
                    minIterator = iterator;
                }
                continue;
            }
            iterator.close();
            iterators.remove(i);
        }
        return minIterator;
    }
    
    @Override
    public Tuple peek() throws SQLException {
        PeekingResultIterator iterator = minIterator();
        return iterator.peek();
    }

    @Override
    public Tuple next() throws SQLException {
        PeekingResultIterator iterator = minIterator();
        return iterator.next();
    }
}
