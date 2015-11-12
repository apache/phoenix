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
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.ServerUtil;

/**
 * Base class for a ResultIterator that does a merge sort on the list of iterators provided.
 * @since 1.2
 */
public abstract class MergeSortResultIterator implements PeekingResultIterator {
    protected final ResultIterators resultIterators;
    protected final ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();
    private List<PeekingResultIterator> iterators;
    private PriorityQueue<MaterializedComparableResultIterator> minHeap;
    private final IteratorComparator itrComparator = new IteratorComparator();

    public MergeSortResultIterator(ResultIterators iterators) {
        this.resultIterators = iterators;
    }

    private List<PeekingResultIterator> getIterators() throws SQLException {
        if (iterators == null) {
            iterators = resultIterators.getIterators();
        }
        return iterators;
    }

    private PriorityQueue<MaterializedComparableResultIterator> getMinHeap() throws SQLException {
        if (minHeap == null) {
            List<PeekingResultIterator> iterators = getIterators();
            minHeap = new PriorityQueue<MaterializedComparableResultIterator>(iterators.size());
            for (PeekingResultIterator itr : iterators) {
                if (itr.peek() == null) {
                    itr.close();
                    continue;
                }
                minHeap.add(new MaterializedComparableResultIterator(itr, itrComparator));
            }
        }
        return minHeap;
    }

    private class IteratorComparator implements Comparator<Tuple> {
        public int compare(Tuple c1, Tuple c2) {
            return MergeSortResultIterator.this.compare(c1, c2);
        }
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

    private MaterializedComparableResultIterator minIterator() throws SQLException {
        PriorityQueue<MaterializedComparableResultIterator> minHeap = getMinHeap();
        MaterializedComparableResultIterator minIterator = minHeap.peek();
        return minIterator;
    }

    @Override
    public Tuple peek() throws SQLException {
        MaterializedComparableResultIterator iterator = minIterator();
        if (iterator == null) {
            return null;
        }
        return iterator.peek();
    }

    @Override
    public Tuple next() throws SQLException {
        MaterializedComparableResultIterator iterator = minIterator();
        if (iterator == null) {
            return null;
        }
        Tuple next = iterator.next();
        minHeap.poll();
        if (iterator.peek() != null) {
            minHeap.add(iterator);
        } else {
            iterator.close();
        }
        return next;
    }
}
