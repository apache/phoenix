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

import java.util.List;

import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.TupleUtil;


/**
 * 
 * ResultIterator that does a merge sort on the list of iterators provided,
 * returning the rows in row key ascending order. The iterators provided
 * must be in row key ascending order.
 *
 * 
 * @since 0.1
 */
public class MergeSortRowKeyResultIterator extends MergeSortResultIterator {
    private final int keyOffset;
    private final int factor;
    
    public MergeSortRowKeyResultIterator(ResultIterators iterators) {
        this(iterators, 0, false);
    }
    
    public MergeSortRowKeyResultIterator(ResultIterators iterators, int keyOffset, boolean isReverse) {
        super(iterators);
        this.keyOffset = keyOffset;
        this.factor = isReverse ? -1 : 1;
    }
   
    @Override
    protected int compare(Tuple t1, Tuple t2) {
        return factor * TupleUtil.compare(t1, t2, tempPtr, keyOffset);
    }

    @Override
    public void explain(List<String> planSteps) {
        resultIterators.explain(planSteps);
        planSteps.add("CLIENT MERGE SORT");
    }

	@Override
	public String toString() {
		return "MergeSortRowKeyResultIterator [keyOffset=" + keyOffset
				+ ", factor=" + factor + "]";
	}
}