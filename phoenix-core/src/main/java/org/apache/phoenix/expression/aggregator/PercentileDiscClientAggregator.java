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
package org.apache.phoenix.expression.aggregator;

import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.*;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * 
 * Built-in function for PERCENTILE_DISC(<expression>) WITHIN GROUP (ORDER BY <expression> ASC/DESC) aggregate function
 *
 * 
 * @since 1.2.1
 */
public class PercentileDiscClientAggregator extends DistinctValueWithCountClientAggregator {

	private final List<Expression> exps;
	ColumnExpression columnExp = null;

	public PercentileDiscClientAggregator(List<Expression> exps, SortOrder sortOrder) {
	    super(sortOrder);
		this.exps = exps;
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
		// Reset buffer so that it gets initialized with the current datatype of the column
		buffer = null;
		if (cachedResult == null) {
			columnExp = (ColumnExpression)exps.get(0);
			// Second exp will be a LiteralExpression of Boolean type indicating
			// whether the ordering to be ASC/DESC
			LiteralExpression isAscendingExpression = (LiteralExpression) exps
					.get(1);
			boolean isAscending = (Boolean) isAscendingExpression.getValue();

			// Third expression will be LiteralExpression
			LiteralExpression percentileExp = (LiteralExpression) exps.get(2);
			float p = ((Number) percentileExp.getValue()).floatValue();
			Map<Object, Integer> sorted = getSortedValueVsCount(isAscending, columnExp.getDataType());
			int currValue = 0;
			Object result = null;
			// Here the Percentile_disc returns the cum_dist() that is greater or equal to the
			// Percentile (p) specified in the query.  So the result set will be of that of the
			// datatype of the column being selected
			for (Entry<Object, Integer> entry : sorted.entrySet()) {
				result = entry.getKey();
				Integer value = entry.getValue();
				currValue += value;
				float cum_dist = (float) currValue / (float) totalCount;
				if (cum_dist >= p) {
					break;
				}
			}
			this.cachedResult = result;
		}
		if (buffer == null) {
			// Initialize based on the datatype
			// columnExp cannot be null
			buffer = new byte[columnExp.getDataType().getByteSize()];
		}
		// Copy the result to the buffer.
		System.arraycopy(columnExp.getDataType().toBytes(this.cachedResult), 0, buffer, 0, buffer.length);
		ptr.set(buffer);
		return true;
	}

	@Override
	protected int getBufferLength() {
		// Will be used in the aggregate() call
		return PDecimal.INSTANCE.getByteSize();
	}

    @Override
    protected PDataType getResultDataType() {
        return columnExp.getDataType();
    }
	
}
