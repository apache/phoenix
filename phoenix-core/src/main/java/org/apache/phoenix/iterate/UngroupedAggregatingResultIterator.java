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

import static org.apache.phoenix.query.QueryConstants.*;

import java.sql.SQLException;

import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.Aggregators;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.PhoenixKeyValueUtil;


public class UngroupedAggregatingResultIterator extends GroupedAggregatingResultIterator {
    private boolean hasRows = false;

    public UngroupedAggregatingResultIterator( PeekingResultIterator resultIterator, Aggregators aggregators) {
        super(resultIterator, aggregators);
    }
    @Override
    public Tuple next() throws SQLException {
        Tuple result = resultIterator.next();
        if (result == null) {
            // Ensure ungrouped aggregregation always returns a row, even if the underlying iterator doesn't.
            if (!hasRows) {
                // We should reset ClientAggregators here in case they are being reused in a new ResultIterator.
                aggregators.reset(aggregators.getAggregators());
                byte[] value = aggregators.toBytes(aggregators.getAggregators());
                result = new SingleKeyValueTuple(
                        PhoenixKeyValueUtil.newKeyValue(UNGROUPED_AGG_ROW_KEY,
                                SINGLE_COLUMN_FAMILY,
                                SINGLE_COLUMN,
                                AGG_TIMESTAMP,
                                value));
            }
        } else {
            Aggregator[] rowAggregators = aggregators.getAggregators();
            aggregators.reset(rowAggregators);
            while (true) {
                aggregators.aggregate(rowAggregators, result);
                Tuple nextResult = resultIterator.peek();
                if (nextResult == null) {
                    break;
                }
                result = resultIterator.next();
            }

            byte[] value = aggregators.toBytes(rowAggregators);
            Tuple tuple = wrapKeyValueAsResult(PhoenixKeyValueUtil .newKeyValue(UNGROUPED_AGG_ROW_KEY, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, AGG_TIMESTAMP, value, 0, value.length));
            result = tuple;
        }
        hasRows = true;
        return result;
    }

	@Override
	public String toString() {
		return "UngroupedAggregatingResultIterator [hasRows=" + hasRows
				+ ", aggregators=" + aggregators + "]";
	}
}
