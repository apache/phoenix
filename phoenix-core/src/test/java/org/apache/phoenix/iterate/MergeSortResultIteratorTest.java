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

import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.AssertResults;
import org.junit.Test;


public class MergeSortResultIteratorTest {
    private final static byte[] A = Bytes.toBytes("a");
    private final static byte[] B = Bytes.toBytes("b");

    @Test
    public void testMergeSort() throws Throwable {
        Tuple[] results1 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
            };
        Tuple[] results2 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1)))
            };
        Tuple[] results3 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
            };
        final List<PeekingResultIterator>results = new ArrayList<PeekingResultIterator>(Arrays.asList(new PeekingResultIterator[] {new MaterializedResultIterator(Arrays.asList(results1)), new MaterializedResultIterator(Arrays.asList(results2)), new MaterializedResultIterator(Arrays.asList(results3))}));

        Tuple[] expectedResults = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1))),
            };

        ResultIterators iterators = new ResultIterators() {

            @Override
            public List<PeekingResultIterator> getIterators() throws SQLException {
                return results;
            }

            @Override
            public int size() {
                return results.size();
            }

            @Override
            public void explain(List<String> planSteps) {
            }
            
			@Override
			public List<KeyRange> getSplits() {
				return Collections.emptyList();
			}

			@Override
			public List<List<Scan>> getScans() {
				return Collections.emptyList();
			}

            @Override
            public void close() throws SQLException {
            }
        };
        ResultIterator scanner = new MergeSortRowKeyResultIterator(iterators);
        AssertResults.assertResults(scanner, expectedResults);
    }

}
