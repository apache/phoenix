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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.aggregator.ClientAggregators;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.AssertResults;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;

public class RowKeyOrderedAggregateResultIteratorTest extends BaseConnectionlessQueryTest {
    private final static byte[] A = Bytes.toBytes("a");
    private final static byte[] B = Bytes.toBytes("b");
    private final static byte[] C = Bytes.toBytes("c");
    private final static byte[] D = Bytes.toBytes("d");

    @Test
    public void testNoSpan() throws Exception {
        Tuple[] results1 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1L))),
            };
        Tuple[] results2 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(2L)))
            };
        Tuple[] results3 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(C, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(3L))),
                new SingleKeyValueTuple(new KeyValue(D, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(4L))),
            };
        final List<PeekingResultIterator>results = Arrays.asList(new PeekingResultIterator[] {new MaterializedResultIterator(Arrays.asList(results1)), new MaterializedResultIterator(Arrays.asList(results2)), new MaterializedResultIterator(Arrays.asList(results3))});
        ResultIterators iterators = new MaterializedResultIterators(results);

        Tuple[] expectedResults = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1L))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(2L))),
                new SingleKeyValueTuple(new KeyValue(C, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(3L))),
                new SingleKeyValueTuple(new KeyValue(D, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(4L))),
            };

        ClientAggregators aggregators = TestUtil.getSingleSumAggregator(getUrl(), PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES));
        ResultIterator scanner = new RowKeyOrderedAggregateResultIterator(iterators, aggregators);
        AssertResults.assertResults(scanner, expectedResults);
    }
    
    @Test
    public void testSpanThree() throws Exception {
        Tuple[] results1 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1L))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(2L)))
            };
        Tuple[] results2 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(3L)))
            };
        Tuple[] results3 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(4L))),
                new SingleKeyValueTuple(new KeyValue(C, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(5L))),
            };
        final List<PeekingResultIterator>results = Arrays.asList(new PeekingResultIterator[] {new MaterializedResultIterator(Arrays.asList(results1)), new MaterializedResultIterator(Arrays.asList(results2)), new MaterializedResultIterator(Arrays.asList(results3))});
        ResultIterators iterators = new MaterializedResultIterators(results);

        Tuple[] expectedResults = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1L))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(9L))),
                new SingleKeyValueTuple(new KeyValue(C, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(5L))),
            };

        ClientAggregators aggregators = TestUtil.getSingleSumAggregator(getUrl(), PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES));
        ResultIterator scanner = new RowKeyOrderedAggregateResultIterator(iterators, aggregators);
        AssertResults.assertResults(scanner, expectedResults);
    }

    @Test
    public void testSpanAll() throws Exception {
        Tuple[] results1 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(2L)))
            };
        Tuple[] results2 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(3L)))
            };
        Tuple[] results3 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(4L))),
            };
        final List<PeekingResultIterator>results = Arrays.asList(new PeekingResultIterator[] {new MaterializedResultIterator(Arrays.asList(results1)), new MaterializedResultIterator(Arrays.asList(results2)), new MaterializedResultIterator(Arrays.asList(results3))});
        ResultIterators iterators = new MaterializedResultIterators(results);

        Tuple[] expectedResults = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(9L))),
            };

        ClientAggregators aggregators = TestUtil.getSingleSumAggregator(getUrl(), PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES));
        ResultIterator scanner = new RowKeyOrderedAggregateResultIterator(iterators, aggregators);
        AssertResults.assertResults(scanner, expectedResults);
    }
    
    @Test
    public void testSpanEnd() throws Exception {
        Tuple[] results1 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(1L))),
            };
        Tuple[] results2 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(2L))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(3L))),
                new SingleKeyValueTuple(new KeyValue(C, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(4L))),
                new SingleKeyValueTuple(new KeyValue(D, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(5L))),
            };
        Tuple[] results3 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(D, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(6L))),
            };
        final List<PeekingResultIterator>results = Arrays.asList(new PeekingResultIterator[] {new MaterializedResultIterator(Arrays.asList(results1)), new MaterializedResultIterator(Arrays.asList(results2)), new MaterializedResultIterator(Arrays.asList(results3))});
        ResultIterators iterators = new MaterializedResultIterators(results);

        Tuple[] expectedResults = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(3L))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(3L))),
                new SingleKeyValueTuple(new KeyValue(C, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(4L))),
                new SingleKeyValueTuple(new KeyValue(D, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, Bytes.toBytes(11L))),
            };

        ClientAggregators aggregators = TestUtil.getSingleSumAggregator(getUrl(), PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES));
        ResultIterator scanner = new RowKeyOrderedAggregateResultIterator(iterators, aggregators);
        AssertResults.assertResults(scanner, expectedResults);
    }

}
