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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.schema.tuple.MultiKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.AssertResults;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class DistinctAggregatingResultIteratorTest {
    private final static byte[] cf = Bytes.toBytes("cf");
    private final static byte[] cq1 = Bytes.toBytes("cq1");
    private final static byte[] cq2 = Bytes.toBytes("cq2");
    private final static byte[] cq3 = Bytes.toBytes("cq3");
    private final static byte[] rowKey1 = Bytes.toBytes("rowKey1");
    private final static byte[] rowKey2 = Bytes.toBytes("rowKey2");
    private final static byte[] rowKey3 = Bytes.toBytes("rowKey3");
    private final static byte[] rowKey4 = Bytes.toBytes("rowKey4");
    private final static byte[] rowKey5 = Bytes.toBytes("rowKey4");

    @Test
    public void testDistinctAggregatingResultIterator() throws Throwable {
        //Test with duplicate
        Tuple[] input1 = new Tuple[] {
                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey1, cf, cq1, PInteger.INSTANCE.toBytes(1)),
                                new KeyValue(rowKey1, cf, cq2, PInteger.INSTANCE.toBytes(2)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey2, cf, cq1, PInteger.INSTANCE.toBytes(11)),
                                new KeyValue(rowKey2, cf, cq2, PInteger.INSTANCE.toBytes(12)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey3, cf, cq1, PInteger.INSTANCE.toBytes(4)),
                                new KeyValue(rowKey3, cf, cq2, PInteger.INSTANCE.toBytes(2)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey1, cf, cq1, PInteger.INSTANCE.toBytes(1)),
                                new KeyValue(rowKey1, cf, cq2, PInteger.INSTANCE.toBytes(2)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey4, cf, cq1, PInteger.INSTANCE.toBytes(7)),
                                new KeyValue(rowKey4, cf, cq2, PInteger.INSTANCE.toBytes(8)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey2, cf, cq1, PInteger.INSTANCE.toBytes(11)),
                                new KeyValue(rowKey2, cf, cq2, PInteger.INSTANCE.toBytes(12)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey5, cf, cq1, PInteger.INSTANCE.toBytes(90)),
                                new KeyValue(rowKey5, cf, cq2, PInteger.INSTANCE.toBytes(100)))),
                null

        };

        Tuple[] result1 = new Tuple[] {
                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey1, cf, cq1, PInteger.INSTANCE.toBytes(1)),
                                new KeyValue(rowKey1, cf, cq2, PInteger.INSTANCE.toBytes(2)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey2, cf, cq1, PInteger.INSTANCE.toBytes(11)),
                                new KeyValue(rowKey2, cf, cq2, PInteger.INSTANCE.toBytes(12)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey3, cf, cq1, PInteger.INSTANCE.toBytes(4)),
                                new KeyValue(rowKey3, cf, cq2, PInteger.INSTANCE.toBytes(2)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey4, cf, cq1, PInteger.INSTANCE.toBytes(7)),
                                new KeyValue(rowKey4, cf, cq2, PInteger.INSTANCE.toBytes(8)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey5, cf, cq1, PInteger.INSTANCE.toBytes(90)),
                                new KeyValue(rowKey5, cf, cq2, PInteger.INSTANCE.toBytes(100))))

        };
        RowProjector mockRowProjector = Mockito.mock(RowProjector.class);
        Mockito.when(mockRowProjector.getColumnCount()).thenReturn(2);

        KeyValueColumnExpression columnExpression1 = new KeyValueColumnExpression(cf, cq1);
        KeyValueColumnExpression columnExpression2 = new KeyValueColumnExpression(cf, cq2);
        final ColumnProjector mockColumnProjector1 = Mockito.mock(ColumnProjector.class);
        Mockito.when(mockColumnProjector1.getExpression()).thenReturn(columnExpression1);
        final ColumnProjector mockColumnProjector2 = Mockito.mock(ColumnProjector.class);
        Mockito.when(mockColumnProjector2.getExpression()).thenReturn(columnExpression2);

        Mockito.when(mockRowProjector.getColumnProjectors()).thenAnswer(
                new Answer<List<ColumnProjector> >() {
                    @Override
                    public List<ColumnProjector> answer(InvocationOnMock invocation) throws Throwable {
                        return Arrays.asList(mockColumnProjector1,mockColumnProjector2);
                    }
                });

        assertResults(
                input1, result1, mockRowProjector);

        //Test with duplicate and null
        Tuple[] input2 = new Tuple[] {
                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey1, cf, cq1, PInteger.INSTANCE.toBytes(1)),
                                new KeyValue(rowKey1, cf, cq2, PInteger.INSTANCE.toBytes(2)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey2, cf, cq1, PInteger.INSTANCE.toBytes(11)),
                                new KeyValue(rowKey2, cf, cq2, PInteger.INSTANCE.toBytes(12)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey3, cf, cq1, PInteger.INSTANCE.toBytes(4)),
                                new KeyValue(rowKey3, cf, cq2, PInteger.INSTANCE.toBytes(2)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey1, cf, cq2, PInteger.INSTANCE.toBytes(1)),
                                new KeyValue(rowKey1, cf, cq1, PInteger.INSTANCE.toBytes(2)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey4, cf, cq1, PInteger.INSTANCE.toBytes(7)),
                                new KeyValue(rowKey4, cf, cq2, PInteger.INSTANCE.toBytes(8)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey2, cf, cq1, PInteger.INSTANCE.toBytes(11)),
                                new KeyValue(rowKey2, cf, cq3, PInteger.INSTANCE.toBytes(12)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey1, cf, cq2, PInteger.INSTANCE.toBytes(1)),
                                new KeyValue(rowKey1, cf, cq1, PInteger.INSTANCE.toBytes(2)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey5, cf, cq1, PInteger.INSTANCE.toBytes(90)),
                                new KeyValue(rowKey5, cf, cq2, PInteger.INSTANCE.toBytes(100)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey2, cf, cq1, PInteger.INSTANCE.toBytes(11)),
                                new KeyValue(rowKey2, cf, cq3, PInteger.INSTANCE.toBytes(12)))),

                null

        };

        Tuple[] result2 = new Tuple[] {
                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey1, cf, cq1, PInteger.INSTANCE.toBytes(1)),
                                new KeyValue(rowKey1, cf, cq2, PInteger.INSTANCE.toBytes(2)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey2, cf, cq1, PInteger.INSTANCE.toBytes(11)),
                                new KeyValue(rowKey2, cf, cq2, PInteger.INSTANCE.toBytes(12)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey3, cf, cq1, PInteger.INSTANCE.toBytes(4)),
                                new KeyValue(rowKey3, cf, cq2, PInteger.INSTANCE.toBytes(2)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey1, cf, cq2, PInteger.INSTANCE.toBytes(1)),
                                new KeyValue(rowKey1, cf, cq1, PInteger.INSTANCE.toBytes(2)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey4, cf, cq1, PInteger.INSTANCE.toBytes(7)),
                                new KeyValue(rowKey4, cf, cq2, PInteger.INSTANCE.toBytes(8)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey2, cf, cq1, PInteger.INSTANCE.toBytes(11)),
                                new KeyValue(rowKey2, cf, cq3, PInteger.INSTANCE.toBytes(12)))),

                new MultiKeyValueTuple(
                        Arrays.<Cell> asList(
                                new KeyValue(rowKey5, cf, cq1, PInteger.INSTANCE.toBytes(90)),
                                new KeyValue(rowKey5, cf, cq2, PInteger.INSTANCE.toBytes(100))))

        };
        assertResults(
                input2, result2, mockRowProjector);

        //Test with no duplicate
        int n = 100;
        Tuple[] input3 = new Tuple[n + 1];
        for(int i = 0; i <= n; i++) {
            byte[] rowKey = PInteger.INSTANCE.toBytes(i);
            input3[i] =  new MultiKeyValueTuple(
                            Arrays.<Cell> asList(
                               new KeyValue(rowKey, cf, cq1, PInteger.INSTANCE.toBytes(i + 1)),
                               new KeyValue(rowKey, cf, cq2, PInteger.INSTANCE.toBytes(i + 2))));
        }
        input3[n] = null;
        Tuple[] result3 = Arrays.copyOfRange(input3, 0, n);
        assertResults(
                input3, result3, mockRowProjector);

        //Test with all duplicate
        Tuple[] input4 = new Tuple[n + 1];
        for(int i = 0; i <= n; i++) {
            byte[] rowKey = PInteger.INSTANCE.toBytes(1);
            input4[i] =  new MultiKeyValueTuple(
                            Arrays.<Cell> asList(
                               new KeyValue(rowKey, cf, cq1, PInteger.INSTANCE.toBytes(2)),
                               new KeyValue(rowKey, cf, cq2, PInteger.INSTANCE.toBytes(3))));
        }
        input4[n] = null;
        Tuple[] result4 = new Tuple[] {input4[0]};
        assertResults(
                input4, result4, mockRowProjector);

    }

    private void assertResults(Tuple[] input, Tuple[] result,  RowProjector rowProjector) throws Exception {
        AggregatingResultIterator mockAggregatingResultIterator =
                Mockito.mock(AggregatingResultIterator.class);
        Mockito.when(mockAggregatingResultIterator.next()).thenReturn(
                input[0], Arrays.copyOfRange(input, 1, input.length));

        DistinctAggregatingResultIterator distinctAggregatingResultIterator =
                new DistinctAggregatingResultIterator(mockAggregatingResultIterator, rowProjector);
        AssertResults.assertResults(
                distinctAggregatingResultIterator, result);
    }

}
