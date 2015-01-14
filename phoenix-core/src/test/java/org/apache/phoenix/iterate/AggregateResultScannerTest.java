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
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_FAMILY_NAME;
import static org.apache.phoenix.query.QueryConstants.SINGLE_COLUMN_NAME;
import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.AggregationManager;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.aggregator.ClientAggregators;
import org.apache.phoenix.expression.function.SingleAggregateFunction;
import org.apache.phoenix.expression.function.SumAggregateFunction;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.PLongColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.util.AssertResults;
import org.apache.phoenix.util.PropertiesUtil;
import org.junit.Test;



public class AggregateResultScannerTest extends BaseConnectionlessQueryTest {
    private final static byte[] A = Bytes.toBytes("a");
    private final static byte[] B = Bytes.toBytes("b");

    @Test
    public void testAggregatingMergeSort() throws Throwable {
        Tuple[] results1 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PLong.INSTANCE.toBytes(1L))),
            };
        Tuple[] results2 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PLong.INSTANCE.toBytes(1L)))
            };
        Tuple[] results3 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PLong.INSTANCE.toBytes(1L))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PLong.INSTANCE.toBytes(1L))),
            };
        Tuple[] results4 = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PLong.INSTANCE.toBytes(1L))),
            };
        final List<PeekingResultIterator>results = new ArrayList<PeekingResultIterator>(Arrays.asList(new PeekingResultIterator[] {
                new MaterializedResultIterator(Arrays.asList(results1)), 
                new MaterializedResultIterator(Arrays.asList(results2)), 
                new MaterializedResultIterator(Arrays.asList(results3)), 
                new MaterializedResultIterator(Arrays.asList(results4))}));

        Tuple[] expectedResults = new Tuple[] {
                new SingleKeyValueTuple(new KeyValue(A, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PLong.INSTANCE.toBytes(3L))),
                new SingleKeyValueTuple(new KeyValue(B, SINGLE_COLUMN_FAMILY, SINGLE_COLUMN, PLong.INSTANCE.toBytes(2L))),
            };

        PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        PhoenixStatement statement = new PhoenixStatement(pconn);
        StatementContext context = new StatementContext(statement, null, new Scan(), new SequenceManager(statement));
        AggregationManager aggregationManager = context.getAggregationManager();
        SumAggregateFunction func = new SumAggregateFunction(Arrays.<Expression>asList(new KeyValueColumnExpression(new PLongColumn() {
            @Override
            public PName getName() {
                return SINGLE_COLUMN_NAME;
            }
            @Override
            public PName getFamilyName() {
                return SINGLE_COLUMN_FAMILY_NAME;
            }
            @Override
            public int getPosition() {
                return 0;
            }
            
            @Override
            public SortOrder getSortOrder() {
            	return SortOrder.getDefault();
            }
            
            @Override
            public Integer getArraySize() {
                return 0;
            }
            
            @Override
            public byte[] getViewConstant() {
                return null;
            }
            
            @Override
            public boolean isViewReferenced() {
                return false;
            }
            
            @Override
            public String getExpressionStr() {
                return null;
            }
        })), null);
        aggregationManager.setAggregators(new ClientAggregators(Collections.<SingleAggregateFunction>singletonList(func), 1));
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
        ResultIterator scanner = new GroupedAggregatingResultIterator(new MergeSortRowKeyResultIterator(iterators), aggregationManager.getAggregators());
        AssertResults.assertResults(scanner, expectedResults);
    }
}
