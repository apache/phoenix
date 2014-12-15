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
package org.apache.phoenix.query;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.SequenceManager;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.iterate.ParallelIterators;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.SpoolingResultIterator;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixParameterMetaData;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.FilterableStatement;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.ScanUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Function;
import com.google.common.collect.Lists;


@RunWith(Parameterized.class)
public class ParallelIteratorsSplitTest extends BaseConnectionlessQueryTest {

    private static final String TABLE_NAME = "TEST_SKIP_RANGE_PARALLEL_ITERATOR";
    private static final String DDL = "CREATE TABLE " + TABLE_NAME + " (id char(3) NOT NULL PRIMARY KEY, \"value\" integer)";
    private static final byte[] Ka1A = Bytes.toBytes("a1A");
    private static final byte[] Ka1B = Bytes.toBytes("a1B");
    private static final byte[] Ka1E = Bytes.toBytes("a1E");
    private static final byte[] Ka1G = Bytes.toBytes("a1G");
    private static final byte[] Ka1I = Bytes.toBytes("a1I");
    private static final byte[] Ka2A = Bytes.toBytes("a2A");

    private final Scan scan;
    private final ScanRanges scanRanges;
    private final List<KeyRange> expectedSplits;

    public ParallelIteratorsSplitTest(Scan scan, ScanRanges scanRanges, List<KeyRange> expectedSplits) {
        this.scan = scan;
        this.scanRanges = scanRanges;
        this.expectedSplits = expectedSplits;
    }

    @Test
    public void testGetSplitsWithSkipScanFilter() throws Exception {
        byte[][] splits = new byte[][] {Ka1A, Ka1B, Ka1E, Ka1G, Ka1I, Ka2A};
        createTestTable(getUrl(),DDL,splits, null);
        Properties props = PropertiesUtil.deepCopy(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(getUrl(), props);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        
        PTable table = pconn.getMetaDataCache().getTable(new PTableKey(pconn.getTenantId(), TABLE_NAME));
        TableRef tableRef = new TableRef(table);
        List<HRegionLocation> regions = pconn.getQueryServices().getAllTableRegions(tableRef.getTable().getPhysicalName().getBytes());
        List<KeyRange> ranges = getSplits(tableRef, scan, regions, scanRanges);
        assertEquals("Unexpected number of splits: " + ranges.size(), expectedSplits.size(), ranges.size());
        for (int i=0; i<expectedSplits.size(); i++) {
            assertEquals(expectedSplits.get(i), ranges.get(i));
        }
    }

    private static KeyRange getKeyRange(byte[] lowerRange, boolean lowerInclusive, byte[] upperRange, boolean upperInclusive) {
        return PChar.INSTANCE.getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive);
    }

    private static KeyRange getKeyRange(String lowerRange, boolean lowerInclusive, String upperRange, boolean upperInclusive) {
        return PChar.INSTANCE.getKeyRange(Bytes.toBytes(lowerRange), lowerInclusive, Bytes.toBytes(upperRange), upperInclusive);
    }
    
    private static KeyRange getKeyRange(String lowerRange, boolean lowerInclusive, byte[] upperRange, boolean upperInclusive) {
        return PChar.INSTANCE.getKeyRange(Bytes.toBytes(lowerRange), lowerInclusive, upperRange, upperInclusive);
    }
    
    private static KeyRange getKeyRange(byte[] lowerRange, boolean lowerInclusive, String upperRange, boolean upperInclusive) {
        return PChar.INSTANCE.getKeyRange(lowerRange, lowerInclusive, Bytes.toBytes(upperRange), upperInclusive);
    }
    
    private static String nextKey(String s) {
        return Bytes.toString(ByteUtil.nextKey(Bytes.toBytes(s)));
    }

    @Parameters(name="{1} {2}")
    public static Collection<Object> data() {
        List<Object> testCases = Lists.newArrayList();
        // Scan range is empty.
        testCases.addAll(
                foreach(ScanRanges.NOTHING,
                        new int[] {1,1,1},
                        new KeyRange[] { }));
        // Scan range is everything.
        testCases.addAll(
                foreach(ScanRanges.EVERYTHING,
                        new int[] {1,1,1},
                        new KeyRange[] {
                            getKeyRange(KeyRange.UNBOUND, true, Ka1A, false),
                            getKeyRange(Ka1A, true, Ka1B, false),
                            getKeyRange(Ka1B, true, Ka1E, false),
                            getKeyRange(Ka1E, true, Ka1G, false),
                            getKeyRange(Ka1G, true, Ka1I, false),
                            getKeyRange(Ka1I, true, Ka2A, false),
                            getKeyRange(Ka2A, true, KeyRange.UNBOUND, false)
                }));
        // Scan range lies inside first region.
        testCases.addAll(
                foreach(new KeyRange[][]{
                        {
                            getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true)
                        },{
                            getKeyRange(Bytes.toBytes("0"), true, Bytes.toBytes("0"), true)
                        },{
                            getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("Z"), true)
                        }},
                    new int[] {1,1,1},
                    new KeyRange[] {
                        getKeyRange("a0A", true, nextKey("a0Z"), false)
                }));
        // Scan range lies in between first and second, intersecting bound on second.
        testCases.addAll(
                foreach(new KeyRange[][]{
                        {
                            getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true)
                        },{
                            getKeyRange(Bytes.toBytes("0"), true, Bytes.toBytes("0"), true),
                            getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true)
                        },{
                            getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true)
                        }},
                    new int[] {1,1,1},
                    new KeyRange[] {
                        getKeyRange("a0A", true, Ka1A, false),
                        getKeyRange(Ka1A, true, Ka1B, false),
                }));
        // Scan range spans third, split into 3 due to concurrency config.
        testCases.addAll(
                foreach(new KeyRange[][]{
                        {
                            getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true)
                        },{
                            getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true)
                        },{
                            getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("E"), false)
                        }},
                    new int[] {1,1,1},
                    new KeyRange[] {
                        getKeyRange(Ka1B, true, Ka1E, false)
                }));
        // Scan range spans third, split into 3 due to concurrency config.
        testCases.addAll(
                foreach(new KeyRange[][]{
                        {
                            getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true)
                        },{
                            getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true)
                        },{
                            getKeyRange(Bytes.toBytes("B"), true, Bytes.toBytes("E"), false)
                        }},
                    new int[] {1,1,1},
                    new KeyRange[] {
                        getKeyRange(Ka1B, true, Ka1E, false),
                }));
        // Scan range spans 2 ranges, split into 4 due to concurrency config.
        testCases.addAll(
                foreach(new KeyRange[][]{
                        {
                            getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true)
                        },{
                            getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true)
                        },{
                            getKeyRange(Bytes.toBytes("F"), true, Bytes.toBytes("H"), false)
                        }},
                    new int[] {1,1,1},
                    new KeyRange[] {
                        getKeyRange("a1F", true, Ka1G, false),
                        getKeyRange(Ka1G, true, "a1H", false),
                }));
        // Scan range spans more than 3 range, no split.
        testCases.addAll(
                foreach(new KeyRange[][]{
                        {
                            getKeyRange(Bytes.toBytes("a"), true, Bytes.toBytes("a"), true),
                            getKeyRange(Bytes.toBytes("b"), true, Bytes.toBytes("b"), true)
                        },{
                            getKeyRange(Bytes.toBytes("1"), true, Bytes.toBytes("1"), true),
                            getKeyRange(Bytes.toBytes("2"), true, Bytes.toBytes("2"), true),
                        },{
                            getKeyRange(Bytes.toBytes("A"), true, Bytes.toBytes("A"), true),
                            getKeyRange(Bytes.toBytes("C"), true, Bytes.toBytes("D"), true),
                            getKeyRange(Bytes.toBytes("G"), true, Bytes.toBytes("G"), true)
                        }},
                    new int[] {1,1,1},
                    new KeyRange[] {
                        getKeyRange(Ka1A, true, Ka1B, false),
                        getKeyRange(Ka1B, true, Ka1E, false),
                        getKeyRange(Ka1G, true, Ka1I, false),
                        getKeyRange(Ka2A, true, nextKey("b2G"), false)
                }));
        return testCases;
    }

    private static RowKeySchema buildSchema(int[] widths) {
        RowKeySchemaBuilder builder = new RowKeySchemaBuilder(10);
        for (final int width : widths) {
            builder.addField(new PDatum() {
                @Override
                public boolean isNullable() {
                    return false;
                }
                @Override
                public PDataType getDataType() {
                    return PChar.INSTANCE;
                }
                @Override
                public Integer getMaxLength() {
                    return width;
                }
                @Override
                public Integer getScale() {
                    return null;
                }
                @Override
                public SortOrder getSortOrder() {
                    return SortOrder.getDefault();
                }
            }, false, SortOrder.getDefault());
        }
        return builder.build();
    }
    
    private static Collection<?> foreach(ScanRanges scanRanges, int[] widths, KeyRange[] expectedSplits) {
         SkipScanFilter filter = new SkipScanFilter(scanRanges.getRanges(), buildSchema(widths));
        Scan scan = new Scan().setFilter(filter).setStartRow(KeyRange.UNBOUND).setStopRow(KeyRange.UNBOUND);
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {scan, scanRanges, Arrays.<KeyRange>asList(expectedSplits)});
        return ret;
    }

    private static Collection<?> foreach(KeyRange[][] ranges, int[] widths, KeyRange[] expectedSplits) {
        RowKeySchema schema = buildSchema(widths);
        List<List<KeyRange>> slots = Lists.transform(Lists.newArrayList(ranges), ARRAY_TO_LIST);
        SkipScanFilter filter = new SkipScanFilter(slots, schema);
        // Always set start and stop key to max to verify we are using the information in skipscan
        // filter over the scan's KMIN and KMAX.
        Scan scan = new Scan().setFilter(filter);
        ScanRanges scanRanges = ScanRanges.create(schema, slots, ScanUtil.getDefaultSlotSpans(ranges.length));
        List<Object> ret = Lists.newArrayList();
        ret.add(new Object[] {scan, scanRanges, Arrays.<KeyRange>asList(expectedSplits)});
        return ret;
    }

    private static final Function<KeyRange[], List<KeyRange>> ARRAY_TO_LIST = 
            new Function<KeyRange[], List<KeyRange>>() {
                @Override 
                public List<KeyRange> apply(KeyRange[] input) {
                    return Lists.newArrayList(input);
                }
    };

    private static List<KeyRange> getSplits(final TableRef tableRef, final Scan scan, final List<HRegionLocation> regions,
            final ScanRanges scanRanges) throws SQLException {
        final List<TableRef> tableRefs = Collections.singletonList(tableRef);
        ColumnResolver resolver = new ColumnResolver() {

            @Override
            public List<TableRef> getTables() {
                return tableRefs;
            }

            @Override
            public TableRef resolveTable(String schemaName, String tableName)
                    throws SQLException {
                throw new UnsupportedOperationException();
            }

            @Override
            public ColumnRef resolveColumn(String schemaName, String tableName, String colName) throws SQLException {
                throw new UnsupportedOperationException();
            }
            
        };
        PhoenixConnection connection = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class);
        final PhoenixStatement statement = new PhoenixStatement(connection);
        final StatementContext context = new StatementContext(statement, resolver, scan, new SequenceManager(statement));
        context.setScanRanges(scanRanges);
        ParallelIterators parallelIterators = new ParallelIterators(new QueryPlan() {

            @Override
            public StatementContext getContext() {
                return context;
            }

            @Override
            public ParameterMetaData getParameterMetaData() {
                return PhoenixParameterMetaData.EMPTY_PARAMETER_META_DATA;
            }

            @Override
            public ExplainPlan getExplainPlan() throws SQLException {
                return ExplainPlan.EMPTY_PLAN;
            }

            @Override
            public ResultIterator iterator() throws SQLException {
                return ResultIterator.EMPTY_ITERATOR;
            }

            @Override
            public long getEstimatedSize() {
                return 0;
            }

            @Override
            public TableRef getTableRef() {
                return tableRef;
            }

            @Override
            public RowProjector getProjector() {
                return RowProjector.EMPTY_PROJECTOR;
            }

            @Override
            public Integer getLimit() {
                return null;
            }

            @Override
            public OrderBy getOrderBy() {
                return OrderBy.EMPTY_ORDER_BY;
            }

            @Override
            public GroupBy getGroupBy() {
                return GroupBy.EMPTY_GROUP_BY;
            }

            @Override
            public List<KeyRange> getSplits() {
                return null;
            }

            @Override
            public FilterableStatement getStatement() {
                return SelectStatement.SELECT_ONE;
            }

            @Override
            public boolean isDegenerate() {
                return false;
            }

            @Override
            public boolean isRowKeyOrdered() {
                return true;
            }

            @Override
            public List<List<Scan>> getScans() {
                return null;
            }
            
        }, null, new SpoolingResultIterator.SpoolingResultIteratorFactory(context.getConnection().getQueryServices()));
        List<KeyRange> keyRanges = parallelIterators.getSplits();
        return keyRanges;
    }
}
