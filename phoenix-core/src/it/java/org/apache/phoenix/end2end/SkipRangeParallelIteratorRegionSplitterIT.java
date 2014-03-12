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
package org.apache.phoenix.end2end;

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.filter.SkipScanFilter;
import org.apache.phoenix.iterate.SkipRangeParallelIteratorRegionSplitter;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.HintNode;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.ColumnRef;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.RowKeySchema.RowKeySchemaBuilder;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * Tests for {@link SkipRangeParallelIteratorRegionSplitter}.
 */
@RunWith(Parameterized.class)
public class SkipRangeParallelIteratorRegionSplitterIT extends BaseClientManagedTimeIT {

    private static final String TABLE_NAME = "TEST_SKIP_RANGE_PARALLEL_ITERATOR";
    private static final String DDL = "CREATE TABLE " + TABLE_NAME + " (id char(3) NOT NULL PRIMARY KEY, \"value\" integer)";
    private static final byte[] Ka1A = Bytes.toBytes("a1A");
    private static final byte[] Ka1B = Bytes.toBytes("a1B");
    private static final byte[] Ka1C = Bytes.toBytes("a1C");
    private static final byte[] Ka1D = Bytes.toBytes("a1D");
    private static final byte[] Ka1E = Bytes.toBytes("a1E");
    private static final byte[] Ka1F = Bytes.toBytes("a1F");
    private static final byte[] Ka1G = Bytes.toBytes("a1G");
    private static final byte[] Ka1H = Bytes.toBytes("a1H");
    private static final byte[] Ka1I = Bytes.toBytes("a1I");
    private static final byte[] Ka2A = Bytes.toBytes("a2A");

    private final Scan scan;
    private final ScanRanges scanRanges;
    private final List<KeyRange> expectedSplits;

    public SkipRangeParallelIteratorRegionSplitterIT(Scan scan, ScanRanges scanRanges, List<KeyRange> expectedSplits) {
        this.scan = scan;
        this.scanRanges = scanRanges;
        this.expectedSplits = expectedSplits;
    }

    @Test
    public void testGetSplitsWithSkipScanFilter() throws Exception {
        byte[][] splits = new byte[][] {Ka1A, Ka1B, Ka1E, Ka1G, Ka1I, Ka2A};
        long ts = nextTimestamp();
        createTestTable(getUrl(),DDL,splits, ts-2);
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + ts;
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        TableRef tableRef = new TableRef(null,pconn.getMetaDataCache().getTable(new PTableKey(pconn.getTenantId(), TABLE_NAME)),ts, false);
        List<HRegionLocation> regions = pconn.getQueryServices().getAllTableRegions(tableRef.getTable().getPhysicalName().getBytes());
        
        conn.close();
        initTableValues();
        List<KeyRange> ranges = getSplits(tableRef, scan, regions, scanRanges);
        assertEquals("Unexpected number of splits: " + ranges.size(), expectedSplits.size(), ranges.size());
        for (int i=0; i<expectedSplits.size(); i++) {
            assertEquals(expectedSplits.get(i), ranges.get(i));
        }
    }

    private static KeyRange getKeyRange(byte[] lowerRange, boolean lowerInclusive, byte[] upperRange, boolean upperInclusive) {
        return PDataType.CHAR.getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive);
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
                        getKeyRange(KeyRange.UNBOUND, true, Ka1A, false)
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
                        getKeyRange(KeyRange.UNBOUND, true, Ka1A, false),
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
                        getKeyRange(Ka1B, true, Ka1C, false),
                        getKeyRange(Ka1C, true, Ka1D, false),
                        getKeyRange(Ka1D, true, Ka1E, false),
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
                        getKeyRange(Ka1B, true, Ka1C, false),
                        getKeyRange(Ka1C, true, Ka1D, false),
                        getKeyRange(Ka1D, true, Ka1E, false),
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
                        getKeyRange(Ka1E, true, Ka1F, false),
                        getKeyRange(Ka1F, true, Ka1G, false),
                        getKeyRange(Ka1G, true, Ka1H, false),
                        getKeyRange(Ka1H, true, Ka1I, false),
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
                        getKeyRange(Ka2A, true, KeyRange.UNBOUND, false)
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
                    return PDataType.CHAR;
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
        Scan scan = new Scan().setFilter(filter).setStartRow(KeyRange.UNBOUND).setStopRow(KeyRange.UNBOUND);
        ScanRanges scanRanges = ScanRanges.create(slots, schema);
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

    private void initTableValues() throws SQLException {
        String url = getUrl() + ";" + PhoenixRuntime.CURRENT_SCN_ATTRIB + "=" + nextTimestamp();
        Properties props = new Properties(TEST_PROPERTIES);
        Connection conn = DriverManager.getConnection(url, props);
        PreparedStatement stmt = conn.prepareStatement(
                "upsert into " + TABLE_NAME + " VALUES (?, ?)");
        stmt.setString(1, new String("a1A"));
        stmt.setInt(2, 1);
        stmt.execute();
        stmt.setString(1, new String("a1E"));
        stmt.setInt(2, 2);
        stmt.execute();
        conn.commit();
        conn.close();
     }
    
    @BeforeClass
    public static void doSetup() throws Exception {
        int targetQueryConcurrency = 3;
        int maxQueryConcurrency = 5;
        Map<String,String> props = Maps.newHashMapWithExpectedSize(3);
        props.put(QueryServices.MAX_QUERY_CONCURRENCY_ATTRIB, Integer.toString(maxQueryConcurrency));
        props.put(QueryServices.TARGET_QUERY_CONCURRENCY_ATTRIB, Integer.toString(targetQueryConcurrency));
        props.put(QueryServices.MAX_INTRA_REGION_PARALLELIZATION_ATTRIB, Integer.toString(Integer.MAX_VALUE));
        // Must update config before starting server
        startServer(getUrl(), new ReadOnlyProps(props.entrySet().iterator()));
    }

    private static List<KeyRange> getSplits(TableRef tableRef, final Scan scan, final List<HRegionLocation> regions,
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
        PhoenixConnection connection = DriverManager.getConnection(getUrl(), TEST_PROPERTIES).unwrap(PhoenixConnection.class);
        StatementContext context = new StatementContext(new PhoenixStatement(connection), resolver, scan);
        context.setScanRanges(scanRanges);
        SkipRangeParallelIteratorRegionSplitter splitter = SkipRangeParallelIteratorRegionSplitter.getInstance(context, tableRef, HintNode.EMPTY_HINT_NODE);
        List<KeyRange> keyRanges = splitter.getSplits();
        Collections.sort(keyRanges, new Comparator<KeyRange>() {
            @Override
            public int compare(KeyRange o1, KeyRange o2) {
                return Bytes.compareTo(o1.getLowerRange(),o2.getLowerRange());
            }
        });
        return keyRanges;
    }
}
