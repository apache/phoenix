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
package org.apache.phoenix.hbase.index.covered;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.BaseRegionScanner;
import org.apache.phoenix.hbase.index.MultiMutation;
import org.apache.phoenix.hbase.index.covered.data.LocalTable;
import org.apache.phoenix.hbase.index.covered.update.ColumnTracker;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.index.IndexMaintainer;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.index.PhoenixIndexMetaData;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.PropertiesUtil;
import org.apache.phoenix.util.TestUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class TestNonTxIndexBuilder extends BaseConnectionlessQueryTest {
    private static final String TEST_TABLE_STRING = "TEST_TABLE";
    private static final String TEST_TABLE_DDL = "CREATE TABLE IF NOT EXISTS " +
            TEST_TABLE_STRING + " (\n" +
        "    ORGANIZATION_ID CHAR(4) NOT NULL,\n" +
        "    ENTITY_ID CHAR(7) NOT NULL,\n" +
        "    SCORE INTEGER,\n" +
        "    LAST_UPDATE_TIME TIMESTAMP\n" +
        "    CONSTRAINT TEST_TABLE_PK PRIMARY KEY (\n" +
        "        ORGANIZATION_ID,\n" +
        "        ENTITY_ID\n" +
        "    )\n" +
        ") VERSIONS=1, MULTI_TENANT=TRUE";
    private static final String TEST_TABLE_INDEX_STRING = "TEST_TABLE_SCORE";
    private static final String TEST_TABLE_INDEX_DDL = "CREATE INDEX IF NOT EXISTS " +
            TEST_TABLE_INDEX_STRING
            + " ON " + TEST_TABLE_STRING + " (SCORE DESC, ENTITY_ID DESC)";
    private static final byte[] ROW = Bytes.toBytes("org1entity1"); //length 4 + 7 (see ddl)
    private static final String FAM_STRING = QueryConstants.DEFAULT_COLUMN_FAMILY;
    private static final byte[] FAM = Bytes.toBytes(FAM_STRING);
    private static final byte[] INDEXED_QUALIFIER = Bytes.toBytes("SCORE");
    private static final byte[] VALUE_1 = Bytes.toBytes(111);
    private static final byte[] VALUE_2 = Bytes.toBytes(222);
    private static final byte[] VALUE_3 = Bytes.toBytes(333);
    private static final byte[] VALUE_4 = Bytes.toBytes(444);
    private static final byte PUT_TYPE = KeyValue.Type.Put.getCode();

    private NonTxIndexBuilder indexBuilder;
    private PhoenixIndexMetaData mockIndexMetaData;
    // Put your current row state in here - the index builder will read from this in LocalTable
    // to determine whether the index has changed.
    // Whatever we return here should match the table DDL (e.g. length of column value)
    private List<Cell> currentRowCells;

    /**
     * Test setup so that {@link NonTxIndexBuilder#getIndexUpdate(Mutation, IndexMetaData)} can be
     * called, where any read requests to
     * {@link LocalTable#getCurrentRowState(Mutation, Collection, boolean)} are read from our test
     * field 'currentRowCells'
     */
    @Before
    public void setup() throws Exception {
        RegionCoprocessorEnvironment env = Mockito.mock(RegionCoprocessorEnvironment.class);
        Configuration conf = new Configuration(false);
        conf.set(NonTxIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());
        Mockito.when(env.getConfiguration()).thenReturn(conf);

        // the following is used by LocalTable#getCurrentRowState()
        Region mockRegion = Mockito.mock(Region.class);
        Mockito.when(env.getRegion()).thenReturn(mockRegion);

        Mockito.when(mockRegion.getScanner(Mockito.any(Scan.class)))
                .thenAnswer(new Answer<RegionScanner>() {
                    @Override
                    public RegionScanner answer(InvocationOnMock invocation) throws Throwable {
                        Scan sArg = (Scan) invocation.getArguments()[0];
                        TimeRange timeRange = sArg.getTimeRange();
                        return getMockTimeRangeRegionScanner(timeRange);
                    }
                });

        // the following is called by PhoenixIndexCodec#getIndexUpserts() , getIndexDeletes()
        HRegionInfo mockRegionInfo = Mockito.mock(HRegionInfo.class);
        Mockito.when(mockRegion.getRegionInfo()).thenReturn(mockRegionInfo);
        Mockito.when(mockRegionInfo.getStartKey()).thenReturn(Bytes.toBytes("a"));
        Mockito.when(mockRegionInfo.getEndKey()).thenReturn(Bytes.toBytes("z"));

        mockIndexMetaData = Mockito.mock(PhoenixIndexMetaData.class);
        Mockito.when(mockIndexMetaData.isImmutableRows()).thenReturn(false);
        Mockito.when(mockIndexMetaData.getIndexMaintainers())
                .thenReturn(Collections.singletonList(getTestIndexMaintainer()));

        indexBuilder = new NonTxIndexBuilder();
        indexBuilder.setup(env);
    }

    // returns a RegionScanner which filters currentRowCells using the given TimeRange.
    // This is called from LocalTable#getCurrentRowState()
    // If testIndexMetaData.ignoreNewerMutations() is not set, default TimeRange is 0 to
    // Long.MAX_VALUE
    private RegionScanner getMockTimeRangeRegionScanner(final TimeRange timeRange) {
        return new BaseRegionScanner(Mockito.mock(RegionScanner.class)) {
            @Override
            public boolean next(List<Cell> results) throws IOException {
                for (Cell cell : currentRowCells) {
                    if (cell.getTimestamp() >= timeRange.getMin()
                            && cell.getTimestamp() < timeRange.getMax()) {
                        results.add(cell);
                    }
                }
                return false; // indicate no more results
            }
        };
    }

    private IndexMaintainer getTestIndexMaintainer() throws Exception {
        Properties props = PropertiesUtil.deepCopy(TestUtil.TEST_PROPERTIES);
        // disable column encoding, makes debugging easier
        props.put(QueryServices.DEFAULT_COLUMN_ENCODED_BYTES_ATRRIB, "0");
        Connection conn = DriverManager.getConnection(getUrl(), props);
        try {
            conn.setAutoCommit(true);
            conn.createStatement().execute(TEST_TABLE_DDL);
            conn.createStatement().execute(TEST_TABLE_INDEX_DDL);
            PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
            PTable table = pconn.getTable(new PTableKey(pconn.getTenantId(), TEST_TABLE_STRING));
            ImmutableBytesWritable ptr = new ImmutableBytesWritable();
            table.getIndexMaintainers(ptr, pconn);
            List<IndexMaintainer> indexMaintainerList =
                    IndexMaintainer.deserialize(ptr, GenericKeyValueBuilder.INSTANCE, true);
            assertEquals(1, indexMaintainerList.size());
            IndexMaintainer indexMaintainer = indexMaintainerList.get(0);
            return indexMaintainer;
        } finally {
            conn.close();
        }
    }

    /**
     * Tests that updating an indexed column results in a DeleteFamily (prior index cell) and a Put
     * (new index cell)
     */
    @Test
    public void testGetMutableIndexUpdate() throws IOException {
        setCurrentRowState(FAM, INDEXED_QUALIFIER, 1, VALUE_1);

        // update ts and value
        Put put = new Put(ROW);
        put.addImmutable(FAM, INDEXED_QUALIFIER, 2, VALUE_2);
        MultiMutation mutation = new MultiMutation(new ImmutableBytesPtr(ROW));
        mutation.addAll(put);

        Collection<Pair<Mutation, byte[]>> indexUpdates =
                indexBuilder.getIndexUpdate(mutation, mockIndexMetaData);
        assertEquals(2, indexUpdates.size());
        assertContains(indexUpdates, 2, ROW, KeyValue.Type.DeleteFamily, FAM,
            new byte[0] /* qual not needed */, 2);
        assertContains(indexUpdates, ColumnTracker.NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP, ROW,
            KeyValue.Type.Put, FAM, QueryConstants.EMPTY_COLUMN_BYTES, 2);
    }

    /**
     * Tests a partial rebuild of a row with multiple versions. 3 versions of the row in data table,
     * and we rebuild the index starting from time t=2
     *
     * There should be one index row version per data row version.
     */
    @Test
    public void testRebuildMultipleVersionRow() throws IOException {
        // when doing a rebuild, we are replaying mutations so we want to ignore newer mutations
        // see LocalTable#getCurrentRowState()
        Mockito.when(mockIndexMetaData.ignoreNewerMutations()).thenReturn(true);

        // the current row state has 3 versions, but if we rebuild as of t=2, scanner in LocalTable
        // should only return first
        Cell currentCell1 = CellUtil.createCell(ROW, FAM, INDEXED_QUALIFIER, 1, PUT_TYPE, VALUE_1);
        Cell currentCell2 = CellUtil.createCell(ROW, FAM, INDEXED_QUALIFIER, 2, PUT_TYPE, VALUE_2);
        Cell currentCell3 = CellUtil.createCell(ROW, FAM, INDEXED_QUALIFIER, 3, PUT_TYPE, VALUE_3);
        Cell currentCell4 = CellUtil.createCell(ROW, FAM, INDEXED_QUALIFIER, 4, PUT_TYPE, VALUE_4);
        setCurrentRowState(Arrays.asList(currentCell4, currentCell3, currentCell2, currentCell1));

        // rebuilder replays mutations starting from t=2
        MultiMutation mutation = new MultiMutation(new ImmutableBytesPtr(ROW));
        Put put = new Put(ROW);
        put.addImmutable(FAM, INDEXED_QUALIFIER, 4, VALUE_4);
        mutation.addAll(put);
        put = new Put(ROW);
        put.addImmutable(FAM, INDEXED_QUALIFIER, 3, VALUE_3);
        mutation.addAll(put);
        put = new Put(ROW);
        put.addImmutable(FAM, INDEXED_QUALIFIER, 2, VALUE_2);
        mutation.addAll(put);

        Collection<Pair<Mutation, byte[]>> indexUpdates =
                indexBuilder.getIndexUpdate(mutation, mockIndexMetaData);
        // 3 puts and 3 deletes (one to hide existing index row for VALUE_1, and two to hide index
        // rows for VALUE_2, VALUE_3)
        assertEquals(6, indexUpdates.size());

        assertContains(indexUpdates, 2, ROW, KeyValue.Type.DeleteFamily, FAM,
            new byte[0] /* qual not needed */, 2);
        assertContains(indexUpdates, ColumnTracker.NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP, ROW,
            KeyValue.Type.Put, FAM, QueryConstants.EMPTY_COLUMN_BYTES, 2);
        assertContains(indexUpdates, 3, ROW, KeyValue.Type.DeleteFamily, FAM,
            new byte[0] /* qual not needed */, 3);
        assertContains(indexUpdates, ColumnTracker.NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP, ROW,
            KeyValue.Type.Put, FAM, QueryConstants.EMPTY_COLUMN_BYTES, 3);
        assertContains(indexUpdates, 4, ROW, KeyValue.Type.DeleteFamily, FAM,
            new byte[0] /* qual not needed */, 4);
        assertContains(indexUpdates, ColumnTracker.NO_NEWER_PRIMARY_TABLE_ENTRY_TIMESTAMP, ROW,
            KeyValue.Type.Put, FAM, QueryConstants.EMPTY_COLUMN_BYTES, 4);
    }

    /**
     * Tests getting an index update for a mutation with 200 versions Before, the issue PHOENIX-3807
     * was causing this test to take >90 seconds, so here we set a timeout of 5 seconds
     */
    @Test(timeout = 10000)
    public void testManyVersions() throws IOException {
        // when doing a rebuild, we are replaying mutations so we want to ignore newer mutations
        // see LocalTable#getCurrentRowState()
        Mockito.when(mockIndexMetaData.ignoreNewerMutations()).thenReturn(true);
        MultiMutation mutation = getMultipleVersionMutation(200);
        currentRowCells = mutation.getFamilyCellMap().get(FAM);

        Collection<Pair<Mutation, byte[]>> indexUpdates =
                indexBuilder.getIndexUpdate(mutation, mockIndexMetaData);
        assertNotEquals(0, indexUpdates.size());
    }

    // Assert that the given collection of indexUpdates contains the given cell
    private void assertContains(Collection<Pair<Mutation, byte[]>> indexUpdates,
            final long mutationTs, final byte[] row, final Type cellType, final byte[] fam,
            final byte[] qual, final long cellTs) {
        Predicate<Pair<Mutation, byte[]>> hasCellPredicate =
                new Predicate<Pair<Mutation, byte[]>>() {
                    @Override
                    public boolean apply(Pair<Mutation, byte[]> input) {
                        assertEquals(TEST_TABLE_INDEX_STRING, Bytes.toString(input.getSecond()));
                        Mutation mutation = input.getFirst();
                        if (mutationTs == mutation.getTimeStamp()) {
                            NavigableMap<byte[], List<Cell>> familyCellMap =
                                    mutation.getFamilyCellMap();
                            Cell updateCell = familyCellMap.get(fam).get(0);
                            if (cellType == KeyValue.Type.codeToType(updateCell.getTypeByte())
                                    && Bytes.compareTo(fam, CellUtil.cloneFamily(updateCell)) == 0
                                    && Bytes.compareTo(qual,
                                        CellUtil.cloneQualifier(updateCell)) == 0
                                    && cellTs == updateCell.getTimestamp()) {
                                return true;
                            }
                        }
                        return false;
                    }
                };
        Optional<Pair<Mutation, byte[]>> tryFind =
                Iterables.tryFind(indexUpdates, hasCellPredicate);
        assertTrue(tryFind.isPresent());
    }

    private void setCurrentRowState(byte[] fam2, byte[] indexedQualifier, int i, byte[] value1) {
        Cell cell = CellUtil.createCell(ROW, FAM, INDEXED_QUALIFIER, 1, PUT_TYPE, VALUE_1);
        currentRowCells = Collections.singletonList(cell);
    }

    private void setCurrentRowState(List<Cell> cells) {
        currentRowCells = cells;
    }

    private MultiMutation getMultipleVersionMutation(int versions) {
        MultiMutation mutation = new MultiMutation(new ImmutableBytesPtr(ROW));
        for (int i = versions - 1; i >= 0; i--) {
            Put put = new Put(ROW);
            put.addImmutable(FAM, INDEXED_QUALIFIER, i, Bytes.toBytes(i));
            mutation.addAll(put);
        }
        return mutation;
    }
}
