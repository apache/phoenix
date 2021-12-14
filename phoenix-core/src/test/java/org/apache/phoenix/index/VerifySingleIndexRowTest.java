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
package org.apache.phoenix.index;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compat.hbase.HbaseCompatCapabilities;
import org.apache.phoenix.coprocessor.GlobalIndexRegionScanner;
import org.apache.phoenix.coprocessor.IndexRebuildRegionScanner;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexVerificationOutputRepository;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.*;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static org.apache.phoenix.coprocessor.GlobalIndexRegionScanner.MUTATION_TS_DESC_COMPARATOR;
import static org.apache.phoenix.query.QueryConstants.UNVERIFIED_BYTES;
import static org.apache.phoenix.query.QueryConstants.VERIFIED_BYTES;
import static org.apache.phoenix.query.QueryConstants.EMPTY_COLUMN_BYTES;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

public class VerifySingleIndexRowTest extends BaseConnectionlessQueryTest {

    private static final int INDEX_TABLE_EXPIRY_SEC = 1;
    private static final String UNEXPECTED_COLUMN = "0:UNEXPECTED_COLUMN";
    public static final String FIRST_ID = "FIRST_ID";
    public static final String SECOND_ID = "SECOND_ID";
    public static final String FIRST_VALUE = "FIRST_VALUE";
    public static final String SECOND_VALUE = "SECOND_VALUE";
    public static final String
            CREATE_TABLE_DDL = "CREATE TABLE IF NOT EXISTS %s (FIRST_ID BIGINT NOT NULL, "
            + "SECOND_ID BIGINT NOT NULL, FIRST_VALUE VARCHAR(20), "
            + "SECOND_VALUE INTEGER "
            + "CONSTRAINT PK PRIMARY KEY(FIRST_ID, SECOND_ID)) COLUMN_ENCODED_BYTES=0";

    public static final String
            CREATE_INDEX_DDL = "CREATE INDEX %s ON %s (SECOND_VALUE) INCLUDE (FIRST_VALUE)";
    public static final String COMPLETE_ROW_UPSERT = "UPSERT INTO %s VALUES (?,?,?,?)";
    public static final String PARTIAL_ROW_UPSERT = "UPSERT INTO %s (%s, %s, %s) VALUES (?,?,?)";
    public static final String DELETE_ROW_DML = "DELETE FROM %s WHERE %s = ?  AND %s = ?";
    public static final String INCLUDED_COLUMN = "0:FIRST_VALUE";

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    private enum TestType {
        //set of mutations matching expected mutations
        VALID_EXACT_MATCH,
        //mix of delete and put mutations
        VALID_MIX_MUTATIONS,
        //only incoming unverified mutations
        VALID_NEW_UNVERIFIED_MUTATIONS,
        //extra mutations mimicking incoming mutations
        VALID_MORE_MUTATIONS,
        // mimicking the case where the data cells expired but index has them still
        VALID_EXTRA_CELL,
        EXPIRED,
        INVALID_EXTRA_CELL,
        INVALID_EMPTY_CELL,
        INVALID_CELL_VALUE,
        INVALID_COLUMN
    }

    public static class UnitTestClock extends EnvironmentEdge {
        long initialTime;
        long delta;

        public UnitTestClock(long delta) {
            initialTime = System.currentTimeMillis() + delta;
            this.delta = delta;
        }

        @Override
        public long currentTime() {
            return System.currentTimeMillis() + delta;
        }
    }

    @Mock
    Result indexRow;
    @Mock
    IndexRebuildRegionScanner rebuildScanner;
    List<Mutation> actualMutationList;
    String schema, table, dataTableFullName, index, indexTableFullName;
    PTable pIndexTable, pDataTable;
    Put put = null;
    Delete delete = null;
    PhoenixConnection pconn;
    IndexToolVerificationResult.PhaseResult actualPR;
    public Map<byte[], List<Mutation>> indexKeyToMutationMap = null;
    Set<byte[]> mostRecentIndexRowKeys;
    private IndexMaintainer indexMaintainer;

    @Before
    public void setup() throws SQLException, IOException {
        MockitoAnnotations.initMocks(this);
        createDBObject();
        createMutationsWithUpserts();
        initializeRebuildScannerAttributes();
        initializeGlobalMockitoSetup();
    }

    public void createDBObject() throws SQLException {
        try(Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
            schema = generateUniqueName();
            table = generateUniqueName();
            index = generateUniqueName();
            dataTableFullName = SchemaUtil.getQualifiedTableName(schema, table);
            indexTableFullName = SchemaUtil.getQualifiedTableName(schema, index);

            conn.createStatement().execute(String.format(CREATE_TABLE_DDL, dataTableFullName));
            conn.createStatement().execute(String.format(CREATE_INDEX_DDL, index, dataTableFullName));
            conn.commit();

            pconn = conn.unwrap(PhoenixConnection.class);
            pIndexTable = pconn.getTable(new PTableKey(pconn.getTenantId(), indexTableFullName));
            pDataTable = pconn.getTable(new PTableKey(pconn.getTenantId(), dataTableFullName));
        }
    }

    private void createMutationsWithUpserts() throws SQLException, IOException {
        deleteRow(2, 3);
        upsertPartialRow(2, 3, "abc");
        upsertCompleteRow(2, 3, "hik", 8);
        upsertPartialRow(2, 3, 10);
        upsertPartialRow(2,3,4);
        deleteRow(2, 3);
        upsertPartialRow(2,3, "def");
        upsertCompleteRow(2, 3, null, 20);
        upsertPartialRow(2,3, "wert");
    }

    private void deleteRow(int key1, int key2) throws SQLException, IOException {
        try(Connection conn = DriverManager.getConnection(getUrl(), new Properties())){
            PreparedStatement ps =
                    conn.prepareStatement(
                            String.format(DELETE_ROW_DML, dataTableFullName, FIRST_ID, SECOND_ID));
            ps.setInt(1, key1);
            ps.setInt(2, key2);
            ps.execute();
            convertUpsertToMutations(conn);
        }
    }

    private void upsertPartialRow(int key1, int key2, String val1)
            throws SQLException, IOException {

        try(Connection conn = DriverManager.getConnection(getUrl(), new Properties())){
            PreparedStatement ps =
                    conn.prepareStatement(
                            String.format(PARTIAL_ROW_UPSERT, dataTableFullName, FIRST_ID, SECOND_ID,
                                    FIRST_VALUE));
            ps.setInt(1, key1);
            ps.setInt(2, key2);
            ps.setString(3, val1);
            ps.execute();
            convertUpsertToMutations(conn);
        }
    }

    private void upsertPartialRow(int key1, int key2, int value1)
            throws SQLException, IOException {

        try(Connection conn = DriverManager.getConnection(getUrl(), new Properties())){
            PreparedStatement
                    ps =
                    conn.prepareStatement(
                            String.format(PARTIAL_ROW_UPSERT, dataTableFullName, FIRST_ID, SECOND_ID,
                                    SECOND_VALUE));
            ps.setInt(1, key1);
            ps.setInt(2, key2);
            ps.setInt(3, value1);
            ps.execute();
            convertUpsertToMutations(conn);
        }
    }

    private void upsertCompleteRow(int key1, int key2, String val1
            , int val2) throws SQLException, IOException {
        try(Connection conn = DriverManager.getConnection(getUrl(), new Properties())) {
            PreparedStatement
                    ps = conn.prepareStatement(String.format(COMPLETE_ROW_UPSERT, dataTableFullName));
            ps.setInt(1, key1);
            ps.setInt(2, key2);
            ps.setString(3, val1);
            ps.setInt(4, val2);
            ps.execute();
            convertUpsertToMutations(conn);
        }
    }

    private void convertUpsertToMutations(Connection conn) throws SQLException, IOException {
        Iterator<Pair<byte[],List<Cell>>>
                dataTableNameAndMutationKeyValuesIter = PhoenixRuntime.getUncommittedDataIterator(conn);
        Pair<byte[], List<Cell>> elem = dataTableNameAndMutationKeyValuesIter.next();
        byte[] key = CellUtil.cloneRow(elem.getSecond().get(0));
        long mutationTS = EnvironmentEdgeManager.currentTimeMillis();

        for (Cell kv : elem.getSecond()) {
            Cell cell =
                CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(CellUtil.cloneRow(kv)).
                    setFamily(CellUtil.cloneFamily(kv)).
                    setQualifier(CellUtil.cloneQualifier(kv)).
                    setTimestamp(mutationTS).
                    setType(kv.getType()).
                    setValue(CellUtil.cloneValue(kv)).build();
            if (cell.getType().equals(Cell.Type.Put)) {
                if (put == null ) {
                    put = new Put(key);
                }
                put.add(cell);
            } else {
                if (delete == null) {
                    delete = new Delete(key);
                }
                delete.add(cell);
            }
        }
    }

    private void initializeRebuildScannerAttributes() {
        when(rebuildScanner.setIndexTableTTL(Matchers.anyInt())).thenCallRealMethod();
        when(rebuildScanner.setIndexMaintainer(Matchers.<IndexMaintainer>any())).thenCallRealMethod();
        when(rebuildScanner.setMaxLookBackInMills(Matchers.anyLong())).thenCallRealMethod();
        rebuildScanner.setIndexTableTTL(HConstants.FOREVER);
        indexMaintainer = pIndexTable.getIndexMaintainer(pDataTable, pconn);
        rebuildScanner.setIndexMaintainer(indexMaintainer);
        // set the maxLookBack to infinite to avoid the compaction
        rebuildScanner.setMaxLookBackInMills(Long.MAX_VALUE);
    }

    private void initializeGlobalMockitoSetup() throws IOException {
        //setup
        when(GlobalIndexRegionScanner.getIndexRowKey(indexMaintainer, put)).thenCallRealMethod();
        when(rebuildScanner.prepareIndexMutations(put, delete, indexKeyToMutationMap, mostRecentIndexRowKeys)).thenCallRealMethod();
        when(rebuildScanner.verifySingleIndexRow(Matchers.<byte[]>any(), Matchers.<List>any(),Matchers.<List>any(), Matchers.<Set>any(), Matchers.<List>any(),
                Matchers.<IndexToolVerificationResult.PhaseResult>any(), Matchers.anyBoolean())).thenCallRealMethod();
        doNothing().when(rebuildScanner)
                .logToIndexToolOutputTable(Matchers.<byte[]>any(),Matchers.<byte[]>any(),
                        Mockito.anyLong(),Mockito.anyLong(), Mockito.anyString(),
                        Matchers.<byte[]>any(), Matchers.<byte[]>any(), Matchers.anyBoolean(),
                    Mockito.<IndexVerificationOutputRepository.IndexVerificationErrorType>any());
        doNothing().when(rebuildScanner)
                .logToIndexToolOutputTable(Matchers.<byte[]>any(),Matchers.<byte[]>any(),
                        Mockito.anyLong(),Mockito.anyLong(), Mockito.anyString(),
                    Matchers.anyBoolean(),
                    Mockito.<IndexVerificationOutputRepository.IndexVerificationErrorType>any());

        //populate the local map to use to create actual mutations
        indexKeyToMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        rebuildScanner.prepareIndexMutations(put, delete, indexKeyToMutationMap, mostRecentIndexRowKeys);
    }

    private byte[] getValidRowKey() {
        return indexKeyToMutationMap.entrySet().iterator().next().getKey();
    }

    @Test
    public void testVerifySingleIndexRow_validIndexRowCount_nonZero() throws IOException {
        IndexToolVerificationResult.PhaseResult expectedPR = getValidPhaseResult();
        for (Map.Entry<byte[], List<Mutation>>
                entry : indexKeyToMutationMap.entrySet()) {
            initializeLocalMockitoSetup(entry, TestType.VALID_EXACT_MATCH);
            //test code
            rebuildScanner.verifySingleIndexRow(indexRow.getRow(), actualMutationList,
                    indexKeyToMutationMap.get(indexRow.getRow()), mostRecentIndexRowKeys, Collections.EMPTY_LIST, actualPR, true);

            assertEquals(actualPR, expectedPR);
        }
    }

    @Test
    public void testVerifySingleIndexRow_validIndexRowCount_moreActual() throws IOException {
        IndexToolVerificationResult.PhaseResult expectedPR = getValidPhaseResult();
        for (Map.Entry<byte[], List<Mutation>>
                entry : indexKeyToMutationMap.entrySet()) {
            initializeLocalMockitoSetup(entry, TestType.VALID_MORE_MUTATIONS);
            //test code

            rebuildScanner.verifySingleIndexRow(indexRow.getRow(), actualMutationList,
                    indexKeyToMutationMap.get(indexRow.getRow()), mostRecentIndexRowKeys, Collections.EMPTY_LIST, actualPR, true);

            assertEquals(actualPR, expectedPR);
        }
    }

    @Test
    public void testVerifySingleIndexRow_allMix() throws IOException {
        IndexToolVerificationResult.PhaseResult expectedPR = getValidPhaseResult();
        for (Map.Entry<byte[], List<Mutation>>
                entry : indexKeyToMutationMap.entrySet()) {
            initializeLocalMockitoSetup(entry, TestType.VALID_MIX_MUTATIONS);
            //test code

            rebuildScanner.verifySingleIndexRow(indexRow.getRow(), actualMutationList,
                    indexKeyToMutationMap.get(indexRow.getRow()), mostRecentIndexRowKeys, Collections.EMPTY_LIST, actualPR, true);

            assertEquals(actualPR, expectedPR);
        }
    }

    @Test
    public void testVerifySingleIndexRow_allUnverified() throws IOException {
        IndexToolVerificationResult.PhaseResult expectedPR = getValidPhaseResult();
        for (Map.Entry<byte[], List<Mutation>>
                entry : indexKeyToMutationMap.entrySet()) {
            initializeLocalMockitoSetup(entry, TestType.VALID_NEW_UNVERIFIED_MUTATIONS);
            //test code

            rebuildScanner.verifySingleIndexRow(indexRow.getRow(), actualMutationList,
                    indexKeyToMutationMap.get(indexRow.getRow()), mostRecentIndexRowKeys, Collections.EMPTY_LIST, actualPR, true);

            assertEquals(actualPR, expectedPR);
        }
    }

    @Test
    public void testVerifySingleIndexRow_expiredIndexRowCount_nonZero() throws IOException {
        IndexToolVerificationResult.PhaseResult
                expectedPR = new IndexToolVerificationResult.PhaseResult(0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
        try {
            for (Map.Entry<byte[], List<Mutation>>
                    entry : indexKeyToMutationMap.entrySet()) {
                initializeLocalMockitoSetup(entry, TestType.EXPIRED);
                expireThisRow();
                //test code
                rebuildScanner.verifySingleIndexRow(indexRow.getRow(), actualMutationList,
                        indexKeyToMutationMap.get(indexRow.getRow()), mostRecentIndexRowKeys, Collections.EMPTY_LIST, actualPR, true);

                assertEquals(actualPR, expectedPR);
            }
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    @Test
    public void testVerifySingleIndexRow_invalidIndexRowCount_cellValue() throws IOException {
        IndexToolVerificationResult.PhaseResult expectedPR = getInvalidPhaseResult();
        expectedPR.setIndexHasExtraCellsCount(1);
        for (Map.Entry<byte[], List<Mutation>>
                entry : indexKeyToMutationMap.entrySet()) {
            initializeLocalMockitoSetup(entry, TestType.INVALID_CELL_VALUE);
            //test code

            rebuildScanner.verifySingleIndexRow(indexRow.getRow(), actualMutationList,
                    indexKeyToMutationMap.get(indexRow.getRow()), mostRecentIndexRowKeys, Collections.EMPTY_LIST, actualPR, true);

            assertEquals(actualPR, expectedPR);
        }
    }

    @Test
    public void testVerifySingleIndexRow_invalidIndexRowCount_emptyCell() throws IOException {
        IndexToolVerificationResult.PhaseResult expectedPR = getInvalidPhaseResult();
        for (Map.Entry<byte[], List<Mutation>>
                entry : indexKeyToMutationMap.entrySet()) {
            initializeLocalMockitoSetup(entry, TestType.INVALID_EMPTY_CELL);
            //test code
            rebuildScanner.verifySingleIndexRow(indexRow.getRow(), actualMutationList,
                    indexKeyToMutationMap.get(indexRow.getRow()), mostRecentIndexRowKeys, Collections.EMPTY_LIST, actualPR, true);

            assertEquals(actualPR, expectedPR);
        }
    }

    @Test
    public void testVerifySingleIndexRow_invalidIndexRowCount_diffColumn() throws IOException {
        IndexToolVerificationResult.PhaseResult expectedPR = getInvalidPhaseResult();
        expectedPR.setIndexHasExtraCellsCount(1);
        for (Map.Entry<byte[], List<Mutation>>
                entry : indexKeyToMutationMap.entrySet()) {
            initializeLocalMockitoSetup(entry, TestType.INVALID_COLUMN);
            //test code
            rebuildScanner.verifySingleIndexRow(indexRow.getRow(), actualMutationList,
                    indexKeyToMutationMap.get(indexRow.getRow()), mostRecentIndexRowKeys, Collections.EMPTY_LIST, actualPR, true);

            assertEquals(actualPR, expectedPR);
        }
    }

    @Test
    public void testVerifySingleIndexRow_invalidIndexRowCount_extraCell() throws IOException {
        IndexToolVerificationResult.PhaseResult expectedPR = getInvalidPhaseResult();
        for (Map.Entry<byte[], List<Mutation>>
                entry : indexKeyToMutationMap.entrySet()) {
            initializeLocalMockitoSetup(entry, TestType.INVALID_EXTRA_CELL);
            //test code
            rebuildScanner.verifySingleIndexRow(indexRow.getRow(), actualMutationList,
                    indexKeyToMutationMap.get(indexRow.getRow()), mostRecentIndexRowKeys, Collections.EMPTY_LIST, actualPR, true);

            assertEquals(actualPR, expectedPR);
        }
    }

    @Test
    public void testVerifySingleIndexRow_expectedMutations_null() throws IOException {
        when(indexRow.getRow()).thenReturn(Bytes.toBytes(1));
        exceptionRule.expect(DoNotRetryIOException.class);
        exceptionRule.expectMessage(IndexRebuildRegionScanner.NO_EXPECTED_MUTATION);
        rebuildScanner.verifySingleIndexRow(indexRow.getRow(), actualMutationList,
                indexKeyToMutationMap.get(indexRow.getRow()), mostRecentIndexRowKeys, Collections.EMPTY_LIST, actualPR, true);
    }

    @Test
    public void testVerifySingleIndexRow_validIndexRowCount_extraCell() throws IOException {
        for (Map.Entry<byte[], List<Mutation>>
                entry : indexKeyToMutationMap.entrySet()) {
            initializeLocalMockitoSetup(entry, TestType.VALID_EXTRA_CELL);
            //test code
            rebuildScanner.verifySingleIndexRow(indexRow.getRow(), actualMutationList,
                    indexKeyToMutationMap.get(indexRow.getRow()), mostRecentIndexRowKeys, Collections.EMPTY_LIST, actualPR, true);

            assertEquals(1, actualPR.getIndexHasExtraCellsCount());
        }
    }

    // Test the major compaction on index table only.
    // There is at least one expected mutation within maxLookBack that has its matching one in the actual list.
    // However there are some expected mutations outside of maxLookBack, which matching ones in actual list may be compacted away.
    // We will report such row as a valid row.
    @Test
    public void testVerifySingleIndexRow_compactionOnIndexTable_atLeastOneExpectedMutationWithinMaxLookBack() throws Exception {
        Assume.assumeTrue(HbaseCompatCapabilities.isMaxLookbackTimeSupported());
        String dataRowKey = "k1";
        byte[] indexRowKey1Bytes = generateIndexRowKey(dataRowKey, "val1");
        ManualEnvironmentEdge injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(1);
        EnvironmentEdgeManager.injectEdge(injectEdge);

        List<Mutation> expectedMutations = new ArrayList<>();
        List<Mutation> actualMutations = new ArrayList<>();
        // change the maxLookBack from infinite to some interval, which allows to simulate the mutation beyond the maxLookBack window.
        long maxLookbackInMills = 10 * 1000;
        rebuildScanner.setMaxLookBackInMills(maxLookbackInMills);

        Put put = new Put(indexRowKey1Bytes);
        Cell cell = getNewCell(indexRowKey1Bytes,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                QueryConstants.EMPTY_COLUMN_BYTES,
                EnvironmentEdgeManager.currentTimeMillis(),
                Cell.Type.Put,
                QueryConstants.VERIFIED_BYTES);
        put.add(cell);
        // This mutation is beyond maxLookBack, so add it to expectedMutations only.
        expectedMutations.add(put);

        // advance the time of maxLookBack, so last mutation will be outside of maxLookBack,
        // next mutation will be within maxLookBack
        injectEdge.incrementValue(maxLookbackInMills);
        put =  new Put(indexRowKey1Bytes);
        cell = getNewCell(indexRowKey1Bytes,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                QueryConstants.EMPTY_COLUMN_BYTES,
                EnvironmentEdgeManager.currentTimeMillis(),
                Cell.Type.Put,
                QueryConstants.VERIFIED_BYTES);
        put.add(cell);
        // This mutation is in both expectedMutations and actualMutations, as it is within the maxLookBack, so it will not get chance to be compacted away
        expectedMutations.add(put);
        actualMutations.add(put);
        Result actualMutationsScanResult = Result.create(Arrays.asList(cell));

        Map<byte[], List<Mutation>> indexKeyToMutationMap = Maps.newTreeMap((Bytes.BYTES_COMPARATOR));
        indexKeyToMutationMap.put(indexRowKey1Bytes, expectedMutations);
        when(rebuildScanner.prepareActualIndexMutations(any(Result.class))).thenReturn(actualMutations);
        when(indexRow.getRow()).thenReturn(indexRowKey1Bytes);
        injectEdge.incrementValue(1);
        IndexToolVerificationResult.PhaseResult actualPR = new IndexToolVerificationResult.PhaseResult();
        Collections.sort(indexKeyToMutationMap.get(indexRow.getRow()), MUTATION_TS_DESC_COMPARATOR);
        // Report this validation as a success
        assertTrue(rebuildScanner.verifySingleIndexRow(indexRow.getRow(), actualMutations,
                indexKeyToMutationMap.get(indexRow.getRow()), mostRecentIndexRowKeys, Collections.EMPTY_LIST, actualPR, false));
        // validIndexRowCount = 1
        IndexToolVerificationResult.PhaseResult expectedPR = new IndexToolVerificationResult.PhaseResult(1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        assertTrue(actualPR.equals(expectedPR));
    }

    // Test the major compaction on index table only.
    // All expected mutations are beyond the maxLookBack, and there are no matching ones in the actual list because of major compaction.
    // We will report such row as an invalid beyond maxLookBack row.
    @Test
    public void testVerifySingleIndexRow_compactionOnIndexTable_noExpectedMutationWithinMaxLookBack() throws Exception {
        Assume.assumeTrue(HbaseCompatCapabilities.isMaxLookbackTimeSupported());
        String dataRowKey = "k1";
        byte[] indexRowKey1Bytes = generateIndexRowKey(dataRowKey, "val1");
        List<Mutation> expectedMutations = new ArrayList<>();
        List<Mutation> actualMutations = new ArrayList<>();
        // change the maxLookBack from infinite to some interval, which allows to simulate the mutation beyond the maxLookBack window.
        long maxLookbackInMills = 10 * 1000;
        rebuildScanner.setMaxLookBackInMills(maxLookbackInMills);

        ManualEnvironmentEdge injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(1);
        EnvironmentEdgeManager.injectEdge(injectEdge);

        Put put = new Put(indexRowKey1Bytes);
        Cell cell = getNewCell(indexRowKey1Bytes,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                QueryConstants.EMPTY_COLUMN_BYTES,
                EnvironmentEdgeManager.currentTimeMillis(),
                Cell.Type.Put,
                VERIFIED_BYTES);
        put.add(cell);
        // This mutation is beyond maxLookBack, so add it to expectedMutations only.
        expectedMutations.add(put);

        injectEdge.incrementValue(maxLookbackInMills);
        put =  new Put(indexRowKey1Bytes);
        cell = getNewCell(indexRowKey1Bytes,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                QueryConstants.EMPTY_COLUMN_BYTES,
                EnvironmentEdgeManager.currentTimeMillis(),
                Cell.Type.Put,
                UNVERIFIED_BYTES);
        put.add(cell);
        // This mutation is actualMutations only, as it is an unverified put
        actualMutations.add(put);
        Result actualMutationsScanResult = Result.create(Arrays.asList(cell));

        Map<byte[], List<Mutation>> indexKeyToMutationMap = Maps.newTreeMap((Bytes.BYTES_COMPARATOR));
        indexKeyToMutationMap.put(indexRowKey1Bytes, expectedMutations);
        mostRecentIndexRowKeys = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        when(rebuildScanner.prepareActualIndexMutations(any(Result.class))).thenReturn(actualMutations);
        when(indexRow.getRow()).thenReturn(indexRowKey1Bytes);

        injectEdge.incrementValue(1);
        IndexToolVerificationResult.PhaseResult actualPR = new IndexToolVerificationResult.PhaseResult();
        // Report this validation as a failure
        assertFalse(rebuildScanner.verifySingleIndexRow(indexRow.getRow(), actualMutations, expectedMutations, mostRecentIndexRowKeys, new ArrayList<Mutation>(), actualPR, true));
        // beyondMaxLookBackInvalidIndexRowCount = 1
        IndexToolVerificationResult.PhaseResult expectedPR = new IndexToolVerificationResult.PhaseResult(0, 0, 0, 0, 0, 1, 0, 0, 0, 0);
        assertTrue(actualPR.equals(expectedPR));
    }

    private static byte[] generateIndexRowKey(String dataRowKey, String dataVal){
        List<Byte> idxKey = new ArrayList<>();
        if (dataVal != null && !dataVal.isEmpty())
            idxKey.addAll(org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.asList(Bytes.toBytes(dataVal)));
        idxKey.add(QueryConstants.SEPARATOR_BYTE);
        idxKey.addAll(org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.asList(Bytes.toBytes(dataRowKey)));
        return org.apache.phoenix.thirdparty.com.google.common.primitives.Bytes.toArray(idxKey);
    }

    private IndexToolVerificationResult.PhaseResult getValidPhaseResult() {
        return new IndexToolVerificationResult.PhaseResult(1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    private IndexToolVerificationResult.PhaseResult getInvalidPhaseResult() {
        return new IndexToolVerificationResult.PhaseResult(0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
    }

    private void initializeLocalMockitoSetup(Map.Entry<byte[], List<Mutation>> entry,
            TestType testType)
            throws IOException {
        actualPR = new IndexToolVerificationResult.PhaseResult();
        byte[] indexKey = entry.getKey();
        when(indexRow.getRow()).thenReturn(indexKey);
        actualMutationList = buildActualIndexMutationsList(testType);
        when(rebuildScanner.prepareActualIndexMutations(indexRow)).thenReturn(actualMutationList);
    }

    private List<Mutation> buildActualIndexMutationsList(TestType testType) {
        List<Mutation> actualMutations = new ArrayList<>();
        actualMutations.addAll(indexKeyToMutationMap.get(indexRow.getRow()));
        if(testType.equals(TestType.EXPIRED)) {
            return actualMutations;
        }
        if(testType.toString().startsWith("VALID")) {
            return getValidActualMutations(testType, actualMutations);
        }
        if(testType.toString().startsWith("INVALID")) {
            return getInvalidActualMutations(testType, actualMutations);
        }
        return null;
    }

    private List <Mutation> getValidActualMutations(TestType testType,
            List<Mutation> actualMutations) {
        List <Mutation> newActualMutations = new ArrayList<>();
        if(testType.equals(TestType.VALID_EXACT_MATCH)) {
            return actualMutations;
        }
        if (testType.equals(TestType.VALID_MIX_MUTATIONS)) {
            newActualMutations.add(getUnverifiedPutMutation(actualMutations.get(0), null));
            newActualMutations.add(getDeleteMutation(actualMutations.get(0), new Long(1)));
            newActualMutations.add(getUnverifiedPutMutation(actualMutations.get(0), null));
        }
        if (testType.equals(TestType.VALID_NEW_UNVERIFIED_MUTATIONS)) {
            newActualMutations.add(getUnverifiedPutMutation(actualMutations.get(0), null));
            newActualMutations.add(getUnverifiedPutMutation(actualMutations.get(0), null));
            newActualMutations.add(getUnverifiedPutMutation(actualMutations.get(0), null));
            newActualMutations.add(getUnverifiedPutMutation(actualMutations.get(0), new Long(1)));
        }
        newActualMutations.addAll(actualMutations);
        if(testType.equals(TestType.VALID_MORE_MUTATIONS)) {
            newActualMutations.add(getUnverifiedPutMutation(actualMutations.get(0), null));
            newActualMutations.add(getDeleteMutation(actualMutations.get(0), null));
            newActualMutations.add(getDeleteMutation(actualMutations.get(0), new Long(1)));
            newActualMutations.add(getUnverifiedPutMutation(actualMutations.get(0), new Long(1)));
        }
        if(testType.equals(TestType.VALID_EXTRA_CELL)) {
            for (Mutation m : newActualMutations) {
                if (m instanceof Put) {
                    List<Cell> origList = m.getFamilyCellMap().firstEntry().getValue();
                    Cell newCell = getNewCell(m.getRow(), CellUtil.cloneFamily(origList.get(0)),
                                    Bytes.toBytes("EXTRACOL"), m.getTimestamp(), Cell.Type.Put,
                                    Bytes.toBytes("asdfg"));
                    byte[] fam = CellUtil.cloneFamily(origList.get(0));
                    m.getFamilyCellMap().get(fam).add(newCell);
                    break;
                }
            }
        }
        return newActualMutations;
    }

    private List <Mutation> getInvalidActualMutations(TestType testType,
            List<Mutation> actualMutations) {
        List <Mutation> newActualMutations = new ArrayList<>();
        newActualMutations.addAll(actualMutations);
        for (Mutation m : actualMutations) {
            newActualMutations.remove(m);
            NavigableMap<byte[], List<Cell>> familyCellMap = m.getFamilyCellMap();
            List<Cell> cellList = familyCellMap.firstEntry().getValue();
            List<Cell> newCellList = new ArrayList<>();
            byte[] fam = CellUtil.cloneFamily(cellList.get(0));
            for (Cell c : cellList) {
                infiltrateCell(c, newCellList, testType);
            }
            familyCellMap.put(fam, newCellList);
            Mutation newM;
            if (m instanceof Put) {
                newM = new Put(m.getRow(), m.getTimestamp(), familyCellMap);
            } else {
                newM = new Delete(m.getRow(), m.getTimestamp(), familyCellMap);
            }
            newActualMutations.add(newM);
        }
        return newActualMutations;
    }

    private void infiltrateCell(Cell c, List<Cell> newCellList, TestType e) {
        Cell newCell;
        Cell emptyCell;
        switch(e) {
        case INVALID_COLUMN:
            newCell = getNewCell(CellUtil.cloneRow(c), CellUtil.cloneFamily(c),
                            Bytes.toBytes(UNEXPECTED_COLUMN),
                            c.getTimestamp(),
                            Cell.Type.Put, Bytes.toBytes("zxcv"));
            newCellList.add(newCell);
            newCellList.add(c);
            break;
        case INVALID_CELL_VALUE:
            if (CellUtil.matchingQualifier(c, EMPTY_COLUMN_BYTES)) {
                newCell = getCellWithPut(c);
                emptyCell = getVerifiedEmptyCell(c);
                newCellList.add(newCell);
                newCellList.add(emptyCell);
            } else {
                newCellList.add(c);
            }
            break;
        case INVALID_EMPTY_CELL:
            if (CellUtil.matchingQualifier(c, EMPTY_COLUMN_BYTES)) {
                newCell =
                        getNewCell(CellUtil.cloneRow(c), CellUtil.cloneFamily(c),
                                CellUtil.cloneQualifier(c), c.getTimestamp(),
                                Cell.Type.Delete, VERIFIED_BYTES);
                newCellList.add(newCell);
            } else {
                newCellList.add(c);
            }
            break;
        case INVALID_EXTRA_CELL:
            newCell = getCellWithPut(c);
            emptyCell = getVerifiedEmptyCell(c);
            newCellList.add(newCell);
            newCellList.add(emptyCell);
            newCellList.add(c);
        }
    }

    private Cell getVerifiedEmptyCell(Cell c) {
        return getNewCell(CellUtil.cloneRow(c), CellUtil.cloneFamily(c),
                indexMaintainer.getEmptyKeyValueQualifier(),
                c.getTimestamp(),
                Cell.Type.Put, VERIFIED_BYTES);
    }

    private Cell getCellWithPut(Cell c) {
        return getNewCell(CellUtil.cloneRow(c),
                CellUtil.cloneFamily(c), Bytes.toBytes(INCLUDED_COLUMN),
                c.getTimestamp(), Cell.Type.Put,
                Bytes.toBytes("zxcv"));
    }

    private void expireThisRow() {
        rebuildScanner.setIndexTableTTL(INDEX_TABLE_EXPIRY_SEC);
        UnitTestClock expiryClock = new UnitTestClock(5000);
        EnvironmentEdgeManager.injectEdge(expiryClock);
    }

    private Mutation getDeleteMutation(Mutation orig, Long ts) {
        Mutation m = new Delete(orig.getRow());
        List<Cell> origList = orig.getFamilyCellMap().firstEntry().getValue();
        ts = ts == null ? EnvironmentEdgeManager.currentTimeMillis() : ts;
        Cell c = getNewPutCell(orig, origList, ts, Cell.Type.DeleteFamilyVersion);
        Cell empty = getEmptyCell(orig, origList, ts, Cell.Type.Put, true);
        byte[] fam = CellUtil.cloneFamily(origList.get(0));
        List<Cell> famCells = Lists.newArrayList();
        m.getFamilyCellMap().put(fam, famCells);
        famCells.add(c);
        famCells.add(empty);
        return m;
    }

    private Mutation getUnverifiedPutMutation(Mutation orig, Long ts) {
        Mutation m = new Put(orig.getRow());
        if (orig.getAttributesMap() != null) {
            for (Map.Entry<String,byte[]> entry : orig.getAttributesMap().entrySet()) {
                m.setAttribute(entry.getKey(), entry.getValue());
            }
        }
        List<Cell> origList = orig.getFamilyCellMap().firstEntry().getValue();
        ts = ts == null ? EnvironmentEdgeManager.currentTimeMillis() : ts;
        Cell c = getNewPutCell(orig, origList, ts, Cell.Type.Put);
        Cell empty = getEmptyCell(orig, origList, ts, Cell.Type.Put, false);
        byte[] fam = CellUtil.cloneFamily(origList.get(0));
        List<Cell> famCells = Lists.newArrayList();
        m.getFamilyCellMap().put(fam, famCells);
        famCells.add(c);
        famCells.add(empty);
        return m;
    }

    private Cell getEmptyCell(Mutation orig, List<Cell> origList, Long ts, Cell.Type type,
            boolean verified) {
        return getNewCell(orig.getRow(), CellUtil.cloneFamily(origList.get(0)),
                indexMaintainer.getEmptyKeyValueQualifier(),
                ts, type, verified ? VERIFIED_BYTES : UNVERIFIED_BYTES);
    }

    private Cell getNewPutCell(Mutation orig, List<Cell> origList, Long ts, Cell.Type type) {
        return getNewCell(orig.getRow(),
                CellUtil.cloneFamily(origList.get(0)), Bytes.toBytes(INCLUDED_COLUMN),
                ts, type, Bytes.toBytes("asdfg"));
    }

    private Cell getNewCell(byte[] row, byte[] family, byte[] qualifier,
                            long timestamp, Cell.Type type, byte[] value) {
      return CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(row).
          setFamily(family).setQualifier(qualifier).
          setTimestamp(timestamp).setType(type).
          setValue(value).build();
    }
}
