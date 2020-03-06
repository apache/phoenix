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

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.coprocessor.IndexRebuildRegionScanner;
import org.apache.phoenix.coprocessor.IndexToolVerificationResult;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.EnvironmentEdge;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;

import static org.apache.phoenix.query.QueryConstants.EMPTY_COLUMN_BYTES;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

public class IndexToolSingleRowVerifyTest extends BaseConnectionlessQueryTest {

    private static final int INDEX_TABLE_EXPIRY_SEC = 1;
    private static final String UNEXPECTED_VALUE = "UNEXPECTED_VALUE";
    public static final String FIRST_ID = "FIRST_ID";
    public static final String SECOND_ID = "SECOND_ID";
    public static final String FIRST_VALUE = "FIRST_VALUE";
    public static final String SECOND_VALUE = "SECOND_VALUE";
    public static final String THIRD_VALUE = "THIRD_VALUE";
    public static final String createTableDDL = "CREATE TABLE IF NOT EXISTS %s (FIRST_ID BIGINT NOT NULL, "
            + "SECOND_ID BIGINT NOT NULL, FIRST_VALUE VARCHAR(20), "
            + "SECOND_VALUE INTEGER, THIRD_VALUE DOUBLE "
            + "CONSTRAINT PK PRIMARY KEY(FIRST_ID, SECOND_ID)) COLUMN_ENCODED_BYTES=0";

    public static final String createIndexDDL = "CREATE INDEX %s ON %s (SECOND_VALUE) INCLUDE (FIRST_VALUE)";
    public static final String completeRowUpsert = "UPSERT INTO %s VALUES (?,?,?,?,?)";
    public static final String partialRowUpsert1 = "UPSERT INTO %s (%s, %s, %s) VALUES (?,?,?)";
    public static final String partialRowUpsert2 = "UPSERT INTO %s (%s, %s, %s, %s) VALUES (?,?,?,?)";

    private enum TestType {
        VALID,
        EXPIRED,
        INVALID_LESS_MUTATIONS,
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
    IndexToolVerificationResult.PhaseResult actualPR;
    IndexToolVerificationResult.PhaseResult expectedPR;
    List<Mutation> actualMutationList;
    Connection conn;
    String schema, table, dataTableFullName, index, indexTableFullName;

    @Before
    public void setup() throws SQLException, IOException, InterruptedException {

        MockitoAnnotations.initMocks(this);
        createDBObject();
        actualPR = new IndexToolVerificationResult.PhaseResult();
        setupMockitoForTest();
    }

    public void createDBObject() throws SQLException {
        conn = DriverManager.getConnection(getUrl(), new Properties());
        schema = generateUniqueName();
        table = generateUniqueName();
        index = generateUniqueName();
        dataTableFullName = SchemaUtil.getQualifiedTableName(schema, table);
        indexTableFullName = SchemaUtil.getQualifiedTableName(schema, index);

        conn.createStatement().execute(String.format(createTableDDL, dataTableFullName));
        conn.createStatement().execute(String.format(createIndexDDL, index, dataTableFullName));
        conn.commit();
    }

    private void setupMockitoForTest() throws SQLException, IOException, InterruptedException {
        //setup
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PTable pIndexTable = pconn.getTable(new PTableKey(pconn.getTenantId(), indexTableFullName));
        PTable pDataTable = pconn.getTable(new PTableKey(pconn.getTenantId(), dataTableFullName));

        PreparedStatement ps = conn.prepareStatement(String.format(partialRowUpsert1,
                dataTableFullName, FIRST_ID, SECOND_ID, SECOND_VALUE));
        ps.setInt(1, 2);
        ps.setInt(2, 3);
        ps.setInt(3, 10);

        ps.execute();

        ps = conn.prepareStatement(String.format(completeRowUpsert, dataTableFullName));
        ps.setInt(1, 2);
        ps.setInt(2, 3);
        ps.setString(3, "abc");
        ps.setInt(4, 8);
        ps.setDouble(5, 5.5);

        ps.execute();

        ps = conn.prepareStatement(String.format(partialRowUpsert1, dataTableFullName, FIRST_ID,
                SECOND_ID, SECOND_VALUE));
        ps.setInt(1, 2);
        ps.setInt(2, 3);
        ps.setInt(3, 4);

        ps.execute();

        ps = conn.prepareStatement(String.format(partialRowUpsert2, dataTableFullName, FIRST_ID,
                SECOND_ID, FIRST_VALUE, THIRD_VALUE));
        ps.setInt(1, 2);
        ps.setInt(2, 3);
        ps.setString(3, "def");
        ps.setDouble(4, 8.5);

        ps.execute();

        ps = conn.prepareStatement(String.format(partialRowUpsert2, dataTableFullName, FIRST_ID,
                SECOND_ID, FIRST_VALUE, SECOND_VALUE));
        ps.setInt(1, 2);
        ps.setInt(2, 3);
        ps.setString(3, null);
        ps.setInt(4, 20);

        ps.execute();

        ps = conn.prepareStatement(String.format(partialRowUpsert1, dataTableFullName, FIRST_ID,
                SECOND_ID, FIRST_VALUE));
        ps.setInt(1, 2);
        ps.setInt(2, 3);
        ps.setString(3, "wert");

        ps.execute();

        rebuildScanner.indexTableTTL = HConstants.FOREVER;
        rebuildScanner.indexMaintainer = pIndexTable.getIndexMaintainer(pDataTable, pconn);
        rebuildScanner.indexKeyToMutationMap = Maps.newTreeMap((Bytes.BYTES_COMPARATOR));
        Iterator<Pair<byte[],List<KeyValue>>>
                dataTableNameAndMutationKeyValuesIter = PhoenixRuntime.getUncommittedDataIterator(conn);
        Put put = null;
        Delete delete = null;
        Pair<byte[], List<KeyValue>> elem = dataTableNameAndMutationKeyValuesIter.next();
        byte[] key = elem.getSecond().get(0).getRow();

        for (KeyValue kv : elem.getSecond()) {
            Cell cell =
                    CellUtil.createCell(kv.getRow(), kv.getFamily(), kv.getQualifier(),
                            EnvironmentEdgeManager.currentTimeMillis(), kv.getType(), kv.getValue());
            Thread.sleep(1);
            if (kv.getType() == (KeyValue.Type.Put.getCode())) {
                if (put == null ) {
                    put = new Put(key);
                }
                put.add(cell);
            } else {
                if (delete == null) {
                    delete = new Delete(key);
                }
                delete.addDeleteMarker(cell);
            }
        }
        when(rebuildScanner.getIndexRowKey(put)).thenCallRealMethod();
        byte [] indexKey = rebuildScanner.getIndexRowKey(put);
        when(rebuildScanner.prepareIndexMutations(put, delete)).thenCallRealMethod();
        when(indexRow.getRow()).thenReturn(indexKey);
        when(rebuildScanner.verifySingleIndexRow(indexRow, actualPR)).thenCallRealMethod();
        doNothing().when(rebuildScanner)
                .logToIndexToolOutputTable(Matchers.<byte[]>any(),Matchers.<byte[]>any(),
                Mockito.anyLong(),Mockito.anyLong(), Mockito.anyString(),
                        Matchers.<byte[]>any(), Matchers.<byte[]>any());
        doNothing().when(rebuildScanner)
                .logToIndexToolOutputTable(Matchers.<byte[]>any(),Matchers.<byte[]>any(),
                Mockito.anyLong(),Mockito.anyLong(), Mockito.anyString());

        //populate the map
        rebuildScanner.prepareIndexMutations(put, delete);
    }

    @Test
    public void testVerifySingleIndexRow_validIndexRowCount_nonZero() throws IOException {
        actualMutationList = buildActualIndexMutationsList(TestType.VALID);
        when(rebuildScanner.prepareActualIndexMutations(indexRow)).thenReturn(actualMutationList);

        expectedPR = new IndexToolVerificationResult.PhaseResult(1, 0, 0, 0);
        //test code
        rebuildScanner.verifySingleIndexRow(indexRow, actualPR);

        //assert
        assertVerificationPhaseResult(actualPR, expectedPR);
    }

    @Test
    public void testVerifySingleIndexRow_expiredIndexRowCount_nonZero() throws IOException {
        actualMutationList = buildActualIndexMutationsList(TestType.EXPIRED);
        when(rebuildScanner.prepareActualIndexMutations(indexRow)).thenReturn(actualMutationList);

        expectedPR = new IndexToolVerificationResult.PhaseResult(0, 1, 0, 0);
        expireThisRow();

        //test code
        rebuildScanner.verifySingleIndexRow(indexRow, actualPR);

        //assert
        assertVerificationPhaseResult(actualPR, expectedPR);
    }

    @Test
    public void testVerifySingleIndexRow_invalidIndexRowCount_lessMutations() throws IOException {
        actualMutationList = buildActualIndexMutationsList(TestType.INVALID_LESS_MUTATIONS);
        when(rebuildScanner.prepareActualIndexMutations(indexRow)).thenReturn(actualMutationList);

        expectedPR = getInvalidPhaseResult();

        //test code
        rebuildScanner.verifySingleIndexRow(indexRow, actualPR);

        //assert
        assertVerificationPhaseResult(actualPR, expectedPR);
    }

    @Test
    public void testVerifySingleIndexRow_invalidIndexRowCount_cellValue() throws IOException {
        actualMutationList = buildActualIndexMutationsList(TestType.INVALID_CELL_VALUE);
        when(rebuildScanner.prepareActualIndexMutations(indexRow)).thenReturn(actualMutationList);

        expectedPR = getInvalidPhaseResult();

        //test code
        rebuildScanner.verifySingleIndexRow(indexRow, actualPR);

        //assert
        assertVerificationPhaseResult(actualPR, expectedPR);
    }

    @Test
    public void testVerifySingleIndexRow_invalidIndexRowCount_emptyCell() throws IOException {
        actualMutationList = buildActualIndexMutationsList(TestType.INVALID_EMPTY_CELL);
        when(rebuildScanner.prepareActualIndexMutations(indexRow)).thenReturn(actualMutationList);

        expectedPR = getInvalidPhaseResult();

        //test code
        rebuildScanner.verifySingleIndexRow(indexRow, actualPR);

        //assert
        assertVerificationPhaseResult(actualPR, expectedPR);
    }

    @Test
    public void testVerifySingleIndexRow_invalidIndexRowCount_diffColumn() throws IOException {
        actualMutationList = buildActualIndexMutationsList(TestType.INVALID_COLUMN);
        when(rebuildScanner.prepareActualIndexMutations(indexRow)).thenReturn(actualMutationList);

        expectedPR = getInvalidPhaseResult();

        //test code
        rebuildScanner.verifySingleIndexRow(indexRow, actualPR);

        //assert
        assertVerificationPhaseResult(actualPR, expectedPR);
    }

    @Test
    public void testVerifySingleIndexRow_invalidIndexRowCount_extraCell() throws IOException {
        actualMutationList = buildActualIndexMutationsList(TestType.INVALID_EXTRA_CELL);
        when(rebuildScanner.prepareActualIndexMutations(indexRow)).thenReturn(actualMutationList);

        expectedPR = getInvalidPhaseResult();

        //test code
        rebuildScanner.verifySingleIndexRow(indexRow, actualPR);

        //assert
        assertVerificationPhaseResult(actualPR, expectedPR);
    }

    private IndexToolVerificationResult.PhaseResult getInvalidPhaseResult() {
        return new IndexToolVerificationResult.PhaseResult(0, 0, 0, 1);
    }

    private List<Mutation> buildActualIndexMutationsList(TestType testType) {
        List<Mutation> actualMutations = new ArrayList<>();
        actualMutations.addAll(rebuildScanner.indexKeyToMutationMap.get(indexRow.getRow()));
        if(testType.equals(TestType.EXPIRED) || testType.equals(TestType.VALID)) {
            return actualMutations;
        }
        if (testType.equals(TestType.INVALID_LESS_MUTATIONS)) {
            return getTotalMutationsLessThanExpected(actualMutations);
        }

        List <Mutation> newActualMutations = new ArrayList<>();
        newActualMutations.addAll(actualMutations);

        for (Mutation m : actualMutations) {
            newActualMutations.remove(m);
            NavigableMap<byte [], List<Cell>> familyCellMap = m.getFamilyCellMap();
            List<Cell> cellList = familyCellMap.firstEntry().getValue();
            List <Cell> newCellList = new ArrayList<>();
            for (Cell c : cellList) {
                infiltrateCell(c, newCellList, testType);
            }
            familyCellMap.put(Bytes.toBytes(0), newCellList);
            m.setFamilyCellMap(familyCellMap);
            newActualMutations.add(m);
        }
        return newActualMutations;
    }

    private void infiltrateCell(Cell c, List<Cell> newCellList, TestType e) {
        Cell newCell;
        switch(e) {
        case INVALID_COLUMN:
            newCell =
                CellUtil.createCell(CellUtil.cloneRow(c), CellUtil.cloneFamily(c),
                        Bytes.toBytes("0:" + UNEXPECTED_VALUE), EnvironmentEdgeManager.currentTimeMillis(),
                        KeyValue.Type.Put.getCode(), Bytes.toBytes("zxcv"));
            newCellList.add(newCell);
            newCellList.add(c);
            break;
        case INVALID_CELL_VALUE:
            if (CellUtil.matchingQualifier(c, Bytes.toBytes("0:"+FIRST_VALUE))) {
                newCell = CellUtil.createCell(CellUtil.cloneRow(c), CellUtil.cloneFamily(c),
                        CellUtil.cloneQualifier(c), EnvironmentEdgeManager.currentTimeMillis(),
                        KeyValue.Type.Put.getCode(),
                        Bytes.toBytes("zxcv"));
                newCellList.add(newCell);
            } else {
                newCellList.add(c);
            }
            break;
        case INVALID_EMPTY_CELL:
            if (CellUtil.matchingQualifier(c, EMPTY_COLUMN_BYTES)) {
                newCell = CellUtil.createCell(CellUtil.cloneRow(c), CellUtil.cloneFamily(c),
                        CellUtil.cloneQualifier(c), c.getTimestamp(),
                        KeyValue.Type.Delete.getCode(),
                        Bytes.toBytes("\\x02"));
                newCellList.add(newCell);
            } else {
                newCellList.add(c);
            }
            break;
        case INVALID_EXTRA_CELL:
            if (CellUtil.matchingQualifier(c, Bytes.toBytes("0:"+FIRST_VALUE))) {
                newCell = CellUtil.createCell(CellUtil.cloneRow(c), CellUtil.cloneFamily(c),
                        CellUtil.cloneQualifier(c), EnvironmentEdgeManager.currentTimeMillis(),
                        KeyValue.Type.Put.getCode(),
                        Bytes.toBytes("zxcv"));
                newCellList.add(newCell);
            }
            newCellList.add(c);
        }
    }

    private void expireThisRow() {
        rebuildScanner.indexTableTTL = INDEX_TABLE_EXPIRY_SEC;
        UnitTestClock expiryClock = new UnitTestClock(5000);
        EnvironmentEdgeManager.injectEdge(expiryClock);
    }

    private List<Mutation> getTotalMutationsLessThanExpected(List<Mutation> actualMutations) {
        Mutation m = actualMutations.get(0);
        NavigableMap<byte [], List<Cell>> familyCellMap = m.getFamilyCellMap();
        List<Cell> cellList = familyCellMap.firstEntry().getValue();
        Cell c = cellList.get(0);
        Cell newCell = CellUtil.createCell(CellUtil.cloneRow(c), CellUtil.cloneFamily(c),
                CellUtil.cloneQualifier(c), EnvironmentEdgeManager.currentTimeMillis(), KeyValue.Type.Put.getCode(),
                    CellUtil.cloneValue(c));
        cellList.add(newCell);
        familyCellMap.put(Bytes.toBytes(0), cellList);
        m.setFamilyCellMap(familyCellMap);
        actualMutations.removeAll(actualMutations);
        actualMutations.add(m);
        return actualMutations;
    }

    private void assertVerificationPhaseResult(IndexToolVerificationResult.PhaseResult actualPR,
            IndexToolVerificationResult.PhaseResult expectedPR) {
        Assert.assertTrue(actualPR.toString().equalsIgnoreCase(expectedPR.toString()));
    }
}
