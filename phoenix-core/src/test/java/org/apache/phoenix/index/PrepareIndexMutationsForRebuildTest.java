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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.IndexRebuildRegionScanner;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PrepareIndexMutationsForRebuildTest extends BaseConnectionlessQueryTest {
    private static String ROW_KEY = "k1";
    private static String TABLE_NAME = "dataTable";
    private static String INDEX_NAME = "idx";

    class SetupInfo {
        public IndexMaintainer indexMaintainer;
        public PTable pDataTable;
    }

    private SetupInfo setup(String tableName,
                            String indexName,
                            String columns,
                            String indexColumns,
                            String pk,
                            String includeColumns) throws Exception {
        try(Connection conn = DriverManager.getConnection(getUrl())) {

            String fullTableName = SchemaUtil.getTableName(SchemaUtil.normalizeIdentifier(""), SchemaUtil.normalizeIdentifier(tableName));
            String fullIndexName = SchemaUtil.getTableName(SchemaUtil.normalizeIdentifier(""), SchemaUtil.normalizeIdentifier(indexName));

            String str1 = String.format("CREATE TABLE %1$s (%2$s CONSTRAINT pk PRIMARY KEY (%3$s)) COLUMN_ENCODED_BYTES=0",
                    fullTableName,
                    columns,
                    pk);
            conn.createStatement().execute(str1);

            String str2 = String.format("CREATE INDEX %1$s ON %2$s (%3$s)",
                    fullIndexName,
                    fullTableName,
                    indexColumns);
            if (!includeColumns.isEmpty())
                str2 += " INCLUDE (" + includeColumns + ")";
            conn.createStatement().execute(str2);

            PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
            PTable pIndexTable = pconn.getTable(new PTableKey(pconn.getTenantId(), fullIndexName));
            PTable pDataTable = pconn.getTable(new PTableKey(pconn.getTenantId(), fullTableName));
            IndexMaintainer im = pIndexTable.getIndexMaintainer(pDataTable, pconn);

            SetupInfo info = new SetupInfo();
            info.indexMaintainer = im;
            info.pDataTable = pDataTable;
            return info;
        }
    }

    @Test
    public void testSinglePutOnIndexColumn() throws Exception {
        SetupInfo info = setup(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, C1 VARCHAR, C2 VARCHAR",
                "C1",
                "ROW_KEY",
                "");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                1,
                Bytes.toBytes("v1"));
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v2"));
        addEmptyColumnToDataPutMutation(dataPut, info.pDataTable, 1);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(info.indexMaintainer,
                dataPut,
                null);

        Put idxPut1 = new Put(generateIndexRowKey("v1"));
        addEmptyColumnToIndexPutMutation(idxPut1, info.indexMaintainer, 1);

        assertEqualMutationList(Arrays.asList((Mutation)idxPut1), actualIndexMutations);
    }

    @Test
    public void testSinglePutOnNonIndexColumn() throws Exception {
        SetupInfo info = setup(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, C1 VARCHAR, C2 VARCHAR",
                "C1",
                "ROW_KEY",
                "");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v2"));
        addEmptyColumnToDataPutMutation(dataPut, info.pDataTable, 1);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(info.indexMaintainer,
                dataPut,
                null);

        Put idxPut1 = new Put(generateIndexRowKey(null));
        addEmptyColumnToIndexPutMutation(idxPut1, info.indexMaintainer, 1);

        assertEqualMutationList(Arrays.asList((Mutation)idxPut1), actualIndexMutations);
    }

    @Test
    public void testDelOnIndexColumn() throws Exception {
        SetupInfo info = setup(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, C1 VARCHAR, C2 VARCHAR",
                "C1",
                "ROW_KEY",
                "");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                1,
                Bytes.toBytes("v1"));
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v2"));
        addEmptyColumnToDataPutMutation(dataPut, info.pDataTable, 1);

        Delete dataDel = new Delete(Bytes.toBytes(ROW_KEY));
        addCellToDelMutation(dataDel,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                2,
                KeyValue.Type.DeleteColumn);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(info.indexMaintainer,
                dataPut,
                dataDel);

        List<Mutation> expectedIndexMutation = new ArrayList<>();

        byte[] idxKeyBytes = generateIndexRowKey("v1");

        Put idxPut1 = new Put(idxKeyBytes);
        addEmptyColumnToIndexPutMutation(idxPut1, info.indexMaintainer, 1);
        expectedIndexMutation.add(idxPut1);

        Put idxPut2 = new Put(generateIndexRowKey(null));
        addEmptyColumnToIndexPutMutation(idxPut2, info.indexMaintainer, 2);
        expectedIndexMutation.add(idxPut2);

        Delete idxDel = new Delete(idxKeyBytes);
        addCellToDelMutation(idxDel,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                null,
                2,
                KeyValue.Type.DeleteFamily);
        expectedIndexMutation.add(idxDel);

        assertEqualMutationList(expectedIndexMutation, actualIndexMutations);
    }

    @Test
    public void testDelOnNonIndexColumn() throws Exception {
        SetupInfo info = setup(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, C1 VARCHAR, C2 VARCHAR",
                "C1",
                "ROW_KEY",
                "");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                1,
                Bytes.toBytes("v1"));
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v2"));
        addEmptyColumnToDataPutMutation(dataPut, info.pDataTable, 1);

        Delete dataDel = new Delete(Bytes.toBytes(ROW_KEY));
        addCellToDelMutation(dataDel,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                2,
                KeyValue.Type.DeleteColumn);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(info.indexMaintainer,
                dataPut,
                dataDel);

        List<Mutation> expectedIndexMutations = new ArrayList<>();

        byte[] idxKeyBytes = generateIndexRowKey("v1");

        Put idxPut1 = new Put(idxKeyBytes);
        addEmptyColumnToIndexPutMutation(idxPut1, info.indexMaintainer, 1);
        expectedIndexMutations.add(idxPut1);

        Put idxPut2 = new Put(idxKeyBytes);
        addEmptyColumnToIndexPutMutation(idxPut2, info.indexMaintainer, 2);
        expectedIndexMutations.add(idxPut2);

        assertEqualMutationList(expectedIndexMutations, actualIndexMutations);
    }

    @Test
    public void testDeleteAllVersions() throws Exception {
        SetupInfo info = setup(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, C1 VARCHAR",
                "C1",
                "ROW_KEY",
                "");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                1,
                Bytes.toBytes("v1"));
        addEmptyColumnToDataPutMutation(dataPut, info.pDataTable, 1);
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                2,
                Bytes.toBytes("v2"));
        addEmptyColumnToDataPutMutation(dataPut, info.pDataTable, 2);

        Delete dataDel = new Delete(Bytes.toBytes(ROW_KEY));
        addCellToDelMutation(dataDel,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                null,
                3,
                KeyValue.Type.DeleteFamily);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(info.indexMaintainer,
                dataPut,
                dataDel);

        List<Mutation> expectedIndexMutations = new ArrayList<>();

        byte[] idxKeyBytes1 = generateIndexRowKey("v1");
        byte[] idxKeyBytes2 = generateIndexRowKey("v2");

        Put idxPut1 = new Put(idxKeyBytes1);
        addEmptyColumnToIndexPutMutation(idxPut1, info.indexMaintainer, 1);
        expectedIndexMutations.add(idxPut1);

        Put idxPut2 = new Put(idxKeyBytes2);
        addEmptyColumnToIndexPutMutation(idxPut2, info.indexMaintainer, 2);
        expectedIndexMutations.add(idxPut2);

        Delete idxDel1 = new Delete(idxKeyBytes1);
        addCellToDelMutation(idxDel1,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                null,
                2,
                KeyValue.Type.DeleteFamily);
        expectedIndexMutations.add(idxDel1);

        Delete idxDel2 = new Delete(idxKeyBytes2);
        addCellToDelMutation(idxDel2,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                null,
                3,
                KeyValue.Type.DeleteFamily);
        expectedIndexMutations.add(idxDel2);

        assertEqualMutationList(expectedIndexMutations, actualIndexMutations);
    }

    @Test
    public void testPutDeleteOnSameTimeStamp() throws Exception {
        SetupInfo info = setup(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, C1 VARCHAR",
                "C1",
                "ROW_KEY",
                "");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                1,
                Bytes.toBytes("v1"));
        addEmptyColumnToDataPutMutation(dataPut, info.pDataTable,1);

        Delete dataDel = new Delete(Bytes.toBytes(ROW_KEY));
        addCellToDelMutation(dataDel,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                1,
                KeyValue.Type.DeleteColumn);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(info.indexMaintainer,
                dataPut,
                dataDel);

        List<Mutation> expectedIndexMutations = new ArrayList<>();

        Put idxPut1 = new Put(generateIndexRowKey(null));
        addEmptyColumnToIndexPutMutation(idxPut1, info.indexMaintainer, 1);
        expectedIndexMutations.add(idxPut1);

        assertEqualMutationList(Arrays.asList((Mutation)idxPut1), actualIndexMutations);
    }

    @Test
    public void testCoveredIndexColumns() throws Exception {
        SetupInfo info = setup(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, C1 VARCHAR, C2 VARCHAR",
                "C1",
                "ROW_KEY",
                "C2");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                1,
                Bytes.toBytes("v1"));
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v2"));
        addEmptyColumnToDataPutMutation(dataPut, info.pDataTable, 1);

        Delete dataDel = new Delete(Bytes.toBytes(ROW_KEY));
        addCellToDelMutation(dataDel,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                2,
                KeyValue.Type.DeleteColumn);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(info.indexMaintainer,
                dataPut,
                dataDel);

        List<Mutation> expectedIndexMutations = new ArrayList<>();
        byte[] idxKeyBytes = generateIndexRowKey("v1");

        Put idxPut1 = new Put(idxKeyBytes);
        addCellToPutMutation(idxPut1,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                Bytes.toBytes("0:C2"),
                1,
                Bytes.toBytes("v2"));
        addEmptyColumnToIndexPutMutation(idxPut1, info.indexMaintainer, 1);
        expectedIndexMutations.add(idxPut1);

        List<Byte> idxKey2 = new ArrayList<>();
        idxKey2.add(QueryConstants.SEPARATOR_BYTE);
        idxKey2.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes(ROW_KEY)));
        byte[] idxKeyBytes2 = com.google.common.primitives.Bytes.toArray(idxKey2);
        Put idxPut2 = new Put(idxKeyBytes2);
        addCellToPutMutation(idxPut2,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                Bytes.toBytes("0:C2"),
                2,
                Bytes.toBytes("v2"));
        addEmptyColumnToIndexPutMutation(idxPut2, info.indexMaintainer, 2);
        expectedIndexMutations.add(idxPut2);

        Delete idxDel = new Delete(idxKeyBytes);
        addCellToDelMutation(idxDel,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                null,
                2,
                KeyValue.Type.DeleteFamily);
        expectedIndexMutations.add(idxDel);

        assertEqualMutationList(expectedIndexMutations, actualIndexMutations);
    }

    @Test
    public void testForMultipleFamilies() throws Exception {
        SetupInfo info = setup(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, CF1.C1 VARCHAR, CF2.C2 VARCHAR",
                "CF1.C1",
                "ROW_KEY",
                "CF2.C2");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        addCellToPutMutation(dataPut,
                Bytes.toBytes("CF1"),
                Bytes.toBytes("C1"),
                1,
                Bytes.toBytes("v1"));
        addCellToPutMutation(dataPut,
                Bytes.toBytes("CF2"),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v2"));
        addEmptyColumnToDataPutMutation(dataPut, info.pDataTable, 1);

        Delete dataDel = new Delete(Bytes.toBytes(ROW_KEY));
        addCellToDelMutation(dataDel,
                Bytes.toBytes("CF1"),
                Bytes.toBytes("C1"),
                2,
                KeyValue.Type.DeleteColumn);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(info.indexMaintainer,
                dataPut,
                dataDel);

        List<Mutation> expectedIndexMutation = new ArrayList<>();

        byte[] idxKeyBytes = generateIndexRowKey("v1");

        Put idxPut1 = new Put(idxKeyBytes);
        addCellToPutMutation(idxPut1,
                Bytes.toBytes("CF2"),
                Bytes.toBytes("CF2:C2"),
                1,
                Bytes.toBytes("v2"));
        addEmptyColumnToIndexPutMutation(idxPut1, info.indexMaintainer, 1);
        expectedIndexMutation.add(idxPut1);

        Put idxPut2 = new Put(generateIndexRowKey(null));
        addCellToPutMutation(idxPut2,
                Bytes.toBytes("CF2"),
                Bytes.toBytes("CF2:C2"),
                2,
                Bytes.toBytes("v2"));
        addEmptyColumnToIndexPutMutation(idxPut2, info.indexMaintainer, 2);
        expectedIndexMutation.add(idxPut2);

        Delete idxDel = new Delete(idxKeyBytes);
        addCellToDelMutation(idxDel,
                Bytes.toBytes("CF2"),
                null,
                2,
                KeyValue.Type.DeleteFamily);
        expectedIndexMutation.add(idxDel);

        assertEqualMutationList(expectedIndexMutation, actualIndexMutations);
    }

    @Test
    public void testSameTypeOfMutationWithDifferentTimeStamp() throws Exception {
        SetupInfo info = setup(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, C1 VARCHAR, C2 VARCHAR",
                "C1",
                "ROW_KEY",
                "");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v2"));
        addEmptyColumnToDataPutMutation(dataPut, info.pDataTable, 1);
        addCellToPutMutation(dataPut,
                info.indexMaintainer.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v3"));
        addEmptyColumnToDataPutMutation(dataPut, info.pDataTable, 2);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(info.indexMaintainer,
                dataPut,
                null);

        byte[] idxKeyBytes = generateIndexRowKey(null);

        Put idxPut1 = new Put(idxKeyBytes);
        addEmptyColumnToIndexPutMutation(idxPut1, info.indexMaintainer, 1);

        Put idxPut2 = new Put(idxKeyBytes);
        addEmptyColumnToIndexPutMutation(idxPut2, info.indexMaintainer, 2);

        assertEqualMutationList(Arrays.asList((Mutation)idxPut1, (Mutation)idxPut2), actualIndexMutations);
    }

    byte[] generateIndexRowKey(String indexVal) {
        List<Byte> idxKey = new ArrayList<>();
        if (indexVal != null && !indexVal.isEmpty())
            idxKey.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes(indexVal)));
        idxKey.add(QueryConstants.SEPARATOR_BYTE);
        idxKey.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes(ROW_KEY)));
        return com.google.common.primitives.Bytes.toArray(idxKey);
    }

    void addCellToPutMutation(Put put, byte[] family, byte[] column, long ts, byte[] value) throws Exception {
        byte[] rowKey = put.getRow();
        Cell cell = new KeyValue(rowKey, family, column, ts, KeyValue.Type.Put, value);
        put.add(cell);
    }

    void addCellToDelMutation(Delete del, byte[] family, byte[] column, long ts, KeyValue.Type type) throws Exception {
        byte[] rowKey = del.getRow();
        Cell cell = new KeyValue(rowKey, family, column, ts, type);
        del.addDeleteMarker(cell);
    }

    void addEmptyColumnToDataPutMutation(Put put, PTable ptable, long ts) throws Exception {
        addCellToPutMutation(put,
                SchemaUtil.getEmptyColumnFamily(ptable),
                QueryConstants.EMPTY_COLUMN_BYTES,
                ts,
                QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
    }

    void addEmptyColumnToIndexPutMutation(Put put, IndexMaintainer im, long ts) throws Exception {
        addCellToPutMutation(put,
                im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                QueryConstants.EMPTY_COLUMN_BYTES,
                ts,
                IndexRegionObserver.VERIFIED_BYTES);
    }

    void assertEqualMutationList(List<Mutation> expectedMutations,
                                 List<Mutation> actualMutations) {
        assertEquals(expectedMutations.size(), actualMutations.size());
        for (Mutation expected : expectedMutations) {
            boolean found = false;
            for (Mutation actual: actualMutations) {
                if (isEqualMutation(expected, actual)) {
                    actualMutations.remove(actual);
                    found = true;
                    break;
                }
            }
            if (!found)
                Assert.fail(String.format("Cannot find mutation:%s", expected));
        }
    }

    boolean isEqualMutation(Mutation expectedMutation, Mutation actualMutation){
        List<Cell> expectedCells = new ArrayList<>();
        for (List<Cell> cells : expectedMutation.getFamilyCellMap().values()) {
            expectedCells.addAll(cells);
        }

        List<Cell> actualCells = new ArrayList<>();
        for (List<Cell> cells : actualMutation.getFamilyCellMap().values()) {
            actualCells.addAll(cells);
        }

        if (expectedCells.size() != actualCells.size())
            return false;
        for(Cell expected : expectedCells) {
            boolean found = false;
            for(Cell actual: actualCells){
                if (isEqualCell(expected, actual)) {
                    actualCells.remove(actual);
                    found = true;
                    break;
                }
            }
            if (!found)
                return false;
        }

        return true;
    }

    boolean isEqualCell(Cell a, Cell b) {
        return CellUtil.matchingRow(a, b)
                && CellUtil.matchingFamily(a, b)
                && CellUtil.matchingQualifier(a, b)
                && CellUtil.matchingTimestamp(a, b)
                && CellUtil.matchingType(a, b)
                && CellUtil.matchingValue(a, b);
    }
}
