package org.apache.phoenix.index;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.IndexRebuildRegionScanner;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.util.GenericKeyValueBuilder;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(PrepareIndexMutationsForRebuildTest.class);


    private IndexMaintainer SetupIndexMaintainer(String tableName,
                                                 String indexName,
                                                 String columns,
                                                 String indexColumns,
                                                 String pk,
                                                 String includeColumns) throws Exception{
        Connection conn = DriverManager.getConnection(getUrl());

        String fullTableName = SchemaUtil.getTableName(SchemaUtil.normalizeIdentifier(""),SchemaUtil.normalizeIdentifier(tableName));
        String fullIndexName = SchemaUtil.getTableName(SchemaUtil.normalizeIdentifier(""),SchemaUtil.normalizeIdentifier(indexName));

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
        PTable pDataTable = pconn.getTable(new PTableKey(pconn.getTenantId(), fullTableName));
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        pDataTable.getIndexMaintainers(ptr, pconn);
        List<IndexMaintainer> indexMaintainers = IndexMaintainer.deserialize(ptr, GenericKeyValueBuilder.INSTANCE, true);
        return indexMaintainers.get(0);
    }

    @Test
    public void TestSinglePutOnIndexColumn() throws Exception{
        IndexMaintainer im = SetupIndexMaintainer(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, C1 VARCHAR, C2 VARCHAR",
                "C1",
                "ROW_KEY",
                "");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        AddCellToPutMutation(dataPut,
                im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                1,
                Bytes.toBytes("v1"));
        AddCellToPutMutation(dataPut,
                im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v2"));
        AddEmptyColumnToDataPutMutation(dataPut, 1);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(im,
                dataPut,
                null);

        List<Byte> idxKey = new ArrayList<>();
        idxKey.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes("v1")));
        idxKey.add(QueryConstants.SEPARATOR_BYTE);
        idxKey.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes(ROW_KEY)));
        Put idxPut1 = new Put(com.google.common.primitives.Bytes.toArray(idxKey));
        AddEmptyColumnToIndexPutMutation(idxPut1, 1);

        assertEqualMutationList(Arrays.asList((Mutation)idxPut1), actualIndexMutations);
    }

    @Test
    public void TestSinglePutOnNonIndexColumn() throws Exception{
        IndexMaintainer im = SetupIndexMaintainer(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, C1 VARCHAR, C2 VARCHAR",
                "C1",
                "ROW_KEY",
                "");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        AddCellToPutMutation(dataPut,
                im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v2"));
        AddEmptyColumnToDataPutMutation(dataPut, 1);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(im,
                dataPut,
                null);

        List<Byte> idxKey = new ArrayList<>();
        idxKey.add(QueryConstants.SEPARATOR_BYTE);
        idxKey.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes(ROW_KEY)));
        Put idxPut1 = new Put(com.google.common.primitives.Bytes.toArray(idxKey));
        AddEmptyColumnToIndexPutMutation(idxPut1, 1);

        assertEqualMutationList(Arrays.asList((Mutation)idxPut1), actualIndexMutations);
    }

    @Test
    public void TestDelOnIndexColumn() throws Exception{
        IndexMaintainer im = SetupIndexMaintainer(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, C1 VARCHAR, C2 VARCHAR",
                "C1",
                "ROW_KEY",
                "");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        AddCellToPutMutation(dataPut,
                im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                1,
                Bytes.toBytes("v1"));
        AddCellToPutMutation(dataPut,
                im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v2"));
        AddEmptyColumnToDataPutMutation(dataPut, 1);

        Delete dataDel = new Delete(Bytes.toBytes(ROW_KEY));
        AddCellToDelMutation(dataDel,
                im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                2,
                KeyValue.Type.DeleteColumn);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(im,
                dataPut,
                dataDel);

        List<Mutation> expectedIndexMutation = new ArrayList<>();

        List<Byte> idxKey = new ArrayList<>();
        idxKey.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes("v1")));
        idxKey.add(QueryConstants.SEPARATOR_BYTE);
        idxKey.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes(ROW_KEY)));
        byte[] idxKeyBytes = com.google.common.primitives.Bytes.toArray(idxKey);

        Put idxPut1 = new Put(idxKeyBytes);
        AddEmptyColumnToIndexPutMutation(idxPut1, 1);
        expectedIndexMutation.add(idxPut1);

        List<Byte> idxKey2 = new ArrayList<>();
        idxKey2.add(QueryConstants.SEPARATOR_BYTE);
        idxKey2.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes(ROW_KEY)));
        Put idxPut2 = new Put(com.google.common.primitives.Bytes.toArray(idxKey2));
        AddEmptyColumnToIndexPutMutation(idxPut2, 2);
        expectedIndexMutation.add(idxPut2);

        Delete idxDel = new Delete(idxKeyBytes);
        AddCellToDelMutation(idxDel,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                null,
                2,
                KeyValue.Type.DeleteFamily);
        expectedIndexMutation.add(idxDel);

        assertEqualMutationList(expectedIndexMutation, actualIndexMutations);
    }

    @Test
    public void TestDelOnNonIndexColumn() throws Exception{
        IndexMaintainer im = SetupIndexMaintainer(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, C1 VARCHAR, C2 VARCHAR",
                "C1",
                "ROW_KEY",
                "");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        AddCellToPutMutation(dataPut,
                im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                1,
                Bytes.toBytes("v1"));
        AddCellToPutMutation(dataPut,
                im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v2"));
        AddEmptyColumnToDataPutMutation(dataPut, 1);

        Delete dataDel = new Delete(Bytes.toBytes(ROW_KEY));
        AddCellToDelMutation(dataDel,
                im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                2,
                KeyValue.Type.DeleteColumn);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(im,
                dataPut,
                dataDel);

        List<Mutation> expectedIndexMutations = new ArrayList<>();

        List<Byte> idxKey = new ArrayList<>();
        idxKey.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes("v1")));
        idxKey.add(QueryConstants.SEPARATOR_BYTE);
        idxKey.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes(ROW_KEY)));
        byte[] idxKeyBytes = com.google.common.primitives.Bytes.toArray(idxKey);

        Put idxPut1 = new Put(idxKeyBytes);
        AddEmptyColumnToIndexPutMutation(idxPut1, 1);
        expectedIndexMutations.add(idxPut1);

        Put idxPut2 = new Put(idxKeyBytes);
        AddEmptyColumnToIndexPutMutation(idxPut2, 2);
        expectedIndexMutations.add(idxPut2);

        assertEqualMutationList(expectedIndexMutations, actualIndexMutations);
    }

    @Test
    public void TestCoveredIndexColumns() throws Exception{
        IndexMaintainer im = SetupIndexMaintainer(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, C1 VARCHAR, C2 VARCHAR",
                "C1",
                "ROW_KEY",
                "C2");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        AddCellToPutMutation(dataPut,
                im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                1,
                Bytes.toBytes("v1"));
        AddCellToPutMutation(dataPut,
                im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v2"));
        AddEmptyColumnToDataPutMutation(dataPut, 1);

        Delete dataDel = new Delete(Bytes.toBytes(ROW_KEY));
        AddCellToDelMutation(dataDel,
                im.getEmptyKeyValueFamily().copyBytesIfNecessary(),
                Bytes.toBytes("C1"),
                2,
                KeyValue.Type.DeleteColumn);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(im,
                dataPut,
                dataDel);

        List<Mutation> expectedIndexMutations = new ArrayList<>();
        List<Byte> idxKey = new ArrayList<>();
        idxKey.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes("v1")));
        idxKey.add(QueryConstants.SEPARATOR_BYTE);
        idxKey.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes(ROW_KEY)));
        byte[] idxKeyBytes = com.google.common.primitives.Bytes.toArray(idxKey);

        Put idxPut1 = new Put(idxKeyBytes);
        AddCellToPutMutation(idxPut1,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                Bytes.toBytes("0:C2"),
                1,
                Bytes.toBytes("v2"));
        AddEmptyColumnToIndexPutMutation(idxPut1, 1);
        expectedIndexMutations.add(idxPut1);

        List<Byte> idxKey2 = new ArrayList<>();
        idxKey2.add(QueryConstants.SEPARATOR_BYTE);
        idxKey2.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes(ROW_KEY)));
        byte[] idxKeyBytes2 = com.google.common.primitives.Bytes.toArray(idxKey2);
        Put idxPut2 = new Put(idxKeyBytes2);
        AddCellToPutMutation(idxPut2,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                Bytes.toBytes("0:C2"),
                2,
                Bytes.toBytes("v2"));
        AddEmptyColumnToIndexPutMutation(idxPut2, 2);
        expectedIndexMutations.add(idxPut2);

        Delete idxDel = new Delete(idxKeyBytes);
        AddCellToDelMutation(idxDel,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                null,
                2,
                KeyValue.Type.DeleteFamily);
        expectedIndexMutations.add(idxDel);

        assertEqualMutationList(expectedIndexMutations, actualIndexMutations);
    }

    @Test
    public void TestForMultipleFamilies() throws Exception {
        IndexMaintainer im = SetupIndexMaintainer(TABLE_NAME,
                INDEX_NAME,
                "ROW_KEY VARCHAR, CF1.C1 VARCHAR, CF2.C2 VARCHAR",
                "CF1.C1",
                "ROW_KEY",
                "CF2.C2");

        Put dataPut = new Put(Bytes.toBytes(ROW_KEY));
        AddCellToPutMutation(dataPut,
                Bytes.toBytes("CF1"),
                Bytes.toBytes("C1"),
                1,
                Bytes.toBytes("v1"));
        AddCellToPutMutation(dataPut,
                Bytes.toBytes("CF2"),
                Bytes.toBytes("C2"),
                1,
                Bytes.toBytes("v2"));
        AddEmptyColumnToDataPutMutation(dataPut, 1);

        Delete dataDel = new Delete(Bytes.toBytes(ROW_KEY));
        AddCellToDelMutation(dataDel,
                Bytes.toBytes("CF1"),
                Bytes.toBytes("C1"),
                2,
                KeyValue.Type.DeleteColumn);

        List<Mutation> actualIndexMutations = IndexRebuildRegionScanner.prepareIndexMutationsForRebuild(im,
                dataPut,
                dataDel);

        List<Mutation> expectedIndexMutation = new ArrayList<>();

        List<Byte> idxKey = new ArrayList<>();
        idxKey.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes("v1")));
        idxKey.add(QueryConstants.SEPARATOR_BYTE);
        idxKey.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes(ROW_KEY)));
        byte[] idxKeyBytes = com.google.common.primitives.Bytes.toArray(idxKey);

        Put idxPut1 = new Put(idxKeyBytes);
        AddCellToPutMutation(idxPut1,
                Bytes.toBytes("CF2"),
                Bytes.toBytes("CF2:C2"),
                1,
                Bytes.toBytes("v2"));
        AddEmptyColumnToIndexPutMutation(idxPut1, "CF2", 1);
        expectedIndexMutation.add(idxPut1);

        List<Byte> idxKey2 = new ArrayList<>();
        idxKey2.add(QueryConstants.SEPARATOR_BYTE);
        idxKey2.addAll(com.google.common.primitives.Bytes.asList(Bytes.toBytes(ROW_KEY)));
        Put idxPut2 = new Put(com.google.common.primitives.Bytes.toArray(idxKey2));
        AddCellToPutMutation(idxPut2,
                Bytes.toBytes("CF2"),
                Bytes.toBytes("CF2:C2"),
                2,
                Bytes.toBytes("v2"));
        AddEmptyColumnToIndexPutMutation(idxPut2, "CF2", 2);
        expectedIndexMutation.add(idxPut2);

        Delete idxDel = new Delete(idxKeyBytes);
        AddCellToDelMutation(idxDel,
                Bytes.toBytes("CF2"),
                null,
                2,
                KeyValue.Type.DeleteFamily);
        expectedIndexMutation.add(idxDel);

        assertEqualMutationList(expectedIndexMutation, actualIndexMutations);
    }


    void AddCellToPutMutation(Put put, byte[] family, byte[] column, long ts,  byte[] value) throws Exception{
        byte[] rowKey = put.getRow();
        Cell cell = new KeyValue(rowKey, family, column, ts, KeyValue.Type.Put, value);
        put.add(cell);
    }

    void AddCellToDelMutation(Delete del, byte[] family, byte[] column, long ts, KeyValue.Type type) throws Exception{
        byte[] rowKey = del.getRow();
        Cell cell = new KeyValue(rowKey, family, column, ts, type);
        del.addDeleteMarker(cell);
    }

    void AddEmptyColumnToDataPutMutation(Put put, long ts) throws Exception{
        AddCellToPutMutation(put,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                QueryConstants.EMPTY_COLUMN_BYTES,
                ts,
                QueryConstants.EMPTY_COLUMN_VALUE_BYTES);
    }

    void AddEmptyColumnToIndexPutMutation(Put put, long ts) throws Exception{
        AddCellToPutMutation(put,
                QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                QueryConstants.EMPTY_COLUMN_BYTES,
                ts,
                IndexRegionObserver.VERIFIED_BYTES);
    }

    void AddEmptyColumnToIndexPutMutation(Put put, String family, long ts) throws Exception{
        AddCellToPutMutation(put,
                Bytes.toBytes(family),
                QueryConstants.EMPTY_COLUMN_BYTES,
                ts,
                IndexRegionObserver.VERIFIED_BYTES);
    }

    void assertEqualMutationList(List<Mutation> expectedMutations,
                                 List<Mutation> actualMutations){
        assertEquals(expectedMutations.size(), actualMutations.size());
        for (Mutation expected : expectedMutations){
            boolean found = false;
            for (Mutation actual: actualMutations){
                if (IsEqualMutation(expected, actual)){
                    actualMutations.remove(actual);
                    found = true;
                    break;
                }
            }
            if (!found)
                Assert.fail(String.format("Cannot find mutation:%s", expected));
        }
    }

    boolean IsEqualMutation(Mutation expectedMutation, Mutation actualMutation){
        List<Cell> expectedCells = new ArrayList<>();
        for (List<Cell> cells : expectedMutation.getFamilyCellMap().values()) {
            expectedCells.addAll(cells);
        }

        List<Cell> actualCells = new ArrayList<>();
        for (List<Cell> cells : actualMutation.getFamilyCellMap().values()) {
            actualCells.addAll(cells);
        }

        assertEquals(expectedCells.size(), actualCells.size());
        for(Cell expected : expectedCells){
            boolean found = false;
            for(Cell actual: actualCells){
                if (IsEqualCell(expected, actual)){
                    actualCells.remove(actual);
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }

        return true;
    }

    boolean IsEqualCell(Cell a, Cell b){
        return CellUtil.matchingRow(a, b)
                && CellUtil.matchingFamily(a, b)
                && CellUtil.matchingQualifier(a, b)
                && CellUtil.matchingTimestamp(a, b)
                && CellUtil.matchingType(a, b)
                && CellUtil.matchingValue(a, b);
    }
}
