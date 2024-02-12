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
package org.apache.phoenix.end2end.index;

import static org.apache.phoenix.end2end.index.IndexCoprocIT.INDEXER_CONFIG;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_FAMILY;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.COLUMN_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_SCHEMA;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CATALOG_TABLE;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_SCHEM;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TENANT_ID;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.GenericTestUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.CoprocessorDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.hbase.index.IndexRegionObserver;
import org.apache.phoenix.hbase.index.Indexer;
import org.apache.phoenix.hbase.index.covered.NonTxIndexBuilder;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.index.GlobalIndexChecker;
import org.apache.phoenix.index.PhoenixIndexBuilder;
import org.apache.phoenix.index.PhoenixIndexCodec;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PRow;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.junit.Assert;

public class IndexTestUtil {

    // the normal db metadata interface is insufficient for all fields needed for an
    // index table test.
    private static final String SELECT_DATA_INDEX_ROW = "SELECT " + COLUMN_FAMILY
            + " FROM "
            + "\""+SYSTEM_CATALOG_SCHEMA +"\""+ ".\"" + SYSTEM_CATALOG_TABLE
            + "\" WHERE "
            + TENANT_ID + " IS NULL AND " + TABLE_SCHEM + "=? AND " + TABLE_NAME + "=? AND " + COLUMN_NAME + " IS NULL AND " + COLUMN_FAMILY + "=?";
    
    public static ResultSet readDataTableIndexRow(Connection conn, String schemaName, String tableName, String indexName) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(SELECT_DATA_INDEX_ROW);
        stmt.setString(1, schemaName);
        stmt.setString(2, tableName);
        stmt.setString(3, indexName);
        return stmt.executeQuery();
    }
    
    
    private static void coerceDataValueToIndexValue(PColumn dataColumn, PColumn indexColumn, ImmutableBytesWritable ptr) {
        PDataType dataType = dataColumn.getDataType();
        // TODO: push to RowKeySchema? 
        SortOrder dataModifier = dataColumn.getSortOrder();
        PDataType indexType = indexColumn.getDataType();
        SortOrder indexModifier = indexColumn.getSortOrder();
        // We know ordinal position will match pk position, because you cannot
        // alter an index table.
        indexType.coerceBytes(ptr, dataType, dataModifier, indexModifier);
    }
    
    public static List<Mutation> generateIndexData(PTable index, PTable table,
            List<Mutation> dataMutations, ImmutableBytesWritable ptr, KeyValueBuilder builder)
            throws SQLException {
        List<Mutation> indexMutations = Lists.newArrayListWithExpectedSize(dataMutations.size());
        for (Mutation dataMutation : dataMutations) {
            indexMutations.addAll(generateIndexData(index, table, dataMutation, ptr, builder));
        }
        return indexMutations;
    }

    public static List<Mutation> generateIndexData(PTable indexTable, PTable dataTable,
            Mutation dataMutation, ImmutableBytesWritable ptr, KeyValueBuilder builder)
            throws SQLException {
        byte[] dataRowKey = dataMutation.getRow();
        RowKeySchema dataRowKeySchema = dataTable.getRowKeySchema();
        List<PColumn> dataPKColumns = dataTable.getPKColumns();
        int i = 0;
        int indexOffset = 0;
        Boolean hasValue;
        // Skip salt column
        int maxOffset = dataRowKey.length;
        dataRowKeySchema.iterator(dataRowKey, ptr, dataTable.getBucketNum() == null ? i : ++i);
        List<PColumn> indexPKColumns = indexTable.getPKColumns();
        List<PColumn> indexColumns = indexTable.getColumns();
        int nIndexColumns = indexPKColumns.size();
        int maxIndexValues = indexColumns.size() - nIndexColumns - indexOffset;
        BitSet indexValuesSet = new BitSet(maxIndexValues);
        byte[][] indexValues = new byte[indexColumns.size() - indexOffset][];
        while ((hasValue = dataRowKeySchema.next(ptr, i, maxOffset)) != null) {
            if (hasValue) {
                PColumn dataColumn = dataPKColumns.get(i);
                PColumn indexColumn = indexTable.getColumnForColumnName(IndexUtil.getIndexColumnName(dataColumn));
                coerceDataValueToIndexValue(dataColumn, indexColumn, ptr);
                indexValues[indexColumn.getPosition()-indexOffset] = ptr.copyBytes();
            }
            i++;
        }
        PRow row;
        long ts = MetaDataUtil.getClientTimeStamp(dataMutation);
        if (dataMutation instanceof Delete && dataMutation.getFamilyCellMap().values().isEmpty()) {
            indexTable.newKey(ptr, indexValues);
            row = indexTable.newRow(builder, ts, ptr, false);
            row.delete();
        } else {
            // If no column families in table, then nothing to look for 
            if (!dataTable.getColumnFamilies().isEmpty()) {
                for (Map.Entry<byte[],List<Cell>> entry : dataMutation.getFamilyCellMap().entrySet()) {
                    PColumnFamily family = dataTable.getColumnFamily(entry.getKey());
                    for (Cell kv : entry.getValue()) {
                        byte[] cq = CellUtil.cloneQualifier(kv);
                        byte[] emptyKVQualifier = EncodedColumnsUtil.getEmptyKeyValueInfo(dataTable).getFirst();
                        if (Bytes.compareTo(emptyKVQualifier, cq) != 0) {
                            try {
                                PColumn dataColumn = family.getPColumnForColumnQualifier(cq);
                                PColumn indexColumn = indexTable.getColumnForColumnName(IndexUtil.getIndexColumnName(family.getName().getString(), dataColumn.getName().getString()));
                                ptr.set(kv.getValueArray(),kv.getValueOffset(),kv.getValueLength());
                                coerceDataValueToIndexValue(dataColumn, indexColumn, ptr);
                                indexValues[indexPKColumns.indexOf(indexColumn)-indexOffset] = ptr.copyBytes();
                                if (!SchemaUtil.isPKColumn(indexColumn)) {
                                    indexValuesSet.set(indexColumn.getPosition()-nIndexColumns-indexOffset);
                                }
                            } catch (ColumnNotFoundException e) {
                                // Ignore as this means that the data column isn't in the index
                            }
                        }
                    }
                }
            }
            indexTable.newKey(ptr, indexValues);
            row = indexTable.newRow(builder, ts, ptr, false);
            int pos = 0;
            while ((pos = indexValuesSet.nextSetBit(pos)) >= 0) {
                int index = nIndexColumns + indexOffset + pos++;
                PColumn indexColumn = indexColumns.get(index);
                row.setValue(indexColumn, indexValues[index]);
            }
        }
        return row.toRowMutations();
    }

    public static void downgradeCoprocs(String physicalTableName,
            String physicalIndexName, Admin admin) throws Exception {
        TableDescriptor baseDescriptor = admin.getDescriptor(TableName.valueOf(physicalTableName));
        TableDescriptorBuilder baseDescBuilder = TableDescriptorBuilder.newBuilder(baseDescriptor);

        assertCoprocsContains(IndexRegionObserver.class, baseDescriptor);
        removeCoproc(IndexRegionObserver.class, baseDescBuilder, admin);

        if (physicalIndexName != null) {
            TableDescriptor indexDescriptor =
                    admin.getDescriptor(TableName.valueOf(physicalIndexName));
            assertCoprocsContains(GlobalIndexChecker.class, indexDescriptor);

            TableDescriptorBuilder indexDescBuilder =
                    TableDescriptorBuilder.newBuilder(indexDescriptor);
            removeCoproc(IndexRegionObserver.class, indexDescBuilder, admin);
            removeCoproc(GlobalIndexChecker.class, indexDescBuilder, admin);
        }

        Map<String, String> props = new HashMap<>();
        props.put(NonTxIndexBuilder.CODEC_CLASS_NAME_KEY, PhoenixIndexCodec.class.getName());
        IndexUtil.enableIndexing(baseDescBuilder, PhoenixIndexBuilder.class.getName(),
                props, QueryServicesOptions.DEFAULT_COPROCESSOR_PRIORITY, QueryConstants.INDEXER_CLASSNAME);
        admin.modifyTable(baseDescBuilder.build());
        baseDescriptor = admin.getDescriptor(TableName.valueOf(physicalTableName));
        TableDescriptor indexDescriptor = null;
        if (physicalIndexName != null) {
            indexDescriptor = admin.getDescriptor(TableName.valueOf(physicalIndexName));
        }
        assertUsingOldCoprocs(baseDescriptor, indexDescriptor);
    }


    public static void assertCoprocsContains(Class clazz, TableDescriptor descriptor) {
        String expectedCoprocName = clazz.getName();
        boolean foundCoproc = isCoprocPresent(descriptor, expectedCoprocName);
        Assert.assertTrue("Could not find coproc " + expectedCoprocName +
                " in descriptor " + descriptor,foundCoproc);
    }


    public static boolean isCoprocPresent(TableDescriptor descriptor, String expectedCoprocName) {
        boolean foundCoproc = false;
        for (CoprocessorDescriptor coprocDesc : descriptor.getCoprocessorDescriptors()){
            if (coprocDesc.getClassName().equals(expectedCoprocName)){
                foundCoproc = true;
                break;
            }
        }
        return foundCoproc;
    }

    public static void removeCoproc(Class clazz, TableDescriptorBuilder descBuilder, Admin admin)
            throws Exception {
        descBuilder.removeCoprocessor(clazz.getName());
        admin.modifyTable(descBuilder.build());
    }

    public static void assertUsingOldCoprocs(TableDescriptor baseDescriptor,
            TableDescriptor indexDescriptor) {
        assertCoprocsContains(Indexer.class, baseDescriptor);
        assertCoprocConfig(baseDescriptor, Indexer.class.getName(),
                INDEXER_CONFIG);
        assertCoprocsNotContains(IndexRegionObserver.class, baseDescriptor);
        if (indexDescriptor != null) {
            assertCoprocsNotContains(IndexRegionObserver.class, indexDescriptor);
            assertCoprocsNotContains(GlobalIndexChecker.class, indexDescriptor);
        }
    }

    public static void assertCoprocConfig(TableDescriptor indexDesc,
            String className, String expectedConfigValue){
        boolean foundConfig = false;
        for (Map.Entry<Bytes, Bytes> entry :
                indexDesc.getValues().entrySet()){
            String propKey = Bytes.toString(entry.getKey().get());
            String propValue = Bytes.toString(entry.getValue().get());
            //Unfortunately, a good API to read coproc properties didn't show up until
            //HBase 2.0. Doing this the painful String-matching way to be compatible with 1.x
            if (propKey.contains("coprocessor")){
                if (propValue.contains(className)){
                    Assert.assertEquals(className + " is configured incorrectly",
                            expectedConfigValue,
                            propValue);
                    foundConfig = true;
                    break;
                }
            }
        }
        Assert.assertTrue("Couldn't find config for " + className, foundConfig);
    }

    public static void assertCoprocsNotContains(Class clazz, TableDescriptor descriptor) {
        String expectedCoprocName = clazz.getName();
        boolean foundCoproc = isCoprocPresent(descriptor, expectedCoprocName);
        Assert.assertFalse("Could find coproc " + expectedCoprocName +
                " in descriptor " + descriptor,foundCoproc);
    }

    public static void assertRowsForEmptyColValue(Connection conn, String tableName,
            byte[] emptyValue) throws SQLException, IOException {
        ConnectionQueryServices cqs = conn.unwrap(PhoenixConnection.class).getQueryServices();
        PTable pTable = conn.unwrap(PhoenixConnection.class).getTable(tableName);
        Table hTable = cqs.getTable(pTable.getPhysicalName().getBytes());

        byte[] emptyKeyValueCF = SchemaUtil.getEmptyColumnFamily(pTable);
        byte[] emptyKeyValueQualifier = EncodedColumnsUtil.getEmptyKeyValueInfo(pTable).getFirst();

        Scan scan = new Scan();
        scan.setFilter(new SingleColumnValueFilter(
                emptyKeyValueCF,
                emptyKeyValueQualifier,
                CompareOperator.NOT_EQUAL,
                new org.apache.hadoop.hbase.filter.BinaryComparator(emptyValue)));
        try (ResultScanner scanner = hTable.getScanner(scan)) {
            assertNull("There are rows with in the table where the empty value is not "
                    + Bytes.toStringBinary(emptyValue), scanner.next());
        }
    }
}