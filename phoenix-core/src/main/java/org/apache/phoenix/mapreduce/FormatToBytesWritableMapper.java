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
package org.apache.phoenix.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.bulkload.TableRowkeyPair;
import org.apache.phoenix.mapreduce.bulkload.TargetTableRefFunctions;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.ImmutableStorageScheme;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.UpsertExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Base class for converting some input source format into {@link ImmutableBytesWritable}s that
 * contains packed in a single byte array values for all columns.
 * Assumes input format is text-based, with one row per line. Depends on an online cluster
 * to retrieve {@link ColumnInfo} from the target table.
 */
public abstract class FormatToBytesWritableMapper<RECORD> extends Mapper<LongWritable, Text, TableRowkeyPair,
        ImmutableBytesWritable> {

    protected static final Logger LOG = LoggerFactory.getLogger(FormatToBytesWritableMapper.class);

    protected static final String COUNTER_GROUP_NAME = "Phoenix MapReduce Import";

    /** Configuration key for the name of the output table */
    public static final String TABLE_NAME_CONFKEY = "phoenix.mapreduce.import.tablename";

    /** Configuration key for the columns to be imported */
    public static final String COLUMN_INFO_CONFKEY = "phoenix.mapreduce.import.columninfos";

    /** Configuration key for the flag to ignore invalid rows */
    public static final String IGNORE_INVALID_ROW_CONFKEY = "phoenix.mapreduce.import.ignoreinvalidrow";

    /** Configuration key for the table names */
    public static final String TABLE_NAMES_CONFKEY = "phoenix.mapreduce.import.tablenames";

    /** Configuration key for the table logical names */
    public static final String LOGICAL_NAMES_CONFKEY = "phoenix.mapreduce.import.logicalnames";

    /**
     * Parses a single input line, returning a {@code T}.
     */
    public interface LineParser<T> {
        T parse(String input) throws IOException;
    }

    protected PhoenixConnection conn;
    protected UpsertExecutor<RECORD, ?> upsertExecutor;
    protected ImportPreUpsertKeyValueProcessor preUpdateProcessor;
    protected List<String> tableNames;
    protected List<String> logicalNames;
    protected MapperUpsertListener<RECORD> upsertListener;

    /*
    lookup table for column index. Index in the List matches to the index in tableNames List
     */
    protected Map<byte[], Integer> columnIndexes;

    protected abstract UpsertExecutor<RECORD,?> buildUpsertExecutor(Configuration conf);
    protected abstract LineParser<RECORD> getLineParser();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        // pass client configuration into driver
        Properties clientInfos = new Properties();
        for (Map.Entry<String, String> entry : conf) {
            clientInfos.setProperty(entry.getKey(), entry.getValue());
        }

        try {
            conn = (PhoenixConnection) QueryUtil.getConnectionOnServer(clientInfos, conf);
            // We are dependent on rolling back before performing commits, so we need to be sure
            // that auto-commit is not turned on
            conn.setAutoCommit(false);

            final String tableNamesConf = conf.get(TABLE_NAMES_CONFKEY);
            final String logicalNamesConf = conf.get(LOGICAL_NAMES_CONFKEY);
            tableNames = TargetTableRefFunctions.NAMES_FROM_JSON.apply(tableNamesConf);
            logicalNames = TargetTableRefFunctions.NAMES_FROM_JSON.apply(logicalNamesConf);

            initColumnIndexes();
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        upsertListener = new MapperUpsertListener<RECORD>(
                context, conf.getBoolean(IGNORE_INVALID_ROW_CONFKEY, true));
        upsertExecutor = buildUpsertExecutor(conf);
        preUpdateProcessor = PhoenixConfigurationUtil.loadPreUpsertProcessor(conf);
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException,
            InterruptedException {
        if (conn == null) {
            throw new RuntimeException("Connection not initialized.");
        }
        try {
            RECORD record = null;
            try {
                record = getLineParser().parse(value.toString());
            } catch (IOException e) {
                context.getCounter(COUNTER_GROUP_NAME, "Parser errors").increment(1L);
                return;
            }

            if (record == null) {
                context.getCounter(COUNTER_GROUP_NAME, "Empty records").increment(1L);
                return;
            }
            upsertExecutor.execute(ImmutableList.<RECORD>of(record));
            Map<Integer, List<KeyValue>> map = new HashMap<>();
            Iterator<Pair<byte[], List<KeyValue>>> uncommittedDataIterator
                    = PhoenixRuntime.getUncommittedDataIterator(conn, true);
            while (uncommittedDataIterator.hasNext()) {
                Pair<byte[], List<KeyValue>> kvPair = uncommittedDataIterator.next();
                List<KeyValue> keyValueList = kvPair.getSecond();
                keyValueList = preUpdateProcessor.preUpsert(kvPair.getFirst(), keyValueList);
                byte[] first = kvPair.getFirst();
                // Create a list of KV for each table
                for (int i = 0; i < tableNames.size(); i++) {
                    if (Bytes.compareTo(Bytes.toBytes(tableNames.get(i)), first) == 0) {
                        if (!map.containsKey(i)) {
                            map.put(i, new ArrayList<KeyValue>());
                        }
                        List<KeyValue> list = map.get(i);
                        for (KeyValue kv : keyValueList) {
                            list.add(kv);
                        }
                        break;
                    }
                }
            }
            for (Map.Entry<Integer, List<KeyValue>> rowEntry : map.entrySet()) {
                int tableIndex = rowEntry.getKey();
                List<KeyValue> lkv = rowEntry.getValue();
                // All KV values combines to a single byte array
                writeAggregatedRow(context, tableNames.get(tableIndex), lkv);
            }
            conn.rollback();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Map all unique pairs <family, name>  to index. Table name is part of TableRowkey, so we do
    not care about it
     */
    private void initColumnIndexes() throws SQLException {
        columnIndexes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        int columnIndex = 0;
        for (int index = 0; index < logicalNames.size(); index++) {
            PTable table = PhoenixRuntime.getTable(conn, logicalNames.get(index));
            if (!table.getImmutableStorageScheme().equals(ImmutableStorageScheme.ONE_CELL_PER_COLUMN)) {
                List<PColumnFamily> cfs = table.getColumnFamilies();
                for (int i = 0; i < cfs.size(); i++) {
                    byte[] family = cfs.get(i).getName().getBytes();
                    byte[] cfn = Bytes.add(family, QueryConstants.NAMESPACE_SEPARATOR_BYTES,
                            QueryConstants.SINGLE_KEYVALUE_COLUMN_QUALIFIER_BYTES);
                    columnIndexes.put(cfn, new Integer(columnIndex));
                    columnIndex++;
                }
            } else {
                List<PColumn> cls = table.getColumns();
                for (int i = 0; i < cls.size(); i++) {
                    PColumn c = cls.get(i);
                    byte[] family = new byte[0];
                    byte[] cq;
                    if (!SchemaUtil.isPKColumn(c)) {
                        family = c.getFamilyName().getBytes();
                        cq = c.getColumnQualifierBytes();
                    } else {
                        cq = c.getName().getBytes();
                    }
                    byte[] cfn = Bytes.add(family, QueryConstants.NAMESPACE_SEPARATOR_BYTES, cq);
                    if (!columnIndexes.containsKey(cfn)) {
                        columnIndexes.put(cfn, new Integer(columnIndex));
                        columnIndex++;
                    }
                }
                byte[] emptyColumnFamily = SchemaUtil.getEmptyColumnFamily(table);
                byte[] emptyKeyValue = EncodedColumnsUtil.getEmptyKeyValueInfo(table).getFirst();
                byte[] cfn = Bytes.add(emptyColumnFamily, QueryConstants.NAMESPACE_SEPARATOR_BYTES, emptyKeyValue);
                columnIndexes.put(cfn, new Integer(columnIndex));
                columnIndex++;
            }
        }
    }

    /**
     * Find the column index which will replace the column name in
     * the aggregated array and will be restored in Reducer
     *
     * @param cell       KeyValue for the column
     * @return column index for the specified cell or -1 if was not found
     */
    private int findIndex(Cell cell) throws IOException {
        byte[] familyName = Bytes.copy(cell.getFamilyArray(), cell.getFamilyOffset(),
                cell.getFamilyLength());
        byte[] cq = Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(),
                cell.getQualifierLength());
        byte[] cfn = Bytes.add(familyName, QueryConstants.NAMESPACE_SEPARATOR_BYTES, cq);
        if(columnIndexes.containsKey(cfn)) {
            return columnIndexes.get(cfn);
        }
        return -1;
    }

    /**
     * Collect all column values for the same Row. RowKey may be different if indexes are involved,
     * so it writes a separate record for each unique RowKey
     *
     * @param context    Current mapper context
     * @param tableName Table index in tableNames list
     * @param lkv        List of KV values that will be combined in a single ImmutableBytesWritable
     * @throws IOException
     * @throws InterruptedException
     */

    private void writeAggregatedRow(Context context, String tableName, List<KeyValue> lkv)
            throws IOException, InterruptedException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
        DataOutputStream outputStream = new DataOutputStream(bos);
        ImmutableBytesWritable outputKey =null;
        if (!lkv.isEmpty()) {
            for (KeyValue cell : lkv) {
                if (outputKey == null || Bytes.compareTo(outputKey.get(), outputKey.getOffset(),
                        outputKey.getLength(), cell.getRowArray(), cell.getRowOffset(), cell
                                .getRowLength()) != 0) {
                    // This a the first RowKey or a different from previous
                    if (outputKey != null) { //It's a different RowKey, so we need to write it
                        ImmutableBytesWritable aggregatedArray =
                                new ImmutableBytesWritable(bos.toByteArray());
                        outputStream.close();
                        context.write(new TableRowkeyPair(tableName, outputKey), aggregatedArray);
                    }
                    outputKey = new ImmutableBytesWritable(cell.getRowArray(), cell.getRowOffset()
                            , cell.getRowLength());
                    bos = new ByteArrayOutputStream(1024);
                    outputStream = new DataOutputStream(bos);
                }
                /*
                The order of aggregation: type, index of column, length of value, value itself
                 */
                int i = findIndex(cell);
                if(i == -1) {
                    //That may happen when we load only local indexes. Since KV pairs for both
                    // table and local index are going to the same physical table at that point
                    // we skip those KVs that are not belongs to loca index
                    continue;
                }
                outputStream.writeByte(cell.getTypeByte());
                WritableUtils.writeVInt(outputStream, i);
                WritableUtils.writeVInt(outputStream, cell.getValueLength());
                outputStream.write(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

            }
            ImmutableBytesWritable aggregatedArray = new ImmutableBytesWritable(bos.toByteArray());
            outputStream.close();
            context.write(new TableRowkeyPair(tableName, outputKey), aggregatedArray);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Write the list of to-import columns to a job configuration.
     *
     * @param conf           configuration to be written to
     * @param columnInfoList list of ColumnInfo objects to be configured for import
     */
    @VisibleForTesting
    static void configureColumnInfoList(Configuration conf, List<ColumnInfo> columnInfoList) {
        conf.set(COLUMN_INFO_CONFKEY, Joiner.on("|").useForNull("").join(columnInfoList));
    }

    /**
     * Build the list of ColumnInfos for the import based on information in the configuration.
     */
    @VisibleForTesting
    static List<ColumnInfo> buildColumnInfoList(Configuration conf) {

        return Lists.newArrayList(
                Iterables.transform(
                        Splitter.on("|").split(conf.get(COLUMN_INFO_CONFKEY)),
                        new Function<String, ColumnInfo>() {
                            @Nullable
                            @Override
                            public ColumnInfo apply(@Nullable String input) {
                                if (input == null || input.isEmpty()) {
                                    // An empty string represents a null that was passed in to
                                    // the configuration, which corresponds to an input column
                                    // which is to be skipped
                                    return null;
                                }
                                return ColumnInfo.fromString(input);
                            }
                        }));
    }

    /**
     * Listener that logs successful upserts and errors to job counters.
     */
    @VisibleForTesting
    static class MapperUpsertListener<T> implements UpsertExecutor.UpsertListener<T> {

        private final Mapper<LongWritable, Text,
                TableRowkeyPair, ImmutableBytesWritable>.Context context;
        private final boolean ignoreRecordErrors;

        private MapperUpsertListener(
                Mapper<LongWritable, Text, TableRowkeyPair, ImmutableBytesWritable>.Context context,
                boolean ignoreRecordErrors) {
            this.context = context;
            this.ignoreRecordErrors = ignoreRecordErrors;
        }

        @Override
        public void upsertDone(long upsertCount) {
            context.getCounter(COUNTER_GROUP_NAME, "Upserts Done").increment(1L);
        }

        @Override
        public void errorOnRecord(T record, Throwable throwable) {
            LOG.error("Error on record " + record, throwable);
            context.getCounter(COUNTER_GROUP_NAME, "Errors on records").increment(1L);
            if (!ignoreRecordErrors) {
                Throwables.propagate(throwable);
            }
        }
    }

    /**
     * A default implementation of {@code ImportPreUpsertKeyValueProcessor} that is used if no
     * specific class is configured. This implementation simply passes through the KeyValue
     * list that is passed in.
     */
    public static class DefaultImportPreUpsertKeyValueProcessor implements
            ImportPreUpsertKeyValueProcessor {

        @Override
        public List<KeyValue> preUpsert(byte[] rowKey, List<KeyValue> keyValues) {
            return keyValues;
        }
    }
}
