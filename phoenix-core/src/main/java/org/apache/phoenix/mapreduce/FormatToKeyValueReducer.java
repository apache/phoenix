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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.util.KeyValueBuilder;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.bulkload.TableRowkeyPair;
import org.apache.phoenix.mapreduce.bulkload.TargetTableRefFunctions;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.EncodedColumnsUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reducer class for the bulkload jobs.
 * Performs similar functionality to {@link KeyValueSortReducer}
 */
public class FormatToKeyValueReducer
        extends Reducer<TableRowkeyPair, ImmutableBytesWritable, TableRowkeyPair, KeyValue> {

    protected static final Logger LOG = LoggerFactory.getLogger(FormatToKeyValueReducer.class);


    protected List<String> tableNames;
    protected List<String> logicalNames;
    protected KeyValueBuilder builder;
    List<List<Pair<byte[], byte[]>>> columnIndexes;
    List<ImmutableBytesPtr> emptyFamilyName;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        // pass client configuration into driver
        Properties clientInfos = new Properties();
        for (Map.Entry<String, String> entry : conf) {
            clientInfos.setProperty(entry.getKey(), entry.getValue());
        }

        try {
            PhoenixConnection conn = (PhoenixConnection) QueryUtil.getConnection(clientInfos, conf);
            builder = conn.getKeyValueBuilder();
            final String tableNamesConf = conf.get(FormatToBytesWritableMapper.PHYSICAL_TABLE_NAMES_CONFKEY);
            final String logicalNamesConf = conf.get(FormatToBytesWritableMapper.LOGICAL_NAMES_CONFKEY);
            tableNames = TargetTableRefFunctions.NAMES_FROM_JSON.apply(tableNamesConf);
            logicalNames = TargetTableRefFunctions.NAMES_FROM_JSON.apply(logicalNamesConf);

            columnIndexes = new ArrayList<>(tableNames.size());
            emptyFamilyName = new ArrayList<>();
            initColumnsMap(conn);
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void initColumnsMap(PhoenixConnection conn) throws SQLException {
        for (String tableName : logicalNames) {
            PTable table = PhoenixRuntime.getTable(conn, tableName);
            emptyFamilyName.add(SchemaUtil.getEmptyColumnFamilyPtr(table));
            List<PColumn> cls = table.getColumns();
            List<Pair<byte[], byte[]>> list = new ArrayList<>(cls.size());
            for (int i = 0; i < cls.size(); i++) {
                PColumn c = cls.get(i);
                if (SchemaUtil.isPKColumn(c)) {
                    list.add(null); // Skip PK column
                    continue;
                }
                byte[] family = c.getFamilyName().getBytes();
                byte[] name = EncodedColumnsUtil.getColumnQualifier(c, table);
                list.add(new Pair<>(family, name));
            }
            columnIndexes.add(list);
        }

    }

    @Override
    protected void reduce(TableRowkeyPair key, Iterable<ImmutableBytesWritable> values,
                          Reducer<TableRowkeyPair, ImmutableBytesWritable, TableRowkeyPair, KeyValue>.Context context)
            throws IOException, InterruptedException {
        TreeSet<KeyValue> map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
        int tableIndex = tableNames.indexOf(key.getTableName());
        List<Pair<byte[], byte[]>> columns = columnIndexes.get(tableIndex);
        for (ImmutableBytesWritable aggregatedArray : values) {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(aggregatedArray.get()));
            while (input.available() != 0) {
                int index = WritableUtils.readVInt(input);
                Pair<byte[], byte[]> pair = columns.get(index);
                byte type = input.readByte();
                ImmutableBytesWritable value = null;
                int len = WritableUtils.readVInt(input);
                if (len > 0) {
                    byte[] array = new byte[len];
                    input.read(array);
                    value = new ImmutableBytesWritable(array);
                }
                KeyValue kv;
                KeyValue.Type kvType = KeyValue.Type.codeToType(type);
                switch (kvType) {
                    case Put: // not null value
                        kv = builder.buildPut(key.getRowkey(),
                                new ImmutableBytesWritable(pair.getFirst()),
                                new ImmutableBytesWritable(pair.getSecond()), value);
                        break;
                    case DeleteColumn: // null value
                        kv = builder.buildDeleteColumns(key.getRowkey(),
                                new ImmutableBytesWritable(pair.getFirst()),
                                new ImmutableBytesWritable(pair.getSecond()));
                        break;
                    default:
                        throw new IOException("Unsupported KeyValue type " + kvType);
                }
                map.add(kv);
            }
            //FIXME: samarth need to supply the right empty column qualifier here.
            KeyValue empty = builder.buildPut(key.getRowkey(),
                    emptyFamilyName.get(tableIndex),
                    QueryConstants.EMPTY_COLUMN_BYTES_PTR, ByteUtil.EMPTY_BYTE_ARRAY_PTR);
            map.add(empty);
            Closeables.closeQuietly(input);
        }
        context.setStatus("Read " + map.getClass());
        int index = 0;
        for (KeyValue kv : map) {
            context.write(key, kv);
            if (++index % 100 == 0) context.setStatus("Wrote " + index);
        }
    }
}
