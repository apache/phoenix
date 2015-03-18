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

import java.io.IOException;
import java.io.StringReader;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.util.CSVCommonsLoader;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.csv.CsvUpsertExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * MapReduce mapper that converts CSV input lines into KeyValues that can be written to HFiles.
 * <p/>
 * KeyValues are produced by executing UPSERT statements on a Phoenix connection and then
 * extracting the created KeyValues and rolling back the statement execution before it is
 * committed to HBase.
 */
public class CsvToKeyValueMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,
        KeyValue> {

    private static final Logger LOG = LoggerFactory.getLogger(CsvToKeyValueMapper.class);

    private static final String COUNTER_GROUP_NAME = "Phoenix MapReduce Import";

    /** Configuration key for the field delimiter for input csv records */
    public static final String FIELD_DELIMITER_CONFKEY = "phoenix.mapreduce.import.fielddelimiter";

    /** Configuration key for the quote char for input csv records */
    public static final String QUOTE_CHAR_CONFKEY = "phoenix.mapreduce.import.quotechar";

    /** Configuration key for the escape char for input csv records */
    public static final String ESCAPE_CHAR_CONFKEY = "phoenix.mapreduce.import.escapechar";

    /** Configuration key for the array element delimiter for input arrays */
    public static final String ARRAY_DELIMITER_CONFKEY = "phoenix.mapreduce.import.arraydelimiter";

    /** Configuration key for the name of the output table */
    public static final String TABLE_NAME_CONFKEY = "phoenix.mapreduce.import.tablename";
    
    /** Configuration key for the name of the output index table */
    public static final String INDEX_TABLE_NAME_CONFKEY = "phoenix.mapreduce.import.indextablename";

    /** Configuration key for the columns to be imported */
    public static final String COLUMN_INFO_CONFKEY = "phoenix.mapreduce.import.columninfos";

    /** Configuration key for the flag to ignore invalid rows */
    public static final String IGNORE_INVALID_ROW_CONFKEY = "phoenix.mapreduce.import.ignoreinvalidrow";

    private PhoenixConnection conn;
    private CsvUpsertExecutor csvUpsertExecutor;
    private MapperUpsertListener upsertListener;
    private CsvLineParser csvLineParser;
    private ImportPreUpsertKeyValueProcessor preUpdateProcessor;
    private byte[] tableName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String jdbcUrl = getJdbcUrl(conf);

        // pass client configuration into driver
        Properties clientInfos = new Properties();
        Iterator<Entry<String, String>> iterator = conf.iterator();
        while(iterator.hasNext()) {
            Entry<String,String> entry = iterator.next();
            clientInfos.setProperty(entry.getKey(), entry.getValue());
        }
        
        // This statement also ensures that the driver class is loaded
        LOG.info("Connection with driver {} with url {}", PhoenixDriver.class.getName(), jdbcUrl);

        try {
            conn = (PhoenixConnection) DriverManager.getConnection(jdbcUrl, clientInfos);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        upsertListener = new MapperUpsertListener(
                context, conf.getBoolean(IGNORE_INVALID_ROW_CONFKEY, true));
        csvUpsertExecutor = buildUpsertExecutor(conf);
        csvLineParser = new CsvLineParser(conf.get(FIELD_DELIMITER_CONFKEY).charAt(0), conf.get(QUOTE_CHAR_CONFKEY).charAt(0),
                conf.get(ESCAPE_CHAR_CONFKEY).charAt(0));

        preUpdateProcessor = PhoenixConfigurationUtil.loadPreUpsertProcessor(conf);
        if(!conf.get(CsvToKeyValueMapper.INDEX_TABLE_NAME_CONFKEY, "").isEmpty()){
        	tableName = Bytes.toBytes(conf.get(CsvToKeyValueMapper.INDEX_TABLE_NAME_CONFKEY));
        } else {
        	tableName = Bytes.toBytes(conf.get(CsvToKeyValueMapper.TABLE_NAME_CONFKEY, ""));
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ImmutableBytesWritable outputKey = new ImmutableBytesWritable();
        try {
            CSVRecord csvRecord = null;
            try {
                csvRecord = csvLineParser.parse(value.toString());
            } catch (IOException e) {
                context.getCounter(COUNTER_GROUP_NAME, "CSV Parser errors").increment(1L);
            }

            if (csvRecord == null) {
                context.getCounter(COUNTER_GROUP_NAME, "Empty records").increment(1L);
                return;
            }
            csvUpsertExecutor.execute(ImmutableList.of(csvRecord));

            Iterator<Pair<byte[], List<KeyValue>>> uncommittedDataIterator
                    = PhoenixRuntime.getUncommittedDataIterator(conn, true);
            while (uncommittedDataIterator.hasNext()) {
                Pair<byte[], List<KeyValue>> kvPair = uncommittedDataIterator.next();
                if (Bytes.compareTo(tableName, kvPair.getFirst()) != 0) {
                	// skip edits for other tables
                	continue;
                }
                List<KeyValue> keyValueList = kvPair.getSecond();
                keyValueList = preUpdateProcessor.preUpsert(kvPair.getFirst(), keyValueList);
                for (KeyValue kv : keyValueList) {
                    outputKey.set(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength());
                    context.write(outputKey, kv);
                }
            }
            conn.rollback();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        try {
            conn.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Build up the JDBC URL for connecting to Phoenix.
     *
     * @return the full JDBC URL for a Phoenix connection
     */
    @VisibleForTesting
    static String getJdbcUrl(Configuration conf) {
        String zkQuorum = conf.get(HConstants.ZOOKEEPER_QUORUM);
        if (zkQuorum == null) {
            throw new IllegalStateException(HConstants.ZOOKEEPER_QUORUM + " is not configured");
        }
        return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
    }

    @VisibleForTesting
    CsvUpsertExecutor buildUpsertExecutor(Configuration conf) {
        String tableName = conf.get(TABLE_NAME_CONFKEY);
        String arraySeparator = conf.get(ARRAY_DELIMITER_CONFKEY,
                                            CSVCommonsLoader.DEFAULT_ARRAY_ELEMENT_SEPARATOR);
        Preconditions.checkNotNull(tableName, "table name is not configured");

        List<ColumnInfo> columnInfoList = buildColumnInfoList(conf);

        return CsvUpsertExecutor.create(conn, tableName, columnInfoList, upsertListener, arraySeparator);
    }

    /**
     * Write the list of to-import columns to a job configuration.
     *
     * @param conf configuration to be written to
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
                                if (input.isEmpty()) {
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
    static class MapperUpsertListener implements CsvUpsertExecutor.UpsertListener {

        private final Context context;
        private final boolean ignoreRecordErrors;

        private MapperUpsertListener(Context context, boolean ignoreRecordErrors) {
            this.context = context;
            this.ignoreRecordErrors = ignoreRecordErrors;
        }

        @Override
        public void upsertDone(long upsertCount) {
            context.getCounter(COUNTER_GROUP_NAME, "Upserts Done").increment(1L);
        }

        @Override
        public void errorOnRecord(CSVRecord csvRecord, String errorMessage) {
            LOG.error("Error on record {}: {}", csvRecord, errorMessage);
            context.getCounter(COUNTER_GROUP_NAME, "Errors on records").increment(1L);
            if (!ignoreRecordErrors) {
                throw new RuntimeException("Error on record, " + errorMessage + ", " +
                        "record =" + csvRecord);
            }
        }
    }

    /**
     * Parses a single CSV input line, returning a {@code CSVRecord}.
     */
    @VisibleForTesting
    static class CsvLineParser {
        private final CSVFormat csvFormat;

        CsvLineParser(char fieldDelimiter, char quote, char escape) {
            this.csvFormat = CSVFormat.DEFAULT
                    .withIgnoreEmptyLines(true)
                    .withDelimiter(fieldDelimiter)
                    .withEscape(escape)
                    .withQuote(quote);
        }

        public CSVRecord parse(String input) throws IOException {
            // TODO Creating a new parser for each line seems terribly inefficient but
            // there's no public way to parse single lines via commons-csv. We should update
            // it to create a LineParser class like this one.
            CSVParser csvParser = new CSVParser(new StringReader(input), csvFormat);
            return Iterables.getFirst(csvParser, null);
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
