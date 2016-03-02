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
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.util.CSVCommonsLoader;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.UpsertExecutor;
import org.apache.phoenix.util.csv.CsvUpsertExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/**
 * MapReduce mapper that converts CSV input lines into KeyValues that can be written to HFiles.
 * <p/>
 * KeyValues are produced by executing UPSERT statements on a Phoenix connection and then
 * extracting the created KeyValues and rolling back the statement execution before it is
 * committed to HBase.
 */
public class CsvToKeyValueMapper extends FormatToBytesWritableMapper<CSVRecord> {

    /** Configuration key for the field delimiter for input csv records */
    public static final String FIELD_DELIMITER_CONFKEY = "phoenix.mapreduce.import.fielddelimiter";

    /** Configuration key for the quote char for input csv records */
    public static final String QUOTE_CHAR_CONFKEY = "phoenix.mapreduce.import.quotechar";

    /** Configuration key for the escape char for input csv records */
    public static final String ESCAPE_CHAR_CONFKEY = "phoenix.mapreduce.import.escapechar";

    /** Configuration key for the array element delimiter for input arrays */
    public static final String ARRAY_DELIMITER_CONFKEY = "phoenix.mapreduce.import.arraydelimiter";

    private CsvLineParser lineParser;

    @Override
    protected LineParser<CSVRecord> getLineParser() {
        return lineParser;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        lineParser = new CsvLineParser(
                CsvBulkImportUtil.getCharacter(conf, FIELD_DELIMITER_CONFKEY),
                CsvBulkImportUtil.getCharacter(conf, QUOTE_CHAR_CONFKEY),
                CsvBulkImportUtil.getCharacter(conf, ESCAPE_CHAR_CONFKEY));
    }

    @VisibleForTesting
    @Override
    protected UpsertExecutor<CSVRecord, ?> buildUpsertExecutor(Configuration conf) {
        String tableName = conf.get(TABLE_NAME_CONFKEY);
        String arraySeparator = conf.get(ARRAY_DELIMITER_CONFKEY,
                                            CSVCommonsLoader.DEFAULT_ARRAY_ELEMENT_SEPARATOR);
        Preconditions.checkNotNull(tableName, "table name is not configured");

        List<ColumnInfo> columnInfoList = buildColumnInfoList(conf);

        return new CsvUpsertExecutor(conn, tableName, columnInfoList, upsertListener, arraySeparator);
    }

    /**
     * Parses a single CSV input line, returning a {@code CSVRecord}.
     */
    @VisibleForTesting
    static class CsvLineParser implements LineParser<CSVRecord> {
        private final CSVFormat csvFormat;

        CsvLineParser(char fieldDelimiter, char quote, char escape) {
            this.csvFormat = CSVFormat.DEFAULT
                    .withIgnoreEmptyLines(true)
                    .withDelimiter(fieldDelimiter)
                    .withEscape(escape)
                    .withQuote(quote);
        }

        @Override
        public CSVRecord parse(String input) throws IOException {
            // TODO Creating a new parser for each line seems terribly inefficient but
            // there's no public way to parse single lines via commons-csv. We should update
            // it to create a LineParser class like this one.
            CSVParser csvParser = new CSVParser(new StringReader(input), csvFormat);
            return Iterables.getFirst(csvParser, null);
        }
    }
}
