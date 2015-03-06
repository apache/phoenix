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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.util.ColumnInfo;

import com.google.common.base.Preconditions;

/**
 * Collection of utility methods for setting up bulk import jobs.
 */
public class CsvBulkImportUtil {

    /**
     * Configure a job configuration for a bulk CSV import.
     *
     * @param conf job configuration to be set up
     * @param tableName name of the table to be imported to, can include a schema name
     * @param fieldDelimiter field delimiter character for the CSV input
     * @param quoteChar quote character for the CSV input
     * @param escapeChar escape character for the CSV input
     * @param arrayDelimiter array delimiter character, can be null
     * @param columnInfoList list of columns to be imported
     * @param ignoreInvalidRows flag to ignore invalid input rows
     */
    public static void initCsvImportJob(Configuration conf, String tableName, char fieldDelimiter, char quoteChar, char escapeChar,
            String arrayDelimiter,  List<ColumnInfo> columnInfoList, boolean ignoreInvalidRows) {

        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(columnInfoList);
        Preconditions.checkArgument(!columnInfoList.isEmpty(), "Column info list is empty");
        conf.set(CsvToKeyValueMapper.TABLE_NAME_CONFKEY, tableName);
        conf.set(CsvToKeyValueMapper.FIELD_DELIMITER_CONFKEY, String.valueOf(fieldDelimiter));
        conf.set(CsvToKeyValueMapper.QUOTE_CHAR_CONFKEY, String.valueOf(quoteChar));
        conf.set(CsvToKeyValueMapper.ESCAPE_CHAR_CONFKEY, String.valueOf(escapeChar));
        if (arrayDelimiter != null) {
            conf.set(CsvToKeyValueMapper.ARRAY_DELIMITER_CONFKEY, arrayDelimiter);
        }
        CsvToKeyValueMapper.configureColumnInfoList(conf, columnInfoList);
        conf.setBoolean(CsvToKeyValueMapper.IGNORE_INVALID_ROW_CONFKEY, ignoreInvalidRows);
    }

    /**
     * Configure an {@link ImportPreUpsertKeyValueProcessor} for a CSV bulk import job.
     *
     * @param conf job configuration
     * @param processorClass class to be used for performing pre-upsert processing
     */
    public static void configurePreUpsertProcessor(Configuration conf,
            Class<? extends ImportPreUpsertKeyValueProcessor> processorClass) {
        conf.setClass(PhoenixConfigurationUtil.UPSERT_HOOK_CLASS_CONFKEY, processorClass,
                ImportPreUpsertKeyValueProcessor.class);
    }
}
