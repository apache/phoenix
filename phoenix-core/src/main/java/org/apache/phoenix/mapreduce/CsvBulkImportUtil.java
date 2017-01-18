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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;

import com.google.common.annotations.VisibleForTesting;

/**
 * Collection of utility methods for setting up bulk import jobs.
 */
public class CsvBulkImportUtil {

    /**
     * Configure a job configuration for a bulk CSV import.
     *
     * @param conf job configuration to be set up
     * @param fieldDelimiter field delimiter character for the CSV input
     * @param quoteChar quote character for the CSV input
     * @param escapeChar escape character for the CSV input
     * @param arrayDelimiter array delimiter character, can be null
     * @param binaryEncoding 
     */
    public static void initCsvImportJob(Configuration conf, char fieldDelimiter, char quoteChar,
            char escapeChar, String arrayDelimiter, String binaryEncoding) {
        setChar(conf, CsvToKeyValueMapper.FIELD_DELIMITER_CONFKEY, fieldDelimiter);
        setChar(conf, CsvToKeyValueMapper.QUOTE_CHAR_CONFKEY, quoteChar);
        setChar(conf, CsvToKeyValueMapper.ESCAPE_CHAR_CONFKEY, escapeChar);
        if (arrayDelimiter != null) {
            conf.set(CsvToKeyValueMapper.ARRAY_DELIMITER_CONFKEY, arrayDelimiter);
        }
        if(binaryEncoding!=null){
            conf.set(QueryServices.UPLOAD_BINARY_DATA_TYPE_ENCODING, binaryEncoding);
        }
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

    @VisibleForTesting
    static void setChar(Configuration conf, String confKey, char charValue) {
        conf.set(confKey, Base64.encodeBytes(Character.toString(charValue).getBytes()));
    }

    @VisibleForTesting
    static Character getCharacter(Configuration conf, String confKey) {
        String strValue = conf.get(confKey);
        if (strValue == null) {
            return null;
        }
        return new String(Base64.decode(strValue)).charAt(0);
    }

    public static Path getOutputPath(Path outputdir, String tableName) {
        return new Path(outputdir,
                tableName.replace(QueryConstants.NAMESPACE_SEPARATOR, QueryConstants.NAME_SEPARATOR));
    }
}
