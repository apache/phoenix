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
package org.apache.phoenix.mapreduce.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.phoenix.mapreduce.FormatToBytesWritableMapper;
import org.apache.phoenix.mapreduce.ImportPreUpsertKeyValueProcessor;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.OutputFormat;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.SourceTable;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

public class PhoenixToolsUtil {

    /** Configuration key for the class name of an ImportPreUpsertKeyValueProcessor */
    public static final String UPSERT_HOOK_CLASS_CONFKEY = "phoenix.mapreduce.import.kvprocessor";
    
    public static final String INDEX_VERIFY_TYPE = "phoenix.mr.index.IndexVerifyType";

    public static final String DISABLE_LOGGING_TYPE = "phoenix.mr.index" +
            ".IndexDisableLoggingType";

    public static final String SCRUTINY_SOURCE_TABLE = "phoenix.mr.scrutiny.source.table";

    public static final String INDEX_TOOL_SOURCE_TABLE = "phoenix.mr.index_tool.source.table";

    public static final String SCRUTINY_OUTPUT_FORMAT = "phoenix.mr.scrutiny.output.format";
    
    public static ImportPreUpsertKeyValueProcessor loadPreUpsertProcessor(Configuration conf) {
        Class<? extends ImportPreUpsertKeyValueProcessor> processorClass = null;
        try {
            processorClass = conf.getClass(
                    UPSERT_HOOK_CLASS_CONFKEY, FormatToBytesWritableMapper.DefaultImportPreUpsertKeyValueProcessor.class,
                    ImportPreUpsertKeyValueProcessor.class);
        } catch (Exception e) {
            throw new IllegalStateException("Couldn't load upsert hook class", e);
        }
    
        return ReflectionUtils.newInstance(processorClass, conf);
    }

    public static void setIndexVerifyType(Configuration configuration, IndexTool.IndexVerifyType verifyType) {
        Preconditions.checkNotNull(configuration);
        configuration.set(INDEX_VERIFY_TYPE, verifyType.getValue());
    }

    public static void setDisableLoggingVerifyType(Configuration configuration,
                                                   IndexTool.IndexDisableLoggingType disableLoggingType) {
        Preconditions.checkNotNull(configuration);
        configuration.set(DISABLE_LOGGING_TYPE, disableLoggingType.getValue());
    }
    

    public static SourceTable getScrutinySourceTable(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return SourceTable.valueOf(configuration.get(SCRUTINY_SOURCE_TABLE));
    }
    

    public static void setIndexToolSourceTable(Configuration configuration,
            IndexScrutinyTool.SourceTable sourceTable) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(sourceTable);
        configuration.set(INDEX_TOOL_SOURCE_TABLE, sourceTable.name());
    }

    public static IndexScrutinyTool.SourceTable getIndexToolSourceTable(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return IndexScrutinyTool.SourceTable.valueOf(configuration.get(INDEX_TOOL_SOURCE_TABLE,
            IndexScrutinyTool.SourceTable.DATA_TABLE_SOURCE.name()));
    }

    public static void setScrutinySourceTable(Configuration configuration,
            SourceTable sourceTable) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(sourceTable);
        configuration.set(SCRUTINY_SOURCE_TABLE, sourceTable.name());
    }
    

    public static OutputFormat getScrutinyOutputFormat(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        return OutputFormat
                .valueOf(configuration.get(SCRUTINY_OUTPUT_FORMAT, OutputFormat.FILE.name()));
    }

    public static void setScrutinyOutputFormat(Configuration configuration,
            OutputFormat outputFormat) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(outputFormat);
        configuration.set(SCRUTINY_OUTPUT_FORMAT, outputFormat.name());
    }

    public static IndexTool.IndexVerifyType getIndexVerifyType(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        String value = configuration.get(INDEX_VERIFY_TYPE, IndexTool.IndexVerifyType.NONE.getValue());
        return IndexTool.IndexVerifyType.fromValue(value);
    }

    public static IndexTool.IndexVerifyType getDisableLoggingVerifyType(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        String value = configuration.get(DISABLE_LOGGING_TYPE, IndexTool.IndexVerifyType.NONE.getValue());
        return IndexTool.IndexVerifyType.fromValue(value);
    }
}
