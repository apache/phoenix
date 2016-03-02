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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.UpsertExecutor;
import org.apache.phoenix.util.json.JsonUpsertExecutor;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * MapReduce mapper that converts JSON input lines into KeyValues that can be written to HFiles.
 * <p/>
 * KeyValues are produced by executing UPSERT statements on a Phoenix connection and then
 * extracting the created KeyValues and rolling back the statement execution before it is
 * committed to HBase.
 */
public class JsonToKeyValueMapper extends FormatToBytesWritableMapper<Map<?, ?>> {

    private LineParser<Map<?, ?>> lineParser;

    @Override
    protected  LineParser<Map<?, ?>> getLineParser() {
        return lineParser;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        lineParser = new JsonLineParser();
    }

    @VisibleForTesting
    @Override
    protected UpsertExecutor<Map<?, ?>, ?> buildUpsertExecutor(Configuration conf) {
        String tableName = conf.get(TABLE_NAME_CONFKEY);
        Preconditions.checkNotNull(tableName, "table name is not configured");

        List<ColumnInfo> columnInfoList = buildColumnInfoList(conf);

        return new JsonUpsertExecutor(conn, tableName, columnInfoList, upsertListener);
    }

    @VisibleForTesting
    static class JsonLineParser implements LineParser<Map<?, ?>> {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public Map<?, ?> parse(String input) throws IOException {
            return mapper.readValue(input, Map.class);
        }
    }
}
