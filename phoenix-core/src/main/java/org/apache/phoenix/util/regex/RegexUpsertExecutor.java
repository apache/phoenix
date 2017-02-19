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
package org.apache.phoenix.util.regex;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.UpsertExecutor;
import org.apache.phoenix.util.json.JsonUpsertExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/** {@link UpsertExecutor} over {@link Map} objects, convert input record into {@link Map} objects by using regex. */
public class RegexUpsertExecutor extends JsonUpsertExecutor {

    protected static final Logger LOG = LoggerFactory.getLogger(RegexUpsertExecutor.class);

    /** Testing constructor. Do not use in prod. */
    @VisibleForTesting
    protected RegexUpsertExecutor(Connection conn, List<ColumnInfo> columnInfoList,
            PreparedStatement stmt, UpsertListener<Map<?, ?>> upsertListener) {
        super(conn, columnInfoList, stmt, upsertListener);
    }

    public RegexUpsertExecutor(Connection conn, String tableName, List<ColumnInfo> columnInfoList,
            UpsertExecutor.UpsertListener<Map<?, ?>> upsertListener) {
        super(conn, tableName, columnInfoList, upsertListener);
    }

    @Override
    protected void execute(Map<?, ?> record) {
        int fieldIndex = 0;
        String colName = null;
        try {
            if (record.size() < conversionFunctions.size()) {
                String message = String.format("Input record does not have enough values based on regex (has %d, but needs %d)",
                        record.size(), conversionFunctions.size());
                throw new IllegalArgumentException(message);
            }
            for (fieldIndex = 0; fieldIndex < conversionFunctions.size(); fieldIndex++) {
                colName = columnInfos.get(fieldIndex).getColumnName();
                Object sqlValue = conversionFunctions.get(fieldIndex).apply(record.get(colName));
                if (sqlValue != null) {
                    preparedStatement.setObject(fieldIndex + 1, sqlValue);
                } else {
                    preparedStatement.setNull(fieldIndex + 1, dataTypes.get(fieldIndex).getSqlType());
                }
            }
            preparedStatement.execute();
            upsertListener.upsertDone(++upsertCount);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                // Even though this is an error we only log it with debug logging because we're notifying the
                // listener, and it can do its own logging if needed
                LOG.debug("Error on record " + record + ", fieldIndex " + fieldIndex + ", colName " + colName, e);
            }
            upsertListener.errorOnRecord(record, new Exception("fieldIndex: " + fieldIndex + ", colName " + colName, e));
        }
    }
}