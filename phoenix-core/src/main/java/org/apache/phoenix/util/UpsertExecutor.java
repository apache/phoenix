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
package org.apache.phoenix.util;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.schema.types.PDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Executes upsert statements on a provided {@code PreparedStatement} based on incoming
 * {@code RECORDS}. An {@link UpsertListener} is notified each time the prepared statement
 * is executed.
 */
public abstract class UpsertExecutor<RECORD, FIELD> implements Closeable {

    /**
     * A listener that is called for events based on incoming JSON data.
     */
    public interface UpsertListener<RECORD> {

        /**
         * Called when an upsert has been sucessfully completed. The given upsertCount is the total number of upserts
         * completed on the caller up to this point.
         *
         * @param upsertCount total number of upserts that have been completed
         */
        void upsertDone(long upsertCount);


        /**
         * Called when executing a prepared statement has failed on a given record.
         *
         * @param record the JSON record that was being upserted when the error occurred
         */
        void errorOnRecord(RECORD record, Throwable throwable);
    }

    private static final Logger LOG = LoggerFactory.getLogger(UpsertExecutor.class);

    protected final Connection conn;
    protected final List<ColumnInfo> columnInfos;
    protected final List<PDataType> dataTypes;
    protected final List<Function<FIELD, Object>> conversionFunctions;
    protected final PreparedStatement preparedStatement;
    protected final UpsertListener<RECORD> upsertListener;
    protected long upsertCount = 0L;
    protected boolean initFinished = false; // allow subclasses to finish initialization

    private static PreparedStatement createStatement(Connection conn, String tableName,
            List<ColumnInfo> columnInfoList) {
        PreparedStatement preparedStatement;
        try {
            String upsertSql = QueryUtil.constructUpsertStatement(tableName, columnInfoList);
            LOG.info("Upserting SQL data with {}", upsertSql);
            preparedStatement = conn.prepareStatement(upsertSql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return preparedStatement;
    }

    /**
     * Construct with the definition of incoming columns, and the statement upon which upsert
     * statements are to be performed.
     */
    public UpsertExecutor(Connection conn, String tableName,
            List<ColumnInfo> columnInfoList, UpsertListener<RECORD> upsertListener) {
        this(conn, columnInfoList, createStatement(conn, tableName, columnInfoList), upsertListener);
    }

    /** Testing constructor. Do not use in prod. */
    @VisibleForTesting
    protected UpsertExecutor(Connection conn, List<ColumnInfo> columnInfoList,
            PreparedStatement preparedStatement, UpsertListener<RECORD> upsertListener) {
        this.conn = conn;
        this.upsertListener = upsertListener;
        this.columnInfos = columnInfoList;
        this.preparedStatement = preparedStatement;
        this.dataTypes = Lists.newArrayList();
        this.conversionFunctions = Lists.newArrayList();
    }

    /**
     * Awkward protocol allows subclass constructors to finish initializing context before
     * proceeding to record processing.
     */
    protected void finishInit() {
        for (ColumnInfo columnInfo : columnInfos) {
            PDataType dataType = PDataType.fromTypeId(columnInfo.getSqlType());
            dataTypes.add(dataType);
            conversionFunctions.add(createConversionFunction(dataType));
        }
        this.initFinished = true;
    }

    /**
     * Execute upserts for each JSON record contained in the given iterable, notifying this instance's
     * {@code UpsertListener} for each completed upsert.
     *
     * @param records iterable of JSON records to be upserted
     */
    public void execute(Iterable<RECORD> records) {
        if (!initFinished) {
            finishInit();
        }
        for (RECORD record : records) {
            execute(record);
        }
    }

    /**
     * Upsert a single record.
     *
     * @param record JSON record containing the data to be upserted
     */
    protected abstract void execute(RECORD record);

    @Override
    public void close() throws IOException {
        try {
            preparedStatement.close();
        } catch (SQLException e) {
            // An exception while closing the prepared statement is most likely a sign of a real problem, so we don't
            // want to hide it with closeQuietly or something similar
            throw new RuntimeException(e);
        }
    }

    protected abstract Function<FIELD, Object> createConversionFunction(PDataType dataType);
}
