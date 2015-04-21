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
package org.apache.phoenix.util.csv;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.csv.CSVRecord;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.QueryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Executes upsert statements on a provided {@code PreparedStatement} based on incoming CSV records, notifying a
 * listener each time the prepared statement is executed.
 */
public class CsvUpsertExecutor implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(CsvUpsertExecutor.class);

    private final String arrayElementSeparator;
    private final Connection conn;
    private final List<PDataType> dataTypes;
    private final List<Function<String,Object>> conversionFunctions;
    private final PreparedStatement preparedStatement;
    private final UpsertListener upsertListener;
    private long upsertCount = 0L;

    /**
     * A listener that is called for events based on incoming CSV data.
     */
    public static interface UpsertListener {

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
         * @param csvRecord the CSV record that was being upserted when the error occurred
         */
        void errorOnRecord(CSVRecord csvRecord, String errorMessage);
    }


    /**
     * Static constructor method for creating a CsvUpsertExecutor.
     *
     * @param conn Phoenix connection upon which upserts are to be performed
     * @param tableName name of the table in which upserts are to be performed
     * @param columnInfoList description of the columns to be upserted to, in the same order as in the CSV input
     * @param upsertListener listener that will be notified of upserts, can be null
     * @param arrayElementSeparator separator string to delimit string representations of arrays
     * @return the created CsvUpsertExecutor
     */
    public static CsvUpsertExecutor create(PhoenixConnection conn, String tableName, List<ColumnInfo> columnInfoList,
            UpsertListener upsertListener, String arrayElementSeparator) {
        PreparedStatement preparedStatement = null;
        try {
            String upsertSql = QueryUtil.constructUpsertStatement(tableName, columnInfoList);
            LOG.info("Upserting SQL data with {}", upsertSql);
            preparedStatement = conn.prepareStatement(upsertSql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new CsvUpsertExecutor(conn, columnInfoList, preparedStatement, upsertListener,
                arrayElementSeparator);
    }

    /**
     * Construct with the definition of incoming columns, and the statement upon which upsert statements
     * are to be performed.
     */
    CsvUpsertExecutor(Connection conn, List<ColumnInfo> columnInfoList, PreparedStatement preparedStatement,
            UpsertListener upsertListener, String arrayElementSeparator) {
        this.conn = conn;
        this.preparedStatement = preparedStatement;
        this.upsertListener = upsertListener;
        this.arrayElementSeparator = arrayElementSeparator;
        this.dataTypes = Lists.newArrayList();
        this.conversionFunctions = Lists.newArrayList();
        for (ColumnInfo columnInfo : columnInfoList) {
            PDataType dataType = PDataType.fromTypeId(columnInfo.getSqlType());
            dataTypes.add(dataType);
            conversionFunctions.add(createConversionFunction(dataType));
        }
    }

    /**
     * Execute upserts for each CSV record contained in the given iterable, notifying this instance's
     * {@code UpsertListener} for each completed upsert.
     *
     * @param csvRecords iterable of CSV records to be upserted
     */
    public void execute(Iterable<CSVRecord> csvRecords) {
        for (CSVRecord csvRecord : csvRecords) {
            execute(csvRecord);
        }
    }

    /**
     * Upsert a single record.
     *
     * @param csvRecord CSV record containing the data to be upserted
     */
    void execute(CSVRecord csvRecord) {
        try {
            if (csvRecord.size() < conversionFunctions.size()) {
                String message = String.format("CSV record does not have enough values (has %d, but needs %d)",
                        csvRecord.size(), conversionFunctions.size());
                throw new IllegalArgumentException(message);
            }
            for (int fieldIndex = 0; fieldIndex < conversionFunctions.size(); fieldIndex++) {
                Object sqlValue = conversionFunctions.get(fieldIndex).apply(csvRecord.get(fieldIndex));
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
                LOG.debug("Error on CSVRecord " + csvRecord, e);
            }
            upsertListener.errorOnRecord(csvRecord, e.getMessage());
        }
    }

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

    private Function<String, Object> createConversionFunction(PDataType dataType) {
        if (dataType.isArrayType()) {
            return new ArrayDatatypeConversionFunction(
                    new StringToArrayConverter(
                            conn,
                            arrayElementSeparator,
                            PDataType.fromTypeId(dataType.getSqlType() - PDataType.ARRAY_TYPE_BASE)));
        } else {
            return new SimpleDatatypeConversionFunction(dataType, this.conn);
        }
    }

    /**
     * Performs typed conversion from String values to a given column value type.
     */
    static class SimpleDatatypeConversionFunction implements Function<String, Object> {

        private final PDataType dataType;
        private final DateUtil.DateTimeParser dateTimeParser;

        SimpleDatatypeConversionFunction(PDataType dataType, Connection conn) {
            Properties props = null;
            try {
                props = conn.getClientInfo();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            this.dataType = dataType;
            if(dataType.isCoercibleTo(PTimestamp.INSTANCE)) {
                // TODO: move to DateUtil
                String dateFormat;
                int dateSqlType = dataType.getResultSetSqlType();
                if (dateSqlType == Types.DATE) {
                    dateFormat = props.getProperty(QueryServices.DATE_FORMAT_ATTRIB,
                            DateUtil.DEFAULT_DATE_FORMAT);
                } else if (dateSqlType == Types.TIME) {
                    dateFormat = props.getProperty(QueryServices.TIME_FORMAT_ATTRIB,
                            DateUtil.DEFAULT_TIME_FORMAT);
                } else {
                    dateFormat = props.getProperty(QueryServices.TIMESTAMP_FORMAT_ATTRIB,
                            DateUtil.DEFAULT_TIMESTAMP_FORMAT);                    
                }
                String timeZoneId = props.getProperty(QueryServices.DATE_FORMAT_TIMEZONE_ATTRIB,
                        QueryServicesOptions.DEFAULT_DATE_FORMAT_TIMEZONE);
                this.dateTimeParser = DateUtil.getDateTimeParser(dateFormat, dataType, timeZoneId);
            } else {
                this.dateTimeParser = null;
            }
        }

        @Nullable
        @Override
        public Object apply(@Nullable String input) {
            if(dateTimeParser != null) {
                long epochTime = dateTimeParser.parseDateTime(input);
                byte[] byteValue = new byte[dataType.getByteSize()];
                dataType.getCodec().encodeLong(epochTime, byteValue, 0);
                return dataType.toObject(byteValue);
            }
            return dataType.toObject(input);
        }
    }

    /**
     * Converts string representations of arrays into Phoenix arrays of the correct type.
     */
    private static class ArrayDatatypeConversionFunction implements Function<String, Object> {

        private final StringToArrayConverter arrayConverter;

        private ArrayDatatypeConversionFunction(StringToArrayConverter arrayConverter) {
            this.arrayConverter = arrayConverter;
        }

        @Nullable
        @Override
        public Object apply(@Nullable String input) {
            try {
                return arrayConverter.toArray(input);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
