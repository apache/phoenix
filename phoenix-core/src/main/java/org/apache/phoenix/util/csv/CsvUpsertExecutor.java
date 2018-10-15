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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.function.EncodeFormat;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PBinary;
import org.apache.phoenix.schema.types.PBoolean;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDataType.PDataCodec;
import org.apache.phoenix.schema.types.PTimestamp;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.UpsertExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;

/** {@link UpsertExecutor} over {@link CSVRecord}s. */
public class CsvUpsertExecutor extends UpsertExecutor<CSVRecord, String> {

    private static final Logger LOG = LoggerFactory.getLogger(CsvUpsertExecutor.class);

    protected final String arrayElementSeparator;

    /** Testing constructor. Do not use in prod. */
    @VisibleForTesting
    protected CsvUpsertExecutor(Connection conn, List<ColumnInfo> columnInfoList,
            PreparedStatement stmt, UpsertListener<CSVRecord> upsertListener,
            String arrayElementSeparator) {
        super(conn, columnInfoList, stmt, upsertListener);
        this.arrayElementSeparator = arrayElementSeparator;
        finishInit();
    }

    public CsvUpsertExecutor(Connection conn, String tableName,
            List<ColumnInfo> columnInfoList, UpsertListener<CSVRecord> upsertListener,
            String arrayElementSeparator) {
        super(conn, tableName, columnInfoList, upsertListener);
        this.arrayElementSeparator = arrayElementSeparator;
        finishInit();
    }

    @Override
    protected void execute(CSVRecord csvRecord) {
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
            upsertListener.errorOnRecord(csvRecord, e);
        }
    }

    @Override
    protected Function<String, Object> createConversionFunction(PDataType dataType) {
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
        private final PDataCodec codec;
        private final DateUtil.DateTimeParser dateTimeParser;
        private final String binaryEncoding;

        SimpleDatatypeConversionFunction(PDataType dataType, Connection conn) {
            Properties props;
            try {
                props = conn.getClientInfo();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            this.dataType = dataType;
            PDataCodec codec = dataType.getCodec();
            if(dataType.isCoercibleTo(PTimestamp.INSTANCE)) {
                codec = DateUtil.getCodecFor(dataType);
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
            this.codec = codec;
            this.binaryEncoding = props.getProperty(QueryServices.UPLOAD_BINARY_DATA_TYPE_ENCODING,
                            QueryServicesOptions.DEFAULT_UPLOAD_BINARY_DATA_TYPE_ENCODING);
        }

        @Nullable
        @Override
        public Object apply(@Nullable String input) {
            if (input == null || input.isEmpty()) {
                return null;
            }
            if (dateTimeParser != null) {
                long epochTime = dateTimeParser.parseDateTime(input);
                byte[] byteValue = new byte[dataType.getByteSize()];
                codec.encodeLong(epochTime, byteValue, 0);
                return dataType.toObject(byteValue);
            } else if (dataType == PBoolean.INSTANCE) {
                switch (input.toLowerCase()) {
                    case "true":
                    case "t":
                    case "1":
                        return Boolean.TRUE;
                    case "false":
                    case "f":
                    case "0":
                        return Boolean.FALSE;
                    default:
                        throw new RuntimeException("Invalid boolean value: '" + input
                                + "', must be one of ['true','t','1','false','f','0']");
                }
            }else if (dataType == PVarbinary.INSTANCE || dataType == PBinary.INSTANCE){
                EncodeFormat format = EncodeFormat.valueOf(binaryEncoding.toUpperCase());
                Object object = null;
                switch (format) {
                    case BASE64:
                        object = Base64.decode(input);
                        if (object == null) { throw new IllegalDataException(
                                "Input: [" + input + "]  is not base64 encoded"); }
                        break;
                    case ASCII:
                        object = Bytes.toBytes(input);
                        break;
                    default:
                        throw new IllegalDataException("Unsupported encoding \"" + binaryEncoding + "\"");
                }
                return object;
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
