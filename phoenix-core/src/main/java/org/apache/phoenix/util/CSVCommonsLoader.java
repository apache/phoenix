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

import java.io.File;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.csv.CsvUpsertExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/***
 * Upserts CSV data using Phoenix JDBC connection
 */
public class CSVCommonsLoader {

    private static final Logger LOG = LoggerFactory.getLogger(CSVCommonsLoader.class);

    public static final String DEFAULT_ARRAY_ELEMENT_SEPARATOR = ":";

    private final PhoenixConnection conn;
    private final String tableName;
    private final List<String> columns;
    private final boolean isStrict;
    boolean userSuppliedMetaCharacters = false;
    private final List<String> customMetaCharacters;
    private PhoenixHeaderSource headerSource = PhoenixHeaderSource.FROM_TABLE;
    private final CSVFormat format;
    private final Map<String,Character> ctrlTable = new HashMap<String,Character>() {
        {   put("1",'\u0001');
            put("2",'\u0002');
            put("3",'\u0003');
            put("4",'\u0004');
            put("5",'\u0005');
            put("6",'\u0006');
            put("7",'\u0007');
            put("8",'\u0008');
            put("9",'\u0009');}};

    private final String arrayElementSeparator;

    public enum PhoenixHeaderSource {
        FROM_TABLE,
        IN_LINE,
        SUPPLIED_BY_USER
    }

    public CSVCommonsLoader(PhoenixConnection conn, String tableName,
            List<String> columns, boolean isStrict) {
        this(conn, tableName, columns, isStrict, null, DEFAULT_ARRAY_ELEMENT_SEPARATOR);
    }

    public CSVCommonsLoader(PhoenixConnection conn, String tableName,
            List<String> columns, boolean isStrict, List<String> customMetaCharacters, String arrayElementSeparator) {
        this.conn = conn;
        this.tableName = tableName;
        this.columns = columns;
        this.isStrict = isStrict;
        this.customMetaCharacters = customMetaCharacters;
        if (customMetaCharacters==null || customMetaCharacters.size()==0) {
            userSuppliedMetaCharacters=false;
        } else if (customMetaCharacters.size()==3) {
            userSuppliedMetaCharacters=true;
        }
        else{
            throw new IllegalArgumentException(
                    String.format("customMetaCharacters must have no elements or three elements. Supplied value is %s",
                            buildStringFromList(customMetaCharacters)));
        }

        // implicit in the columns value.
        if (columns !=null && !columns.isEmpty()) {
            headerSource = PhoenixHeaderSource.SUPPLIED_BY_USER;
        }
        else if (columns != null && columns.isEmpty()) {
            headerSource = PhoenixHeaderSource.IN_LINE;
        }

        this.arrayElementSeparator = arrayElementSeparator;
        this.format = buildFormat();
    }

    public CSVFormat getFormat() {
        return format;
    }

    /**
     * default settings
     * delimiter = ',' 
     * quoteChar = '"',
     * escape = null
     * recordSeparator = CRLF, CR, or LF
     * ignore empty lines allows the last data line to have a recordSeparator
     *
     * @return CSVFormat based on constructor settings.
     */
    private CSVFormat buildFormat() {
        CSVFormat format = CSVFormat.DEFAULT
                .withIgnoreEmptyLines(true);
        if (userSuppliedMetaCharacters) {
            // list error checking handled in constructor above.
            // use 0 to keep default setting
            String delimiter = customMetaCharacters.get(0);
            String quote = customMetaCharacters.get(1);
            String escape = customMetaCharacters.get(2);

            if (!"0".equals(delimiter)) {
                format = format.withDelimiter(getCustomMetaCharacter(delimiter));
            }
            if (!"0".equals(quote)) {
                format = format.withQuoteChar(getCustomMetaCharacter(quote));
            }
            if (!"0".equals(quote)) {
                format = format.withEscape(getCustomMetaCharacter(escape));
            }

        }
        switch(headerSource) {
        case FROM_TABLE:
            // obtain headers from table, so format should not expect a header.
            break;
        case IN_LINE:
            // an empty string array triggers csv loader to grab the first line as the header
            format = format.withHeader(new String[0]);
            break;
        case SUPPLIED_BY_USER:
            // a populated string array supplied by the user
            format = format.withHeader(columns.toArray(new String[columns.size()]));
            break;
        default:
            throw new RuntimeException("Header source was unable to be inferred.");

        }
        return format;
    }


    public char getCustomMetaCharacter(String field) {
        if(this.ctrlTable.containsKey(field)) {
            return this.ctrlTable.get(field);
        } else {
            return field.charAt(0);
        }
    }

    /**
     * Upserts data from CSV file.
     *
     * Data is batched up based on connection batch size.
     * Column PDataType is read from metadata and is used to convert
     * column value to correct type before upsert.
     *
     * The constructor determines the format for the CSV files.
     *
     * @param fileName
     * @throws Exception
     */
    public void upsert(String fileName) throws Exception {
        CSVParser parser = CSVParser.parse(new File(fileName),
                format);
        upsert(parser);
    }

    public void upsert(Reader reader) throws Exception {
        CSVParser parser = new CSVParser(reader,format);
        upsert(parser);
    }

    private static <T> String buildStringFromList(List<T> list) {
        return Joiner.on(", ").useForNull("null").join(list);
    }

    /**
     * Data is batched up based on connection batch size.
     * Column PDataType is read from metadata and is used to convert
     * column value to correct type before upsert.
     *
     * The format is determined by the supplied csvParser.

     * @param csvParser
     *            CSVParser instance
     * @throws Exception
     */
    public void upsert(CSVParser csvParser) throws Exception {
        List<ColumnInfo> columnInfoList = buildColumnInfoList(csvParser);

        boolean wasAutoCommit = conn.getAutoCommit();
        try {
            conn.setAutoCommit(false);
            long start = System.currentTimeMillis();
            CsvUpsertListener upsertListener = new CsvUpsertListener(conn, conn.getMutateBatchSize());
            CsvUpsertExecutor csvUpsertExecutor = CsvUpsertExecutor.create(conn, tableName,
                    columnInfoList, upsertListener, arrayElementSeparator);

            csvUpsertExecutor.execute(csvParser);
            csvUpsertExecutor.close();

            conn.commit();
            double elapsedDuration = ((System.currentTimeMillis() - start) / 1000.0);
            System.out.println("CSV Upsert complete. " + upsertListener.getTotalUpsertCount()
                    + " rows upserted");
            System.out.println("Time: " + elapsedDuration + " sec(s)\n");

        } finally {

            // release reader resources.
            if (csvParser != null) {
                csvParser.close();
            }
            if (wasAutoCommit) {
                conn.setAutoCommit(true);
            }
        }
    }

    private List<ColumnInfo> buildColumnInfoList(CSVParser parser) throws SQLException {
        List<String> columns = this.columns;
        switch (headerSource) {
        case FROM_TABLE:
            System.out.println(String.format("csv columns from database."));
            break;
        case IN_LINE:
            columns = new ArrayList<String>();
            for (String colName : parser.getHeaderMap().keySet()) {
                columns.add(colName); // iterates in column order
            }
            System.out.println(String.format("csv columns from header line. length=%s, %s",
                    columns.size(), buildStringFromList(columns)));
            break;
        case SUPPLIED_BY_USER:
            System.out.println(String.format("csv columns from user. length=%s, %s",
                    columns.size(), buildStringFromList(columns)));
            break;
        default:
            throw new IllegalStateException("parser has unknown column source.");
        }
        return generateColumnInfo(conn, tableName, columns, isStrict);
    }

    /**
     * Get list of ColumnInfos that contain Column Name and its associated
     * PDataType for an import. The supplied list of columns can be null -- if it is non-null,
     * it represents a user-supplied list of columns to be imported.
     *
     * @param conn Phoenix connection from which metadata will be read
     * @param tableName Phoenix table name whose columns are to be checked. Can include a schema
     *                  name
     * @param columns user-supplied list of import columns, can be null
     * @param strict if true, an exception will be thrown if unknown columns are supplied
     */
    public static List<ColumnInfo> generateColumnInfo(Connection conn,
            String tableName, List<String> columns, boolean strict)
            throws SQLException {
        Map<String, Integer> columnNameToTypeMap = Maps.newLinkedHashMap();
        DatabaseMetaData dbmd = conn.getMetaData();
        int unfoundColumnCount = 0;
        // TODO: escape wildcard characters here because we don't want that
        // behavior here
        String escapedTableName = StringUtil.escapeLike(tableName);
        String[] schemaAndTable = escapedTableName.split("\\.");
        ResultSet rs = null;
        try {
            rs = dbmd.getColumns(null, (schemaAndTable.length == 1 ? ""
                    : schemaAndTable[0]),
                    (schemaAndTable.length == 1 ? escapedTableName
                            : schemaAndTable[1]), null);
            while (rs.next()) {
                String sqlTypeName = rs.getString(QueryUtil.DATA_TYPE_NAME_POSITION);
                columnNameToTypeMap.put(
                        rs.getString(QueryUtil.COLUMN_NAME_POSITION),
                        PDataType.fromSqlTypeName(sqlTypeName).getSqlType());
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
        }
        List<ColumnInfo> columnInfoList = Lists.newArrayList();
        if (columns == null) {
            for (Map.Entry<String, Integer> entry : columnNameToTypeMap
                    .entrySet()) {
                columnInfoList.add(new ColumnInfo(entry.getKey(), entry.getValue()));
            }
        } else {
            // Leave "null" as indication to skip b/c it doesn't exist
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i).trim();
                Integer sqlType = columnNameToTypeMap.get(columnName);
                if (sqlType == null) {
                    if (strict) {
                        throw new SQLExceptionInfo.Builder(
                                SQLExceptionCode.COLUMN_NOT_FOUND)
                                .setColumnName(columnName)
                                .setTableName(tableName).build()
                                .buildException();
                    }
                    unfoundColumnCount++;
                } else {
                    columnInfoList.add(new ColumnInfo(columnName, sqlType));
                }
            }
            if (unfoundColumnCount == columns.size()) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.COLUMN_NOT_FOUND)
                        .setColumnName(
                                Arrays.toString(columns.toArray(new String[0])))
                        .setTableName(tableName).build().buildException();
            }
        }
        return columnInfoList;
    }

    static class CsvUpsertListener implements CsvUpsertExecutor.UpsertListener {

        private final PhoenixConnection conn;
        private final int upsertBatchSize;
        private long totalUpserts = 0L;

        CsvUpsertListener(PhoenixConnection conn, int upsertBatchSize) {
            this.conn = conn;
            this.upsertBatchSize = upsertBatchSize;
        }

        @Override
        public void upsertDone(long upsertCount) {
            totalUpserts = upsertCount;
            if (upsertCount % upsertBatchSize == 0) {
                if (upsertCount % 1000 == 0) {
                    LOG.info("Processed upsert #{}", upsertCount);
                }
                try {
                    LOG.info("Committing after {} records", upsertCount);
                    conn.commit();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void errorOnRecord(CSVRecord csvRecord, String errorMessage) {
            LOG.error("Error upserting record {}: {}", csvRecord, errorMessage);
        }

        /**
         * Get the total number of upserts that this listener has been notified about up until now.
         *
         * @return the total count of upserts
         */
        public long getTotalUpsertCount() {
            return totalUpserts;
        }
    }
}
