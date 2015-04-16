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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.phoenix.schema.types.PDataType.ARRAY_TYPE_SUFFIX;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nullable;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.expression.OrderByExpression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.monitoring.Metric;
import org.apache.phoenix.monitoring.PhoenixMetrics;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.KeyValueSchema.KeyValueSchemaBuilder;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnFamily;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.types.PDataType;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 *
 * Collection of non JDBC compliant utility methods
 *
 *
 * @since 0.1
 */
public class PhoenixRuntime {
    /**
     * Use this connection property to control HBase timestamps
     * by specifying your own long timestamp value at connection time. All
     * queries will use this as the upper bound of the time range for scans
     * and DDL, and DML will use this as t he timestamp for key values.
     */
    public static final String CURRENT_SCN_ATTRIB = "CurrentSCN";

    /**
     * Root for the JDBC URL that the Phoenix accepts accepts.
     */
    public final static String JDBC_PROTOCOL = "jdbc:phoenix";
    public final static char JDBC_PROTOCOL_TERMINATOR = ';';
    public final static char JDBC_PROTOCOL_SEPARATOR = ':';

    @Deprecated
    public final static String EMBEDDED_JDBC_PROTOCOL = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

    /**
     * Use this connection property to control the number of rows that are
     * batched together on an UPSERT INTO table1... SELECT ... FROM table2.
     * It's only used when autoCommit is true and your source table is
     * different than your target table or your SELECT statement has a
     * GROUP BY clause.
     */
    public final static String UPSERT_BATCH_SIZE_ATTRIB = "UpsertBatchSize";

    /**
     * Use this connection property to help with fairness of resource allocation
     * for the client and server. The value of the attribute determines the
     * bucket used to rollup resource usage for a particular tenant/organization. Each tenant
     * may only use a percentage of total resources, governed by the {@link org.apache.phoenix.query.QueryServices}
     * configuration properties
     */
    public static final String TENANT_ID_ATTRIB = "TenantId";

    /**
     * Use this connection property prefix for annotations that you want to show up in traces and log lines emitted by Phoenix.
     * This is useful for annotating connections with information available on the client (e.g. user or session identifier) and
     * having these annotation automatically passed into log lines and traces by Phoenix.
     */
    public static final String ANNOTATION_ATTRIB_PREFIX = "phoenix.annotation.";

    /**
     * Use this connection property to explicity enable or disable auto-commit on a new connection.
     */
    public static final String AUTO_COMMIT_ATTRIB = "AutoCommit";

    /**
     * Use this connection property to explicitly set read consistency level on a new connection.
     */
    public static final String CONSISTENCY_ATTRIB = "Consistency";

    /**
     * Use this as the zookeeper quorum name to have a connection-less connection. This enables
     * Phoenix-compatible HFiles to be created in a map/reduce job by creating tables,
     * upserting data into them, and getting the uncommitted state through {@link #getUncommittedData(Connection)}
     */
    public final static String CONNECTIONLESS = "none";

    private static final String HEADER_IN_LINE = "in-line";
    private static final String SQL_FILE_EXT = ".sql";
    private static final String CSV_FILE_EXT = ".csv";

    /**
     * Provides a mechanism to run SQL scripts against, where the arguments are:
     * 1) connection URL string
     * 2) one or more paths to either SQL scripts or CSV files
     * If a CurrentSCN property is set on the connection URL, then it is incremented
     * between processing, with each file being processed by a new connection at the
     * increment timestamp value.
     */
    public static void main(String [] args) {

        ExecutionCommand execCmd = ExecutionCommand.parseArgs(args);
        String jdbcUrl = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + execCmd.getConnectionString();

        int exitStatus = 0;

        PhoenixConnection conn = null;
        try {
            Properties props = new Properties();
            conn = DriverManager.getConnection(jdbcUrl, props)
                    .unwrap(PhoenixConnection.class);

            for (String inputFile : execCmd.getInputFiles()) {
                if (inputFile.endsWith(SQL_FILE_EXT)) {
                    PhoenixRuntime.executeStatements(conn,
                            new FileReader(inputFile), Collections.emptyList());
                } else if (inputFile.endsWith(CSV_FILE_EXT)) {

                    String tableName = execCmd.getTableName();
                    if (tableName == null) {
                        tableName = SchemaUtil.normalizeIdentifier(
                                inputFile.substring(inputFile.lastIndexOf(File.separatorChar) + 1,
                                        inputFile.length() - CSV_FILE_EXT.length()));
                    }
                    CSVCommonsLoader csvLoader =
                            new CSVCommonsLoader(conn, tableName, execCmd.getColumns(),
                                    execCmd.isStrict(), execCmd.getFieldDelimiter(),
                                    execCmd.getQuoteCharacter(), execCmd.getEscapeCharacter(),
                                    execCmd.getArrayElementSeparator());
                    csvLoader.upsert(inputFile);
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
            exitStatus = 1;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    //going to shut jvm down anyway. So might as well feast on it.
                }
            }
            System.exit(exitStatus);
        }
    }

    public static final String PHOENIX_TEST_DRIVER_URL_PARAM = "test=true";

    private PhoenixRuntime() {
    }

    /**
     * Runs a series of semicolon-terminated SQL statements using the connection provided, returning
     * the number of SQL statements executed. Note that if the connection has specified an SCN through
     * the {@link org.apache.phoenix.util.PhoenixRuntime#CURRENT_SCN_ATTRIB} connection property, then the timestamp
     * is bumped up by one after each statement execution.
     * @param conn an open JDBC connection
     * @param reader a reader for semicolumn separated SQL statements
     * @param binds the binds for all statements
     * @return the number of SQL statements that were executed
     * @throws IOException
     * @throws SQLException
     */
    public static int executeStatements(Connection conn, Reader reader, List<Object> binds) throws IOException,SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        // Turn auto commit to true when running scripts in case there's DML
        pconn.setAutoCommit(true);
        return pconn.executeStatements(reader, binds, System.out);
    }

    /**
     * Get the list of uncommitted KeyValues for the connection. Currently used to write an
     * Phoenix-compliant HFile from a map/reduce job.
     * @param conn an open JDBC connection
     * @return the list of HBase mutations for uncommitted data
     * @throws SQLException
     */
    @Deprecated
    public static List<KeyValue> getUncommittedData(Connection conn) throws SQLException {
        Iterator<Pair<byte[],List<KeyValue>>> iterator = getUncommittedDataIterator(conn);
        if (iterator.hasNext()) {
            return iterator.next().getSecond();
        }
        return Collections.emptyList();
    }

    /**
     * Get the list of uncommitted KeyValues for the connection. Currently used to write an
     * Phoenix-compliant HFile from a map/reduce job.
     * @param conn an open JDBC connection
     * @return the list of HBase mutations for uncommitted data
     * @throws SQLException
     */
    public static Iterator<Pair<byte[],List<KeyValue>>> getUncommittedDataIterator(Connection conn) throws SQLException {
        return getUncommittedDataIterator(conn, false);
    }

    /**
     * Get the list of uncommitted KeyValues for the connection. Currently used to write an
     * Phoenix-compliant HFile from a map/reduce job.
     * @param conn an open JDBC connection
     * @return the list of HBase mutations for uncommitted data
     * @throws SQLException
     */
    public static Iterator<Pair<byte[],List<KeyValue>>> getUncommittedDataIterator(Connection conn, boolean includeMutableIndexes) throws SQLException {
        final PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        final Iterator<Pair<byte[],List<Mutation>>> iterator = pconn.getMutationState().toMutations(includeMutableIndexes);
        return new Iterator<Pair<byte[],List<KeyValue>>>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Pair<byte[], List<KeyValue>> next() {
                Pair<byte[],List<Mutation>> pair = iterator.next();
                List<KeyValue> keyValues = Lists.newArrayListWithExpectedSize(pair.getSecond().size() * 5); // Guess-timate 5 key values per row
                for (Mutation mutation : pair.getSecond()) {
                    for (List<Cell> keyValueList : mutation.getFamilyCellMap().values()) {
                        for (Cell keyValue : keyValueList) {
                            keyValues.add(org.apache.hadoop.hbase.KeyValueUtil.ensureKeyValue(keyValue));
                        }
                    }
                }
                Collections.sort(keyValues, pconn.getKeyValueBuilder().getKeyValueComparator());
                return new Pair<byte[], List<KeyValue>>(pair.getFirst(),keyValues);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

    public static PTable getTable(Connection conn, String name) throws SQLException {
        PTable table = null;
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        try {
            name = SchemaUtil.normalizeIdentifier(name);
            table = pconn.getMetaDataCache().getTable(new PTableKey(pconn.getTenantId(), name));
        } catch (TableNotFoundException e) {
            String schemaName = SchemaUtil.getSchemaNameFromFullName(name);
            String tableName = SchemaUtil.getTableNameFromFullName(name);
            MetaDataMutationResult result = new MetaDataClient(pconn).updateCache(schemaName, tableName);
            if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                throw e;
            }
            table = result.getTable();
        }
        return table;
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
     */
    public static List<ColumnInfo> generateColumnInfo(Connection conn,
            String tableName, List<String> columns)
            throws SQLException {

        PTable table = PhoenixRuntime.getTable(conn, tableName);
        List<ColumnInfo> columnInfoList = Lists.newArrayList();
        Set<String> unresolvedColumnNames = new TreeSet<String>();
        if (columns == null) {
            // use all columns in the table
            for(PColumn pColumn : table.getColumns()) {
               int sqlType = pColumn.getDataType().getSqlType();
               columnInfoList.add(new ColumnInfo(pColumn.toString(), sqlType)); 
            }
        } else {
            // Leave "null" as indication to skip b/c it doesn't exist
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                try {
                    ColumnInfo columnInfo = PhoenixRuntime.getColumnInfo(table, columnName);
                    columnInfoList.add(columnInfo);
                } catch (ColumnNotFoundException cnfe) {
                    unresolvedColumnNames.add(columnName);
                } catch (AmbiguousColumnException ace) {
                    unresolvedColumnNames.add(columnName);
                }
            }
        }
        // if there exists columns that cannot be resolved, error out.
        if (unresolvedColumnNames.size()>0) {
                StringBuilder exceptionMessage = new StringBuilder();
                boolean first = true;
                exceptionMessage.append("Unable to resolve these column names:\n");
                for (String col : unresolvedColumnNames) {
                    if (first) first = false;
                    else exceptionMessage.append(",");
                    exceptionMessage.append(col);
                }
                exceptionMessage.append("\nAvailable columns with column families:\n");
                first = true;
                for (PColumn pColumn : table.getColumns()) {
                    if (first) first = false;
                    else exceptionMessage.append(",");
                    exceptionMessage.append(pColumn.toString());
                }
                throw new SQLException(exceptionMessage.toString());
      }
       return columnInfoList;
    }

    /**
     * Returns the column info for the given column for the given table.
     *
     * @param table
     * @param columnName User-specified column name. May be family-qualified or bare.
     * @return columnInfo associated with the column in the table
     * @throws SQLException if parameters are null or if column is not found or if column is ambiguous.
     */
    public static ColumnInfo getColumnInfo(PTable table, String columnName) throws SQLException {
        if (table==null) {
            throw new SQLException("Table must not be null.");
        }
        if (columnName==null) {
            throw new SQLException("columnName must not be null.");
        }
        PColumn pColumn = null;
        if (columnName.contains(QueryConstants.NAME_SEPARATOR)) {
            String[] tokens = columnName.split(QueryConstants.NAME_SEPARATOR_REGEX);
            if (tokens.length!=2) {
                throw new SQLException(String.format("Unable to process column %s, expected family-qualified name.",columnName));
            }
            String familyName = tokens[0];
            String familyColumn = tokens[1];
            PColumnFamily family = table.getColumnFamily(familyName);
            pColumn = family.getColumn(familyColumn);
        } else {
            pColumn = table.getColumn(columnName);
        }
        return getColumnInfo(pColumn);
    }

   /**
     * Constructs a column info for the supplied pColumn
     * @param pColumn
     * @return columnInfo
     * @throws SQLException if the parameter is null.
     */
    public static ColumnInfo getColumnInfo(PColumn pColumn) throws SQLException {
        if (pColumn==null) {
            throw new SQLException("pColumn must not be null.");
        }
        int sqlType = pColumn.getDataType().getSqlType();
        ColumnInfo columnInfo = new ColumnInfo(pColumn.toString(),sqlType);
        return columnInfo;
    }

   /**
     * Represents the parsed commandline parameters definining the command or commands to be
     * executed.
     */
    static class ExecutionCommand {
        private String connectionString;
        private List<String> columns;
        private String tableName;
        private char fieldDelimiter;
        private char quoteCharacter;
        private Character escapeCharacter;
        private String arrayElementSeparator;
        private boolean strict;
        private List<String> inputFiles;

        /**
         * Factory method to build up an {@code ExecutionCommand} based on supplied parameters.
         */
        public static ExecutionCommand parseArgs(String[] args) {
            Option tableOption = new Option("t", "table", true,
                    "Overrides the table into which the CSV data is loaded and is case sensitive");
            Option headerOption = new Option("h", "header", true, "Overrides the column names to" +
                    " which the CSV data maps and is case sensitive. A special value of " +
                    "in-line indicating that the first line of the CSV file determines the " +
                    "column to which the data maps");
            Option strictOption = new Option("s", "strict", false, "Use strict mode by throwing " +
                    "an exception if a column name doesn't match during CSV loading");
            Option delimiterOption = new Option("d", "delimiter", true,
                    "Field delimiter for CSV loader. A digit is interpreted as " +
                    "1 -> ctrl A, 2 -> ctrl B ... 9 -> ctrl I.");
            Option quoteCharacterOption = new Option("q", "quote-character", true,
                    "Quote character for CSV loader. A digit is interpreted as a control " +
                            "character");
            Option escapeCharacterOption = new Option("e", "escape-character", true,
                    "Escape character for CSV loader. A digit is interpreted as a control " +
                            "character");
            Option arrayValueSeparatorOption = new Option("a", "array-separator", true,
                    "Define the array element separator, defaults to ':'");
            Options options = new Options();
            options.addOption(tableOption);
            options.addOption(headerOption);
            options.addOption(strictOption);
            options.addOption(delimiterOption);
            options.addOption(quoteCharacterOption);
            options.addOption(escapeCharacterOption);
            options.addOption(arrayValueSeparatorOption);

            CommandLineParser parser = new PosixParser();
            CommandLine cmdLine = null;
            try {
                cmdLine = parser.parse(options, args);
            } catch (ParseException e) {
                usageError(options);
            }

            ExecutionCommand execCmd = new ExecutionCommand();

            if (cmdLine.hasOption(tableOption.getOpt())) {
                execCmd.tableName = cmdLine.getOptionValue(tableOption.getOpt());
            }

            if (cmdLine.hasOption(headerOption.getOpt())) {
                String columnString = cmdLine.getOptionValue(headerOption.getOpt());
                if (HEADER_IN_LINE.equals(columnString)) {
                    execCmd.columns = ImmutableList.of();
                } else {
                    execCmd.columns = ImmutableList.copyOf(
                            Splitter.on(",").trimResults().split(columnString));
                }
            }

            execCmd.strict = cmdLine.hasOption(strictOption.getOpt());
            execCmd.fieldDelimiter = getCharacter(
                    cmdLine.getOptionValue(delimiterOption.getOpt(), ","));
            execCmd.quoteCharacter = getCharacter(
                    cmdLine.getOptionValue(quoteCharacterOption.getOpt(), "\""));

            if (cmdLine.hasOption(escapeCharacterOption.getOpt())) {
                execCmd.escapeCharacter = getCharacter(
                        cmdLine.getOptionValue(escapeCharacterOption.getOpt(), "\\"));
            }

            execCmd.arrayElementSeparator = cmdLine.getOptionValue(
                    arrayValueSeparatorOption.getOpt(),
                    CSVCommonsLoader.DEFAULT_ARRAY_ELEMENT_SEPARATOR);


            List<String> argList = Lists.newArrayList(cmdLine.getArgList());
            if (argList.isEmpty()) {
                usageError("Connection string to HBase must be supplied", options);
            }
            execCmd.connectionString = argList.remove(0);
            List<String> inputFiles = Lists.newArrayList();
            for (String arg : argList) {
                if (arg.endsWith(CSV_FILE_EXT) || arg.endsWith(SQL_FILE_EXT)) {
                    inputFiles.add(arg);
                } else {
                    usageError("Don't know how to interpret argument '" + arg + "'", options);
                }
            }

            if (inputFiles.isEmpty()) {
                usageError("At least one input file must be supplied", options);
            }

            execCmd.inputFiles = inputFiles;



            return execCmd;
        }

        private static char getCharacter(String s) {
            if (s.length() > 1) {
                throw new IllegalArgumentException("Invalid single character: '" + s + "'");
            }
            return s.charAt(0);
        }

        private static void usageError(String errorMsg, Options options) {
            System.out.println(errorMsg);
            usageError(options);
        }

        private static void usageError(Options options) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(
                    "psql [-t table-name] [-h comma-separated-column-names | in-line] [-d " +
                            "field-delimiter-char quote-char escape-char]<zookeeper>  " +
                            "<path-to-sql-or-csv-file>...",
                    options);
            System.out.println("Examples:\n" +
                    "  psql localhost my_ddl.sql\n" +
                    "  psql localhost my_ddl.sql my_table.csv\n" +
                    "  psql -t MY_TABLE my_cluster:1825 my_table2012-Q3.csv\n" +
                    "  psql -t MY_TABLE -h COL1,COL2,COL3 my_cluster:1825 my_table2012-Q3.csv\n" +
                    "  psql -t MY_TABLE -h COL1,COL2,COL3 -d : my_cluster:1825 my_table2012-Q3.csv");
            System.exit(-1);
        }

        public String getConnectionString() {
            return connectionString;
        }

        public List<String> getColumns() {
            return columns;
        }

        public String getTableName() {
            return tableName;
        }

        public char getFieldDelimiter() {
            return fieldDelimiter;
        }

        public char getQuoteCharacter() {
            return quoteCharacter;
        }

        public Character getEscapeCharacter() {
            return escapeCharacter;
        }

        public String getArrayElementSeparator() {
            return arrayElementSeparator;
        }

        public List<String> getInputFiles() {
            return inputFiles;
        }

        public boolean isStrict() {
            return strict;
        }
    }
    
    /**
     * Encode the primary key values from the table as a byte array. The values must
     * be in the same order as the primary key constraint. If the connection and
     * table are both tenant-specific, the tenant ID column must not be present in
     * the values.
     * @param conn an open connection
     * @param fullTableName the full table name
     * @param values the values of the primary key columns ordered in the same order
     *  as the primary key constraint
     * @return the encoded byte array
     * @throws SQLException if the table cannot be found or the incorrect number of
     *  of values are provided
     * @see #decodePK(Connection, String, byte[]) to decode the byte[] back to the
     *  values
     */
    @Deprecated
    public static byte[] encodePK(Connection conn, String fullTableName, Object[] values) throws SQLException {
        PTable table = getTable(conn, fullTableName);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        int offset = (table.getBucketNum() == null ? 0 : 1) + (table.isMultiTenant() && pconn.getTenantId() != null ? 1 : 0);
        List<PColumn> pkColumns = table.getPKColumns();
        if (pkColumns.size() - offset != values.length) {
            throw new SQLException("Expected " + (pkColumns.size() - offset) + " but got " + values.length);
        }
        PDataType type = null;
        TrustedByteArrayOutputStream output = new TrustedByteArrayOutputStream(table.getRowKeySchema().getEstimatedValueLength());
        try {
            for (int i = offset; i < pkColumns.size(); i++) {
                if (type != null && !type.isFixedWidth()) {
                    output.write(QueryConstants.SEPARATOR_BYTE);
                }
                type = pkColumns.get(i).getDataType();

                //for fixed width data types like CHAR and BINARY, we need to pad values to be of max length.
                Object paddedObj = type.pad(values[i - offset], pkColumns.get(i).getMaxLength());
                byte[] value = type.toBytes(paddedObj);
                output.write(value);
            }
            return output.toByteArray();
        } finally {
            try {
                output.close();
            } catch (IOException e) {
                throw new RuntimeException(e); // Impossible
            }
        }
    }

    /**
     * Decode a byte array value back into the Object values of the
     * primary key constraint. If the connection and table are both
     * tenant-specific, the tenant ID column is not expected to have
     * been encoded and will not appear in the returned values.
     * @param conn an open connection
     * @param name the full table name
     * @param encodedValue the value that was encoded with {@link #encodePK(Connection, String, Object[])}
     * @return the Object values encoded in the byte array value
     * @throws SQLException
     */
    @Deprecated
    public static Object[] decodePK(Connection conn, String name, byte[] value) throws SQLException {
        PTable table = getTable(conn, name);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        int offset = (table.getBucketNum() == null ? 0 : 1) + (table.isMultiTenant() && pconn.getTenantId() != null ? 1 : 0);
        int nValues = table.getPKColumns().size() - offset;
        RowKeySchema schema = table.getRowKeySchema();
        Object[] values = new Object[nValues];
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        schema.iterator(value, ptr);
        int i = 0;
        int fieldIdx = offset;
        while (i < nValues && schema.next(ptr, fieldIdx, value.length) != null) {
            values[i] = schema.getField(fieldIdx).getDataType().toObject(ptr);
            i++;
            fieldIdx++;
        }
        return values;
    }

    /**
     * Returns the opitmized query plan used by phoenix for executing the sql.
     * @param stmt to return the plan for
     * @throws SQLException
     */
    public static QueryPlan getOptimizedQueryPlan(PreparedStatement stmt) throws SQLException {
        checkNotNull(stmt);
        QueryPlan plan = stmt.unwrap(PhoenixPreparedStatement.class).optimizeQuery();
        return plan;
    }
    
    /**
     * Whether or not the query plan has any order by expressions.
     * @param plan
     * @return
     */
    public static boolean hasOrderBy(QueryPlan plan) {
        checkNotNull(plan);
        List<OrderByExpression> orderBys = plan.getOrderBy().getOrderByExpressions();
        return orderBys != null && !orderBys.isEmpty(); 
    }
    
    public static int getLimit(QueryPlan plan) {
        checkNotNull(plan);
        return plan.getLimit() == null ? 0 : plan.getLimit();
    }
    
    private static String addQuotes(String str) {
        return str == null ? str : "\"" + str + "\"";
    }
    /**
     *
     * @param columns - Initialized empty list to be filled with the pairs of column family name and column name for columns that are used 
     * as row key for the query plan. Column family names are optional and hence the first part of the pair is nullable.
     * Column names and family names are enclosed in double quotes to allow for case sensitivity and for presence of 
     * special characters. Salting column and view index id column are not included. If the connection is tenant specific 
     * and the table used by the query plan is multi-tenant, then the tenant id column is not included as well.
     * @param plan - query plan to get info for.
     * @param conn - connection used to generate the query plan. Caller should take care of closing the connection appropriately.
     * @param forDataTable - if true, then family names and column names correspond to the data table even if the query plan uses
     * the secondary index table. If false, and if the query plan uses the secondary index table, then the family names and column 
     * names correspond to the index table.
     * @throws SQLException
     */
    public static void getPkColsForSql(List<Pair<String, String>> columns, QueryPlan plan, Connection conn, boolean forDataTable) throws SQLException {
        checkNotNull(columns);
        checkNotNull(plan);
        checkNotNull(conn);
        List<PColumn> pkColumns = getPkColumns(plan.getTableRef().getTable(), conn, forDataTable);
        String columnName;
        String familyName;
        for (PColumn pCol : pkColumns ) {
            columnName = addQuotes(pCol.getName().getString());
            familyName = pCol.getFamilyName() != null ? addQuotes(pCol.getFamilyName().getString()) : null;
            columns.add(new Pair<String, String>(familyName, columnName));
        }
    }

    /**
     * @param columns - Initialized empty list to be filled with the pairs of column family name and column name for columns that are used 
     * as row key for the query plan. Column family names are optional and hence the first part of the pair is nullable.
     * Column names and family names are enclosed in double quotes to allow for case sensitivity and for presence of 
     * special characters. Salting column and view index id column are not included. If the connection is tenant specific 
     * and the table used by the query plan is multi-tenant, then the tenant id column is not included as well.
     * @param datatypes - Initialized empty list to be filled with the corresponding data type for the columns in @param columns. 
     * @param plan - query plan to get info for
     * @param conn - phoenix connection used to generate the query plan. Caller should take care of closing the connection appropriately.
     * @param forDataTable - if true, then column names and data types correspond to the data table even if the query plan uses
     * the secondary index table. If false, and if the query plan uses the secondary index table, then the column names and data 
     * types correspond to the index table.
     * @throws SQLException
     */
    public static void getPkColsDataTypesForSql(List<Pair<String, String>> columns, List<String> dataTypes, QueryPlan plan, Connection conn, boolean forDataTable) throws SQLException {
        checkNotNull(columns);
        checkNotNull(dataTypes);
        checkNotNull(plan);
        checkNotNull(conn);
        List<PColumn> pkColumns = getPkColumns(plan.getTableRef().getTable(), conn, forDataTable);
        String columnName;
        String familyName;
        for (PColumn pCol : pkColumns) {
            String sqlTypeName = getSqlTypeName(pCol);
            dataTypes.add(sqlTypeName);
            columnName = addQuotes(pCol.getName().getString());
            familyName = pCol.getFamilyName() != null ? addQuotes(pCol.getFamilyName().getString()) : null;
            columns.add(new Pair<String, String>(familyName, columnName));
        }
    }
    
    /**
     * 
     * @param pCol
     * @return sql type name that could be used in DDL statements, dynamic column types etc. 
     */
    public static String getSqlTypeName(PColumn pCol) {
        PDataType dataType = pCol.getDataType();
        Integer maxLength = pCol.getMaxLength();
        Integer scale = pCol.getScale();
        return dataType.isArrayType() ? getArraySqlTypeName(maxLength, scale, dataType) : appendMaxLengthAndScale(maxLength, scale, dataType.getSqlTypeName());
    }
    
    public static String getArraySqlTypeName(@Nullable Integer maxLength, @Nullable Integer scale, PDataType arrayType) {
        String baseTypeSqlName = PDataType.arrayBaseType(arrayType).getSqlTypeName();
        return appendMaxLengthAndScale(maxLength, scale, baseTypeSqlName) + " " + ARRAY_TYPE_SUFFIX; // for ex - decimal(10,2) ARRAY
    }

    private static String appendMaxLengthAndScale(@Nullable Integer maxLength, @Nullable Integer scale, String sqlTypeName) {
        if (maxLength != null) {
             sqlTypeName = sqlTypeName + "(" + maxLength;
             if (scale != null) {
               sqlTypeName = sqlTypeName + "," + scale; // has both max length and scale. For ex- decimal(10,2)
             }       
             sqlTypeName = sqlTypeName + ")";
        }
        return sqlTypeName;
    }
    
    private static List<PColumn> getPkColumns(PTable ptable, Connection conn, boolean forDataTable) throws SQLException {
        PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
        List<PColumn> pkColumns = ptable.getPKColumns();
        
        // Skip the salting column and the view index id column if present.
        // Skip the tenant id column too if the connection is tenant specific and the table used by the query plan is multi-tenant
        int offset = (ptable.getBucketNum() == null ? 0 : 1) + (ptable.isMultiTenant() && pConn.getTenantId() != null ? 1 : 0) + (ptable.getViewIndexId() == null ? 0 : 1);
        
        // get a sublist of pkColumns by skipping the offset columns.
        pkColumns = pkColumns.subList(offset, pkColumns.size());
        
        if (ptable.getType() == PTableType.INDEX && forDataTable) {
            // index tables have the same schema name as their parent/data tables.
            String fullDataTableName = ptable.getParentName().getString();
            
            // Get the corresponding columns of the data table.
            List<PColumn> dataColumns = IndexUtil.getDataColumns(fullDataTableName, pkColumns, pConn);
            pkColumns = dataColumns;
        }
        return pkColumns;
    }
    
    /**
     * 
     * @param conn connection that was used for reading/generating value.
     * @param fullTableName fully qualified table name
     * @param values values of the columns
     * @param columns list of pair of column that includes column family as first part and column name as the second part.
     * Column family is optional and hence nullable. Columns in the list have to be in the same order as the order of occurence
     * of their values in the object array.
     * @return values encoded in a byte array 
     * @throws SQLException
     * @see {@link #decodeValues(Connection, String, byte[], List)}
     */
    public static byte[] encodeValues(Connection conn, String fullTableName, Object[] values, List<Pair<String, String>> columns) throws SQLException {
        PTable table = getTable(conn, fullTableName);
        List<PColumn> pColumns = getPColumns(table, columns);
        List<Expression> expressions = new ArrayList<Expression>(pColumns.size());
        int i = 0;
        for (PColumn col : pColumns) {
            Object value = values[i];
            // for purposes of encoding, sort order of the columns doesn't matter.
            Expression expr = LiteralExpression.newConstant(value, col.getDataType(), col.getMaxLength(), col.getScale());
            expressions.add(expr);
            i++;
        }
        KeyValueSchema kvSchema = buildKeyValueSchema(pColumns);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ValueBitSet valueSet = ValueBitSet.newInstance(kvSchema);
        return kvSchema.toBytes(expressions.toArray(new Expression[0]), valueSet, ptr);
    }
    
    
    /**
     * 
     * @param conn connection that was used for reading/generating value.
     * @param fullTableName fully qualified table name
     * @param value byte value of the columns concatenated as a single byte array. @see {@link #encodeValues(Connection, String, Object[], List)}
     * @param columns list of column names for the columns that have their respective values
     * present in the byte array. The column names should be in the same order as their values are in the byte array.
     * The column name includes both family name, if present, and column name.
     * @return decoded values for each column
     * @throws SQLException
     * 
     */
    public static Object[] decodeValues(Connection conn, String fullTableName, byte[] value, List<Pair<String, String>> columns) throws SQLException {
        PTable table = getTable(conn, fullTableName);
        KeyValueSchema kvSchema = buildKeyValueSchema(getPColumns(table, columns));
        ImmutableBytesWritable ptr = new ImmutableBytesWritable(value);
        ValueBitSet valueSet = ValueBitSet.newInstance(kvSchema);
        valueSet.clear();
        valueSet.or(ptr);
        int maxOffset = ptr.getOffset() + ptr.getLength();
        Boolean hasValue;
        kvSchema.iterator(ptr);
        int i = 0;
        List<Object> values = new ArrayList<Object>();
        while(hasValue = kvSchema.next(ptr, i, maxOffset, valueSet) != null) {
            if(hasValue) {
                values.add(kvSchema.getField(i).getDataType().toObject(ptr));
            }
            i++;
        }
        return values.toArray();
    }
    
    private static KeyValueSchema buildKeyValueSchema(List<PColumn> columns) {
        KeyValueSchemaBuilder builder = new KeyValueSchemaBuilder(getMinNullableIndex(columns));
        for (PColumn col : columns) {
            builder.addField(col);
        }
        return builder.build();
    }
    
    private static int getMinNullableIndex(List<PColumn> columns) {
        int minNullableIndex = columns.size();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).isNullable()) {
                minNullableIndex = i;
                break;
            }
         }
        return minNullableIndex;
    }
    
   /**
     * @param table table to get the {@code PColumn} for
     * @param columns list of pair of column that includes column family as first part and column name as the second part.
     * Column family is optional and hence nullable. 
     * @return list of {@code PColumn} for fullyQualifiedColumnNames
     * @throws SQLException 
     */
    private static List<PColumn> getPColumns(PTable table, List<Pair<String, String>> columns) throws SQLException {
        List<PColumn> pColumns = new ArrayList<PColumn>(columns.size());
        for (Pair<String, String> column : columns) {
            pColumns.add(getPColumn(table, column.getFirst(), column.getSecond()));
        }
        return pColumns;
    }
    
    private static PColumn getPColumn(PTable table, @Nullable String familyName, String columnName) throws SQLException {
        if (table==null) {
            throw new SQLException("Table must not be null.");
        }
        if (columnName==null) {
            throw new SQLException("columnName must not be null.");
        }
        // normalize and remove quotes from family and column names before looking up.
        familyName = SchemaUtil.normalizeIdentifier(familyName);
        columnName = SchemaUtil.normalizeIdentifier(columnName);
        PColumn pColumn = null;
        if (familyName != null) {
            PColumnFamily family = table.getColumnFamily(familyName);
            pColumn = family.getColumn(columnName);
        } else {
            pColumn = table.getColumn(columnName);
        }
        return pColumn;
    }
    
    /**
     * Get expression that may be used to evaluate the tenant ID of a given row in a
     * multi-tenant table. Both the SYSTEM.CATALOG table and the SYSTEM.SEQUENCE
     * table are considered multi-tenant.
     * @param conn open Phoenix connection
     * @param fullTableName full table name
     * @return An expression that may be evaluated for a row in the provided table or
     * null if the table is not a multi-tenant table. 
     * @throws SQLException if the table name is not found, a TableNotFoundException
     * is thrown. If a multi-tenant local index is supplied a SQLFeatureNotSupportedException
     * is thrown.
     */
    public static Expression getTenantIdExpression(Connection conn, String fullTableName) throws SQLException {
        PTable table = getTable(conn, fullTableName);
        // TODO: consider setting MULTI_TENANT = true for SYSTEM.CATALOG and SYSTEM.SEQUENCE
        if (!SchemaUtil.isMetaTable(table) && !SchemaUtil.isSequenceTable(table) && !table.isMultiTenant()) {
            return null;
        }
        if (table.getIndexType() == IndexType.LOCAL) {
            /*
             * With some hackery, we could deduce the tenant ID from a multi-tenant local index,
             * however it's not clear that we'd want to maintain the same prefixing of the region
             * start key, as the region boundaries may end up being different on a cluster being
             * replicated/backed-up to (which is the use case driving the method).
             */
            throw new SQLFeatureNotSupportedException();
        }
        
        int pkPosition = table.getBucketNum() == null ? 0 : 1;
        List<PColumn> pkColumns = table.getPKColumns();
        return new RowKeyColumnExpression(pkColumns.get(pkPosition), new RowKeyValueAccessor(pkColumns, pkPosition));
    }
    
    /**
     * Exposes the various internal phoenix metrics. 
     */
    public static Collection<Metric> getInternalPhoenixMetrics() {
        return PhoenixMetrics.getMetrics();
    }
}
