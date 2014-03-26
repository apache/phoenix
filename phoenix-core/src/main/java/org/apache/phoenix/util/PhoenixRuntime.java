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
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
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
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.MetaDataClient;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.TableNotFoundException;

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
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    //going to shut jvm down anyway. So might as well feast on it.
                }
            }
            System.exit(0);
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
    
    private static PTable getTable(Connection conn, String name) throws SQLException {
        PTable table = null;
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        try {
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
                byte[] value = type.toBytes(values[i - offset]);
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
     * @param value the value that was encoded with {@link #encodePK(Connection, String, Object[])}
     * @return the Object values encoded in the byte array value
     * @throws SQLException
     */
    public static Object[] decodePK(Connection conn, String name, byte[] value) throws SQLException {
        PTable table = getTable(conn, name);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        int offset = (table.getBucketNum() == null ? 0 : 1) + (table.isMultiTenant() && pconn.getTenantId() != null ? 1 : 0);
        RowKeySchema schema = table.getRowKeySchema();
        int nValues = schema.getMaxFields()-offset;
        Object[] values = new Object[nValues];
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        int i = 0;
        schema.iterator(value, ptr, offset);
        while (i < nValues && schema.next(ptr, i, value.length) != null) {
            values[i] = schema.getField(i).getDataType().toObject(ptr);
            i++;
        }
        return values;
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
}
