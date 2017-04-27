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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
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
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.apache.phoenix.monitoring.GlobalMetric;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.query.QueryServices;
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
import org.apache.phoenix.schema.RowKeyValueAccessor;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.tephra.util.TxUtils;

import com.google.common.base.Joiner;
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
     * Root for the JDBC URL that the Phoenix accepts accepts.
     */
    public final static String JDBC_PROTOCOL = "jdbc:phoenix";
    /**
     * Root for the JDBC URL used by the thin driver. Duplicated here to avoid dependencies
     * between modules.
     */
    public final static String JDBC_THIN_PROTOCOL = "jdbc:phoenix:thin";
    public final static char JDBC_PROTOCOL_TERMINATOR = ';';
    public final static char JDBC_PROTOCOL_SEPARATOR = ':';

    @Deprecated
    public final static String EMBEDDED_JDBC_PROTOCOL = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

    /**
     * Use this connection property to control HBase timestamps
     * by specifying your own long timestamp value at connection time. All
     * queries will use this as the upper bound of the time range for scans
     * and DDL, and DML will use this as t he timestamp for key values.
     */
    public static final String CURRENT_SCN_ATTRIB = "CurrentSCN";

    /**
     * Use this connection property to help with fairness of resource allocation
     * for the client and server. The value of the attribute determines the
     * bucket used to rollup resource usage for a particular tenant/organization. Each tenant
     * may only use a percentage of total resources, governed by the {@link org.apache.phoenix.query.QueryServices}
     * configuration properties
     */
    public static final String TENANT_ID_ATTRIB = "TenantId";

    /**
     * Use this connection property to prevent an upgrade from occurring when
     * connecting to a new server version.
     */
    public static final String NO_UPGRADE_ATTRIB = "NoUpgrade";
    /**
     * Use this connection property to control the number of rows that are
     * batched together on an UPSERT INTO table1... SELECT ... FROM table2.
     * It's only used when autoCommit is true and your source table is
     * different than your target table or your SELECT statement has a
     * GROUP BY clause.
     */
    public final static String UPSERT_BATCH_SIZE_ATTRIB = "UpsertBatchSize";

    /**
     * Use this connection property to control the number of bytes that are
     * batched together on an UPSERT INTO table1... SELECT ... FROM table2.
     * It's only used when autoCommit is true and your source table is
     * different than your target table or your SELECT statement has a
     * GROUP BY clause. Overrides the value of UpsertBatchSize.
     */
    public final static String UPSERT_BATCH_SIZE_BYTES_ATTRIB = "UpsertBatchSizeBytes";


    /**
     * Use this connection property to explicitly enable or disable auto-commit on a new connection.
     */
    public static final String AUTO_COMMIT_ATTRIB = "AutoCommit";

    /**
     * Use this connection property to explicitly set read consistency level on a new connection.
     */
    public static final String CONSISTENCY_ATTRIB = "Consistency";

    /**
     * Use this connection property to explicitly enable or disable request level metric collection.
     */
    public static final String REQUEST_METRIC_ATTRIB = "RequestMetric";

    /**
     * All Phoenix specific connection properties
     * TODO: use enum instead
     */
    public final static String[] CONNECTION_PROPERTIES = {
            CURRENT_SCN_ATTRIB,
            TENANT_ID_ATTRIB,
            UPSERT_BATCH_SIZE_ATTRIB,
            AUTO_COMMIT_ATTRIB,
            CONSISTENCY_ATTRIB,
            REQUEST_METRIC_ATTRIB,
            };

    /**
     * Use this as the zookeeper quorum name to have a connection-less connection. This enables
     * Phoenix-compatible HFiles to be created in a map/reduce job by creating tables,
     * upserting data into them, and getting the uncommitted state through {@link #getUncommittedData(Connection)}
     */
    public final static String CONNECTIONLESS = "none";
    
    /**
     * Use this connection property prefix for annotations that you want to show up in traces and log lines emitted by Phoenix.
     * This is useful for annotating connections with information available on the client (e.g. user or session identifier) and
     * having these annotation automatically passed into log lines and traces by Phoenix.
     */
    public static final String ANNOTATION_ATTRIB_PREFIX = "phoenix.annotation.";

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
            if (execCmd.isLocalIndexUpgrade()) {
                props.setProperty(QueryServices.LOCAL_INDEX_CLIENT_UPGRADE_ATTRIB, "false");
            }
            if (execCmd.binaryEncoding != null) {
                props.setProperty(QueryServices.UPLOAD_BINARY_DATA_TYPE_ENCODING, execCmd.binaryEncoding);
            }
            conn = DriverManager.getConnection(jdbcUrl, props).unwrap(PhoenixConnection.class);
            conn.setRunningUpgrade(true);
            if (execCmd.isMapNamespace()) {
                String srcTable = execCmd.getSrcTable();
                System.out.println("Starting upgrading table:" + srcTable + "... please don't kill it in between!!");
                UpgradeUtil.upgradeTable(conn, srcTable);
                UpgradeUtil.mapChildViewsToNamespace(conn, srcTable,props);
            } else if (execCmd.isUpgrade()) {
                if (conn.getClientInfo(PhoenixRuntime.CURRENT_SCN_ATTRIB) != null) { throw new SQLException(
                        "May not specify the CURRENT_SCN property when upgrading"); }
                if (conn.getClientInfo(PhoenixRuntime.TENANT_ID_ATTRIB) != null) { throw new SQLException(
                        "May not specify the TENANT_ID_ATTRIB property when upgrading"); }
                if (execCmd.getInputFiles().isEmpty()) {
                    List<String> tablesNeedingUpgrade = UpgradeUtil.getPhysicalTablesWithDescRowKey(conn);
                    if (tablesNeedingUpgrade.isEmpty()) {
                        String msg = "No tables are required to be upgraded due to incorrect row key order (PHOENIX-2067 and PHOENIX-2120)";
                        System.out.println(msg);
                    } else {
                        String msg = "The following tables require upgrade due to a bug causing the row key to be incorrectly ordered (PHOENIX-2067 and PHOENIX-2120):\n"
                                + Joiner.on(' ').join(tablesNeedingUpgrade);
                        System.out.println("WARNING: " + msg);
                    }
                    List<String> unsupportedTables = UpgradeUtil.getPhysicalTablesWithDescVarbinaryRowKey(conn);
                    if (!unsupportedTables.isEmpty()) {
                        String msg = "The following tables use an unsupported VARBINARY DESC construct and need to be changed:\n"
                                + Joiner.on(' ').join(unsupportedTables);
                        System.out.println("WARNING: " + msg);
                    }
                } else {
                    UpgradeUtil.upgradeDescVarLengthRowKeys(conn, execCmd.getInputFiles(), execCmd.isBypassUpgrade());
                }
            } else if(execCmd.isLocalIndexUpgrade()) {
                UpgradeUtil.upgradeLocalIndexes(conn);
            } else {
                for (String inputFile : execCmd.getInputFiles()) {
                    if (inputFile.endsWith(SQL_FILE_EXT)) {
                        PhoenixRuntime.executeStatements(conn, new FileReader(inputFile), Collections.emptyList());
                    } else if (inputFile.endsWith(CSV_FILE_EXT)) {

                        String tableName = execCmd.getTableName();
                        if (tableName == null) {
                            tableName = SchemaUtil.normalizeIdentifier(
                                    inputFile.substring(inputFile.lastIndexOf(File.separatorChar) + 1,
                                            inputFile.length() - CSV_FILE_EXT.length()));
                        }
                        CSVCommonsLoader csvLoader = new CSVCommonsLoader(conn, tableName, execCmd.getColumns(),
                                execCmd.isStrict(), execCmd.getFieldDelimiter(), execCmd.getQuoteCharacter(),
                                execCmd.getEscapeCharacter(), execCmd.getArrayElementSeparator());
                        csvLoader.upsert(inputFile);
                    }
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
    public static final String SCHEMA_ATTRIB = "schema";

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

    public static PTable getTableNoCache(Connection conn, String name) throws SQLException {
        String schemaName = SchemaUtil.getSchemaNameFromFullName(name);
        String tableName = SchemaUtil.getTableNameFromFullName(name);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        MetaDataMutationResult result = new MetaDataClient(pconn).updateCache(pconn.getTenantId(),
                schemaName, tableName, true);
        if(result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
            throw new TableNotFoundException(schemaName, tableName);
        }

        return result.getTable();

    }
    /**
     * 
     * @param conn
     * @param name requires a pre-normalized table name or a pre-normalized schema and table name
     * @return
     * @throws SQLException
     */
    public static PTable getTable(Connection conn, String name) throws SQLException {
        PTable table = null;
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        try {
            table = pconn.getTable(new PTableKey(pconn.getTenantId(), name));
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
        PTable table = PhoenixRuntime.getTable(conn, SchemaUtil.normalizeFullTableName(tableName));
        List<ColumnInfo> columnInfoList = Lists.newArrayList();
        Set<String> unresolvedColumnNames = new TreeSet<String>();
        if (columns == null || columns.isEmpty()) {
            // use all columns in the table
        	int offset = (table.getBucketNum() == null ? 0 : 1);
        	for (int i = offset; i < table.getColumns().size(); i++) {
        	   PColumn pColumn = table.getColumns().get(i);
               columnInfoList.add(PhoenixRuntime.getColumnInfo(pColumn)); 
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
            pColumn = family.getPColumnForColumnName(familyColumn);
        } else {
            pColumn = table.getColumnForColumnName(columnName);
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
        if (pColumn == null) {
            throw new SQLException("pColumn must not be null.");
        }
        return ColumnInfo.create(pColumn.toString(), pColumn.getDataType().getSqlType(),
                pColumn.getMaxLength(), pColumn.getScale());
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
        private boolean isUpgrade;
        private boolean isBypassUpgrade;
        private boolean mapNamespace;
        private String srcTable;
        private boolean localIndexUpgrade;
        private String binaryEncoding;

        /**
         * Factory method to build up an {@code ExecutionCommand} based on supplied parameters.
         */
        public static ExecutionCommand parseArgs(String[] args) {
            Option tableOption = new Option("t", "table", true,
                    "Overrides the table into which the CSV data is loaded and is case sensitive");
            Option binaryEncodingOption = new Option("b", "binaryEncoding", true,
                    "Specifies binary encoding");
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
            Option upgradeOption = new Option("u", "upgrade", false, "Upgrades tables specified as arguments " +
                    "by rewriting them with the correct row key for descending columns. If no arguments are " +
                    "specified, then tables that need to be upgraded will be displayed without being upgraded. " +
                    "Use the -b option to bypass the rewrite if you know that your data does not need to be upgrade. " +
                    "This would only be the case if you have not relied on auto padding for BINARY and CHAR data, " +
                    "but instead have always provided data up to the full max length of the column. See PHOENIX-2067 " +
                    "and PHOENIX-2120 for more information. " +
                    "Note that " + QueryServices.THREAD_TIMEOUT_MS_ATTRIB + " and hbase.regionserver.lease.period " +
                    "parameters must be set very high to prevent timeouts when upgrading.");
            Option bypassUpgradeOption = new Option("b", "bypass-upgrade", false,
                    "Used in conjunction with the -u option to bypass the rewrite during upgrade if you know that your data does not need to be upgrade. " +
                    "This would only be the case if you have not relied on auto padding for BINARY and CHAR data, " +
                    "but instead have always provided data up to the full max length of the column. See PHOENIX-2067 " +
                    "and PHOENIX-2120 for more information. ");
            Option mapNamespaceOption = new Option("m", "map-namespace", true,
                    "Used to map table to a namespace matching with schema, require "+ QueryServices.IS_NAMESPACE_MAPPING_ENABLED +
                    " to be enabled");
            Option localIndexUpgradeOption = new Option("l", "local-index-upgrade", false,
                "Used to upgrade local index data by moving index data from separate table to "
                + "separate column families in the same table.");
            Options options = new Options();
            options.addOption(tableOption);
            options.addOption(headerOption);
            options.addOption(strictOption);
            options.addOption(delimiterOption);
            options.addOption(quoteCharacterOption);
            options.addOption(escapeCharacterOption);
            options.addOption(arrayValueSeparatorOption);
            options.addOption(upgradeOption);
            options.addOption(bypassUpgradeOption);
            options.addOption(mapNamespaceOption);
            options.addOption(localIndexUpgradeOption);
            options.addOption(binaryEncodingOption);

            CommandLineParser parser = new PosixParser();
            CommandLine cmdLine = null;
            try {
                cmdLine = parser.parse(options, args);
            } catch (ParseException e) {
                usageError(options);
            }

            ExecutionCommand execCmd = new ExecutionCommand();
            execCmd.connectionString = "";
            if(cmdLine.hasOption(mapNamespaceOption.getOpt())){
                execCmd.mapNamespace = true;
                execCmd.srcTable = validateTableName(cmdLine.getOptionValue(mapNamespaceOption.getOpt()));
            }
            if (cmdLine.hasOption(tableOption.getOpt())) {
                execCmd.tableName = cmdLine.getOptionValue(tableOption.getOpt());
            }
            
            if (cmdLine.hasOption(binaryEncodingOption.getOpt())) {
                execCmd.binaryEncoding = cmdLine.getOptionValue(binaryEncodingOption.getOpt());
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
            
            if (cmdLine.hasOption(upgradeOption.getOpt())) {
                execCmd.isUpgrade = true;
            }

            if (cmdLine.hasOption(bypassUpgradeOption.getOpt())) {
                if (!execCmd.isUpgrade()) {
                    usageError("The bypass-upgrade option may only be used in conjunction with the -u option", options);
                }
                execCmd.isBypassUpgrade = true;
            }
            if(cmdLine.hasOption(localIndexUpgradeOption.getOpt())) {
                execCmd.localIndexUpgrade = true;
            }

            List<String> argList = Lists.newArrayList(cmdLine.getArgList());
            if (argList.isEmpty()) {
                usageError("At least one input file must be supplied", options);
            }
            List<String> inputFiles = Lists.newArrayList();
            int i = 0;
            for (String arg : argList) {
                if (execCmd.isUpgrade || arg.endsWith(CSV_FILE_EXT) || arg.endsWith(SQL_FILE_EXT)) {
                    inputFiles.add(arg);
                } else {
                    if (i == 0) {
                        execCmd.connectionString = arg;
                    } else {
                        usageError("Don't know how to interpret argument '" + arg + "'", options);
                    }
                }
                i++;
            }

            if (inputFiles.isEmpty() && !execCmd.isUpgrade && !execCmd.isMapNamespace() && !execCmd.isLocalIndexUpgrade()) {
                usageError("At least one input file must be supplied", options);
            }

            execCmd.inputFiles = inputFiles;

            return execCmd;
        }

        private static String validateTableName(String tableName) {
            if (tableName.contains(QueryConstants.NAMESPACE_SEPARATOR)) {
                throw new IllegalArgumentException(
                        "tablename:" + tableName + " cannot have '" + QueryConstants.NAMESPACE_SEPARATOR + "' ");
            } else {
                return tableName;
            }

        }

        private static char getCharacter(String s) {
            String unescaped = StringEscapeUtils.unescapeJava(s);
            if (unescaped.length() > 1) {
                throw new IllegalArgumentException("Invalid single character: '" + unescaped + "'");
            }
            return unescaped.charAt(0);
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
                    "  psql my_ddl.sql\n" +
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

        public boolean isUpgrade() {
            return isUpgrade;
        }

        public boolean isBypassUpgrade() {
            return isBypassUpgrade;
        }

        public boolean isMapNamespace() {
            return mapNamespace;
        }

        public String getSrcTable() {
            return srcTable;
        }

        public boolean isLocalIndexUpgrade() {
            return localIndexUpgrade;
        }
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
    * Get the column family, column name pairs that make up the row key of the table that will be queried.
    * @param conn - connection used to generate the query plan. Caller should take care of closing the connection appropriately.
    * @param plan - query plan to get info for.
    * @return the pairs of column family name and column name columns in the data table that make up the row key for
    * the table used in the query plan. Column family names are optional and hence the first part of the pair is nullable.
    * Column names and family names are enclosed in double quotes to allow for case sensitivity and for presence of 
    * special characters. Salting column and view index id column are not included. If the connection is tenant specific 
    * and the table used by the query plan is multi-tenant, then the tenant id column is not included as well.
    * @throws SQLException
    */
    public static List<Pair<String, String>> getPkColsForSql(Connection conn, QueryPlan plan) throws SQLException {
        checkNotNull(plan);
        checkNotNull(conn);
        List<PColumn> pkColumns = getPkColumns(plan.getTableRef().getTable(), conn);
        List<Pair<String, String>> columns = Lists.newArrayListWithExpectedSize(pkColumns.size());
        String columnName;
        String familyName;
        for (PColumn pCol : pkColumns ) {
            columnName = addQuotes(pCol.getName().getString());
            familyName = pCol.getFamilyName() != null ? addQuotes(pCol.getFamilyName().getString()) : null;
            columns.add(new Pair<String, String>(familyName, columnName));
        }
        return columns;
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
    @Deprecated
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
     * @param dataTypes - Initialized empty list to be filled with the corresponding data type for the columns in @param columns.
     * @param plan - query plan to get info for
     * @param conn - phoenix connection used to generate the query plan. Caller should take care of closing the connection appropriately.
     * @param forDataTable - if true, then column names and data types correspond to the data table even if the query plan uses
     * the secondary index table. If false, and if the query plan uses the secondary index table, then the column names and data 
     * types correspond to the index table.
     * @throws SQLException
     */
    @Deprecated
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
        return getSqlTypeName(dataType, maxLength, scale);
    }

    public static String getSqlTypeName(PDataType dataType, Integer maxLength, Integer scale) {
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
    
    @Deprecated
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
    
    private static List<PColumn> getPkColumns(PTable ptable, Connection conn) throws SQLException {
        PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
        List<PColumn> pkColumns = ptable.getPKColumns();
        
        // Skip the salting column and the view index id column if present.
        // Skip the tenant id column too if the connection is tenant specific and the table used by the query plan is multi-tenant
        int offset = (ptable.getBucketNum() == null ? 0 : 1) + (ptable.isMultiTenant() && pConn.getTenantId() != null ? 1 : 0) + (ptable.getViewIndexId() == null ? 0 : 1);
        
        // get a sublist of pkColumns by skipping the offset columns.
        pkColumns = pkColumns.subList(offset, pkColumns.size());
        
        if (ptable.getType() == PTableType.INDEX) {
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
    @Deprecated
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
     * @param value byte value of the columns concatenated as a single byte array. @see {@link #encodeColumnValues(Connection, String, Object[], List)}
     * @param columns list of column names for the columns that have their respective values
     * present in the byte array. The column names should be in the same order as their values are in the byte array.
     * The column name includes both family name, if present, and column name.
     * @return decoded values for each column
     * @throws SQLException
     * 
     */
    @Deprecated
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
    public static byte[] encodeColumnValues(Connection conn, String fullTableName, Object[] values, List<Pair<String, String>> columns) throws SQLException {
        PTable table = getTable(conn, fullTableName);
        List<PColumn> pColumns = getColumns(table, columns);
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
     * @param value byte value of the columns concatenated as a single byte array. @see {@link #encodeColumnValues(Connection, String, Object[], List)}
     * @param columns list of column names for the columns that have their respective values
     * present in the byte array. The column names should be in the same order as their values are in the byte array.
     * The column name includes both family name, if present, and column name.
     * @return decoded values for each column
     * @throws SQLException
     * 
     */
    public static Object[] decodeColumnValues(Connection conn, String fullTableName, byte[] value, List<Pair<String, String>> columns) throws SQLException {
        PTable table = getTable(conn, fullTableName);
        KeyValueSchema kvSchema = buildKeyValueSchema(getColumns(table, columns));
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
    @Deprecated
    private static List<PColumn> getPColumns(PTable table, List<Pair<String, String>> columns) throws SQLException {
        List<PColumn> pColumns = new ArrayList<PColumn>(columns.size());
        for (Pair<String, String> column : columns) {
            pColumns.add(getPColumn(table, column.getFirst(), column.getSecond()));
        }
        return pColumns;
    }
    
    @Deprecated
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
            pColumn = family.getPColumnForColumnName(columnName);
        } else {
            pColumn = table.getColumnForColumnName(columnName);
        }
        return pColumn;
    }
    
    /**
     * @param table table to get the {@code PColumn} for
     * @param columns list of pair of column that includes column family as first part and column name as the second part.
     * Column family is optional and hence nullable. 
     * @return list of {@code PColumn} for fullyQualifiedColumnNames
     * @throws SQLException 
     */
    private static List<PColumn> getColumns(PTable table, List<Pair<String, String>> columns) throws SQLException {
        List<PColumn> pColumns = new ArrayList<PColumn>(columns.size());
        for (Pair<String, String> column : columns) {
            pColumns.add(getColumn(table, column.getFirst(), column.getSecond()));
        }
        return pColumns;
    }

    private static PColumn getColumn(PTable table, @Nullable String familyName, String columnName) throws SQLException {
        if (table==null) {
            throw new SQLException("Table must not be null.");
        }
        if (columnName==null) {
            throw new SQLException("columnName must not be null.");
        }
        // normalize and remove quotes from family and column names before looking up.
        familyName = SchemaUtil.normalizeIdentifier(familyName);
        columnName = SchemaUtil.normalizeIdentifier(columnName);
        // Column names are always for the data table, so we must translate them if
        // we're dealing with an index table.
        if (table.getType() == PTableType.INDEX) {
            columnName = IndexUtil.getIndexColumnName(familyName, columnName);
        }
        PColumn pColumn = null;
        if (familyName != null) {
            PColumnFamily family = table.getColumnFamily(familyName);
            pColumn = family.getPColumnForColumnName(columnName);
        } else {
            pColumn = table.getColumnForColumnName(columnName);
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
        return getFirstPKColumnExpression(table);
    }
    
    /**
     * Get expression that may be used to evaluate to the value of the first
     * column of a given row in a Phoenix table.
     * @param conn open Phoenix connection
     * @param fullTableName full table name
     * @return An expression that may be evaluated for a row in the provided table. 
     * @throws SQLException if the table name is not found, a TableNotFoundException
     * is thrown. If a local index is supplied a SQLFeatureNotSupportedException
     * is thrown.
     */
    public static Expression getFirstPKColumnExpression(Connection conn, String fullTableName) throws SQLException {
        PTable table = getTable(conn, fullTableName);
        return getFirstPKColumnExpression(table);
    }

    private static Expression getFirstPKColumnExpression(PTable table) throws SQLException {
        if (table.getIndexType() == IndexType.LOCAL) {
            /*
             * With some hackery, we could deduce the tenant ID from a multi-tenant local index,
             * however it's not clear that we'd want to maintain the same prefixing of the region
             * start key, as the region boundaries may end up being different on a cluster being
             * replicated/backed-up to (which is the use case driving the method).
             */
            throw new SQLFeatureNotSupportedException();
        }
        
        // skip salt and viewIndexId columns.
        int pkPosition = (table.getBucketNum() == null ? 0 : 1) + (table.getViewIndexId() == null ? 0 : 1);
        List<PColumn> pkColumns = table.getPKColumns();
        return new RowKeyColumnExpression(pkColumns.get(pkPosition), new RowKeyValueAccessor(pkColumns, pkPosition));
    }
    
    /**
     * Exposes the various internal phoenix metrics collected at the client JVM level. 
     */
    public static Collection<GlobalMetric> getGlobalPhoenixClientMetrics() {
        return GlobalClientMetrics.getMetrics();
    }
    
    /**
     * 
     * @return whether or not the global client metrics are being collected
     */
    public static boolean areGlobalClientMetricsBeingCollected() {
        return GlobalClientMetrics.isMetricsEnabled();
    }
    
    /**
     * Method to expose the metrics associated with performing reads using the passed result set. A typical pattern is:
     * 
     * <pre>
     * {@code
     * Map<String, Map<String, Long>> overAllQueryMetrics = null;
     * Map<String, Map<String, Long>> requestReadMetrics = null;
     * try (ResultSet rs = stmt.executeQuery()) {
     *    while(rs.next()) {
     *      .....
     *    }
     *    overAllQueryMetrics = PhoenixRuntime.getOverAllReadRequestMetrics(rs);
     *    requestReadMetrics = PhoenixRuntime.getRequestReadMetrics(rs);
     *    PhoenixRuntime.resetMetrics(rs);
     * }
     * </pre>
     * 
     * @param rs
     *            result set to get the metrics for
     * @return a map of (table name) -> (map of (metric name) -> (metric value))
     * @throws SQLException
     */
    public static Map<String, Map<String, Long>> getRequestReadMetrics(ResultSet rs) throws SQLException {
        PhoenixResultSet resultSet = rs.unwrap(PhoenixResultSet.class);
        return resultSet.getReadMetrics();
    }

    /**
     * Method to expose the overall metrics associated with executing a query via phoenix. A typical pattern of
     * accessing request level read metrics and overall read query metrics is:
     * 
     * <pre>
     * {@code
     * Map<String, Map<String, Long>> overAllQueryMetrics = null;
     * Map<String, Map<String, Long>> requestReadMetrics = null;
     * try (ResultSet rs = stmt.executeQuery()) {
     *    while(rs.next()) {
     *      .....
     *    }
     *    overAllQueryMetrics = PhoenixRuntime.getOverAllReadRequestMetrics(rs);
     *    requestReadMetrics = PhoenixRuntime.getRequestReadMetrics(rs);
     *    PhoenixRuntime.resetMetrics(rs);
     * }
     * </pre>
     * 
     * @param rs
     *            result set to get the metrics for
     * @return a map of metric name -> metric value
     * @throws SQLException
     */
    public static Map<String, Long> getOverAllReadRequestMetrics(ResultSet rs) throws SQLException {
        PhoenixResultSet resultSet = rs.unwrap(PhoenixResultSet.class);
        return resultSet.getOverAllRequestReadMetrics();
    }

    /**
     * Method to expose the metrics associated with sending over mutations to HBase. These metrics are updated when
     * commit is called on the passed connection. Mutation metrics are accumulated for the connection till
     * {@link #resetMetrics(Connection)} is called or the connection is closed. Example usage:
     * 
     * <pre>
     * {@code
     * Map<String, Map<String, Long>> mutationWriteMetrics = null;
     * Map<String, Map<String, Long>> mutationReadMetrics = null;
     * try (Connection conn = DriverManager.getConnection(url)) {
     *    conn.createStatement.executeUpdate(dml1);
     *    ....
     *    conn.createStatement.executeUpdate(dml2);
     *    ...
     *    conn.createStatement.executeUpdate(dml3);
     *    ...
     *    conn.commit();
     *    mutationWriteMetrics = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(conn);
     *    mutationReadMetrics = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(conn);
     *    PhoenixRuntime.resetMetrics(rs);
     * }
     * </pre>
     *  
     * @param conn
     *            connection to get the metrics for
     * @return a map of (table name) -> (map of (metric name) -> (metric value))
     * @throws SQLException
     */
    public static Map<String, Map<String, Long>> getWriteMetricsForMutationsSinceLastReset(Connection conn) throws SQLException {
        PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
        return pConn.getMutationMetrics();
    }

    /**
     * Method to expose the read metrics associated with executing a dml statement. These metrics are updated when
     * commit is called on the passed connection. Read metrics are accumulated till {@link #resetMetrics(Connection)} is
     * called or the connection is closed. Example usage:
     * 
     * <pre>
     * {@code
     * Map<String, Map<String, Long>> mutationWriteMetrics = null;
     * Map<String, Map<String, Long>> mutationReadMetrics = null;
     * try (Connection conn = DriverManager.getConnection(url)) {
     *    conn.createStatement.executeUpdate(dml1);
     *    ....
     *    conn.createStatement.executeUpdate(dml2);
     *    ...
     *    conn.createStatement.executeUpdate(dml3);
     *    ...
     *    conn.commit();
     *    mutationWriteMetrics = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(conn);
     *    mutationReadMetrics = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(conn);
     *    PhoenixRuntime.resetMetrics(rs);
     * }
     * </pre> 
     * @param conn
     *            connection to get the metrics for
     * @return  a map of (table name) -> (map of (metric name) -> (metric value))
     * @throws SQLException
     */
    public static Map<String, Map<String, Long>> getReadMetricsForMutationsSinceLastReset(Connection conn) throws SQLException {
        PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
        return pConn.getReadMetrics();
    }

    /**
     * Reset the read metrics collected in the result set.
     * 
     * @see {@link #getRequestReadMetrics(ResultSet)} {@link #getOverAllReadRequestMetrics(ResultSet)}
     * @param rs
     * @throws SQLException
     */
    public static void resetMetrics(ResultSet rs) throws SQLException {
        PhoenixResultSet prs = rs.unwrap(PhoenixResultSet.class);
        prs.resetMetrics();
    }
    
    /**
     * Reset the mutation and reads-for-mutations metrics collected in the connection.
     * 
     * @see {@link #getReadMetricsForMutationsSinceLastReset(Connection)} {@link #getWriteMetricsForMutationsSinceLastReset(Connection)}
     * @param conn
     * @throws SQLException
     */
    public static void resetMetrics(Connection conn) throws SQLException {
        PhoenixConnection pConn = conn.unwrap(PhoenixConnection.class);
        pConn.clearMetrics();
    }
    
    /**
     * Use this utility function to ensure that a timestamp is in milliseconds across transactional and
     * non transactional tables. This expects that the Cell timestamp is based either on wall clock
     * time or transaction manager nanos wall clock time.
     * @param tsOfCell Cell time stamp to be converted.
     * @return wall clock time in milliseconds (i.e. Epoch time) of a given Cell time stamp.
     */
    public static long getWallClockTimeFromCellTimeStamp(long tsOfCell) {
        return TxUtils.isPreExistingVersion(tsOfCell) ? tsOfCell : TransactionUtil.convertToMilliseconds(tsOfCell);
    }

    public static long getCurrentScn(ReadOnlyProps props) {
        String scn = props.get(CURRENT_SCN_ATTRIB);
        return scn != null ? Long.parseLong(scn) : HConstants.LATEST_TIMESTAMP;
    }
 }
