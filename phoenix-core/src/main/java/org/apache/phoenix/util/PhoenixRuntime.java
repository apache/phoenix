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
    
    private static final String TABLE_OPTION = "-t";
    private static final String HEADER_OPTION = "-h";
    private static final String STRICT_OPTION = "-s";
    private static final String CSV_OPTION = "-d";
    private static final String ARRAY_ELEMENT_SEP_OPTION = "-a";
    private static final String HEADER_IN_LINE = "in-line";
    private static final String SQL_FILE_EXT = ".sql";
    private static final String CSV_FILE_EXT = ".csv";

    private static void usageError() {
        System.err.println("Usage: psql [-t table-name] [-h comma-separated-column-names | in-line] [-d field-delimiter-char quote-char escape-char]<zookeeper>  <path-to-sql-or-csv-file>...\n" +
                "  By default, the name of the CSV file (case insensitive) is used to determine the Phoenix table into which the CSV data is loaded\n" +
                "  and the ordinal value of the columns determines the mapping.\n" +
                "  -t overrides the table into which the CSV data is loaded and is case sensitive.\n" +
                "  -h overrides the column names to which the CSV data maps and is case sensitive.\n" +
                "     A special value of in-line indicating that the first line of the CSV file\n" +
                "     determines the column to which the data maps.\n" +
                "  -s uses strict mode by throwing an exception if a column name doesn't match during CSV loading.\n" +
                "  -d uses custom delimiters for CSV loader, need to specify single char for field delimiter, phrase delimiter, and escape char.\n" +
                "     number is NOT usually a delimiter and shall be taken as 1 -> ctrl A, 2 -> ctrl B ... 9 -> ctrl I. \n" +
                "  -a define the array element separator, defaults to ':'\n" +
                "Examples:\n" +
                "  psql localhost my_ddl.sql\n" +
                "  psql localhost my_ddl.sql my_table.csv\n" +
                "  psql -t MY_TABLE my_cluster:1825 my_table2012-Q3.csv\n" +
                "  psql -t MY_TABLE -h COL1,COL2,COL3 my_cluster:1825 my_table2012-Q3.csv\n" +
                "  psql -t MY_TABLE -h COL1,COL2,COL3 -d 1 2 3 my_cluster:1825 my_table2012-Q3.csv\n"
        );
        System.exit(-1);
    }
    /**
     * Provides a mechanism to run SQL scripts against, where the arguments are:
     * 1) connection URL string
     * 2) one or more paths to either SQL scripts or CSV files
     * If a CurrentSCN property is set on the connection URL, then it is incremented
     * between processing, with each file being processed by a new connection at the
     * increment timestamp value.
     */
    public static void main(String [] args) {
        if (args.length < 2) {
            usageError();
        }
        PhoenixConnection conn = null;
        try {
            String tableName = null;
            List<String> columns = null;
            boolean isStrict = false;
            String arrayElementSeparator = CSVCommonsLoader.DEFAULT_ARRAY_ELEMENT_SEPARATOR;
            List<String> customMetaCharacters = new ArrayList<String>();

            int i = 0;
            for (; i < args.length; i++) {
                if (TABLE_OPTION.equals(args[i])) {
                    if (++i == args.length || tableName != null) {
                        usageError();
                    }
                    tableName = args[i];
                } else if (HEADER_OPTION.equals(args[i])) {
                    if (++i >= args.length || columns != null) {
                        usageError();
                    }
                    String header = args[i];
                    if (HEADER_IN_LINE.equals(header)) {
                        columns = Collections.emptyList();
                    } else {
                        columns = Lists.newArrayList();
                        StringTokenizer tokenizer = new StringTokenizer(header,",");
                        while(tokenizer.hasMoreTokens()) {
                            columns.add(tokenizer.nextToken());
                        }
                    }
                } else if (STRICT_OPTION.equals(args[i])) {
                    isStrict = true;
                } else if (CSV_OPTION.equals(args[i])) {
                    for(int j=0; j < 3; j++) {
                        if(args[++i].length()==1){
                            customMetaCharacters.add(args[i]);
                        } else {
                            usageError();
                        }
                    }
                } else if (ARRAY_ELEMENT_SEP_OPTION.equals(args[i])) {
                    arrayElementSeparator = args[++i];
                } else {
                    break;
                }
            }
            if (i == args.length) {
                usageError();
            }
            
            Properties props = new Properties();
            String connectionUrl = JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + args[i++];
            conn = DriverManager.getConnection(connectionUrl, props).unwrap(PhoenixConnection.class);
            
            for (; i < args.length; i++) {
                String fileName = args[i];
                if (fileName.endsWith(SQL_FILE_EXT)) {
               		PhoenixRuntime.executeStatements(conn, new FileReader(args[i]), Collections.emptyList());
                } else if (fileName.endsWith(CSV_FILE_EXT)) {
                    if (tableName == null) {
                        tableName = SchemaUtil.normalizeIdentifier(fileName.substring(fileName.lastIndexOf(File.separatorChar) + 1, fileName.length()-CSV_FILE_EXT.length()));
                    }
                    CSVCommonsLoader csvLoader = 
                    		new CSVCommonsLoader(conn, tableName, columns, isStrict, customMetaCharacters, arrayElementSeparator);
                    csvLoader.upsert(fileName);
                } else {
                    usageError();
                }
                Long scn = conn.getSCN();
                // If specifying SCN, increment it between processing files to allow
                // for later files to see earlier files tables.
                if (scn != null) {
                    scn++;
                    props.setProperty(PhoenixRuntime.CURRENT_SCN_ATTRIB, scn.toString());
                    conn.close();
                    conn = DriverManager.getConnection(connectionUrl, props).unwrap(PhoenixConnection.class);
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        } finally {
            if(conn != null) {
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
     * @param fullTableName the full table name
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
}
