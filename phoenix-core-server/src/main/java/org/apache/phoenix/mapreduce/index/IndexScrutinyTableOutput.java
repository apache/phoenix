/**
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
package org.apache.phoenix.mapreduce.index;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhoenixJobCounters;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.SourceTable;
import org.apache.phoenix.mapreduce.index.SourceTargetColumnNames.DataSourceColNames;
import org.apache.phoenix.mapreduce.index.SourceTargetColumnNames.IndexSourceColNames;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;

import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;
import org.apache.phoenix.thirdparty.com.google.common.collect.Iterables;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 *
 * IndexScrutiny MapReduce output table DDL and methods to get queries against the output tables
 *
 */
public class IndexScrutinyTableOutput {

    /**
     * This table holds the invalid rows in the source table (either missing a target, or a bad
     * covered column value). Dynamic columns hold the original source and target table column data.
     */
    public static final String OUTPUT_TABLE_NAME = "PHOENIX_INDEX_SCRUTINY";
    public static final String SCRUTINY_EXECUTE_TIME_COL_NAME = "SCRUTINY_EXECUTE_TIME";
    public static final String TARGET_TABLE_COL_NAME = "TARGET_TABLE";
    public static final String SOURCE_TABLE_COL_NAME = "SOURCE_TABLE";
    public static final String OUTPUT_TABLE_DDL =
            "CREATE TABLE IF NOT EXISTS " + OUTPUT_TABLE_NAME + "\n" +
            "(\n" +
            "    " + SOURCE_TABLE_COL_NAME + " VARCHAR NOT NULL,\n" +
            "    " + TARGET_TABLE_COL_NAME + " VARCHAR NOT NULL,\n" +
            "    " + SCRUTINY_EXECUTE_TIME_COL_NAME + " BIGINT NOT NULL,\n" +
            "    SOURCE_ROW_PK_HASH VARCHAR NOT NULL,\n" +
            "    SOURCE_TS BIGINT,\n" +
            "    TARGET_TS BIGINT,\n" +
            "    HAS_TARGET_ROW BOOLEAN,\n" +
            "    BEYOND_MAX_LOOKBACK BOOLEAN,\n" +
            "    CONSTRAINT PK PRIMARY KEY\n" +
            "    (\n" +
            "        " + SOURCE_TABLE_COL_NAME + ",\n" +
            "        " + TARGET_TABLE_COL_NAME + ",\n" +
            "        " + SCRUTINY_EXECUTE_TIME_COL_NAME + ",\n" + // time at which the scrutiny ran
            "        SOURCE_ROW_PK_HASH\n" + //  this hash makes the PK unique
            "    )\n" + // dynamic columns consisting of the source and target columns will follow
            ")  COLUMN_ENCODED_BYTES = 0 "; //column encoding not supported with dyn columns (PHOENIX-5107)
    public static final String OUTPUT_TABLE_BEYOND_LOOKBACK_DDL = "" +
        "ALTER TABLE " + OUTPUT_TABLE_NAME + "\n" +
        " ADD IF NOT EXISTS BEYOND_MAX_LOOKBACK BOOLEAN";

    /**
     * This table holds metadata about a scrutiny job - result counters and queries to fetch invalid
     * row data from the output table. The queries contain the dynamic columns which are equivalent
     * to the original source/target table columns
     */
    public static final String OUTPUT_METADATA_TABLE_NAME = "PHOENIX_INDEX_SCRUTINY_METADATA";
    public static final String OUTPUT_METADATA_DDL =
            "CREATE TABLE IF NOT EXISTS " + OUTPUT_METADATA_TABLE_NAME + "\n" +
            "(\n" +
            "    " + SOURCE_TABLE_COL_NAME + " VARCHAR NOT NULL,\n" +
            "    " + TARGET_TABLE_COL_NAME + " VARCHAR NOT NULL,\n" +
            "    " + SCRUTINY_EXECUTE_TIME_COL_NAME + " BIGINT NOT NULL,\n" +
            "    SOURCE_TYPE VARCHAR,\n" + // source is either data or index table
            "    CMD_LINE_ARGS VARCHAR,\n" + // arguments the tool was run with
            "    INPUT_RECORDS BIGINT,\n" +
            "    FAILED_RECORDS BIGINT,\n" +
            "    VALID_ROW_COUNT BIGINT,\n" +
            "    INVALID_ROW_COUNT BIGINT,\n" +
            "    INCORRECT_COVERED_COL_VAL_COUNT BIGINT,\n" +
            "    BATCHES_PROCESSED_COUNT BIGINT,\n" +
            "    SOURCE_DYNAMIC_COLS VARCHAR,\n" +
            "    TARGET_DYNAMIC_COLS VARCHAR,\n" +
            "    INVALID_ROWS_QUERY_ALL VARCHAR,\n" + // stored sql query to fetch all the invalid rows from the output table
            "    INVALID_ROWS_QUERY_MISSING_TARGET VARCHAR,\n" +  // stored sql query to fetch all the invalid rows which are missing a target row
            "    INVALID_ROWS_QUERY_BAD_COVERED_COL_VAL VARCHAR,\n" + // stored sql query to fetch all the invalid rows which have bad covered column values
            "    INVALID_ROWS_QUERY_BEYOND_MAX_LOOKBACK VARCHAR,\n" + // stored sql query to fetch all the potentially invalid rows which are before max lookback age
            "    BEYOND_MAX_LOOKBACK_COUNT BIGINT,\n" +
            "    CONSTRAINT PK PRIMARY KEY\n" +
            "    (\n" +
            "        " + SOURCE_TABLE_COL_NAME + ",\n" +
            "        " + TARGET_TABLE_COL_NAME + ",\n" +
            "        " + SCRUTINY_EXECUTE_TIME_COL_NAME + "\n" +
            "    )\n" +
            ")\n";
    public static final String OUTPUT_METADATA_BEYOND_LOOKBACK_COUNTER_DDL = "" +
        "ALTER TABLE " + OUTPUT_METADATA_TABLE_NAME + "\n" +
        " ADD IF NOT EXISTS INVALID_ROWS_QUERY_BEYOND_MAX_LOOKBACK VARCHAR, \n" +
        " BEYOND_MAX_LOOKBACK_COUNT BIGINT";

    public static final String UPSERT_METADATA_SQL = "UPSERT INTO " + OUTPUT_METADATA_TABLE_NAME + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    /**
     * Gets the parameterized upsert sql to the output table Used by the scrutiny MR job to write
     * its results
     * @param sourceDynamicCols list of source columns with their types
     * @param targetDynamicCols list of target columns with their types
     * @param connection connection to use
     * @throws SQLException
     */
    public static String constructOutputTableUpsert(List<String> sourceDynamicCols,
            List<String> targetDynamicCols, Connection connection) throws SQLException {
        List<String> outputTableColumns = getOutputTableColumns(connection);

        // construct a dynamic column upsert into the output table
        List<String> upsertCols =
                Lists.newArrayList(
                    Iterables.concat(outputTableColumns, sourceDynamicCols, targetDynamicCols));
        String upsertStmt =
                QueryUtil.constructUpsertStatement(IndexScrutinyTableOutput.OUTPUT_TABLE_NAME,
                    upsertCols, null);
        return upsertStmt;
    }

    /**
     * Get the sql to store as INVALID_ROWS_QUERY_ALL in the output metadata table
     * @param conn
     * @param columnNames
     * @param scrutinyTimeMillis
     * @return
     * @throws SQLException
     */
    public static String getSqlQueryAllInvalidRows(Connection conn,
            SourceTargetColumnNames columnNames, long scrutinyTimeMillis) throws SQLException {
        String paramQuery = getAllInvalidParamQuery(conn, columnNames);
        paramQuery = bindPkCols(columnNames, scrutinyTimeMillis, paramQuery);
        return paramQuery;
    }

    /**
     * Get the sql to store as INVALID_ROWS_QUERY_MISSING_TARGET in the output metadata table
     * @param conn
     * @param columnNames
     * @param scrutinyTimeMillis
     * @return
     * @throws SQLException
     */
    public static String getSqlQueryMissingTargetRows(Connection conn,
            SourceTargetColumnNames columnNames, long scrutinyTimeMillis) throws SQLException {
        String paramQuery = getHasTargetRowQuery(conn, columnNames, scrutinyTimeMillis);
        return paramQuery.replaceFirst("\\?", "false");
    }

    /**
     * Get the sql to store as INVALID_ROWS_QUERY_BAD_COVERED_COL_VAL in the output metadata table
     * @param conn
     * @param columnNames
     * @param scrutinyTimeMillis
     * @return
     * @throws SQLException
     */
    public static String getSqlQueryBadCoveredColVal(Connection conn,
            SourceTargetColumnNames columnNames, long scrutinyTimeMillis) throws SQLException {
        String paramQuery = getHasTargetRowQuery(conn, columnNames, scrutinyTimeMillis);
        return paramQuery.replaceFirst("\\?", "true");
    }

    public static String getSqlQueryBeyondMaxLookback(Connection conn,
    SourceTargetColumnNames columnNames, long scrutinyTimeMillis) throws SQLException {
        String whereQuery =
            constructOutputTableQuery(conn, columnNames,
                getPksCsv() + ", " + SchemaUtil.getEscapedFullColumnName("HAS_TARGET_ROW")
                    + ", " + SchemaUtil.getEscapedFullColumnName("BEYOND_MAX_LOOKBACK"));
        String inClause =
            " IN " + QueryUtil.constructParameterizedInClause(getPkCols().size() + 2, 1);
        String paramQuery = whereQuery + inClause;
        paramQuery = bindPkCols(columnNames, scrutinyTimeMillis, paramQuery);
        paramQuery = paramQuery.replaceFirst("\\?", "false"); //has_target_row false
        paramQuery = paramQuery.replaceFirst("\\?", "true"); //beyond_max_lookback true
        return paramQuery;
    }

    /**
     * Query the metadata table for the given columns
     * @param conn connection to use
     * @param selectCols columns to select from the metadata table
     * @param qSourceTableName source table full name
     * @param qTargetTableName target table full name
     * @param scrutinyTimeMillis time when scrutiny was run
     * @return
     * @throws SQLException
     */
    public static ResultSet queryMetadata(Connection conn, List<String> selectCols,
            String qSourceTableName, String qTargetTableName, long scrutinyTimeMillis)
            throws SQLException {
        PreparedStatement ps = conn.prepareStatement(constructMetadataParamQuery(selectCols));
        ps.setString(1, qSourceTableName);
        ps.setString(2, qTargetTableName);
        ps.setLong(3, scrutinyTimeMillis);
        return ps.executeQuery();
    }

    public static ResultSet queryAllLatestMetadata(Connection conn, String qSourceTableName,
                                                   String qTargetTableName) throws SQLException {
        String sql = "SELECT MAX(" + SCRUTINY_EXECUTE_TIME_COL_NAME + ") " +
            "FROM " + OUTPUT_METADATA_TABLE_NAME +
            " WHERE " + SOURCE_TABLE_COL_NAME + " = ?" + " AND " + TARGET_TABLE_COL_NAME + "= ?";
        PreparedStatement stmt = conn.prepareStatement(sql);
        stmt.setString(1, qSourceTableName);
        stmt.setString(2, qTargetTableName);
        ResultSet rs = stmt.executeQuery();
        long scrutinyTimeMillis = 0L;
        if (rs.next()){
            scrutinyTimeMillis = rs.getLong(1);
        } //even if we didn't find one, still need to do a query to return the right columns
        return queryAllMetadata(conn, qSourceTableName, qTargetTableName, scrutinyTimeMillis);
    }

    /**
     * Query the metadata table for all columns
     * @param conn connection to use
     * @param qSourceTableName source table full name
     * @param qTargetTableName target table full name
     * @param scrutinyTimeMillis time when scrutiny was run
     * @return
     * @throws SQLException
     */
    public static ResultSet queryAllMetadata(Connection conn, String qSourceTableName,
            String qTargetTableName, long scrutinyTimeMillis) throws SQLException {
        PTable pMetadata = conn.unwrap(PhoenixConnection.class).getTable(
                OUTPUT_METADATA_TABLE_NAME);
        List<String> metadataCols = SchemaUtil.getColumnNames(pMetadata.getColumns());
        return queryMetadata(conn, metadataCols, qSourceTableName, qTargetTableName,
            scrutinyTimeMillis);
    }

    /**
     * Writes the results of the given jobs to the metadata table
     * @param conn connection to use
     * @param cmdLineArgs arguments the {@code IndexScrutinyTool} was run with
     * @param completedJobs completed MR jobs
     * @throws IOException
     * @throws SQLException
     */
    public static void writeJobResults(Connection conn, String[] cmdLineArgs, List<Job> completedJobs) throws IOException, SQLException {
        PreparedStatement pStmt = conn.prepareStatement(UPSERT_METADATA_SQL);
        for (Job job : completedJobs) {
            Configuration conf = job.getConfiguration();
            String qDataTable = PhoenixConfigurationUtil.getScrutinyDataTableName(conf);
            PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);
            final PTable pdataTable = phoenixConnection.getTable(qDataTable);
            final String qIndexTable = PhoenixConfigurationUtil.getScrutinyIndexTableName(conf);
            final PTable pindexTable = phoenixConnection.getTable(qIndexTable);
            SourceTable sourceTable = PhoenixConfigurationUtil.getScrutinySourceTable(conf);
            long scrutinyExecuteTime =
                    PhoenixConfigurationUtil.getScrutinyExecuteTimestamp(conf);
            SourceTargetColumnNames columnNames =
                    SourceTable.DATA_TABLE_SOURCE.equals(sourceTable)
                            ? new DataSourceColNames(pdataTable,
                                    pindexTable)
                            : new IndexSourceColNames(pdataTable,
                                    pindexTable);

            Counters counters = job.getCounters();
            int index = 1;
            pStmt.setString(index++, columnNames.getQualifiedSourceTableName());
            pStmt.setString(index++, columnNames.getQualifiedTargetTableName());
            pStmt.setLong(index++, scrutinyExecuteTime);
            pStmt.setString(index++, sourceTable.name());
            pStmt.setString(index++, Arrays.toString(cmdLineArgs));
            pStmt.setLong(index++, counters.findCounter(PhoenixJobCounters.INPUT_RECORDS).getValue());
            pStmt.setLong(index++, counters.findCounter(PhoenixJobCounters.FAILED_RECORDS).getValue());
            pStmt.setLong(index++, counters.findCounter(PhoenixScrutinyJobCounters.VALID_ROW_COUNT).getValue());
            pStmt.setLong(index++, counters.findCounter(PhoenixScrutinyJobCounters.INVALID_ROW_COUNT).getValue());
            pStmt.setLong(index++, counters.findCounter(PhoenixScrutinyJobCounters.BAD_COVERED_COL_VAL_COUNT).getValue());
            pStmt.setLong(index++, counters.findCounter(PhoenixScrutinyJobCounters.BATCHES_PROCESSED_COUNT).getValue());
            pStmt.setString(index++, Arrays.toString(columnNames.getSourceDynamicCols().toArray()));
            pStmt.setString(index++, Arrays.toString(columnNames.getTargetDynamicCols().toArray()));
            pStmt.setString(index++, getSqlQueryAllInvalidRows(conn, columnNames, scrutinyExecuteTime));
            pStmt.setString(index++, getSqlQueryMissingTargetRows(conn, columnNames, scrutinyExecuteTime));
            pStmt.setString(index++, getSqlQueryBadCoveredColVal(conn, columnNames, scrutinyExecuteTime));
            pStmt.setString(index++, getSqlQueryBeyondMaxLookback(conn, columnNames, scrutinyExecuteTime));
            pStmt.setLong(index++,
                counters.findCounter(PhoenixScrutinyJobCounters.BEYOND_MAX_LOOKBACK_COUNT).getValue());
            pStmt.addBatch();
        }
        pStmt.executeBatch();
        conn.commit();
    }

    /**
     * Get the parameterized query to return all the invalid rows from a scrutiny job
     */
    static String constructMetadataParamQuery(List<String> metadataSelectCols) {
        String pkColsCsv = getPksCsv();
        String query =
                QueryUtil.constructSelectStatement(OUTPUT_METADATA_TABLE_NAME, metadataSelectCols,
                    pkColsCsv, null, true);
        String inClause = " IN " + QueryUtil.constructParameterizedInClause(3, 1);
        return query + inClause;
    }

    private static String getAllInvalidParamQuery(Connection conn,
            SourceTargetColumnNames columnNames) throws SQLException {
        String whereQuery = constructOutputTableQuery(conn, columnNames, getPksCsv());
        String inClause = " IN " + QueryUtil.constructParameterizedInClause(getPkCols().size(), 1);
        String paramQuery = whereQuery + inClause;
        return paramQuery;
    }

    private static String bindPkCols(SourceTargetColumnNames columnNames, long scrutinyTimeMillis,
            String paramQuery) {
        paramQuery =
                paramQuery.replaceFirst("\\?",
                    "'" + columnNames.getQualifiedSourceTableName() + "'");
        paramQuery =
                paramQuery.replaceFirst("\\?",
                    "'" + columnNames.getQualifiedTargetTableName() + "'");
        paramQuery = paramQuery.replaceFirst("\\?", scrutinyTimeMillis + "");
        return paramQuery;
    }

    private static String getHasTargetRowQuery(Connection conn, SourceTargetColumnNames columnNames,
            long scrutinyTimeMillis) throws SQLException {
        String whereQuery =
                constructOutputTableQuery(conn, columnNames,
                    getPksCsv() + ", " + SchemaUtil.getEscapedFullColumnName("HAS_TARGET_ROW"));
        String inClause =
                " IN " + QueryUtil.constructParameterizedInClause(getPkCols().size() + 1, 1);
        String paramQuery = whereQuery + inClause;
        paramQuery = bindPkCols(columnNames, scrutinyTimeMillis, paramQuery);
        return paramQuery;
    }

    private static String getPksCsv() {
        String pkColsCsv = Joiner.on(",").join(SchemaUtil.getEscapedFullColumnNames(getPkCols()));
        return pkColsCsv;
    }

    private static List<String> getPkCols() {
        return Arrays.asList(SOURCE_TABLE_COL_NAME, TARGET_TABLE_COL_NAME,
            SCRUTINY_EXECUTE_TIME_COL_NAME);
    }

    private static String constructOutputTableQuery(Connection connection,
            SourceTargetColumnNames columnNames, String conditions) throws SQLException {
        PTable pOutputTable = connection.unwrap(PhoenixConnection.class).getTable(
                OUTPUT_TABLE_NAME);
        List<String> outputTableColumns = SchemaUtil.getColumnNames(pOutputTable.getColumns());
        List<String> selectCols =
                Lists.newArrayList(
                    Iterables.concat(outputTableColumns, columnNames.getUnqualifiedSourceColNames(),
                        columnNames.getUnqualifiedTargetColNames()));
        String dynamicCols =
                Joiner.on(",").join(Iterables.concat(columnNames.getSourceDynamicCols(),
                    columnNames.getTargetDynamicCols()));
        // dynamic defined after the table name
        // https://phoenix.apache.org/dynamic_columns.html
        String dynamicTableName = OUTPUT_TABLE_NAME + "(" + dynamicCols + ")";
        return QueryUtil.constructSelectStatement(dynamicTableName, selectCols, conditions, null, true);
    }

    private static List<String> getOutputTableColumns(Connection connection) throws SQLException {
        PTable pOutputTable =
                connection.unwrap(PhoenixConnection.class).getTable(OUTPUT_TABLE_NAME);
        List<String> outputTableColumns = SchemaUtil.getColumnNames(pOutputTable.getColumns());
        return outputTableColumns;
    }

}
