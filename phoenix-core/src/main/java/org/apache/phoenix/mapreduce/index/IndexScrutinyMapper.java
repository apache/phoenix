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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.PhoenixJobCounters;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.OutputFormat;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.SourceTable;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * Mapper that reads from the data table and checks the rows against the index table
 */
public class IndexScrutinyMapper extends Mapper<NullWritable, PhoenixIndexDBWritable, Text, Text> {

    private static final Logger LOG = LoggerFactory.getLogger(IndexScrutinyMapper.class);
    private Connection connection;
    private List<ColumnInfo> targetTblColumnMetadata;
    private long batchSize;
    // holds a batch of rows from the table the mapper is iterating over
    // Each row is a pair - the row TS, and the row values
    private List<Pair<Long, List<Object>>> currentBatchValues = new ArrayList<>();
    private String targetTableQuery;
    private int numTargetPkCols;
    private boolean outputInvalidRows;
    private OutputFormat outputFormat = OutputFormat.FILE;
    private String qSourceTable;
    private String qTargetTable;
    private long executeTimestamp;
    private int numSourcePkCols;
    private final PhoenixIndexDBWritable indxWritable = new PhoenixIndexDBWritable();
    private List<ColumnInfo> sourceTblColumnMetadata;

    // used to write results to the output table
    private Connection outputConn;
    private PreparedStatement outputUpsertStmt;
    private long outputMaxRows;
    private MessageDigest md5;

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
        final Configuration configuration = context.getConfiguration();
        try {
            // get a connection with correct CURRENT_SCN (so incoming writes don't throw off the
            // scrutiny)
            final Properties overrideProps = new Properties();
            String scn = configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
            overrideProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, scn);
            connection = ConnectionUtil.getOutputConnection(configuration, overrideProps);
            connection.setAutoCommit(false);
            batchSize = PhoenixConfigurationUtil.getScrutinyBatchSize(configuration);
            outputInvalidRows =
                    PhoenixConfigurationUtil.getScrutinyOutputInvalidRows(configuration);
            outputFormat = PhoenixConfigurationUtil.getScrutinyOutputFormat(configuration);
            executeTimestamp = PhoenixConfigurationUtil.getScrutinyExecuteTimestamp(configuration);

            // get the index table and column names
            String qDataTable = PhoenixConfigurationUtil.getScrutinyDataTableName(configuration);
            final PTable pdataTable = PhoenixRuntime.getTable(connection, qDataTable);
            final String qIndexTable =
                    PhoenixConfigurationUtil.getScrutinyIndexTableName(configuration);
            final PTable pindexTable = PhoenixRuntime.getTable(connection, qIndexTable);

            // set the target table based on whether we're running the MR over the data or index
            // table
            SourceTable sourceTable =
                    PhoenixConfigurationUtil.getScrutinySourceTable(configuration);
            SourceTargetColumnNames columnNames =
                    SourceTable.DATA_TABLE_SOURCE.equals(sourceTable)
                            ? new SourceTargetColumnNames.DataSourceColNames(pdataTable,
                                    pindexTable)
                            : new SourceTargetColumnNames.IndexSourceColNames(pdataTable,
                                    pindexTable);
            qSourceTable = columnNames.getQualifiedSourceTableName();
            qTargetTable = columnNames.getQualifiedTargetTableName();
            List<String> targetColNames = columnNames.getTargetColNames();
            List<String> sourceColNames = columnNames.getSourceColNames();
            List<String> targetPkColNames = columnNames.getTargetPkColNames();
            String targetPksCsv =
                    Joiner.on(",").join(SchemaUtil.getEscapedFullColumnNames(targetPkColNames));
            numSourcePkCols = columnNames.getSourcePkColNames().size();
            numTargetPkCols = targetPkColNames.size();

            if (outputInvalidRows && OutputFormat.TABLE.equals(outputFormat)) {
                outputConn = ConnectionUtil.getOutputConnection(configuration, new Properties());
                String upsertQuery = PhoenixConfigurationUtil.getUpsertStatement(configuration);
                this.outputUpsertStmt = outputConn.prepareStatement(upsertQuery);
            }
            outputMaxRows = PhoenixConfigurationUtil.getScrutinyOutputMax(configuration);

            // Create the query against the target table
            // Our query projection should be all the index column names (or their data table
            // equivalent
            // name)
            targetTableQuery =
                    QueryUtil.constructSelectStatement(qTargetTable, columnNames.getCastedTargetColNames(), targetPksCsv,
                        Hint.NO_INDEX, false) + " IN ";
            targetTblColumnMetadata =
                    PhoenixRuntime.generateColumnInfo(connection, qTargetTable, targetColNames);
            sourceTblColumnMetadata =
                    PhoenixRuntime.generateColumnInfo(connection, qSourceTable, sourceColNames);
            LOG.info("Target table base query: " + targetTableQuery);
            md5 = MessageDigest.getInstance("MD5");
        } catch (SQLException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void map(NullWritable key, PhoenixIndexDBWritable record, Context context)
            throws IOException, InterruptedException {
        try {
            final List<Object> values = record.getValues();

            context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);
            currentBatchValues.add(new Pair<>(record.getRowTs(), values));
            if (context.getCounter(PhoenixJobCounters.INPUT_RECORDS).getValue() % batchSize != 0) {
                // if we haven't hit the batch size, just report progress and move on to next record
                context.progress();
                return;
            } else {
                // otherwise, process the batch
                processBatch(context);
            }
            context.progress(); // Make sure progress is reported to Application Master.
        } catch (SQLException | IllegalArgumentException e) {
            LOG.error(" Error while read/write of a record ", e);
            context.getCounter(PhoenixJobCounters.FAILED_RECORDS).increment(1);
            throw new IOException(e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if (connection != null) {
            try {
                processBatch(context);
                connection.close();
                if (outputConn != null) {
                    outputConn.close();
                }
            } catch (SQLException e) {
                LOG.error("Error while closing connection in the PhoenixIndexMapper class ", e);
                throw new IOException(e);
            }
        }
    }

    private void processBatch(Context context)
            throws SQLException, IOException, InterruptedException {
        if (currentBatchValues.size() == 0) return;
        context.getCounter(PhoenixScrutinyJobCounters.BATCHES_PROCESSED_COUNT).increment(1);
        // our query selection filter should be the PK columns of the target table (index or data
        // table)
        String inClause =
                QueryUtil.constructParameterizedInClause(numTargetPkCols,
                    currentBatchValues.size());
        String indexQuery = targetTableQuery + inClause;
        try (PreparedStatement targetStatement = connection.prepareStatement(indexQuery)) {
            // while we build the PreparedStatement, we also maintain a hash of the target table
            // PKs,
            // which we use to join against the results of the query on the target table
            Map<String, Pair<Long, List<Object>>> targetPkToSourceValues =
                    buildTargetStatement(targetStatement);

            // fetch results from the target table and output invalid rows
            queryTargetTable(context, targetStatement, targetPkToSourceValues);

            // any source values we have left over are invalid (e.g. data table rows without
            // corresponding index row)
            context.getCounter(PhoenixScrutinyJobCounters.INVALID_ROW_COUNT)
                    .increment(targetPkToSourceValues.size());
            if (outputInvalidRows) {
                for (Pair<Long, List<Object>> sourceRowWithoutTargetRow : targetPkToSourceValues.values()) {
                    List<Object> valuesWithoutTarget = sourceRowWithoutTargetRow.getSecond();
                    if (OutputFormat.FILE.equals(outputFormat)) {
                        context.write(
                            new Text(Arrays.toString(valuesWithoutTarget.toArray())),
                            new Text("Target row not found"));
                    } else if (OutputFormat.TABLE.equals(outputFormat)) {
                        writeToOutputTable(context, valuesWithoutTarget, null, sourceRowWithoutTargetRow.getFirst(), -1L);
                    }
                }
            }
            if (outputInvalidRows && OutputFormat.TABLE.equals(outputFormat)) {
                outputUpsertStmt.executeBatch(); // write out invalid rows to output table
                outputConn.commit();
            }
            currentBatchValues.clear();
        }
    }

    private Map<String, Pair<Long, List<Object>>> buildTargetStatement(PreparedStatement targetStatement)
            throws SQLException {
        Map<String, Pair<Long, List<Object>>> targetPkToSourceValues =
                new HashMap<>(currentBatchValues.size());
        int rsIndex = 1;
        for (Pair<Long, List<Object>> batchTsRow : currentBatchValues) {
            List<Object> batchRow = batchTsRow.getSecond();
            // our original query against the source table (which provided the batchRow) projected
            // with the data table PK cols first, so the first numTargetPkCols form the PK
            String targetPkHash = getPkHash(batchRow.subList(0, numTargetPkCols));
            targetPkToSourceValues.put(targetPkHash, batchTsRow);
            for (int i = 0; i < numTargetPkCols; i++) {
                ColumnInfo targetPkInfo = targetTblColumnMetadata.get(i);
                Object value = batchRow.get(i);
                if (value == null) {
                    targetStatement.setNull(rsIndex++, targetPkInfo.getSqlType());
                } else {
                    targetStatement.setObject(rsIndex++, value, targetPkInfo.getSqlType());
                }
            }
        }
        return targetPkToSourceValues;
    }

    private void queryTargetTable(Context context, PreparedStatement targetStatement,
            Map<String, Pair<Long, List<Object>>> targetPkToSourceValues)
            throws SQLException, IOException, InterruptedException {
        ResultSet targetResultSet = targetStatement.executeQuery();

        while (targetResultSet.next()) {
            indxWritable.readFields(targetResultSet);
            List<Object> targetValues = indxWritable.getValues();
            // first grab the PK and try to join against the source input
            // the query is such that first numTargetPkCols of the resultSet is the PK
            List<Object> pkObjects = new ArrayList<>(numTargetPkCols);
            for (int i = 0; i < numTargetPkCols; i++) {
                Object pkPart = targetResultSet.getObject(i + 1);
                pkObjects.add(pkPart);
            }
            Long targetTS = targetResultSet.unwrap(PhoenixResultSet.class).getCurrentRow().getValue(0).getTimestamp();
            String targetPk = getPkHash(pkObjects);

            // use the pk to fetch the source table column values
            Pair<Long, List<Object>> sourceTsValues = targetPkToSourceValues.get(targetPk);

            Long sourceTS = sourceTsValues.getFirst();
            List<Object> sourceValues = sourceTsValues.getSecond();
            // compare values starting after the PK (i.e. covered columns)
            boolean isIndexedCorrectly =
                    compareValues(numTargetPkCols, targetValues, sourceValues, context);
            if (isIndexedCorrectly) {
                context.getCounter(PhoenixScrutinyJobCounters.VALID_ROW_COUNT).increment(1);
            } else {
                context.getCounter(PhoenixScrutinyJobCounters.INVALID_ROW_COUNT).increment(1);
                if (outputInvalidRows) {
                    outputInvalidRow(context, sourceValues, targetValues, sourceTS, targetTS);
                }
            }
            targetPkToSourceValues.remove(targetPk);
        }
    }

    private void outputInvalidRow(Context context, List<Object> sourceValues,
            List<Object> targetValues, long sourceTS, long targetTS) throws SQLException, IOException, InterruptedException {
        if (OutputFormat.FILE.equals(outputFormat)) {
            context.write(new Text(Arrays.toString(sourceValues.toArray())),
                new Text(Arrays.toString(targetValues.toArray())));
        } else if (OutputFormat.TABLE.equals(outputFormat)) {
            writeToOutputTable(context, sourceValues, targetValues, sourceTS, targetTS);
        }
    }

    // pass in null targetValues if the target row wasn't found
    private void writeToOutputTable(Context context, List<Object> sourceValues, List<Object> targetValues, long sourceTS, long targetTS)
            throws SQLException {
        if (context.getCounter(PhoenixScrutinyJobCounters.INVALID_ROW_COUNT).getValue() > outputMaxRows) {
            return;
        }
        int index = 1;
        outputUpsertStmt.setString(index++, qSourceTable); // SOURCE_TABLE
        outputUpsertStmt.setString(index++, qTargetTable); // TARGET_TABLE
        outputUpsertStmt.setLong(index++, executeTimestamp); // SCRUTINY_EXECUTE_TIME
        outputUpsertStmt.setString(index++, getPkHash(sourceValues.subList(0, numSourcePkCols))); // SOURCE_ROW_PK_HASH
        outputUpsertStmt.setLong(index++, sourceTS); // SOURCE_TS
        outputUpsertStmt.setLong(index++, targetTS); // TARGET_TS
        outputUpsertStmt.setBoolean(index++, targetValues != null); // HAS_TARGET_ROW
        index = setStatementObjects(sourceValues, index, sourceTblColumnMetadata);
        if (targetValues != null) {
            index = setStatementObjects(targetValues, index, targetTblColumnMetadata);
        } else { // for case where target row wasn't found, put nulls in prepared statement
            for (int i = 0; i < sourceValues.size(); i++) {
                outputUpsertStmt.setNull(index++, targetTblColumnMetadata.get(i).getSqlType());
            }
        }
        outputUpsertStmt.addBatch();
    }

    private int setStatementObjects(List<Object> values, int index, List<ColumnInfo> colMetadata)
            throws SQLException {
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            ColumnInfo colInfo = colMetadata.get(i);
            if (value != null) {
                outputUpsertStmt.setObject(index++, value, colInfo.getSqlType());
            } else {
                outputUpsertStmt.setNull(index++, colInfo.getSqlType());
            }
        }
        return index;
    }

    private boolean compareValues(int startIndex, List<Object> targetValues,
            List<Object> sourceValues, Context context) throws SQLException {
        if (targetValues == null || sourceValues == null) return false;
        for (int i = startIndex; i < sourceValues.size(); i++) {
            Object targetValue = targetValues.get(i);
            Object sourceValue = sourceValues.get(i);
            if (targetValue != null && !targetValue.equals(sourceValue)) {
                context.getCounter(PhoenixScrutinyJobCounters.BAD_COVERED_COL_VAL_COUNT)
                        .increment(1);
                return false;
            }
        }
        return true;
    }

    private String getPkHash(List<Object> pkObjects) {
        try {
            for (int i = 0; i < pkObjects.size(); i++) {
                md5.update(sourceTblColumnMetadata.get(i).getPDataType().toBytes(pkObjects.get(i)));
            }
            return Hex.encodeHexString(md5.digest());
        } finally {
            md5.reset();
        }
    }
}
