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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.phoenix.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.compat.hbase.coprocessor.CompatBaseScannerRegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.PhoenixJobCounters;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.OutputFormat;
import org.apache.phoenix.mapreduce.index.IndexScrutinyTool.SourceTable;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.parse.HintNode.Hint;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableType;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Joiner;

/**
 * Mapper that reads from the data table and checks the rows against the index table
 */
public class IndexScrutinyMapper extends Mapper<NullWritable, PhoenixIndexDBWritable, Text, Text> {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexScrutinyMapper.class);
    protected Connection connection;
    private List<ColumnInfo> targetTblColumnMetadata;
    private long batchSize;
    // holds a batch of rows from the table the mapper is iterating over
    // Each row is a pair - the row TS, and the row values
    protected List<Pair<Long, List<Object>>> currentBatchValues = new ArrayList<>();
    protected String targetTableQuery;
    protected int numTargetPkCols;
    protected boolean outputInvalidRows;
    protected OutputFormat outputFormat = OutputFormat.FILE;
    private String qSourceTable;
    private String qTargetTable;
    private long executeTimestamp;
    private int numSourcePkCols;
    private final PhoenixIndexDBWritable indxWritable = new PhoenixIndexDBWritable();
    private List<ColumnInfo> sourceTblColumnMetadata;

    // used to write results to the output table
    protected Connection outputConn;
    protected PreparedStatement outputUpsertStmt;
    private long outputMaxRows;
    private MessageDigest md5;
    private long ttl;
    private long scnTimestamp;
    private long maxLookbackAgeMillis;

    protected long getScrutinyTs(){
        return scnTimestamp;
    }

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
            scnTimestamp = Long.parseLong(scn);
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
            LOGGER.info("Target table base query: " + targetTableQuery);
            md5 = MessageDigest.getInstance("MD5");
            ttl = getTableTtl();
            maxLookbackAgeMillis = CompatBaseScannerRegionObserver.getMaxLookbackInMillis(configuration);
        } catch (SQLException | NoSuchAlgorithmException e) {
            tryClosingResourceSilently(this.outputUpsertStmt);
            tryClosingResourceSilently(this.connection);
            tryClosingResourceSilently(this.outputConn);
            throw new RuntimeException(e);
        }
        postSetup();
    }

    protected void postSetup() {

    }

    private static void tryClosingResourceSilently(AutoCloseable res) {
        if (res != null) {
            try {
                res.close();
            } catch (Exception e) {
                LOGGER.error("Closing resource: " + res + " failed :", e);
            }
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
            LOGGER.error(" Error while read/write of a record ", e);
            context.getCounter(PhoenixJobCounters.FAILED_RECORDS).increment(1);
            throw new IOException(e);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        tryClosingResourceSilently(this.outputUpsertStmt);
        IOException throwException = null;
        if (connection != null) {
            try {
                processBatch(context);
                connection.close();
            } catch (SQLException e) {
                LOGGER.error("Error while closing connection in the PhoenixIndexMapper class ", e);
                throwException = new IOException(e);
            }
        }
        tryClosingResourceSilently(this.outputConn);
        if (throwException != null) {
            throw throwException;
        }
    }

    protected void processBatch(Context context)
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

            preQueryTargetTable();
            // fetch results from the target table and output invalid rows
            queryTargetTable(context, targetStatement, targetPkToSourceValues);

            //check if any invalid rows are just temporary side effects of ttl or compaction,
            //and if so remove them from the list and count them as separate metrics
            categorizeInvalidRows(context, targetPkToSourceValues);

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

    protected void preQueryTargetTable() { }

    protected void categorizeInvalidRows(Context context,
                                         Map<String, Pair<Long,
            List<Object>>> targetPkToSourceValues) {
        Set<Map.Entry<String, Pair<Long, List<Object>>>>
                entrySet = targetPkToSourceValues.entrySet();

        Iterator<Map.Entry<String, Pair<Long, List<Object>>>> itr = entrySet.iterator();

        // iterate and remove items simultaneously
        while(itr.hasNext()) {
            Map.Entry<String, Pair<Long, List<Object>>> entry = itr.next();
            Pair<Long, List<Object>> sourceValues = entry.getValue();
            Long sourceTS = sourceValues.getFirst();
            if (hasRowExpiredOnSource(sourceTS, ttl)) {
                context.getCounter(PhoenixScrutinyJobCounters.EXPIRED_ROW_COUNT).increment(1L);
                itr.remove(); //don't output to the scrutiny table
            } else if (isRowOlderThanMaxLookback(sourceTS)) {
                context.getCounter(PhoenixScrutinyJobCounters.BEYOND_MAX_LOOKBACK_COUNT).increment(1L);
                //still output to the scrutiny table just in case it's useful
            } else {
                // otherwise it's invalid (e.g. data table rows without corresponding index row)
                context.getCounter(PhoenixScrutinyJobCounters.INVALID_ROW_COUNT)
                    .increment(1L);
            }
        }
    }

    protected boolean hasRowExpiredOnSource(Long sourceTS, Long ttl) {
        long currentTS = EnvironmentEdgeManager.currentTimeMillis();
        return ttl != Integer.MAX_VALUE && sourceTS + ttl*1000 < currentTS;
    }

    protected boolean isRowOlderThanMaxLookback(Long sourceTS){
        if (maxLookbackAgeMillis == CompatBaseScannerRegionObserver.DEFAULT_PHOENIX_MAX_LOOKBACK_AGE * 1000){
            return false;
        }
        long now =  EnvironmentEdgeManager.currentTimeMillis();
        long maxLookBackTimeMillis = now - maxLookbackAgeMillis;
        return sourceTS <= maxLookBackTimeMillis;
    }

    private int getTableTtl() throws SQLException, IOException {
        PTable pSourceTable = PhoenixRuntime.getTable(connection, qSourceTable);
        if (pSourceTable.getType() == PTableType.INDEX
                && pSourceTable.getIndexType() == PTable.IndexType.LOCAL) {
            return Integer.MAX_VALUE;
        }
        ConnectionQueryServices
                cqsi = connection.unwrap(PhoenixConnection.class).getQueryServices();
        String physicalTable = getSourceTableName(pSourceTable,
                SchemaUtil.isNamespaceMappingEnabled(null, cqsi.getProps()));
        TableDescriptor tableDesc;
        try (Admin admin = cqsi.getAdmin()) {
            tableDesc = admin.getDescriptor(TableName
                .valueOf(physicalTable));
        }
        return tableDesc.getColumnFamily(SchemaUtil.getEmptyColumnFamily(pSourceTable)).getTimeToLive();
    }

    @VisibleForTesting
    public static String getSourceTableName(PTable pSourceTable, boolean isNamespaceEnabled) {
        String sourcePhysicalName = pSourceTable.getPhysicalName().getString();
        String physicalTable, table, schema;
        if (pSourceTable.getType() == PTableType.VIEW
                || MetaDataUtil.isViewIndex(sourcePhysicalName)) {
            // in case of view and view index ptable, getPhysicalName() returns hbase tables
            // i.e. without _IDX_ and with _IDX_ respectively
            physicalTable = sourcePhysicalName;
        } else {
            table = pSourceTable.getTableName().toString();
            schema = pSourceTable.getSchemaName().toString();
            physicalTable = SchemaUtil
                    .getPhysicalHBaseTableName(schema, table, isNamespaceEnabled).toString();
        }
        return physicalTable;
    }

    protected Map<String, Pair<Long, List<Object>>> buildTargetStatement(PreparedStatement targetStatement)
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

    protected void queryTargetTable(Context context, PreparedStatement targetStatement,
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
    protected void writeToOutputTable(Context context, List<Object> sourceValues, List<Object> targetValues, long sourceTS, long targetTS)
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
        outputUpsertStmt.setBoolean(index++, isRowOlderThanMaxLookback(sourceTS));
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
            if (sourceValue == null && targetValue == null) {
                continue;
            } else if (sourceValue != null && targetValue != null) {
                if (sourceValue.getClass().isArray()) {
                    if (compareArrayTypes(sourceValue, targetValue)) {
                        continue;
                    }
                } else {
                    if (targetValue.equals(sourceValue)) {
                        continue;
                    }
                }
            } 
            context.getCounter(PhoenixScrutinyJobCounters.BAD_COVERED_COL_VAL_COUNT).increment(1);
            return false;
        }
        return true;
    }

    private boolean compareArrayTypes(Object sourceValue, Object targetValue) {
        if (sourceValue.getClass().getComponentType().equals(byte.class)) {
            return Arrays.equals((byte[]) sourceValue, (byte[]) targetValue);
        } else if (sourceValue.getClass().getComponentType().equals(char.class)) {
            return Arrays.equals((char[]) sourceValue, (char[]) targetValue);
        } else if (sourceValue.getClass().getComponentType().equals(boolean.class)) {
            return Arrays.equals((boolean[]) sourceValue, (boolean[]) targetValue);
        } else if (sourceValue.getClass().getComponentType().equals(double.class)) {
            return Arrays.equals((double[]) sourceValue, (double[]) targetValue);
        } else if (sourceValue.getClass().getComponentType().equals(int.class)) {
            return Arrays.equals((int[]) sourceValue, (int[]) targetValue);
        } else if (sourceValue.getClass().getComponentType().equals(short.class)) {
            return Arrays.equals((short[]) sourceValue, (short[]) targetValue);
        } else if (sourceValue.getClass().getComponentType().equals(long.class)) {
            return Arrays.equals((long[]) sourceValue, (long[]) targetValue);
        } else if (sourceValue.getClass().getComponentType().equals(float.class)) {
            return Arrays.equals((float[]) sourceValue, (float[]) targetValue);
        }
        return false;
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
