/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.workload;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.PherfConstants.GeneratePhoenixStats;
import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.WriteParams;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.exception.PherfException;
import org.apache.phoenix.pherf.result.DataLoadThreadTime;
import org.apache.phoenix.pherf.result.DataLoadTimeSummary;
import org.apache.phoenix.pherf.result.ResultUtil;
import org.apache.phoenix.pherf.rules.DataValue;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.pherf.util.PhoenixUtil;
import org.apache.phoenix.pherf.util.RowCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteWorkload implements Workload {
    private static final Logger logger = LoggerFactory.getLogger(WriteWorkload.class);

    public static final String USE_BATCH_API_PROPERTY = "pherf.default.dataloader.batchApi";

    private final PhoenixUtil pUtil;
    private final XMLConfigParser parser;
    private final RulesApplier rulesApplier;
    private final ResultUtil resultUtil;
    private final ExecutorService pool;
    private final WriteParams writeParams;
    private final Scenario scenario;
    private final long threadSleepDuration;

    private final int threadPoolSize;
    private final int batchSize;
    private final GeneratePhoenixStats generateStatistics;
    private final boolean useBatchApi;

    public WriteWorkload(XMLConfigParser parser) throws Exception {
        this(PhoenixUtil.create(), parser, GeneratePhoenixStats.NO);
    }
    
    public WriteWorkload(XMLConfigParser parser, GeneratePhoenixStats generateStatistics) throws Exception {
        this(PhoenixUtil.create(), parser, generateStatistics);
    }

    public WriteWorkload(PhoenixUtil util, XMLConfigParser parser, GeneratePhoenixStats generateStatistics) throws Exception {
        this(util, parser, null, generateStatistics);
    }

    public WriteWorkload(PhoenixUtil phoenixUtil, XMLConfigParser parser, Scenario scenario, GeneratePhoenixStats generateStatistics)
            throws Exception {
        this(phoenixUtil, PherfConstants.create().getProperties(PherfConstants.PHERF_PROPERTIES,
                false),
                parser, scenario, generateStatistics);
    }

    /**
     * Default the writers to use up all available cores for threads. If writeParams are used in
     * the config files, they will override the defaults. writeParams are used for read/write mixed
     * workloads.
     * TODO extract notion of the scenario list and have 1 write workload per scenario
     *
     * @param phoenixUtil {@link org.apache.phoenix.pherf.util.PhoenixUtil} Query helper
     * @param properties  {@link java.util.Properties} default properties to use
     * @param parser      {@link org.apache.phoenix.pherf.configuration.XMLConfigParser}
     * @param scenario    {@link org.apache.phoenix.pherf.configuration.Scenario} If null is passed
     *                    it will run against all scenarios in the parsers list.
     * @throws Exception
     */
    public WriteWorkload(PhoenixUtil phoenixUtil, Properties properties, XMLConfigParser parser,
            Scenario scenario, GeneratePhoenixStats generateStatistics) throws Exception {
        this.pUtil = phoenixUtil;
        this.parser = parser;
        this.rulesApplier = new RulesApplier(parser);
        this.resultUtil = new ResultUtil();
        this.generateStatistics = generateStatistics;

        // Overwrite defaults properties with those given in the configuration. This indicates the
        // scenario is a R/W mixed workload.
        if (scenario != null) {
            this.scenario = scenario;
            writeParams = scenario.getWriteParams();
            threadSleepDuration = writeParams.getThreadSleepDuration();
        } else {
            writeParams = null;
            this.scenario = null;
            threadSleepDuration = 0;
        }

        int size = Integer.parseInt(properties.getProperty("pherf.default.dataloader.threadpool"));

        // Should addBatch/executeBatch be used? Default: false
        this.useBatchApi = Boolean.getBoolean(USE_BATCH_API_PROPERTY);

        this.threadPoolSize = (size == 0) ? Runtime.getRuntime().availableProcessors() : size;

        // TODO Move pool management up to WorkloadExecutor
        this.pool = Executors.newFixedThreadPool(this.threadPoolSize);

        String
                bSize =
                (writeParams == null) || (writeParams.getBatchSize() == Long.MIN_VALUE) ?
                        properties.getProperty("pherf.default.dataloader.batchsize") :
                        String.valueOf(writeParams.getBatchSize());
        this.batchSize =
                (bSize == null) ? PherfConstants.DEFAULT_BATCH_SIZE : Integer.parseInt(bSize);
    }

    @Override public void complete() {
        pool.shutdownNow();
    }

    public Runnable execute() throws Exception {
        return new Runnable() {
            @Override public void run() {
                try {
                    DataLoadTimeSummary dataLoadTimeSummary = new DataLoadTimeSummary();
                    DataLoadThreadTime dataLoadThreadTime = new DataLoadThreadTime();

                    if (WriteWorkload.this.scenario == null) {
                        for (Scenario scenario : getParser().getScenarios()) {
                            exec(dataLoadTimeSummary, dataLoadThreadTime, scenario);
                        }
                    } else {
                        exec(dataLoadTimeSummary, dataLoadThreadTime, WriteWorkload.this.scenario);
                    }
                    resultUtil.write(dataLoadTimeSummary);
                    resultUtil.write(dataLoadThreadTime);

                } catch (Exception e) {
                    logger.warn("", e);
                }
            }
        };
    }

    private synchronized void exec(DataLoadTimeSummary dataLoadTimeSummary,
            DataLoadThreadTime dataLoadThreadTime, Scenario scenario) throws Exception {
        logger.info("\nLoading " + scenario.getRowCount() + " rows for " + scenario.getTableName());
        long start = System.currentTimeMillis();
        
        // Execute any Scenario DDL before running workload
        pUtil.executeScenarioDdl(scenario);
        
        List<Future<Info>> writeBatches = getBatches(dataLoadThreadTime, scenario);

        waitForBatches(dataLoadTimeSummary, scenario, start, writeBatches);

        // Update Phoenix Statistics
        if (this.generateStatistics == GeneratePhoenixStats.YES) {
        	logger.info("Updating Phoenix table statistics...");
        	pUtil.updatePhoenixStats(scenario.getTableName(), scenario);
        	logger.info("Stats update done!");
        } else {
        	logger.info("Phoenix table stats update not requested.");
        }
    }

    private List<Future<Info>> getBatches(DataLoadThreadTime dataLoadThreadTime, Scenario scenario)
            throws Exception {
        RowCalculator
                rowCalculator =
                new RowCalculator(getThreadPoolSize(), scenario.getRowCount());
        List<Future<Info>> writeBatches = new ArrayList<>();

        for (int i = 0; i < getThreadPoolSize(); i++) {
            List<Column>
                    phxMetaCols =
                    pUtil.getColumnsFromPhoenix(scenario.getSchemaName(),
                            scenario.getTableNameWithoutSchemaName(), pUtil.getConnection(scenario.getTenantId()));
            int threadRowCount = rowCalculator.getNext();
            logger.info(
                    "Kick off thread (#" + i + ")for upsert with (" + threadRowCount + ") rows.");
            Future<Info>
                    write =
                    upsertData(scenario, phxMetaCols, scenario.getTableName(), threadRowCount,
                            dataLoadThreadTime, this.useBatchApi);
            writeBatches.add(write);
        }
        if (writeBatches.isEmpty()) {
            throw new PherfException(
                    "Holy shit snacks! Throwing up hands in disbelief and exiting. Could not write data for some unknown reason.");
        }

        return writeBatches;
    }

    private void waitForBatches(DataLoadTimeSummary dataLoadTimeSummary, Scenario scenario,
            long start, List<Future<Info>> writeBatches)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        int sumRows = 0, sumDuration = 0;
        // Wait for all the batch threads to complete
        for (Future<Info> write : writeBatches) {
            Info writeInfo = write.get();
            sumRows += writeInfo.getRowCount();
            sumDuration += writeInfo.getDuration();
            logger.info("Executor (" + this.hashCode() + ") writes complete with row count ("
                    + writeInfo.getRowCount() + ") in Ms (" + writeInfo.getDuration() + ")");
        }
        long testDuration = System.currentTimeMillis() - start;
        logger.info("Writes completed with total row count (" + sumRows
                + ") with total elapsed time of (" + testDuration
                + ") ms and total CPU execution time of (" + sumDuration + ") ms");
        dataLoadTimeSummary
                .add(scenario.getTableName(), sumRows, (int) testDuration);
    }

    public Future<Info> upsertData(final Scenario scenario, final List<Column> columns,
            final String tableName, final int rowCount,
            final DataLoadThreadTime dataLoadThreadTime, final boolean useBatchApi) {
        Future<Info> future = pool.submit(new Callable<Info>() {
            @Override public Info call() throws Exception {
                int rowsCreated = 0;
                long start = 0, last = 0, duration, totalDuration;
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Connection connection = null;
                PreparedStatement stmt = null;
                try {
                    connection = pUtil.getConnection(scenario.getTenantId());
                    long logStartTime = System.currentTimeMillis();
                    long
                            maxDuration =
                            (WriteWorkload.this.writeParams == null) ?
                                    Long.MAX_VALUE :
                                    WriteWorkload.this.writeParams.getExecutionDurationInMs();

                    last = start = System.currentTimeMillis();
                    String sql = buildSql(columns, tableName);
                    stmt = connection.prepareStatement(sql);
                    for (long i = rowCount; (i > 0) && ((System.currentTimeMillis() - logStartTime)
                            < maxDuration); i--) {
                        stmt = buildStatement(scenario, columns, stmt, simpleDateFormat);
                        if (useBatchApi) {
                            stmt.addBatch();
                        } else {
                            rowsCreated += stmt.executeUpdate();
                        }
                        if ((i % getBatchSize()) == 0) {
                            if (useBatchApi) {
                                int[] results = stmt.executeBatch();
                                for (int x = 0; x < results.length; x++) {
                                    int result = results[x];
                                    if (result < 1) {
                                        final String msg =
                                            "Failed to write update in batch (update count="
                                                + result + ")";
                                        throw new RuntimeException(msg);
                                    }
                                    rowsCreated += result;
                                }
                            }
                            connection.commit();
                            duration = System.currentTimeMillis() - last;
                            logger.info("Writer (" + Thread.currentThread().getName()
                                    + ") committed Batch. Total " + getBatchSize()
                                    + " rows for this thread (" + this.hashCode() + ") in ("
                                    + duration + ") Ms");

                            if (i % PherfConstants.LOG_PER_NROWS == 0 && i != 0) {
                                dataLoadThreadTime
                                        .add(tableName, Thread.currentThread().getName(), i,
                                                System.currentTimeMillis() - logStartTime);
                                logStartTime = System.currentTimeMillis();
                            }

                            // Pause for throttling if configured to do so
                            Thread.sleep(threadSleepDuration);
                            // Re-compute the start time for the next batch
                            last = System.currentTimeMillis();
                        }
                    }
                } finally {
                    // Need to keep the statement open to send the remaining batch of updates
                    if (!useBatchApi && stmt != null) {
                      stmt.close();
                    }
                    if (connection != null) {
                        if (useBatchApi && stmt != null) {
                            int[] results = stmt.executeBatch();
                            for (int x = 0; x < results.length; x++) {
                                int result = results[x];
                                if (result < 1) {
                                    final String msg =
                                        "Failed to write update in batch (update count="
                                            + result + ")";
                                    throw new RuntimeException(msg);
                                }
                                rowsCreated += result;
                            }
                            // Close the statement after our last batch execution.
                            stmt.close();
                        }

                        try {
                            connection.commit();
                            duration = System.currentTimeMillis() - start;
                            logger.info("Writer ( " + Thread.currentThread().getName()
                                    + ") committed Final Batch. Duration (" + duration + ") Ms");
                            connection.close();
                        } catch (SQLException e) {
                            // Swallow since we are closing anyway
                            e.printStackTrace();
                        }
                    }
                }
                totalDuration = System.currentTimeMillis() - start;
                return new Info(totalDuration, rowsCreated);
            }
        });
        return future;
    }

    private PreparedStatement buildStatement(Scenario scenario, List<Column> columns,
            PreparedStatement statement, SimpleDateFormat simpleDateFormat) throws Exception {
        int count = 1;
        for (Column column : columns) {

            DataValue dataValue = getRulesApplier().getDataForRule(scenario, column);
            switch (column.getType()) {
            case VARCHAR:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.VARCHAR);
                } else {
                    statement.setString(count, dataValue.getValue());
                }
                break;
            case CHAR:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.CHAR);
                } else {
                    statement.setString(count, dataValue.getValue());
                }
                break;
            case DECIMAL:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.DECIMAL);
                } else {
                    statement.setBigDecimal(count, new BigDecimal(dataValue.getValue()));
                }
                break;
            case INTEGER:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.INTEGER);
                } else {
                    statement.setInt(count, Integer.parseInt(dataValue.getValue()));
                }
                break;
            case DATE:
                if (dataValue.getValue().equals("")) {
                    statement.setNull(count, Types.DATE);
                } else {
                    Date
                            date =
                            new java.sql.Date(
                                    simpleDateFormat.parse(dataValue.getValue()).getTime());
                    statement.setDate(count, date);
                }
                break;
            default:
                break;
            }
            count++;
        }
        return statement;
    }

    private String buildSql(final List<Column> columns, final String tableName) {
        StringBuilder builder = new StringBuilder();
        builder.append("upsert into ");
        builder.append(tableName);
        builder.append(" (");
        int count = 1;
        for (Column column : columns) {
            builder.append(column.getName());
            if (count < columns.size()) {
                builder.append(",");
            } else {
                builder.append(")");
            }
            count++;
        }
        builder.append(" VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            if (i < columns.size() - 1) {
                builder.append("?,");
            } else {
                builder.append("?)");
            }
        }
        return builder.toString();
    }

    public XMLConfigParser getParser() {
        return parser;
    }

    public RulesApplier getRulesApplier() {
        return rulesApplier;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    private class Info {

        private final int rowCount;
        private final long duration;

        public Info(long duration, int rows) {
            this.duration = duration;
            this.rowCount = rows;
        }

        public long getDuration() {
            return duration;
        }

        public int getRowCount() {
            return rowCount;
        }
    }
}
