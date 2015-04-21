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

package org.apache.phoenix.pherf.loaddata;

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

import org.apache.phoenix.pherf.result.ResultUtil;
import org.apache.phoenix.pherf.util.ResourceList;
import org.apache.phoenix.pherf.util.RowCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.pherf.PherfConstants;
import org.apache.phoenix.pherf.configuration.Column;
import org.apache.phoenix.pherf.configuration.DataModel;
import org.apache.phoenix.pherf.configuration.Scenario;
import org.apache.phoenix.pherf.configuration.XMLConfigParser;
import org.apache.phoenix.pherf.exception.PherfException;
import org.apache.phoenix.pherf.result.DataLoadThreadTime;
import org.apache.phoenix.pherf.result.DataLoadTimeSummary;
import org.apache.phoenix.pherf.rules.DataValue;
import org.apache.phoenix.pherf.rules.RulesApplier;
import org.apache.phoenix.pherf.util.PhoenixUtil;

public class DataLoader {
    private static final Logger logger = LoggerFactory.getLogger(DataLoader.class);
    private final PhoenixUtil pUtil = new PhoenixUtil();
    private final XMLConfigParser parser;
    private final RulesApplier rulesApplier;
    private final ResultUtil resultUtil;
    private final ExecutorService pool;
    private final Properties properties;

    private final int threadPoolSize;
    private final int batchSize;

    public DataLoader(XMLConfigParser parser) throws Exception {
        this(new ResourceList().getProperties(), parser);
    }

    /**
     * Default the writers to use up all available cores for threads.
     *
     * @param parser
     * @throws Exception
     */
    public DataLoader(Properties properties, XMLConfigParser parser) throws Exception {
        this.parser = parser;
        this.properties = properties;
        this.rulesApplier = new RulesApplier(this.parser);
        this.resultUtil = new ResultUtil();
        int size = Integer.parseInt(properties.getProperty("pherf.default.dataloader.threadpool"));
        this.threadPoolSize = (size == 0) ? Runtime.getRuntime().availableProcessors() : size;
        this.pool = Executors.newFixedThreadPool(this.threadPoolSize);
        String bSize = properties.getProperty("pherf.default.dataloader.batchsize");
        this.batchSize = (bSize == null) ? PherfConstants.DEFAULT_BATCH_SIZE : Integer.parseInt(bSize);
    }

    public void execute() throws Exception {
        try {
            DataModel model = getParser().getDataModels().get(0);
            DataLoadTimeSummary dataLoadTimeSummary = new DataLoadTimeSummary();
            DataLoadThreadTime dataLoadThreadTime = new DataLoadThreadTime();

            for (Scenario scenario : getParser().getScenarios()) {
                List<Future> writeBatches = new ArrayList<Future>();
                logger.info("\nLoading " + scenario.getRowCount()
                        + " rows for " + scenario.getTableName());
                long start = System.currentTimeMillis();

                RowCalculator rowCalculator = new RowCalculator(getThreadPoolSize(), scenario.getRowCount());
                for (int i = 0; i < getThreadPoolSize(); i++) {
                    List<Column> phxMetaCols = pUtil.getColumnsFromPhoenix(
                            scenario.getSchemaName(),
                            scenario.getTableNameWithoutSchemaName(),
                            pUtil.getConnection());
                    int threadRowCount = rowCalculator.getNext();
                    logger.info("Kick off thread (#" + i + ")for upsert with (" + threadRowCount + ") rows.");
                    Future<Info> write = upsertData(scenario, phxMetaCols,
                            scenario.getTableName(), threadRowCount, dataLoadThreadTime);
                    writeBatches.add(write);
                }

                if (writeBatches.isEmpty()) {
                    throw new PherfException(
                            "Holy shit snacks! Throwing up hands in disbelief and exiting. Could not write data for some unknown reason.");
                }

                int sumRows = 0, sumDuration = 0;
                // Wait for all the batch threads to complete
                for (Future<Info> write : writeBatches) {
                    Info writeInfo = write.get();
                    sumRows += writeInfo.getRowCount();
                    sumDuration += writeInfo.getDuration();
                    logger.info("Executor writes complete with row count ("
                                    + writeInfo.getRowCount()
                                    + ") in Ms ("
                                    + writeInfo.getDuration() + ")");
                }
                logger.info("Writes completed with total row count (" + sumRows
                        + ") with total time of(" + sumDuration + ") Ms");
                dataLoadTimeSummary.add(scenario.getTableName(), sumRows, (int) (System.currentTimeMillis() - start));


                // always update stats for Phoenix base tables
                updatePhoenixStats(scenario.getTableName());
            }
            resultUtil.write(dataLoadTimeSummary);
            resultUtil.write(dataLoadThreadTime);

        } finally {
            pool.shutdown();
        }
    }

    /**
     * TODO Move this method to PhoenixUtil
     * Update Phoenix table stats
     *
     * @param tableName
     * @throws Exception
     */
    public void updatePhoenixStats(String tableName) throws Exception {
        logger.info("Updating stats for " + tableName);
        pUtil.executeStatement("UPDATE STATISTICS " + tableName);
    }

    public void printTableColumns(Scenario scenario) throws Exception {
        Connection connection = null;
        try {
            connection = pUtil.getConnection();
            List<Column> columnList = pUtil.getColumnsFromPhoenix(
                    scenario.getSchemaName(),
                    scenario.getTableNameWithoutSchemaName(), connection);

            logger.debug("\n\nColumns from metadata:");
            for (Column column : columnList) {
                logger.debug("\nColumn name: [" + column.getName()
                        + "]; type: [" + column.getType() + "]; length: ["
                        + column.getLength() + "]");
            }

            if (null != scenario.getDataOverride()) {
                logger.debug("\n\nColumns from override:");
                for (Column column : scenario.getDataOverride().getColumn()) {
                    logger.debug("\nColumn name: [" + column.getName() + "]; DataSequence: [" + column.getDataSequence()
                            + "]; length: [" + column.getLength() + "]");
                }
            }

        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    // Swallow since we are closing anyway
                    e.printStackTrace();
                }
            }
        }
    }

    public Future<Info> upsertData(final Scenario scenario,
                                   final List<Column> columns, final String tableName,
                                   final int rowCount, final DataLoadThreadTime dataLoadThreadTime) {
        Future<Info> future = pool.submit(new Callable<Info>() {
            @Override
            public Info call() throws Exception {
                int rowsCreated = 0;
                Info info = null;
                long start = 0, duration = 0, totalDuration = 0;
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Connection connection = null;
                try {
                    connection = pUtil.getConnection();
                    long logStartTime = System.currentTimeMillis();
                    for (int i = 0; i < rowCount; i++) {
                        String sql = buildSql(columns, tableName);
                        PreparedStatement stmt = connection
                                .prepareStatement(sql);
                        stmt = buildStatement(scenario, columns, stmt, simpleDateFormat);
                        start = System.currentTimeMillis();
                        rowsCreated += stmt.executeUpdate();
                        stmt.close();
                        if ((i % getBatchSize()) == 0) {
                            connection.commit();
                            duration = System.currentTimeMillis() - start;
                            logger.info("Committed Batch. Total " + tableName + " rows for this thread (" + this.hashCode() + ") in ("
                                    + duration + ") Ms");

                            if (i % PherfConstants.LOG_PER_NROWS == 0 && i != 0) {
                                dataLoadThreadTime.add(tableName, Thread.currentThread().getName(), i, System.currentTimeMillis() - logStartTime);
                                logStartTime = System.currentTimeMillis();
                            }
                        }
                    }
                } finally {
                    if (connection != null) {
                        try {
                            connection.commit();
                            duration = System.currentTimeMillis() - start;
                            logger.info("Committed Final Batch. Duration (" + duration + ") Ms");
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

    private PreparedStatement buildStatement(Scenario scenario,
                                             List<Column> columns, PreparedStatement statement, SimpleDateFormat simpleDateFormat) throws Exception {
        int count = 1;
        for (Column column : columns) {

            DataValue dataValue = getRulesApplier().getDataForRule(scenario,
                    column);
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
                        statement.setBigDecimal(count,
                                new BigDecimal(dataValue.getValue()));
                    }
                    break;
                case INTEGER:
                    if (dataValue.getValue().equals("")) {
                        statement.setNull(count, Types.INTEGER);
                    } else {
                        statement.setInt(count,
                                Integer.parseInt(dataValue.getValue()));
                    }
                    break;
                case DATE:
                    if (dataValue.getValue().equals("")) {
                        statement.setNull(count, Types.DATE);
                    } else {
                        Date date = new java.sql.Date(simpleDateFormat.parse(dataValue.getValue()).getTime());
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
            this(0, 0, 0, duration, rows);
        }

        public Info(int regionSize, int completedIterations, int timesSeen,
                    long duration, int rows) {
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
