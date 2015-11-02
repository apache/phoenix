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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.Date;

import org.apache.phoenix.pherf.result.DataModelResult;
import org.apache.phoenix.pherf.result.ResultManager;
import org.apache.phoenix.pherf.result.RunTime;
import org.apache.phoenix.pherf.result.ThreadTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.pherf.configuration.Query;
import org.apache.phoenix.pherf.util.PhoenixUtil;

class MultiThreadedRunner implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MultiThreadedRunner.class);
    private Query query;
    private ThreadTime threadTime;
    private PhoenixUtil pUtil = PhoenixUtil.create();
    private String threadName;
    private DataModelResult dataModelResult;
    private long numberOfExecutions;
    private long executionDurationInMs;
    private static long lastResultWritten = System.currentTimeMillis() - 1000;
    private final ResultManager resultManager;

    /**
     * MultiThreadedRunner
     *
     * @param threadName
     * @param query
     * @param dataModelResult
     * @param threadTime
     * @param numberOfExecutions
     * @param executionDurationInMs
     */
    MultiThreadedRunner(String threadName, Query query, DataModelResult dataModelResult,
            ThreadTime threadTime, long numberOfExecutions, long executionDurationInMs, boolean writeRuntimeResults) {
        this.query = query;
        this.threadName = threadName;
        this.threadTime = threadTime;
        this.dataModelResult = dataModelResult;
        this.numberOfExecutions = numberOfExecutions;
        this.executionDurationInMs = executionDurationInMs;
       	this.resultManager = new ResultManager(dataModelResult.getName(), writeRuntimeResults);
    }

    /**
     * Executes run for a minimum of number of execution or execution duration
     */
    @Override
    public void run() {
        logger.info("\n\nThread Starting " + threadName + " ; " + query.getStatement() + " for "
                + numberOfExecutions + "times\n\n");
        Long start = System.currentTimeMillis();
        for (long i = numberOfExecutions; (i > 0 && ((System.currentTimeMillis() - start)
                < executionDurationInMs)); i--) {
            try {
                synchronized (resultManager) {
                    timedQuery();
                    if ((System.currentTimeMillis() - lastResultWritten) > 1000) {
                        resultManager.write(dataModelResult);
                        lastResultWritten = System.currentTimeMillis();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        // Make sure all result have been dumped before exiting
        resultManager.flush();

        logger.info("\n\nThread exiting." + threadName + "\n\n");
    }

    private synchronized ThreadTime getThreadTime() {
        return threadTime;
    }

    /**
     * Timed query execution
     *
     * @throws Exception
     */
    private void timedQuery() throws Exception {
        boolean
                isSelectCountStatement =
                query.getStatement().toUpperCase().trim().contains("COUNT(*)") ? true : false;

        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        Long start = System.currentTimeMillis();
        Date startDate = Calendar.getInstance().getTime();
        String exception = null;
        long resultRowCount = 0;

        try {
            conn = pUtil.getConnection(query.getTenantId());
            statement = conn.prepareStatement(query.getStatement());
            boolean isQuery = statement.execute();
            if (isQuery) {
                rs = statement.getResultSet();
                while (rs.next()) {
                    if (null != query.getExpectedAggregateRowCount()) {
                        if (rs.getLong(1) != query.getExpectedAggregateRowCount())
                            throw new RuntimeException(
                                    "Aggregate count " + rs.getLong(1) + " does not match expected "
                                            + query.getExpectedAggregateRowCount());
                    }

                    if (isSelectCountStatement) {
                        resultRowCount = rs.getLong(1);
                    } else {
                        resultRowCount++;
                    }
                }
            } else {
                conn.commit();
            }
        } catch (Exception e) {
            e.printStackTrace();
            exception = e.getMessage();
        } finally {
            getThreadTime().getRunTimesInMs().add(new RunTime(exception, startDate, resultRowCount,
                    (int) (System.currentTimeMillis() - start)));

            if (rs != null) rs.close();
            if (statement != null) statement.close();
            if (conn != null) conn.close();
        }
    }
}