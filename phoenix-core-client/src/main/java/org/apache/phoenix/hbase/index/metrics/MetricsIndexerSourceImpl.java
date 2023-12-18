/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hbase.index.metrics;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

/**
 * Implementation for tracking Phoenix Indexer metrics.
 */
public class MetricsIndexerSourceImpl extends BaseSourceImpl implements MetricsIndexerSource {

    private final MetricHistogram indexPrepareTimeHisto;
    private final MutableFastCounter slowIndexPrepareCalls;
    private final MetricHistogram indexWriteTimeHisto;
    private final MutableFastCounter slowIndexWriteCalls;
    private final MetricHistogram preWALRestoreTimeHisto;
    private final MutableFastCounter slowPreWALRestoreCalls;
    private final MetricHistogram postPutTimeHisto;
    private final MutableFastCounter slowPostPutCalls;
    private final MetricHistogram postDeleteTimeHisto;
    private final MutableFastCounter slowPostDeleteCalls;
    private final MetricHistogram postOpenTimeHisto;
    private final MutableFastCounter slowPostOpenCalls;
    private final MetricHistogram duplicateKeyTimeHisto;
    private final MutableFastCounter slowDuplicateKeyCalls;

    private final MetricHistogram preIndexUpdateTimeHisto;
    private final MetricHistogram postIndexUpdateTimeHisto;
    private final MetricHistogram preIndexUpdateFailureTimeHisto;
    private final MetricHistogram postIndexUpdateFailureTimeHisto;
    private final MutableFastCounter preIndexUpdateFailures;
    private final MutableFastCounter postIndexUpdateFailures;

    public MetricsIndexerSourceImpl() {
        this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
    }

    public MetricsIndexerSourceImpl(String metricsName, String metricsDescription,
        String metricsContext, String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

        indexPrepareTimeHisto = getMetricsRegistry().newHistogram(INDEX_PREPARE_TIME, INDEX_PREPARE_TIME_DESC);
        slowIndexPrepareCalls = getMetricsRegistry().newCounter(SLOW_INDEX_PREPARE, SLOW_INDEX_PREPARE_DESC, 0L);
        indexWriteTimeHisto = getMetricsRegistry().newHistogram(INDEX_WRITE_TIME, INDEX_WRITE_TIME_DESC);
        slowIndexWriteCalls = getMetricsRegistry().newCounter(SLOW_INDEX_WRITE, SLOW_INDEX_WRITE_DESC, 0L);
        preWALRestoreTimeHisto = getMetricsRegistry().newHistogram(PRE_WAL_RESTORE_TIME, PRE_WAL_RESTORE_TIME_DESC);
        slowPreWALRestoreCalls = getMetricsRegistry().newCounter(SLOW_PRE_WAL_RESTORE, SLOW_PRE_WAL_RESTORE_DESC, 0L);
        postPutTimeHisto = getMetricsRegistry().newHistogram(POST_PUT_TIME, POST_PUT_TIME_DESC);
        slowPostPutCalls = getMetricsRegistry().newCounter(SLOW_POST_PUT, SLOW_POST_PUT_DESC, 0L);
        postDeleteTimeHisto = getMetricsRegistry().newHistogram(POST_DELETE_TIME, POST_DELETE_TIME_DESC);
        slowPostDeleteCalls = getMetricsRegistry().newCounter(SLOW_POST_DELETE, SLOW_POST_DELETE_DESC, 0L);
        postOpenTimeHisto = getMetricsRegistry().newHistogram(POST_OPEN_TIME, POST_OPEN_TIME_DESC);
        slowPostOpenCalls = getMetricsRegistry().newCounter(SLOW_POST_OPEN, SLOW_POST_OPEN_DESC, 0L);
        duplicateKeyTimeHisto = getMetricsRegistry().newHistogram(DUPLICATE_KEY_TIME, DUPLICATE_KEY_TIME_DESC);
        slowDuplicateKeyCalls = getMetricsRegistry().newCounter(SLOW_DUPLICATE_KEY, SLOW_DUPLICATE_KEY_DESC, 0L);

        postIndexUpdateTimeHisto = getMetricsRegistry().newHistogram(
                POST_INDEX_UPDATE_TIME, POST_INDEX_UPDATE_TIME_DESC);
        preIndexUpdateTimeHisto = getMetricsRegistry().newHistogram(
                PRE_INDEX_UPDATE_TIME, PRE_INDEX_UPDATE_TIME_DESC);
        postIndexUpdateFailureTimeHisto = getMetricsRegistry().newHistogram(
                POST_INDEX_UPDATE_FAILURE_TIME, POST_INDEX_UPDATE_FAILURE_TIME_DESC);
        preIndexUpdateFailureTimeHisto = getMetricsRegistry().newHistogram(
                PRE_INDEX_UPDATE_FAILURE_TIME, PRE_INDEX_UPDATE_FAILURE_TIME_DESC);
        postIndexUpdateFailures = getMetricsRegistry().newCounter(
                POST_INDEX_UPDATE_FAILURE, POST_INDEX_UPDATE_FAILURE_DESC, 0L);
        preIndexUpdateFailures = getMetricsRegistry().newCounter(
                PRE_INDEX_UPDATE_FAILURE, PRE_INDEX_UPDATE_FAILURE_DESC, 0L);
    }

    @Override
    public void updateIndexPrepareTime(String dataTableName, long t) {
        incrementTableSpecificHistogram(INDEX_PREPARE_TIME, dataTableName, t);
        indexPrepareTimeHisto.add(t);
    }

    @Override
    public void updateIndexWriteTime(String dataTableName, long t) {
        incrementTableSpecificHistogram(INDEX_WRITE_TIME, dataTableName, t);
        indexWriteTimeHisto.add(t);
    }

    @Override
    public void updatePreWALRestoreTime(String dataTableName, long t) {
        incrementTableSpecificHistogram(PRE_WAL_RESTORE_TIME, dataTableName, t);
        preWALRestoreTimeHisto.add(t);
    }

    @Override
    public void updatePostPutTime(String dataTableName, long t) {
        incrementTableSpecificHistogram(POST_PUT_TIME, dataTableName, t);
        postPutTimeHisto.add(t);
    }

    @Override
    public void updatePostDeleteTime(String dataTableName, long t) {
        incrementTableSpecificHistogram(POST_DELETE_TIME, dataTableName, t);
        postDeleteTimeHisto.add(t);
    }

    @Override
    public void updatePostOpenTime(String dataTableName, long t) {
        incrementTableSpecificHistogram(POST_OPEN_TIME, dataTableName, t);
        postOpenTimeHisto.add(t);
    }

    @Override
    public void incrementNumSlowIndexPrepareCalls(String dataTableName) {
        incrementTableSpecificCounter(SLOW_INDEX_PREPARE, dataTableName);
        slowIndexPrepareCalls.incr();
    }

    @Override
    public void incrementNumSlowIndexWriteCalls(String dataTableName) {
        incrementTableSpecificCounter(SLOW_INDEX_WRITE, dataTableName);
        slowIndexWriteCalls.incr();
    }

    @Override
    public void incrementNumSlowPreWALRestoreCalls(String dataTableName) {
        incrementTableSpecificCounter(SLOW_PRE_WAL_RESTORE, dataTableName);
        slowPreWALRestoreCalls.incr();
    }

    @Override
    public void incrementNumSlowPostPutCalls(String dataTableName) {
        incrementTableSpecificCounter(SLOW_POST_PUT, dataTableName);
        slowPostPutCalls.incr();
    }

    @Override
    public void incrementNumSlowPostDeleteCalls(String dataTableName) {
        incrementTableSpecificCounter(SLOW_POST_DELETE, dataTableName);
        slowPostDeleteCalls.incr();
    }

    @Override
    public void incrementNumSlowPostOpenCalls(String dataTableName) {
        incrementTableSpecificCounter(SLOW_POST_OPEN, dataTableName);
        slowPostOpenCalls.incr();
    }

    @Override
    public void updateDuplicateKeyCheckTime(String dataTableName, long t) {
        incrementTableSpecificHistogram(DUPLICATE_KEY_TIME, dataTableName, t);
        duplicateKeyTimeHisto.add(t);
    }

    @Override
    public void incrementSlowDuplicateKeyCheckCalls(String dataTableName) {
        incrementTableSpecificCounter(SLOW_DUPLICATE_KEY, dataTableName);
        slowDuplicateKeyCalls.incr();
    }

    @Override
    public void updatePreIndexUpdateTime(String dataTableName, long t) {
        incrementTableSpecificHistogram(PRE_INDEX_UPDATE_TIME, dataTableName, t);
        preIndexUpdateTimeHisto.add(t);
    }

    @Override
    public void updatePostIndexUpdateTime(String dataTableName, long t) {
        incrementTableSpecificHistogram(POST_INDEX_UPDATE_TIME, dataTableName, t);
        postIndexUpdateTimeHisto.add(t);
    }

    @Override
    public void updatePreIndexUpdateFailureTime(String dataTableName, long t) {
        incrementTableSpecificHistogram(PRE_INDEX_UPDATE_FAILURE_TIME, dataTableName, t);
        preIndexUpdateFailureTimeHisto.add(t);
    }

    @Override
    public void updatePostIndexUpdateFailureTime(String dataTableName, long t) {
        incrementTableSpecificHistogram(POST_INDEX_UPDATE_FAILURE_TIME, dataTableName, t);
        postIndexUpdateFailureTimeHisto.add(t);
    }

    @Override
    public void incrementPreIndexUpdateFailures(String dataTableName) {
        incrementTableSpecificCounter(PRE_INDEX_UPDATE_FAILURE, dataTableName);
        preIndexUpdateFailures.incr();
    }

    @Override
    public void incrementPostIndexUpdateFailures(String dataTableName) {
        incrementTableSpecificCounter(POST_INDEX_UPDATE_FAILURE, dataTableName);
        postIndexUpdateFailures.incr();
    }

    private void incrementTableSpecificCounter(String baseCounterName, String tableName) {
        MutableFastCounter indexSpecificCounter =
            getMetricsRegistry().getCounter(getCounterName(baseCounterName, tableName), 0);
        indexSpecificCounter.incr();
    }

    private void incrementTableSpecificHistogram(String baseCounterName, String tableName, long t) {
        MetricHistogram tableSpecificHistogram =
            getMetricsRegistry().getHistogram(getCounterName(baseCounterName, tableName));
        tableSpecificHistogram.add(t);
    }

    private String getCounterName(String baseCounterName, String tableName) {
        return baseCounterName + "." + tableName;
    }
}