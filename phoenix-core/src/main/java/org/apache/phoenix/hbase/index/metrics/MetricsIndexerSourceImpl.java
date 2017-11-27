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
    }

    @Override
    public void updateIndexPrepareTime(long t) {
        indexPrepareTimeHisto.add(t);
    }

    @Override
    public void updateIndexWriteTime(long t) {
        indexWriteTimeHisto.add(t);
    }

    @Override
    public void updatePreWALRestoreTime(long t) {
        preWALRestoreTimeHisto.add(t);
    }

    @Override
    public void updatePostPutTime(long t) {
        postPutTimeHisto.add(t);
    }

    @Override
    public void updatePostDeleteTime(long t) {
        postDeleteTimeHisto.add(t);
    }

    @Override
    public void updatePostOpenTime(long t) {
        postOpenTimeHisto.add(t);
    }

    @Override
    public void incrementNumSlowIndexPrepareCalls() {
        slowIndexPrepareCalls.incr();
    }

    @Override
    public void incrementNumSlowIndexWriteCalls() {
        slowIndexWriteCalls.incr();
    }

    @Override
    public void incrementNumSlowPreWALRestoreCalls() {
        slowPreWALRestoreCalls.incr();
    }

    @Override
    public void incrementNumSlowPostPutCalls() {
        slowPostPutCalls.incr();
    }

    @Override
    public void incrementNumSlowPostDeleteCalls() {
        slowPostDeleteCalls.incr();
    }

    @Override
    public void incrementNumSlowPostOpenCalls() {
        slowPostOpenCalls.incr();
    }

    @Override
    public void updateDuplicateKeyCheckTime(long t) {
        duplicateKeyTimeHisto.add(t);
    }

    @Override
    public void incrementSlowDuplicateKeyCheckCalls() {
        slowDuplicateKeyCalls.incr();
    }
}