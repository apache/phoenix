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

package org.apache.phoenix.schema.metrics;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

public class MetricsMetadataSourceImpl extends BaseSourceImpl implements MetricsMetadataSource {

    private final MutableFastCounter createExportCount;
    private final MetricHistogram createExportTimeHisto;

    private final MutableFastCounter createExportFailureCount;
    private final MetricHistogram createExportFailureTimeHisto;

    private final MutableFastCounter alterExportCount;
    private final MetricHistogram alterExportTimeHisto;

    private final MutableFastCounter alterExportFailureCount;
    private final MetricHistogram alterExportFailureTimeHisto;

    private final MutableFastCounter createTableCount;
    private final MutableFastCounter createViewCount;
    private final MutableFastCounter createIndexCount;
    private final MutableFastCounter createSchemaCount;
    private final MutableFastCounter createFunctionCount;

    private final MutableFastCounter alterAddColumnCount;
    private final MutableFastCounter alterDropColumnCount;

    private final MutableFastCounter dropTableCount;
    private final MutableFastCounter dropViewCount;
    private final MutableFastCounter dropIndexCount;
    private final MutableFastCounter dropSchemaCount;
    private final MutableFastCounter dropFunctionCount;

    private final MutableFastCounter metadataCacheUsedSize;
    private final MutableFastCounter metadataCacheHitCount;
    private final MutableFastCounter metadataCacheMissCount;
    private final MutableFastCounter metadataCacheEvictionCount;
    private final MutableFastCounter metadataCacheRemovalCount;
    private final MutableFastCounter metadataCacheAddCount;

    public MetricsMetadataSourceImpl() {
        this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
    }

    public MetricsMetadataSourceImpl(String metricsName, String metricsDescription,
        String metricsContext, String metricsJmxContext) {
        super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

        createExportCount = getMetricsRegistry().newCounter(CREATE_EXPORT_COUNT,
            CREATE_EXPORT_COUNT_DESC, 0L);
        createExportTimeHisto = getMetricsRegistry().newHistogram(CREATE_EXPORT_TIME, CREATE_EXPORT_TIME_DESC);

        createExportFailureCount = getMetricsRegistry().newCounter(CREATE_EXPORT_FAILURE_COUNT,
            CREATE_EXPORT_FAILURE_COUNT_DESC, 0L);
        createExportFailureTimeHisto = getMetricsRegistry().newHistogram(CREATE_EXPORT_FAILURE_TIME,
            CREATE_EXPORT_FAILURE_TIME_DESC);

        alterExportCount = getMetricsRegistry().newCounter(ALTER_EXPORT_COUNT,
            ALTER_EXPORT_COUNT_DESC, 0L);
        alterExportTimeHisto = getMetricsRegistry().newHistogram(ALTER_EXPORT_TIME, ALTER_EXPORT_TIME_DESC);

        alterExportFailureCount = getMetricsRegistry().newCounter(ALTER_EXPORT_FAILURE_COUNT,
            ALTER_EXPORT_FAILURE_COUNT_DESC, 0L);
        alterExportFailureTimeHisto = getMetricsRegistry().newHistogram(ALTER_EXPORT_FAILURE_TIME,
            ALTER_EXPORT_FAILURE_TIME_DESC);

        createTableCount = getMetricsRegistry().newCounter(CREATE_TABLE_COUNT,
                CREATE_TABLE_COUNT_DESC, 0L);
        createViewCount = getMetricsRegistry().newCounter(CREATE_VIEW_COUNT,
                CREATE_VIEW_COUNT_DESC, 0L);
        createIndexCount = getMetricsRegistry().newCounter(CREATE_INDEX_COUNT,
                CREATE_INDEX_COUNT_DESC, 0L);
        createFunctionCount = getMetricsRegistry().newCounter(CREATE_FUNCTION_COUNT,
                CREATE_FUNCTION_COUNT_DESC, 0L);
        createSchemaCount = getMetricsRegistry().newCounter(CREATE_SCHEMA_COUNT,
                CREATE_SCHEMA_COUNT_DESC, 0L);

        alterAddColumnCount = getMetricsRegistry().newCounter(ALTER_ADD_COLUMN_COUNT,
                ALTER_ADD_COLUMN_COUNT_DESC, 0L);
        alterDropColumnCount = getMetricsRegistry().newCounter(ALTER_DROP_COLUMN_COUNT,
                ALTER_DROP_COLUMN_COUNT_DESC, 0L);

        dropTableCount = getMetricsRegistry().newCounter(DROP_TABLE_COUNT,
                DROP_TABLE_COUNT_DESC, 0L);
        dropViewCount = getMetricsRegistry().newCounter(DROP_VIEW_COUNT,
                DROP_VIEW_COUNT_DESC, 0L);
        dropIndexCount = getMetricsRegistry().newCounter(DROP_INDEX_COUNT,
                DROP_INDEX_COUNT_DESC, 0L);
        dropSchemaCount = getMetricsRegistry().newCounter(DROP_SCHEMA_COUNT,
                DROP_SCHEMA_COUNT_DESC, 0L);
        dropFunctionCount = getMetricsRegistry().newCounter(DROP_FUNCTION_COUNT,
                DROP_FUNCTION_COUNT_DESC, 0L);

        metadataCacheUsedSize = getMetricsRegistry().newCounter(METADATA_CACHE_ESTIMATED_USED_SIZE,
                METADATA_CACHE_ESTIMATED_USED_SIZE_DESC, 0L);
        metadataCacheHitCount = getMetricsRegistry().newCounter(METADATA_CACHE_HIT_COUNT,
                METADATA_CACHE_HIT_COUNT_DESC, 0L);
        metadataCacheMissCount = getMetricsRegistry().newCounter(METADATA_CACHE_MISS_COUNT,
                METADATA_CACHE_MISS_COUNT_DESC, 0L);
        metadataCacheEvictionCount = getMetricsRegistry().newCounter(METADATA_CACHE_EVICTION_COUNT,
                METADATA_CACHE_EVICTION_COUNT_DESC, 0L);
        metadataCacheRemovalCount = getMetricsRegistry().newCounter(METADATA_CACHE_REMOVAL_COUNT,
                METADATA_CACHE_REMOVAL_COUNT_DESC, 0L);
        metadataCacheAddCount = getMetricsRegistry().newCounter(METADATA_CACHE_ADD_COUNT,
                METADATA_CACHE_ADD_COUNT_DESC, 0L);
    }

    @Override public void incrementCreateExportCount() {
        createExportCount.incr();
    }

    @Override public void updateCreateExportTime(long t) {
        createExportTimeHisto.add(t);
    }

    @Override public void incrementCreateExportFailureCount() {
        createExportFailureCount.incr();
    }

    @Override public void updateCreateExportFailureTime(long t) {
        createExportFailureTimeHisto.add(t);
    }

    @Override public void incrementAlterExportCount() {
        alterExportCount.incr();
    }

    @Override public void updateAlterExportTime(long t) {
        alterExportTimeHisto.add(t);
    }

    @Override public void incrementAlterExportFailureCount() {
        alterExportFailureCount.incr();
    }

    @Override public void updateAlterExportFailureTime(long t) {
        alterExportFailureTimeHisto.add(t);
    }

    @Override public void incrementCreateTableCount() {
        createTableCount.incr();
    }

    @Override public void incrementCreateViewCount() {
        createViewCount.incr();
    }

    @Override public void incrementCreateIndexCount() {
        createIndexCount.incr();
    }

    @Override public void incrementCreateSchemaCount() {
        createSchemaCount.incr();
    }

    @Override public void incrementCreateFunctionCount() {
        createFunctionCount.incr();
    }

    @Override public void incrementAlterAddColumnCount() {
        alterAddColumnCount.incr();
    }

    @Override public void incrementAlterDropColumnCount() {
        alterDropColumnCount.incr();
    }

    @Override public void incrementDropTableCount() {
        dropTableCount.incr();
    }

    @Override public void incrementDropViewCount() {
        dropViewCount.incr();
    }

    @Override public void incrementDropIndexCount() {
        dropIndexCount.incr();
    }

    @Override public void incrementDropSchemaCount() {
        dropSchemaCount.incr();
    }

    @Override public void incrementDropFunctionCount() {
        dropFunctionCount.incr();
    }

    @Override
    public void incrementMetadataCacheUsedSize(long estimatedSize) {
        metadataCacheUsedSize.incr(estimatedSize);
    }

    @Override
    public void decrementMetadataCacheUsedSize(long estimatedSize) {
        metadataCacheUsedSize.incr(-estimatedSize);
    }

    @Override
    public void incrementMetadataCacheHitCount() {
        metadataCacheHitCount.incr();
    }

    @Override
    public void incrementMetadataCacheMissCount() {
        metadataCacheMissCount.incr();
    }

    @Override
    public void incrementMetadataCacheEvictionCount() {
        metadataCacheEvictionCount.incr();
    }

    @Override
    public void incrementMetadataCacheRemovalCount() {
        metadataCacheRemovalCount.incr();
    }

    @Override
    public void incrementMetadataCacheAddCount() {
        metadataCacheAddCount.incr();
    }
}
