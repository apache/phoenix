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

package org.apache.phoenix.monitoring;

import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.end2end.ParallelStatsDisabledTest;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.metrics.MetricsMetadataSource;
import org.apache.phoenix.schema.metrics.MetricsMetadataSourceImpl;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.ReadOnlyProps;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Map;

@Category(ParallelStatsDisabledTest.class)
public class MetadataMetricsIT extends ParallelStatsDisabledIT {

    @BeforeClass
    public static void setup() throws Exception {
        Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(Long.MAX_VALUE));
        // disable renewing leases as this will force spooling to happen.
        props.put(QueryServices.RENEW_LEASE_ENABLED, String.valueOf(false));
        setUpTestDriver(new ReadOnlyProps(props.entrySet().iterator()));
    }

    @Test
    public void testCreateMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementCreateExportCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.CREATE_EXPORT_COUNT, registry);

        metadataSource.incrementCreateExportFailureCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.CREATE_EXPORT_FAILURE_COUNT, registry);

        long time = 10L;
        metadataSource.updateCreateExportTime(time);
        IndexMetricsIT.verifyHistogram(MetricsMetadataSource.CREATE_EXPORT_TIME, registry, time);

        metadataSource.updateCreateExportFailureTime(time);
        IndexMetricsIT.verifyHistogram(MetricsMetadataSource.CREATE_EXPORT_FAILURE_TIME, registry, time);
    }

    @Test
    public void testAlterMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementAlterExportCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.ALTER_EXPORT_COUNT, registry);

        metadataSource.incrementAlterExportFailureCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.ALTER_EXPORT_FAILURE_COUNT, registry);

        long time = 10L;
        metadataSource.updateAlterExportTime(time);
        IndexMetricsIT.verifyHistogram(MetricsMetadataSource.ALTER_EXPORT_TIME, registry, time);

        metadataSource.updateAlterExportFailureTime(time);
        IndexMetricsIT.verifyHistogram(MetricsMetadataSource.ALTER_EXPORT_FAILURE_TIME, registry, time);
    }
}
