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
    public void testCreateTableMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementCreateTableCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.CREATE_TABLE_COUNT, registry);
    }

    @Test
    public void testCreateViewMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementCreateViewCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.CREATE_VIEW_COUNT, registry);
    }

    @Test
    public void testCreateIndexMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementCreateIndexCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.CREATE_INDEX_COUNT, registry);
    }

    @Test
    public void testCreateSchemaMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementCreateSchemaCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.CREATE_SCHEMA_COUNT, registry);
    }

    @Test
    public void testCreateFunctionMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementCreateFunctionCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.CREATE_FUNCTION_COUNT, registry);
    }

    @Test
    public void testAlterAddColumnsMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementAlterAddColumnCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.ALTER_ADD_COLUMN_COUNT, registry);
    }

    @Test
    public void testAlterDropColumnsMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementAlterDropColumnCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.ALTER_DROP_COLUMN_COUNT, registry);
    }

    @Test
    public void testDropTableMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementDropTableCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.DROP_TABLE_COUNT, registry);
    }

    @Test
    public void testDropViewMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementDropViewCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.DROP_VIEW_COUNT, registry);
    }

    @Test
    public void testDropIndexMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementDropIndexCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.DROP_INDEX_COUNT, registry);
    }

    @Test
    public void testDropSchemaMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementDropSchemaCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.DROP_SCHEMA_COUNT, registry);
    }

    @Test
    public void testDropFunctionMetrics() {
        MetricsMetadataSourceImpl metadataSource = new MetricsMetadataSourceImpl();
        DynamicMetricsRegistry registry = metadataSource.getMetricsRegistry();

        metadataSource.incrementDropFunctionCount();
        IndexMetricsIT.verifyCounter(MetricsMetadataSource.DROP_FUNCTION_COUNT, registry);
    }
}