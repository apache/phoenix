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

import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.phoenix.log.LogLevel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.apache.phoenix.monitoring.MetricType.RESULT_SET_TIME_MS;
import static org.apache.phoenix.monitoring.MetricType.WALL_CLOCK_TIME_MS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;

@RunWith(MockitoJUnitRunner.class)
public class MetricUtilTest {

    @Test
    public void testGetMetricsStopWatchWithMetricsTrue() throws Exception {
        MetricsStopWatch metricsStopWatch = MetricUtil.getMetricsStopWatch(true,
                LogLevel.OFF, WALL_CLOCK_TIME_MS);
        assertTrue(metricsStopWatch.getMetricsEnabled());
        metricsStopWatch.start();

        metricsStopWatch =  MetricUtil.getMetricsStopWatch(false,
                LogLevel.INFO, RESULT_SET_TIME_MS);
        assertTrue(metricsStopWatch.getMetricsEnabled());
    }

    @Test
    public void testGetMetricsStopWatchWithMetricsFalse() throws Exception {
        MetricsStopWatch metricsStopWatch = MetricUtil.getMetricsStopWatch(false,
                LogLevel.OFF, WALL_CLOCK_TIME_MS);
        assertFalse(metricsStopWatch.getMetricsEnabled());
    }

    @Test
    //Check that MetricsSystemImpl has a String "prefix" field in the Hadoop version we test with
    public void testInternalMetricsField() throws NoSuchFieldException,
            SecurityException, IllegalArgumentException, IllegalAccessException {
        MetricsSystemImpl metrics = (MetricsSystemImpl) DefaultMetricsSystem.instance();
        Field prefixField = MetricsSystemImpl.class.getDeclaredField("prefix");
        prefixField.setAccessible(true);
        String oldValue = (String)prefixField.get(metrics);
        prefixField.set(metrics, "dummy");
        prefixField.set(metrics, oldValue);
        prefixField.setAccessible(false);
    }
}
