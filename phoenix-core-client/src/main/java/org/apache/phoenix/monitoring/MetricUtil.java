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

import java.lang.reflect.Field;

import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.phoenix.log.LogLevel;
import org.apache.phoenix.monitoring.CombinableMetric.NoOpRequestMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricUtil.class);

    public static CombinableMetric getCombinableMetric(boolean isRequestMetricsEnabled,
                                                       LogLevel connectionLogLevel,
                                                       MetricType type) {
        if (!type.isLoggingEnabled(connectionLogLevel) && !isRequestMetricsEnabled) {
            return NoOpRequestMetric.INSTANCE; }
        return new CombinableMetricImpl(type);
    }

    public static MetricsStopWatch getMetricsStopWatch(boolean isRequestMetricsEnabled,
                                                       LogLevel connectionLogLevel,
                                                       MetricType type) {
        if(!type.isLoggingEnabled(connectionLogLevel) && !isRequestMetricsEnabled) {
            return new MetricsStopWatch(false); }
        return new MetricsStopWatch(true);
    }

    // We need to cover the case when JmxCacheBuster has just stopped the HBase metrics
    // system, and not accidentally overwrite the DefaultMetricsSystem singleton.
    // See PHOENIX-6699
    public static boolean isDefaultMetricsInitialized() {
        try {
            MetricsSystemImpl metrics = (MetricsSystemImpl) DefaultMetricsSystem.instance();
            Field prefixField = MetricsSystemImpl.class.getDeclaredField("prefix");
            prefixField.setAccessible(true);
            String prefix = (String) prefixField.get(metrics);
            prefixField.setAccessible(false);
            if (prefix != null) {
                return true;
            }
        } catch (Exception e) {
            LOGGER.error("Exception trying to determine if HBase metrics is initialized", e);
        }
        return false;
    }
}
