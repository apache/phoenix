/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.metrics;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

public class Metrics {

    private static final Log LOG = LogFactory.getLog(Metrics.class);

  private static volatile MetricsSystem manager = DefaultMetricsSystem.instance();

    private static boolean initialized;

    /** This must match the prefix that we are using in the hadoop-metrics2 config on the client */
    public static final String METRICS_SYSTEM_NAME = "phoenix";
    public static MetricsSystem initialize() {
        // if the jars aren't on the classpath, then we don't start the metrics system
        if (manager == null) {
            LOG.warn("Phoenix metrics could not be initialized - no MetricsManager found!");
            return null;
        }
        // only initialize the metrics system once
        synchronized (Metrics.class) {
            if (!initialized) {
                LOG.info("Initializing metrics system: " + Metrics.METRICS_SYSTEM_NAME);
                manager.init(Metrics.METRICS_SYSTEM_NAME);
                initialized = true;
            }
        }
        return manager;
    }

    private static volatile boolean sinkInitialized = false;

    /**
     * Mark that the metrics/tracing sink has been initialized
     */
    public static void markSinkInitialized() {
        sinkInitialized = true;
    }

    public static void ensureConfigured() {
        if (!sinkInitialized) {
            LOG.warn("Phoenix metrics2/tracing sink was not started. Should be it be?");
        }
    }
}