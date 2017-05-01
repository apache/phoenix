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

package org.apache.phoenix.queryserver.metrics;


import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.monitoring.GlobalMetric;
import org.apache.phoenix.queryserver.metrics.sink.PqsSink;
import org.apache.phoenix.util.PhoenixRuntime;

import java.util.Collection;

public class PqsGlobalMetrics implements Runnable {

    protected static final Log LOG = LogFactory.getLog(PqsGlobalMetrics.class);
    public final static String global = "global";
    private Collection<GlobalMetric> phoenixGlobalMetricsCollection;
    private PqsSink pqsSink;
    private static final MetricRegistry metrics = new MetricRegistry();

    public PqsGlobalMetrics(PqsSink pqsSink) {
        phoenixGlobalMetricsCollection = PhoenixRuntime.getGlobalPhoenixClientMetrics();
        this.pqsSink = pqsSink;
        for(final GlobalMetric globalMetric:phoenixGlobalMetricsCollection) {
            metrics.register(MetricRegistry.name(PqsGlobalMetrics.class,
                    global,globalMetric.getName()),
                    new Gauge<Long>() {
                        @Override
                        public Long getValue() {
                            return globalMetric.getValue();
                        }
                    });
        }

    }

    @Override
    public void run() {
        while (true) {
            int delay = PqsConfiguration.getReportingInterval();
            //besides writing to JMX, the global metrics is also written to pqsSink
            //default pqsSink is slf4j ( logger)
            phoenixGlobalMetricsCollection = PhoenixRuntime.getGlobalPhoenixClientMetrics();
            this.pqsSink.writeGlobal(phoenixGlobalMetricsCollection);
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                LOG.error(" Sleep thread interrupted for metrics collection ");
            }
        }
    }





}
