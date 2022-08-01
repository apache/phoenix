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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Gauge;
import org.apache.hadoop.hbase.metrics.Histogram;
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.Metric;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.phoenix.query.QueryServicesOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contents mostly copied from GlobalMetricRegistriesAdapter class from hbase-hadoop2-compat
 * The adapter attaches HBase's MetricRegistry to Hadoop's DefaultMetricsSystem
 * Note: This DOES NOT handle dynamic attach/detach of registries
 */
public class GlobalMetricRegistriesAdapter {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(GlobalMetricRegistriesAdapter.class);
    private static GlobalMetricRegistriesAdapter INSTANCE = new GlobalMetricRegistriesAdapter();

    private GlobalMetricRegistriesAdapter() {
        if (MetricUtil.isDefaultMetricsInitialized()) {
            // Prevent clobbering the default metrics HBase has set up in
            // RS or Master while JmxCacheBuster shuts the Metrics down
            LOGGER.info("HBase metrics is already initialized. "
                    + "Skipping Phoenix metrics initialization.");
            return;
        }
        DefaultMetricsSystem.initialize("Phoenix");
        JvmMetrics.initSingleton("Phoenix", "");
    }

    public static GlobalMetricRegistriesAdapter getInstance() {
        return INSTANCE;
    }

    public void registerMetricRegistry(MetricRegistry registry) {
        if (registry == null) {
            LOGGER.warn("Registry cannot be registered with Hadoop Metrics 2 since it is null.");
            return;
        }

        HBaseMetrics2HadoopMetricsAdapter adapter = new HBaseMetrics2HadoopMetricsAdapter(registry);
        adapter.registerToDefaultMetricsSystem();
    }

    /**
     * Class to convert HBase Metric Objects to Hadoop Metrics2 Metric Objects
     */
    private static class HBaseMetrics2HadoopMetricsAdapter implements MetricsSource {
        private static final Logger LOGGER =
                LoggerFactory.getLogger(HBaseMetrics2HadoopMetricsAdapter.class);
        private final MetricRegistry registry;
        private final String metricTag;

        private HBaseMetrics2HadoopMetricsAdapter(MetricRegistry registry) {
            this.registry = registry;
            metricTag = QueryServicesOptions.withDefaults().getClientMetricTag();
        }

        private void registerToDefaultMetricsSystem() {
            MetricRegistryInfo info = registry.getMetricRegistryInfo();
            LOGGER.info("Registering " + info.getMetricsJmxContext() +
                    " " + info.getMetricsDescription() + " into DefaultMetricsSystem");
            DefaultMetricsSystem.instance().register(info.getMetricsJmxContext(), info.getMetricsDescription(), this);
        }

        private void snapshotAllMetrics(MetricRegistry metricRegistry, MetricsCollector collector) {
            MetricRegistryInfo hbaseMetricRegistryInfo = metricRegistry.getMetricRegistryInfo();
            MetricsInfo hadoopMetricsInfo = Interns.info(hbaseMetricRegistryInfo.getMetricsName(), hbaseMetricRegistryInfo.getMetricsDescription());
            MetricsRecordBuilder builder = collector.addRecord(hadoopMetricsInfo);
            builder.setContext(hbaseMetricRegistryInfo.getMetricsContext());
            builder.tag(hadoopMetricsInfo, metricTag);
            this.snapshotAllMetrics(metricRegistry, builder);
        }

        private void snapshotAllMetrics(MetricRegistry metricRegistry, MetricsRecordBuilder builder) {
            Map<String, Metric> metrics = metricRegistry.getMetrics();
            Iterator iterator = metrics.entrySet().iterator();

            while(iterator.hasNext()) {
                Entry<String, Metric> e = (Entry)iterator.next();
                String name = StringUtils.capitalize(e.getKey());
                Metric metric = e.getValue();
                if (metric instanceof Gauge) {
                    this.addGauge(name, (Gauge)metric, builder);
                } else if (metric instanceof Counter) {
                    this.addCounter(name, (Counter)metric, builder);
                } else if (metric instanceof Histogram) {
                    this.addHistogram(name, (Histogram)metric, builder);
                } else if (metric instanceof Meter) {
                    this.addMeter(name, (Meter)metric, builder);
                } else if (metric instanceof Timer) {
                    this.addTimer(name, (Timer)metric, builder);
                } else {
                    LOGGER.info("Ignoring unknown Metric class " + metric.getClass().getName());
                }
            }
        }

        private void addGauge(String name, Gauge<?> gauge, MetricsRecordBuilder builder) {
            MetricsInfo info = Interns.info(name, "");
            Object o = gauge.getValue();
            if (o instanceof Integer) {
                builder.addGauge(info, (Integer)o);
            } else if (o instanceof Long) {
                builder.addGauge(info, (Long)o);
            } else if (o instanceof Float) {
                builder.addGauge(info, (Float)o);
            } else if (o instanceof Double) {
                builder.addGauge(info, (Double)o);
            } else {
                LOGGER.warn("Ignoring Gauge (" + name + ") with unhandled type: " + o.getClass());
            }

        }

        private void addCounter(String name, Counter counter, MetricsRecordBuilder builder) {
            MetricsInfo info = Interns.info(name, "");
            builder.addCounter(info, counter.getCount());
        }

        private void addHistogram(String name, Histogram histogram, MetricsRecordBuilder builder) {
            MutableHistogram.snapshot(name, "", histogram, builder, true);
        }

        private void addMeter(String name, Meter meter, MetricsRecordBuilder builder) {
            builder.addGauge(Interns.info(name + "_count", ""), meter.getCount());
            builder.addGauge(Interns.info(name + "_mean_rate", ""), meter.getMeanRate());
            builder.addGauge(Interns.info(name + "_1min_rate", ""), meter.getOneMinuteRate());
            builder.addGauge(Interns.info(name + "_5min_rate", ""), meter.getFiveMinuteRate());
            builder.addGauge(Interns.info(name + "_15min_rate", ""), meter.getFifteenMinuteRate());
        }

        private void addTimer(String name, Timer timer, MetricsRecordBuilder builder) {
            this.addMeter(name, timer.getMeter(), builder);
            this.addHistogram(name, timer.getHistogram(), builder);
        }

        @Override
        public void getMetrics(MetricsCollector metricsCollector, boolean b) {
            this.snapshotAllMetrics(this.registry, metricsCollector);
        }

    }
}
