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
import org.apache.phoenix.trace.TracingCompat;

/**
 * Simple sink that just logs the output of all the metrics that start with
 * {@link TracingCompat#METRIC_SOURCE_KEY}
 */
public class LoggingSink implements MetricsWriter {

    private static final Log LOG = LogFactory.getLog(LoggingSink.class);

    @Override
    public void initialize() {
    }

    @Override
    public void addMetrics(PhoenixMetricsRecord record) {
        // we could wait until flush, but this is a really lightweight process, so we just write
        // them
        // as soon as we get them
        if (!LOG.isDebugEnabled()) {
            return;
        }
        LOG.debug("Found record:" + record.name());
        for (PhoenixAbstractMetric metric : record.metrics()) {
            // just print the metric we care about
            if (metric.getName().startsWith(TracingCompat.METRIC_SOURCE_KEY)) {
                LOG.debug("\t metric:" + metric);
            }
        }
    }

    @Override
    public void flush() {
    }
}