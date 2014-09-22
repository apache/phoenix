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
package org.apache.phoenix.trace;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class DisableableMetricsWriter implements MetricsSink {

    private static final Log LOG = LogFactory.getLog(DisableableMetricsWriter.class);
    private PhoenixMetricsSink writer;
    private AtomicBoolean disabled = new AtomicBoolean(false);

    public DisableableMetricsWriter(PhoenixMetricsSink writer) {
        this.writer = writer;
    }

    @Override
    public void init(SubsetConfiguration config) {
        if (this.disabled.get()) return;
        writer.init(config);
    }

    @Override
    public void flush() {
        if (this.disabled.get()) {
            clear();
            return;
        }
        writer.flush();

    }

    @Override
    public void putMetrics(MetricsRecord record) {
        if (this.disabled.get()) return;
        writer.putMetrics(record);
    }

    public void disable() {
        this.disabled.set(true);
    }

    public void enable() {
        this.disabled.set(false);
    }

    public void clear() {
        // clear any pending writes
        try {
            writer.clearForTesting();
        } catch (SQLException e) {
            LOG.error("Couldn't clear the delgate writer when flush called and disabled", e);
        }
    }

    public PhoenixMetricsSink getDelegate() {
        return this.writer;
    }
}
