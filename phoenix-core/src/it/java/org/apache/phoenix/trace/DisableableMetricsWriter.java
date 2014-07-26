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

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.metrics.MetricsWriter;
import org.apache.phoenix.metrics.PhoenixMetricsRecord;

/**
 *
 */
public class DisableableMetricsWriter implements MetricsWriter {

    private static final Log LOG = LogFactory.getLog(DisableableMetricsWriter.class);
    private PhoenixTableMetricsWriter writer;
    private AtomicBoolean disabled = new AtomicBoolean(false);

    public DisableableMetricsWriter(PhoenixTableMetricsWriter writer) {
        this.writer = writer;
    }

    @Override
    public void initialize() {
        if (this.disabled.get()) return;
        writer.initialize();
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
    public void addMetrics(PhoenixMetricsRecord record) {
        if (this.disabled.get()) return;
        writer.addMetrics(record);
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

    public PhoenixTableMetricsWriter getDelegate() {
        return this.writer;
    }
}
