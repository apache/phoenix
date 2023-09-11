/**
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
package org.apache.phoenix.monitoring.connectionqueryservice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.monitoring.HistogramDistribution;

/**
 * Histogram Metrics for Connection Query Service Metrics.
 * 1. Connection count
 * 2. Internal Connection Count.
 */
public class ConnectionQueryServicesMetricsHistograms {
    private String connectionQueryServicesName;
    private ConnectionQueryServicesHistogram connectionQueryServiceOpenInternalSizeHistogram;
    private ConnectionQueryServicesHistogram connectionQueryServicesOpenConnSizeHistogram;

    public ConnectionQueryServicesMetricsHistograms(String connectionQueryServiceName,
                                                   Configuration conf) {
        connectionQueryServicesName = connectionQueryServiceName;
        connectionQueryServiceOpenInternalSizeHistogram = new ConnectionQueryServicesHistogram(
                "PhoenixInternalOpenConn",
                "histogram for number of open internal phoenix connections", conf);
        connectionQueryServicesOpenConnSizeHistogram = new ConnectionQueryServicesHistogram(
                "PhoenixOpenConn", "histogram for number of open phoenix connections", conf);
    }

    public String getConnectionQueryServicesName() {
        return this.connectionQueryServicesName;
    }

    @SuppressWarnings(value = "EI_EXPOSE_REP",
            justification = "It's only used in internally for metrics storage")
    public ConnectionQueryServicesHistogram getConnectionQueryServicesInternalOpenConnHisto() {
        return connectionQueryServiceOpenInternalSizeHistogram;
    }

    @SuppressWarnings(value = "EI_EXPOSE_REP",
            justification = "It's only used in internally for metrics storage")
    public ConnectionQueryServicesHistogram getConnectionQueryServicesOpenConnHisto() {
        return connectionQueryServicesOpenConnSizeHistogram;
    }

    public List<HistogramDistribution> getConnectionQueryServicesHistogramsDistribution() {
        List<HistogramDistribution> list = new ArrayList(Arrays.asList(
            this.connectionQueryServiceOpenInternalSizeHistogram.getRangeHistogramDistribution(),
            this.connectionQueryServicesOpenConnSizeHistogram.getRangeHistogramDistribution()));
        return Collections.unmodifiableList(list);
    }
}
