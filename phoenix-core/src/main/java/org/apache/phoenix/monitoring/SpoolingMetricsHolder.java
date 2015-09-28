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


/**
 * Class that encapsulates the various metrics associated with the spooling done by phoenix as part of servicing a
 * request.
 */
public class SpoolingMetricsHolder {

    private final CombinableMetric spoolFileSizeMetric;
    private final CombinableMetric numSpoolFileMetric;
    public static final SpoolingMetricsHolder NO_OP_INSTANCE = new SpoolingMetricsHolder(new ReadMetricQueue(false), "");

    public SpoolingMetricsHolder(ReadMetricQueue readMetrics, String tableName) {
        this.spoolFileSizeMetric = readMetrics.allotMetric(MetricType.SPOOL_FILE_SIZE, tableName);
        this.numSpoolFileMetric = readMetrics.allotMetric(MetricType.SPOOL_FILE_COUNTER, tableName);
    }

    public CombinableMetric getSpoolFileSizeMetric() {
        return spoolFileSizeMetric;
    }

    public CombinableMetric getNumSpoolFileMetric() {
        return numSpoolFileMetric;
    }
}
