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

import org.HdrHistogram.Histogram;

import java.util.HashMap;
import java.util.Map;

public class UtilizationHistogram extends PercentileHistogram {

    public UtilizationHistogram(long maxUtil, String name) {
        super(maxUtil, name);
    }

    protected Map<String, Long> generateDistributionMap(Histogram snapshot) {
        Map<String, Long> metrics = new HashMap<>();
        metrics.put(NUM_OPS_METRIC_NAME, snapshot.getTotalCount());
        metrics.put(MIN_METRIC_NAME, snapshot.getMinValue());
        metrics.put(MAX_METRIC_NAME, snapshot.getMaxValue());
        metrics.put(TWENTY_FIFTH_PERCENTILE_METRIC_NAME, snapshot.getValueAtPercentile(25));
        metrics.put(MEDIAN_METRIC_NAME, snapshot.getValueAtPercentile(50));
        metrics.put(SEVENTY_FIFTH_PERCENTILE_METRIC_NAME,
                snapshot.getValueAtPercentile(75));
        metrics.put(NINETIETH_PERCENTILE_METRIC_NAME, snapshot.getValueAtPercentile(90));
        metrics.put(NINETY_FIFTH_PERCENTILE_METRIC_NAME, snapshot.getValueAtPercentile(95));
        return metrics;
    }
}
