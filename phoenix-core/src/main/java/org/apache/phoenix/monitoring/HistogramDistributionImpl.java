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

import java.util.Map;

public class HistogramDistributionImpl implements HistogramDistribution {
    private final String histoName;
    private final long min;
    private final long max;
    private final long count;
    private final Map<String, Long> rangeDistribution;

    public HistogramDistributionImpl(String histoName, long min, long max, long count, Map<String, Long> distributionMap ) {
        this.histoName = histoName;
        this.min  = min;
        this.max = max;
        this.count = count;
        this.rangeDistribution = distributionMap;
    }

    @Override
    public long getMin() {
        return min;
    }

    @Override
    public long getMax() {
        return max;
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public String getHistoName() {
        return histoName;
    }

    @Override
    //The caller making the list immutable
    public Map<String, Long> getRangeDistributionMap() {
        return rangeDistribution;
    }

}
