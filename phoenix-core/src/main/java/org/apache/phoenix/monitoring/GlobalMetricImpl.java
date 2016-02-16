/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.phoenix.monitoring;

import java.util.concurrent.atomic.AtomicLong;

public class GlobalMetricImpl implements GlobalMetric {

    private AtomicLong numberOfSamples = new AtomicLong(0);
    private Metric metric;

    public GlobalMetricImpl(MetricType type) {
        this.metric = new AtomicMetric(type);
    }

    /**
     * Reset the internal state. Typically called after metric information has been collected and a new phase of
     * collection is being requested for the next interval.
     */
    @Override
    public void reset() {
        metric.reset();
        numberOfSamples.set(0);
    }

    @Override
    public long getNumberOfSamples() {
        return numberOfSamples.get();
    }

    @Override
    public long getTotalSum() {
        return metric.getValue();
    }

    @Override
    public void change(long delta) {
        metric.change(delta);
        numberOfSamples.incrementAndGet();
    }

    @Override
    public void increment() {
        metric.increment();
        numberOfSamples.incrementAndGet();
    }

    @Override
    public String getName() {
        return metric.getName();
    }

    @Override
    public String getDescription() {
        return metric.getDescription();
    }

    @Override
    public long getValue() {
        return metric.getValue();
    }

    @Override
    public String getCurrentMetricState() {
        return metric.getCurrentMetricState() + ", Number of samples: " + numberOfSamples.get();
    }

    @Override
    public void decrement() {
        metric.decrement();
        numberOfSamples.incrementAndGet();
    }
}
