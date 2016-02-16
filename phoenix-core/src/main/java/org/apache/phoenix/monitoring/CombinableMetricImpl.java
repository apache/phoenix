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

import static com.google.common.base.Preconditions.checkArgument;

public class CombinableMetricImpl implements CombinableMetric {

    private final Metric metric;

    public CombinableMetricImpl(MetricType type) {
        metric = new NonAtomicMetric(type);
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
    public void change(long delta) {
        metric.change(delta);
    }

    @Override
    public void increment() {
        metric.increment();
    }

    @Override
    public String getCurrentMetricState() {
        return metric.getCurrentMetricState();
    }

    @Override
    public void reset() {
        metric.reset();
    }

    @Override
    public String getPublishString() {
        return getCurrentMetricState();
    }

    @Override
    public CombinableMetric combine(CombinableMetric metric) {
        checkArgument(this.getClass().equals(metric.getClass()));
        this.metric.change(metric.getValue());
        return this;
    }

    @Override
    public void decrement() {
        metric.decrement();
    }

}
