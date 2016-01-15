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
 * Version of {@link Metric} that can be used when the metric isn't getting concurrently modified/accessed by multiple
 * threads and the memory consistency effects of happen-before can be established. For example - phoenix client side
 * metrics are modified/accessed by only one thread at a time. Further, the actions of threads in the phoenix client
 * thread pool happen-before the actions of the thread that performs the aggregation of metrics. This makes
 * {@link NonAtomicMetric} a good fit for storing Phoenix's client side request level metrics.
 */
class NonAtomicMetric implements Metric {

    private final MetricType type;
    private long value;

    public NonAtomicMetric(MetricType type) {
        this.type = type;
    }

    @Override
    public String getName() {
        return type.name();
    }

    @Override
    public String getDescription() {
        return type.description();
    }

    @Override
    public long getValue() {
        return value;
    }

    @Override
    public void change(long delta) {
        value += delta;
    }

    @Override
    public void increment() {
        value++;
    }

    @Override
    public String getCurrentMetricState() {
        return getName() + ": " + value;
    }

    @Override
    public void reset() {
        value = 0;
    }

    @Override
    public void decrement() {
        value--;
    }

}
