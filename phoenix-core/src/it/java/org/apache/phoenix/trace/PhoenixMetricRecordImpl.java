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

import java.util.Collection;
import java.util.List;

import org.apache.phoenix.metrics.PhoenixAbstractMetric;
import org.apache.phoenix.metrics.PhoenixMetricTag;
import org.apache.phoenix.metrics.PhoenixMetricsRecord;

import com.google.common.collect.Lists;

/**
 *
 */
public class PhoenixMetricRecordImpl implements PhoenixMetricsRecord {

    private String name;
    private String description;
    private final List<PhoenixAbstractMetric> metrics = Lists.newArrayList();
    private final List<PhoenixMetricTag> tags = Lists.newArrayList();

    public PhoenixMetricRecordImpl(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public void addMetric(PhoenixAbstractMetric metric) {
        this.metrics.add(metric);
    }

    public void addTag(PhoenixMetricTag tag) {
        this.tags.add(tag);
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String description() {
        return this.description;
    }

    @Override
    public Iterable<PhoenixAbstractMetric> metrics() {
        return metrics;
    }

    @Override
    public Collection<PhoenixMetricTag> tags() {
        return tags;
    }
}