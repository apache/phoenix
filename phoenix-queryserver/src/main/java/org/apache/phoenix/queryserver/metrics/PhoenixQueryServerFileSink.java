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

package org.apache.phoenix.queryserver.metrics;


import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.monitoring.GlobalMetric;
import org.apache.phoenix.monitoring.PhoenixQueryServerSink;

import java.util.Collection;
import java.util.Map;


public class PhoenixQueryServerFileSink implements PhoenixQueryServerSink {


    @Inject
    Configuration configuration;

    @Override
    public void close() {

    }

    @Override
    public void flush() {

    }

    @Override
    public void init() {
        // TBD
        //get hold of configuration property
        //write out the data to a File
    }

    @Override
    public void putMetrics(Map<String, Map<String, Long>> metricsMap) {
        System.out.println(metricsMap.toString());
    }

    @Override
    public void putMetricsOverall(Map<String, Long> metricsMap) {
        System.out.println(metricsMap.toString());
    }

    @Override
    public void putGlobalMetrics(Collection<GlobalMetric> globalMetrics) {
        System.out.println(globalMetrics.toString());
    }
}
