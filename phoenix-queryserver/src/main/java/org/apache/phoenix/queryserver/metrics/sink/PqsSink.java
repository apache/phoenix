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

package org.apache.phoenix.queryserver.metrics.sink;


import org.apache.phoenix.monitoring.GlobalMetric;
import static org.apache.phoenix.queryserver.metrics.PqsMetricsSystem.MetricType;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class PqsSink implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PqsFileSink.class);

    public void writeMapOfMap(Map<String, Map<String, Long>> metricsData,
                              String metricsType) {

        String json = null;
        Map<String,Map<String, Map<String, Long>>> data = new HashMap<>();
        data.put(metricsType,metricsData);
        try {
            json = new ObjectMapper().writeValueAsString(data);
        }catch(IOException ioe) {
            LOG.error(" error while creating json string ",json);
        } finally {
            writeJson(json, MetricType.request);
        }

    }


    public void writeMap(Map<String, Long> metricsData,
                         String metricsType) {

        String json = null;
        Map<String,Map<String, Long>> data = new HashMap<>();
        data.put(metricsType,metricsData);
        try {
            json = new ObjectMapper().writeValueAsString(data);
        }catch(IOException ioe) {
            LOG.error(" error while creating json string ",json);
        } finally {
            writeJson(json, MetricType.request);
        }

    }

    public void writeGlobal(Collection<GlobalMetric> globalMetrics) {
        String json = null;
        Map<String,Collection<GlobalMetric>> data = new HashMap<>();
        data.put("global",globalMetrics);
        try {
            json = new ObjectMapper().writeValueAsString(data);
        }catch(IOException ioe) {
            LOG.error(" error while creating json string ",json);
        } finally {
            writeJson(json, MetricType.global);
        }
    }


    public abstract void writeJson(String json, MetricType type);

    public abstract void close();
}
