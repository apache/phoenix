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
package org.apache.phoenix.log;

import java.util.Map;

import org.apache.phoenix.monitoring.MetricType;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;
import com.lmax.disruptor.EventTranslator;

class RingBufferEventTranslator implements EventTranslator<RingBufferEvent> {
    private String queryId;
    private ImmutableMap<QueryLogInfo, Object> queryInfo;
    private LogLevel connectionLogLevel;
    private Map<String, Map<MetricType, Long>> readMetrics;
    private Map<MetricType, Long> overAllMetrics;
    
    public RingBufferEventTranslator(String queryId) {
        this.queryId=queryId;
    }

    @Override
    public void translateTo(RingBufferEvent event, long sequence) {
        event.setQueryId(queryId);
        event.setQueryInfo(queryInfo);
        event.setReadMetrics(readMetrics);
        event.setOverAllMetrics(overAllMetrics);
        event.setConnectionLogLevel(connectionLogLevel);
        clear();
    }

    private void clear() {
        setQueryInfo(null,null,null,null);
    }
   
    public void setQueryInfo(LogLevel logLevel, ImmutableMap<QueryLogInfo, Object> queryInfo, Map<String, Map<MetricType, Long>> readMetrics,
            Map<MetricType, Long> overAllMetrics) {
        this.queryInfo = queryInfo;
        this.connectionLogLevel = logLevel;
        this.readMetrics = readMetrics;
        this.overAllMetrics=overAllMetrics;
    }

}
