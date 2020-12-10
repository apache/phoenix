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
import com.lmax.disruptor.EventFactory;

 class RingBufferEvent {
    private String queryId;
    private LogLevel connectionLogLevel;
    private ImmutableMap<QueryLogInfo, Object> queryInfo;
    private Map<String, Map<MetricType, Long>> readMetrics;
    private Map<MetricType, Long> overAllMetrics;
    
    public static final Factory FACTORY = new Factory();
    
    /**
     * Creates the events that will be put in the RingBuffer.
     */
    private static class Factory implements EventFactory<RingBufferEvent> {
        @Override
        public RingBufferEvent newInstance() {
            final RingBufferEvent result = new RingBufferEvent();
            return result;
        }
    }

    public void clear() {
        this.queryInfo=null;
        this.queryId=null;
    }

   
    public String getQueryId() {
        return queryId;
    }

    public static Factory getFactory() {
        return FACTORY;
    }

    public void setQueryInfo(ImmutableMap<QueryLogInfo, Object> queryInfo) {
        this.queryInfo=queryInfo;
        
    }

    public void setQueryId(String queryId) {
        this.queryId=queryId;
        
    }

    public ImmutableMap<QueryLogInfo, Object> getQueryInfo() {
        return queryInfo;
        
    }

    public LogLevel getConnectionLogLevel() {
        return connectionLogLevel;
    }


    public void setConnectionLogLevel(LogLevel connectionLogLevel) {
        this.connectionLogLevel = connectionLogLevel;
    }


    public Map<String, Map<MetricType, Long>> getReadMetrics() {
        return readMetrics;
    }


    public void setReadMetrics(Map<String, Map<MetricType, Long>> readMetrics) {
        this.readMetrics = readMetrics;
    }


    public Map<MetricType, Long> getOverAllMetrics() {
        return overAllMetrics;
    }


    public void setOverAllMetrics(Map<MetricType, Long> overAllMetrics) {
        this.overAllMetrics = overAllMetrics;
    }

    

}
