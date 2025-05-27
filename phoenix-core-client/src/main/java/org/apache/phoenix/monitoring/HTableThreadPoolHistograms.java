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

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

import java.util.List;

public class HTableThreadPoolHistograms {
    public enum Tag {
        server,
        connectionProfile,
    }

    final private UtilizationHistogram activeThreadsHisto;
    final private UtilizationHistogram queuedSizeHisto;

    public HTableThreadPoolHistograms(long maxThreadPoolSize, long maxQueueSize) {
        activeThreadsHisto = new UtilizationHistogram(maxThreadPoolSize, "ActiveThreadsCount");
        queuedSizeHisto = new UtilizationHistogram(maxQueueSize, "QueueSize");
    }

    public void updateActiveThreads(long activeThreads) {
        activeThreadsHisto.addValue(activeThreads);
    }

    public void updateQueuedSize(long queuedSize) {
        queuedSizeHisto.addValue(queuedSize);
    }

    public void addServerTag(String value) {
        addTag(Tag.server.name(), value);
    }

    public void addConnectionProfileTag(String value) {
        addTag(Tag.connectionProfile.name(), value);
    }

    public void addTag(String key, String value) {
        activeThreadsHisto.addTag(key, value);
        queuedSizeHisto.addTag(key, value);
    }

    public List<HistogramDistribution> getThreadPoolHistogramsDistribution() {
        return ImmutableList.of(activeThreadsHisto.getPercentileHistogramDistribution(),
                queuedSizeHisto.getPercentileHistogramDistribution());
    }
}
