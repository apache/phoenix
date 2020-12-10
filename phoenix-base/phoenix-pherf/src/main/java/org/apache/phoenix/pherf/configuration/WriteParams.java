/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.phoenix.pherf.configuration;

import javax.xml.bind.annotation.XmlAttribute;

public class WriteParams {
    private int writerThreadCount;
    private long threadSleepDuration;
    private long batchSize;
    private long executionDurationInMs;

    public WriteParams() {
        this.batchSize = Long.MIN_VALUE;
        this.writerThreadCount = Integer.MIN_VALUE;
        this.threadSleepDuration = Long.MIN_VALUE;
        this.executionDurationInMs = Long.MAX_VALUE;
    }

    public long getThreadSleepDuration() {
        return threadSleepDuration;
    }

    @SuppressWarnings("unused")
    public void setThreadSleepDuration(long threadSleepDuration) {
        this.threadSleepDuration = threadSleepDuration;
    }

    public long getBatchSize() {
        return batchSize;
    }

    @SuppressWarnings("unused")
    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public int getWriterThreadCount() {
        return writerThreadCount;
    }

    @SuppressWarnings("unused")
    public void setWriterThreadCount(int writerThreadCount) {
        this.writerThreadCount = writerThreadCount;
    }

    @XmlAttribute()
    public long getExecutionDurationInMs() {
        return executionDurationInMs;
    }

    @SuppressWarnings("unused")
    public void setExecutionDurationInMs(long executionDurationInMs) {
        this.executionDurationInMs = executionDurationInMs;
    }
}
