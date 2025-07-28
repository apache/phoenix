/*
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
package org.apache.phoenix.replication.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.phoenix.replication.ReplicationLogFileTracker;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogProcessorImpl;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogReplayFileTrackerImpl;
import org.apache.phoenix.replication.metrics.ReplicationLogFileTrackerMetricValues;

import java.net.URI;

/**
 * Concrete implementation of ReplicationLogFileTracker for replay operations.
 * Tracks and manages replication log files in the "in" subdirectory for replay purposes.
 */
public class ReplicationLogReplayFileTracker extends ReplicationLogFileTracker {

    public ReplicationLogReplayFileTracker(final Configuration conf, final String haGroupName, final FileSystem fileSystem, final URI rootURI) {
        super(conf, haGroupName, fileSystem, rootURI);
    }

    public static final String IN_SUBDIRECTORY = "in";

    @Override
    protected String getNewLogSubDirectoryName() {
        return IN_SUBDIRECTORY;
    }

    @Override
    protected MetricsReplicationLogReplayFileTrackerImpl createMetricsSource() {
        return new MetricsReplicationLogReplayFileTrackerImpl(haGroupName);
    }
}
