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
package org.apache.phoenix.replication.reader;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.replication.ReplicationLogGroup;
import org.apache.phoenix.replication.ReplicationStateTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;

public class ReplicationReplayLogDiscoveryTest {

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private Configuration conf;
    private FileSystem localFs;
    private URI standbyUri;

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        localFs = FileSystem.getLocal(conf);
        standbyUri = new Path(testFolder.toString()).toUri();
        conf.set(ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY, standbyUri.toString());
    }

    @After
    public void tearDown() throws IOException {
        localFs.delete(new Path(testFolder.getRoot().toURI()), true);
    }

    @Test
    public void testGetExecutorThreadNameFormat() {
        // Create ReplicationReplayLogDiscovery instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, "testGroup", localFs, standbyUri);
        ReplicationStateTracker stateTracker = new ReplicationReplayStateTracker();
        ReplicationReplayLogDiscovery discovery = new ReplicationReplayLogDiscovery(fileTracker, stateTracker);

        // Test that it returns the expected constant value
        String result = discovery.getExecutorThreadNameFormat();
        assertEquals("Should return the expected thread name format",
            "Phoenix-Replication-Replay-%d", result);
    }

    @Test
    public void testGetReplayIntervalSeconds() {
        // Create ReplicationReplayLogDiscovery instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, "testGroup", localFs, standbyUri);
        ReplicationStateTracker stateTracker = new ReplicationReplayStateTracker();
        ReplicationReplayLogDiscovery discovery = new ReplicationReplayLogDiscovery(fileTracker, stateTracker);

        // Test default value when no custom config is set
        long defaultResult = discovery.getReplayIntervalSeconds();
        assertEquals("Should return default value when no custom config is set",
            ReplicationReplayLogDiscovery.DEFAULT_REPLAY_INTERVAL_SECONDS, defaultResult);

        // Test custom value when config is set
        conf.setLong(ReplicationReplayLogDiscovery.REPLICATION_REPLAY_INTERVAL_SECONDS_KEY, 120L);
        long customResult = discovery.getReplayIntervalSeconds();
        assertEquals("Should return custom value when config is set",
            120L, customResult);
    }

    @Test
    public void testGetShutdownTimeoutSeconds() {
        // Create ReplicationReplayLogDiscovery instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, "testGroup", localFs, standbyUri);
        ReplicationStateTracker stateTracker = new ReplicationReplayStateTracker();
        ReplicationReplayLogDiscovery discovery = new ReplicationReplayLogDiscovery(fileTracker, stateTracker);

        // Test default value when no custom config is set
        long defaultResult = discovery.getShutdownTimeoutSeconds();
        assertEquals("Should return default value when no custom config is set",
            ReplicationReplayLogDiscovery.DEFAULT_SHUTDOWN_TIMEOUT_SECONDS, defaultResult);

        // Test custom value when config is set
        conf.setLong(ReplicationReplayLogDiscovery.REPLICATION_REPLAY_SHUTDOWN_TIMEOUT_SECONDS_KEY, 45L);
        long customResult = discovery.getShutdownTimeoutSeconds();
        assertEquals("Should return custom value when config is set",
            45L, customResult);
    }

    @Test
    public void testGetExecutorThreadCount() {
        // Create ReplicationReplayLogDiscovery instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, "testGroup", localFs, standbyUri);
        ReplicationStateTracker stateTracker = new ReplicationReplayStateTracker();
        ReplicationReplayLogDiscovery discovery = new ReplicationReplayLogDiscovery(fileTracker, stateTracker);

        // Test default value when no custom config is set
        int defaultResult = discovery.getExecutorThreadCount();
        assertEquals("Should return default value when no custom config is set",
            ReplicationReplayLogDiscovery.DEFAULT_EXECUTOR_THREAD_COUNT, defaultResult);

        // Test custom value when config is set
        conf.setInt(ReplicationReplayLogDiscovery.REPLICATION_REPLAY_EXECUTOR_THREAD_COUNT_KEY, 3);
        int customResult = discovery.getExecutorThreadCount();
        assertEquals("Should return custom value when config is set",
            3, customResult);
    }

    @Test
    public void testGetInProgressDirectoryProcessProbability() {
        // Create ReplicationReplayLogDiscovery instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, "testGroup", localFs, standbyUri);
        ReplicationStateTracker stateTracker = new ReplicationReplayStateTracker();
        ReplicationReplayLogDiscovery discovery = new ReplicationReplayLogDiscovery(fileTracker, stateTracker);

        // Test default value when no custom config is set
        double defaultResult = discovery.getInProgressDirectoryProcessProbability();
        assertEquals("Should return default value when no custom config is set",
            ReplicationReplayLogDiscovery.DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY, defaultResult, 0.001);

        // Test custom value when config is set
        conf.setDouble(ReplicationReplayLogDiscovery.REPLICATION_REPLAY_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY_KEY, 10.5);
        double customResult = discovery.getInProgressDirectoryProcessProbability();
        assertEquals("Should return custom value when config is set",
            10.5, customResult, 0.001);
    }

    @Test
    public void testGetWaitingBufferPercentage() {
        // Create ReplicationReplayLogDiscovery instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, "testGroup", localFs, standbyUri);
        ReplicationStateTracker stateTracker = new ReplicationReplayStateTracker();
        ReplicationReplayLogDiscovery discovery = new ReplicationReplayLogDiscovery(fileTracker, stateTracker);

        // Test default value when no custom config is set
        double defaultResult = discovery.getWaitingBufferPercentage();
        assertEquals("Should return default value when no custom config is set",
            ReplicationReplayLogDiscovery.DEFAULT_WAITING_BUFFER_PERCENTAGE, defaultResult, 0.001);

        // Test custom value when config is set
        conf.setDouble(ReplicationReplayLogDiscovery.REPLICATION_REPLAY_WAITING_BUFFER_PERCENTAGE_KEY, 20.0);
        double customResult = discovery.getWaitingBufferPercentage();
        assertEquals("Should return custom value when config is set",
            20.0, customResult, 0.001);
    }

}
