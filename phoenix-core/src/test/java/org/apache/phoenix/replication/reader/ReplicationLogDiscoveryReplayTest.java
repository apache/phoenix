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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.replication.ReplicationLogTracker;
import org.apache.phoenix.replication.ReplicationLogGroup;
import org.apache.phoenix.replication.ReplicationRound;
import org.apache.phoenix.replication.ReplicationShardDirectoryManager;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTrackerReplayImpl;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

public class ReplicationLogDiscoveryReplayTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogDiscoveryReplayTest.class);

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private Configuration conf;
    private FileSystem localFs;
    private URI standbyUri;
    private static final String haGroupName = "testGroup";
    private static final MetricsReplicationLogTracker METRICS_REPLICATION_LOG_TRACKER = new MetricsReplicationLogTrackerReplayImpl(haGroupName);

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        localFs = FileSystem.getLocal(conf);
        standbyUri = testFolder.getRoot().toURI();
        conf.set(ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY, standbyUri.toString());
    }

    @After
    public void tearDown() throws IOException {
        localFs.delete(new Path(testFolder.getRoot().toURI()), true);
    }

    /**
     * Tests that the executor thread name format is correctly configured.
     */
    @Test
    public void testGetExecutorThreadNameFormat() throws IOException {
        // Create ReplicationLogDiscoveryReplay instance
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        ReplicationLogDiscoveryReplay discovery = new ReplicationLogDiscoveryReplay(fileTracker);

        // Test that it returns the expected constant value
        String result = discovery.getExecutorThreadNameFormat();
        assertEquals("Should return the expected thread name format",
            "Phoenix-ReplicationLogDiscoveryReplay-%d", result);
    }

    /**
     * Tests the replay interval configuration with default and custom values.
     */
    @Test
    public void testGetReplayIntervalSeconds() throws IOException  {
        // Create ReplicationLogDiscoveryReplay instance
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        ReplicationLogDiscoveryReplay discovery = new ReplicationLogDiscoveryReplay(fileTracker);

        // Test default value when no custom config is set
        long defaultResult = discovery.getReplayIntervalSeconds();
        assertEquals("Should return default value when no custom config is set",
            ReplicationLogDiscoveryReplay.DEFAULT_REPLAY_INTERVAL_SECONDS, defaultResult);

        // Test custom value when config is set
        conf.setLong(ReplicationLogDiscoveryReplay.REPLICATION_REPLAY_INTERVAL_SECONDS_KEY, 120L);
        long customResult = discovery.getReplayIntervalSeconds();
        assertEquals("Should return custom value when config is set",
            120L, customResult);
    }

    /**
     * Tests the shutdown timeout configuration with default and custom values.
     */
    @Test
    public void testGetShutdownTimeoutSeconds() throws IOException  {
        // Create ReplicationLogDiscoveryReplay instance
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        ReplicationLogDiscoveryReplay discovery = new ReplicationLogDiscoveryReplay(fileTracker);

        // Test default value when no custom config is set
        long defaultResult = discovery.getShutdownTimeoutSeconds();
        assertEquals("Should return default value when no custom config is set",
            ReplicationLogDiscoveryReplay.DEFAULT_SHUTDOWN_TIMEOUT_SECONDS, defaultResult);

        // Test custom value when config is set
        conf.setLong(ReplicationLogDiscoveryReplay.REPLICATION_REPLAY_SHUTDOWN_TIMEOUT_SECONDS_KEY, 45L);
        long customResult = discovery.getShutdownTimeoutSeconds();
        assertEquals("Should return custom value when config is set",
            45L, customResult);
    }

    /**
     * Tests the executor thread count configuration with default and custom values.
     */
    @Test
    public void testGetExecutorThreadCount() throws IOException {
        // Create ReplicationLogDiscoveryReplay instance
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        ReplicationLogDiscoveryReplay discovery = new ReplicationLogDiscoveryReplay(fileTracker);

        // Test default value when no custom config is set
        int defaultResult = discovery.getExecutorThreadCount();
        assertEquals("Should return default value when no custom config is set",
            ReplicationLogDiscoveryReplay.DEFAULT_EXECUTOR_THREAD_COUNT, defaultResult);

        // Test custom value when config is set
        conf.setInt(ReplicationLogDiscoveryReplay.REPLICATION_REPLAY_EXECUTOR_THREAD_COUNT_KEY, 3);
        int customResult = discovery.getExecutorThreadCount();
        assertEquals("Should return custom value when config is set",
            3, customResult);
    }

    /**
     * Tests the in-progress directory processing probability configuration.
     */
    @Test
    public void testGetInProgressDirectoryProcessProbability() throws IOException {
        // Create ReplicationLogDiscoveryReplay instance
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        ReplicationLogDiscoveryReplay discovery = new ReplicationLogDiscoveryReplay(fileTracker);

        // Test default value when no custom config is set
        double defaultResult = discovery.getInProgressDirectoryProcessProbability();
        assertEquals("Should return default value when no custom config is set",
            ReplicationLogDiscoveryReplay.DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY, defaultResult, 0.001);

        // Test custom value when config is set
        conf.setDouble(ReplicationLogDiscoveryReplay.REPLICATION_REPLAY_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY_KEY, 10.5);
        double customResult = discovery.getInProgressDirectoryProcessProbability();
        assertEquals("Should return custom value when config is set",
            10.5, customResult, 0.001);
    }

    /**
     * Tests the waiting buffer percentage configuration with default and custom values.
     */
    @Test
    public void testGetWaitingBufferPercentage() throws IOException {
        // Create ReplicationLogDiscoveryReplay instance
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        ReplicationLogDiscoveryReplay discovery = new ReplicationLogDiscoveryReplay(fileTracker);

        // Test default value when no custom config is set
        double defaultResult = discovery.getWaitingBufferPercentage();
        assertEquals("Should return default value when no custom config is set",
            ReplicationLogDiscoveryReplay.DEFAULT_WAITING_BUFFER_PERCENTAGE, defaultResult, 0.001);

        // Test custom value when config is set
        conf.setDouble(ReplicationLogDiscoveryReplay.REPLICATION_REPLAY_WAITING_BUFFER_PERCENTAGE_KEY, 20.0);
        double customResult = discovery.getWaitingBufferPercentage();
        assertEquals("Should return custom value when config is set",
            20.0, customResult, 0.001);
    }

    /**
     * Tests initialization in DEGRADED state with both in-progress and new files present.
     * Validates that lastRoundProcessed uses minimum timestamp and lastRoundInSync is preserved.
     */
    @Test
    public void testInitializeLastRoundProcessed_DegradedStateWithInProgressAndNewFiles() throws IOException {
        long currentTime = 1704153600000L; // 2024-01-02 00:00:00
        long inProgressFileTimestamp = 1704153420000L; // Earlier timestamp (00:57:00) - 3 min before current
        long newFileTimestamp = 1704153540000L; // Middle timestamp (00:59:00) - 1 min before current
        long lastSyncStateTime = 1704153480000L; // Between in-progress and new file (00:58:00)
        long roundTimeMills = 60000L; // 1 minute

        // Expected: lastRoundProcessed uses minimum timestamp (in-progress file)
        long expectedEndTime = (inProgressFileTimestamp / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
        ReplicationRound expectedLastRoundProcessed = new ReplicationRound(expectedEndTime - roundTimeMills, expectedEndTime);

        // Expected: lastRoundInSync uses minimum of lastSyncStateTime and file timestamps
        long expectedSyncEndTime = (inProgressFileTimestamp / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
        ReplicationRound expectedLastRoundInSync = new ReplicationRound(expectedSyncEndTime - roundTimeMills, expectedSyncEndTime);

        testInitializeLastRoundProcessedHelper(
                currentTime,
                lastSyncStateTime,
                newFileTimestamp,
                inProgressFileTimestamp,
                HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_WRITER,
                expectedLastRoundProcessed,
                expectedLastRoundInSync,
                ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED,
                false);
    }

    @Test
    public void testInitializeLastRoundProcessed_DegradedStateWithBothFilesNewFileIsMin() throws IOException {
        long currentTime = 1704153600000L; // 2024-01-02 00:00:00
        long newFileTimestamp = 1704153420000L; // Earlier timestamp (00:57:00) - 3 min before current
        long inProgressFileTimestamp = 1704153540000L; // Later timestamp (00:59:00) - 1 min before current
        long lastSyncStateTime = 1704153480000L; // Between new and in-progress file (00:58:00)
        long roundTimeMills = 60000L; // 1 minute

        // Expected: lastRoundProcessed uses minimum timestamp (new file)
        long expectedEndTime = (newFileTimestamp / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
        ReplicationRound expectedLastRoundProcessed = new ReplicationRound(expectedEndTime - roundTimeMills, expectedEndTime);

        // Expected: lastRoundInSync uses minimum of lastSyncStateTime and file timestamps
        long expectedSyncEndTime = (newFileTimestamp / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
        ReplicationRound expectedLastRoundInSync = new ReplicationRound(expectedSyncEndTime - roundTimeMills, expectedSyncEndTime);

        testInitializeLastRoundProcessedHelper(
                currentTime,
                lastSyncStateTime,
                newFileTimestamp,
                inProgressFileTimestamp,
                HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_WRITER,
                expectedLastRoundProcessed,
                expectedLastRoundInSync,
                ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED,
                false);
    }

    @Test
    public void testInitializeLastRoundProcessed_DegradedStateWithLastSyncStateAsMin() throws IOException {
        long newFileTimestamp = 1704240060000L;
        long lastSyncStateTime = 1704240030000L;
        long currentTime = 1704240900000L;
        long roundTimeMills = 60000L; // 1 minute

        // Expected: lastRoundProcessed uses minimum of new files and current time
        long expectedEndTime = (newFileTimestamp / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
        ReplicationRound expectedLastRoundProcessed = new ReplicationRound(expectedEndTime - roundTimeMills, expectedEndTime);

        // Expected: lastRoundInSync uses minimum of lastSyncStateTime and file timestamps
        long expectedSyncEndTime = (lastSyncStateTime / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
        ReplicationRound expectedLastRoundInSync = new ReplicationRound(expectedSyncEndTime - roundTimeMills, expectedSyncEndTime);

        testInitializeLastRoundProcessedHelper(
                currentTime,
                lastSyncStateTime,
                newFileTimestamp,
                null, // no in-progress file
                HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY,
                expectedLastRoundProcessed,
                expectedLastRoundInSync,
                ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED,
                false);
    }

    @Test
    public void testInitializeLastRoundProcessed_DegradedStateWithNoFiles() throws IOException {
        long currentTime = 1704326400000L;
        long lastSyncStateTime = 1704326300000L;
        long roundTimeMills = 60000L; // 1 minute

        // Expected: lastRoundProcessed uses current time
        long expectedEndTime = (currentTime / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
        ReplicationRound expectedLastRoundProcessed = new ReplicationRound(expectedEndTime - roundTimeMills, expectedEndTime);

        // Expected: lastRoundInSync uses minimum of lastSyncStateTime and current time
        long expectedSyncEndTime = (lastSyncStateTime / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
        ReplicationRound expectedLastRoundInSync = new ReplicationRound(expectedSyncEndTime - roundTimeMills, expectedSyncEndTime);

        testInitializeLastRoundProcessedHelper(
                currentTime,
                lastSyncStateTime,
                null, // no new file
                null, // no in-progress file
                HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_WRITER,
                expectedLastRoundProcessed,
                expectedLastRoundInSync,
                ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED,
                false);
    }

    @Test
    public void testInitializeLastRoundProcessed_SyncStateWithInProgressFiles() throws IOException {
        long currentTime = 1704412800000L;
        long inProgressTimestamp = 1704412680000L; // 2 min before current time
        long roundTimeMills = 60000L; // 1 minute

        // Expected: uses in-progress file timestamp
        long expectedEndTime = (inProgressTimestamp / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
        ReplicationRound expectedLastRoundProcessed = new ReplicationRound(expectedEndTime - roundTimeMills, expectedEndTime);

        // Expected: lastRoundInSync equals lastRoundProcessed in SYNC state
        ReplicationRound expectedLastRoundInSync = expectedLastRoundProcessed;

        testInitializeLastRoundProcessedHelper(
                currentTime,
                null, // SYNC state - lastSyncStateTime not used
                null, // no new file
                inProgressTimestamp,
                HAGroupStoreRecord.HAGroupState.STANDBY,
                expectedLastRoundProcessed,
                expectedLastRoundInSync,
                ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC,
                false);
    }

    @Test
    public void testInitializeLastRoundProcessed_SyncStateWithNewFiles() throws IOException {
        long currentTime = 1704499200000L;
        long newFileTimestamp = 1704499080000L; // 2 min before current time
        long roundTimeMills = 60000L; // 1 minute

        // Expected: uses new file timestamp
        long expectedEndTime = (newFileTimestamp / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
        ReplicationRound expectedLastRoundProcessed = new ReplicationRound(expectedEndTime - roundTimeMills, expectedEndTime);

        // Expected: lastRoundInSync equals lastRoundProcessed in SYNC state
        ReplicationRound expectedLastRoundInSync = expectedLastRoundProcessed;

        testInitializeLastRoundProcessedHelper(
                currentTime,
                null, // SYNC state - lastSyncStateTime not used
                newFileTimestamp,
                null, // no in-progress file
                HAGroupStoreRecord.HAGroupState.STANDBY,
                expectedLastRoundProcessed,
                expectedLastRoundInSync,
                ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC,
                false);
    }

    @Test
    public void testInitializeLastRoundProcessed_SyncStateWithBothFiles() throws IOException {
        long currentTime = 1704499200000L;
        long inProgressTimestamp = 1704499020000L; // Earlier timestamp - 3 min before current
        long newFileTimestamp = 1704499140000L; // Later timestamp - 1 min before current
        long roundTimeMills = 60000L; // 1 minute

        // Expected: uses minimum timestamp (in-progress file)
        long expectedEndTime = (inProgressTimestamp / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
        ReplicationRound expectedLastRoundProcessed = new ReplicationRound(expectedEndTime - roundTimeMills, expectedEndTime);

        // Expected: lastRoundInSync equals lastRoundProcessed in SYNC state
        ReplicationRound expectedLastRoundInSync = expectedLastRoundProcessed;

        testInitializeLastRoundProcessedHelper(
                currentTime,
                null, // SYNC state - lastSyncStateTime not used
                newFileTimestamp,
                inProgressTimestamp,
                HAGroupStoreRecord.HAGroupState.STANDBY,
                expectedLastRoundProcessed,
                expectedLastRoundInSync,
                ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC,
                false);
    }

    @Test
    public void testInitializeLastRoundProcessed_SyncStateWithNoFiles() throws IOException {
        long currentTime = 1704585600000L;
        long roundTimeMills = 60000L; // 1 minute

        // Expected: uses current time when no files exist
        long expectedEndTime = (currentTime / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
        ReplicationRound expectedLastRoundProcessed = new ReplicationRound(expectedEndTime - roundTimeMills, expectedEndTime);

        // Expected: lastRoundInSync equals lastRoundProcessed in SYNC state
        ReplicationRound expectedLastRoundInSync = expectedLastRoundProcessed;

        testInitializeLastRoundProcessedHelper(
                currentTime,
                null, // SYNC state - lastSyncStateTime not used
                null, // no new file
                null, // no in-progress file
                HAGroupStoreRecord.HAGroupState.STANDBY,
                expectedLastRoundProcessed,
                expectedLastRoundInSync,
                ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC,
                false);
    }

    /**
     * Tests initializeLastRoundProcessed in STANDBY_TO_ACTIVE state.
     * Validates that failoverPending is set to true when HA group state is STANDBY_TO_ACTIVE.
     */
    @Test
    public void testInitializeLastRoundProcessed_StandbyToActiveState() throws IOException {
        long currentTime = 1704153600000L; // 2024-01-02 00:00:00

        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        fileTracker.init();
        long roundTimeMills = fileTracker.getReplicationShardDirectoryManager()
                .getReplicationRoundDurationSeconds() * 1000L;

        ReplicationRound expectedLastRoundProcessed = new ReplicationRound(currentTime - roundTimeMills, currentTime);
        ReplicationRound expectedLastRoundInSync = new ReplicationRound(currentTime - roundTimeMills, currentTime);

        testInitializeLastRoundProcessedHelper(
                currentTime,
                null,
                null,
                null,
                HAGroupStoreRecord.HAGroupState.STANDBY_TO_ACTIVE,
                expectedLastRoundProcessed,
                expectedLastRoundInSync,
                ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC,
                true);
    }

    /**
     * Helper method to test initializeLastRoundProcessed with various file and state configurations.
     * Handles file creation, state setup, and validation of lastRoundProcessed and lastRoundInSync.
     *
     * @param currentTime Current time for the test
     * @param lastSyncStateTime Last sync state time for HAGroupStoreRecord (use null for SYNC state)
     * @param newFileTimestamp Timestamp for new file (use null to skip creating new file)
     * @param inProgressFileTimestamp Timestamp for in-progress file (use null to skip creating in-progress file)
     * @param haGroupState HAGroupState for the test
     * @param expectedLastRoundProcessed Expected lastRoundProcessed after initialization
     * @param expectedLastRoundInSync Expected lastRoundInSync after initialization
     * @param expectedReplayState Expected ReplicationReplayState after initialization
     * @param expectedFailoverPending Expected failoverPending value after initialization
     */
    private void testInitializeLastRoundProcessedHelper(
            long currentTime,
            Long lastSyncStateTime,
            Long newFileTimestamp,
            Long inProgressFileTimestamp,
            HAGroupStoreRecord.HAGroupState haGroupState,
            ReplicationRound expectedLastRoundProcessed,
            ReplicationRound expectedLastRoundInSync,
            ReplicationLogDiscoveryReplay.ReplicationReplayState expectedReplayState,
            boolean expectedFailoverPending) throws IOException {

        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        fileTracker.init();

        try {
            long roundTimeMills = fileTracker.getReplicationShardDirectoryManager()
                    .getReplicationRoundDurationSeconds() * 1000L;

            // Create in-progress file if timestamp provided
            if (inProgressFileTimestamp != null) {
                Path inProgressDir = fileTracker.getInProgressDirPath();
                localFs.mkdirs(inProgressDir);
                Path inProgressFile = new Path(inProgressDir, inProgressFileTimestamp + "_rs-1_uuid.plog");
                localFs.create(inProgressFile, true).close();
            }

            // Create new file if timestamp provided
            if (newFileTimestamp != null) {
                ReplicationRound newFileRound = new ReplicationRound(
                        newFileTimestamp - roundTimeMills, newFileTimestamp);
                Path shardPath = fileTracker.getReplicationShardDirectoryManager()
                        .getShardDirectory(newFileRound.getStartTime());
                localFs.mkdirs(shardPath);
                Path newFile = new Path(shardPath, newFileTimestamp + "_rs-1.plog");
                localFs.create(newFile, true).close();
            }

            // Create HAGroupStoreRecord
            long recordTime = lastSyncStateTime != null ? lastSyncStateTime : currentTime;
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(
                    HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName, haGroupState, recordTime);

            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);

            try {
                TestableReplicationLogDiscoveryReplay discovery =
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);
                discovery.initializeLastRoundProcessed();

                // Verify lastRoundProcessed
                ReplicationRound lastRoundProcessed = discovery.getLastRoundProcessed();
                assertNotNull("Last round processed should not be null", lastRoundProcessed);
                assertEquals("Last round processed should match expected",
                        expectedLastRoundProcessed, lastRoundProcessed);

                // Verify lastRoundInSync
                ReplicationRound lastRoundInSync = discovery.getLastRoundInSync();
                assertNotNull("Last round in sync should not be null", lastRoundInSync);
                assertEquals("Last round in sync should match expected",
                        expectedLastRoundInSync, lastRoundInSync);

                // Verify state
                assertEquals("Replication replay state should match expected",
                        expectedReplayState, discovery.getReplicationReplayState());

                // Verify failoverPending
                assertEquals("Failover pending should match expected",
                        expectedFailoverPending, discovery.getFailoverPending());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }

    /**
     * Tests replay in SYNC state processing multiple rounds.
     * Validates that both lastRoundProcessed and lastRoundInSync advance together.
     */
    @Test
    public void testReplay_SyncState_ProcessMultipleRounds() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);

        try {
            long initialEndTime = 1704153600000L; // 2024-01-02 00:00:00
            long roundTimeMills = fileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;
            long bufferMillis = (long) (roundTimeMills * 0.15);
            long totalWaitTime = roundTimeMills + bufferMillis;

            // Create HAGroupStoreRecord for SYNC state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.STANDBY, initialEndTime);

            // Set current time to allow processing 3 rounds
            long currentTime = initialEndTime + (3 * totalWaitTime);
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);

            try {
                TestableReplicationLogDiscoveryReplay discovery =
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);
                ReplicationRound initialRound = new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime);
                discovery.setLastRoundProcessed(initialRound);
                discovery.setLastRoundInSync(initialRound);
                discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC);

                discovery.replay();

                // Verify processRound was called 3 times
                assertEquals("processRound should be called 3 times", 3, discovery.getProcessRoundCallCount());

                // Verify the rounds passed to processRound
                List<ReplicationRound> processedRounds = discovery.getProcessedRounds();
                ReplicationRound expectedRound1 = new ReplicationRound(initialEndTime, initialEndTime + roundTimeMills);
                assertEquals("First round should match expected", expectedRound1, processedRounds.get(0));

                ReplicationRound expectedRound2 = new ReplicationRound(initialEndTime + roundTimeMills, initialEndTime + (2 * roundTimeMills));
                assertEquals("Second round should match expected", expectedRound2, processedRounds.get(1));

                ReplicationRound expectedRound3 = new ReplicationRound(initialEndTime + (2 * roundTimeMills), initialEndTime + (3 * roundTimeMills));
                assertEquals("Third round should match expected", expectedRound3, processedRounds.get(2));

                // Verify lastRoundProcessed was updated to 3rd round
                ReplicationRound lastRoundProcessed = discovery.getLastRoundProcessed();
                assertNotNull("Last round processed should not be null", lastRoundProcessed);
                assertEquals("Last round processed should be updated to 3rd round",
                        expectedRound3, lastRoundProcessed);

                // Verify lastRoundInSync was also updated in SYNC state
                ReplicationRound lastRoundInSync = discovery.getLastRoundInSync();
                assertNotNull("Last round in sync should not be null", lastRoundInSync);
                assertEquals("Last round in sync should be updated to match last round processed in SYNC state",
                        expectedRound3, lastRoundInSync);

                // Verify state remains SYNC
                assertEquals("State should remain SYNC",
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC,
                        discovery.getReplicationReplayState());

                // Verify triggerFailover was not called
                assertEquals("triggerFailover should not be called", 0, discovery.getTriggerFailoverCallCount());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }

    /**
     * Tests replay in DEGRADED state processing multiple rounds.
     * Validates that lastRoundProcessed advances but lastRoundInSync is preserved.
     */
    @Test
    public void testReplay_DegradedState_MultipleRounds() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);

        try {
            long initialEndTime = 1704240000000L;
            long roundTimeMills = fileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;
            long bufferMillis = (long) (roundTimeMills * 0.15);
            long totalWaitTime = roundTimeMills + bufferMillis;

            // Create HAGroupStoreRecord for DEGRADED state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_WRITER, initialEndTime);

            // Set current time to allow processing 3 rounds
            long currentTime = initialEndTime + (3 * totalWaitTime);
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);

            try {
                TestableReplicationLogDiscoveryReplay discovery =
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);
                discovery.setLastRoundProcessed(new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime));
                ReplicationRound lastRoundInSyncBeforeReplay = new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime);
                discovery.setLastRoundInSync(lastRoundInSyncBeforeReplay);
                discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED);

                discovery.replay();

                // Verify processRound was called 3 times
                assertEquals("processRound should be called 3 times", 3, discovery.getProcessRoundCallCount());

                // Verify the rounds passed to processRound
                List<ReplicationRound> processedRounds = discovery.getProcessedRounds();
                ReplicationRound expectedRound1 = new ReplicationRound(initialEndTime, initialEndTime + roundTimeMills);
                assertEquals("First round should match expected", expectedRound1, processedRounds.get(0));

                ReplicationRound expectedRound2 = new ReplicationRound(initialEndTime + roundTimeMills, initialEndTime + (2 * roundTimeMills));
                assertEquals("Second round should match expected", expectedRound2, processedRounds.get(1));

                ReplicationRound expectedRound3 = new ReplicationRound(initialEndTime + (2 * roundTimeMills), initialEndTime + (3 * roundTimeMills));
                assertEquals("Third round should match expected", expectedRound3, processedRounds.get(2));

                // Verify lastRoundProcessed was updated
                ReplicationRound lastRoundAfterReplay = discovery.getLastRoundProcessed();
                assertEquals("Last round processed should be updated to 3rd round in DEGRADED state",
                        expectedRound3, lastRoundAfterReplay);

                // Verify lastRoundInSync was NOT updated (preserved in DEGRADED state)
                ReplicationRound lastRoundInSyncAfterReplay = discovery.getLastRoundInSync();
                assertEquals("Last round in sync should NOT be updated in DEGRADED state",
                        lastRoundInSyncBeforeReplay, lastRoundInSyncAfterReplay);

                // Verify state remains DEGRADED
                assertEquals("State should remain DEGRADED",
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED,
                        discovery.getReplicationReplayState());

                // Verify triggerFailover was not called
                assertEquals("triggerFailover should not be called", 0, discovery.getTriggerFailoverCallCount());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }

    /**
     * Tests replay in SYNCED_RECOVERY state with rewind to lastRoundInSync.
     * Validates that processing rewinds and re-processes from lastRoundInSync.
     */
    @Test
    public void testReplay_SyncedRecoveryState_RewindToLastInSync() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);

        try {
            long initialEndTime = 1704326400000L;
            long roundTimeMills = fileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;
            long bufferMillis = (long) (roundTimeMills * 0.15);
            long totalWaitTime = roundTimeMills + bufferMillis;

            // Create HAGroupStoreRecord for STANDBY state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.STANDBY, initialEndTime);

            // Set current time to allow processing multiple rounds
            long currentTime = initialEndTime + (5 * totalWaitTime);
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);

            try {
                TestableReplicationLogDiscoveryReplay discovery =
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);

                // Set initial state: lastRoundProcessed is ahead, lastRoundInSync is behind
                ReplicationRound lastInSyncRound = new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime);
                ReplicationRound currentRound = new ReplicationRound(initialEndTime + (2 * roundTimeMills), initialEndTime + (3 * roundTimeMills));

                discovery.setLastRoundProcessed(currentRound);
                discovery.setLastRoundInSync(lastInSyncRound);
                discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNCED_RECOVERY);

                discovery.replay();

                // Verify processRound was called 6 times
                // Flow: 1 round in SYNCED_RECOVERY (triggers rewind), then 5 rounds in SYNC
                // getFirstRoundToProcess() uses lastRoundInSync.endTime = initialEndTime
                // After processing first round in SYNCED_RECOVERY, it rewinds to lastRoundInSync
                // Then continues processing from initialEndTime again (re-processing first round)
                assertEquals("processRound should be called 6 times", 6, discovery.getProcessRoundCallCount());

                // Verify the first round - starts from lastRoundInSync.endTime = initialEndTime (via getFirstRoundToProcess)
                List<ReplicationRound> processedRounds = discovery.getProcessedRounds();
                ReplicationRound expectedFirstRound = new ReplicationRound(initialEndTime, initialEndTime + roundTimeMills);
                assertEquals("First round (SYNCED_RECOVERY) should start from lastRoundInSync.endTime",
                        expectedFirstRound, processedRounds.get(0));

                // After SYNCED_RECOVERY rewind, processing restarts from lastRoundInSync.endTime = initialEndTime
                // This means round 1 is processed again, then rounds 2-5
                ReplicationRound expectedSecondRound = new ReplicationRound(initialEndTime, initialEndTime + roundTimeMills);
                assertEquals("Second round (first in SYNC, re-processing) should match expected",
                        expectedSecondRound, processedRounds.get(1));

                ReplicationRound expectedThirdRound = new ReplicationRound(initialEndTime + roundTimeMills, initialEndTime + (2 * roundTimeMills));
                assertEquals("Third round should match expected",
                        expectedThirdRound, processedRounds.get(2));

                ReplicationRound expectedFourthRound = new ReplicationRound(initialEndTime + (2 * roundTimeMills), initialEndTime + (3 * roundTimeMills));
                assertEquals("Fourth round should match expected",
                        expectedFourthRound, processedRounds.get(3));

                ReplicationRound expectedFifthRound = new ReplicationRound(initialEndTime + (3 * roundTimeMills), initialEndTime + (4 * roundTimeMills));
                assertEquals("Fifth round should match expected",
                        expectedFifthRound, processedRounds.get(4));

                ReplicationRound expectedSixthRound = new ReplicationRound(initialEndTime + (4 * roundTimeMills), initialEndTime + (5 * roundTimeMills));
                assertEquals("Sixth round should match expected",
                        expectedSixthRound, processedRounds.get(5));

                // Verify lastRoundProcessed was updated
                ReplicationRound lastRoundProcessed = discovery.getLastRoundProcessed();
                assertNotNull("Last round processed should not be null", lastRoundProcessed);
                assertEquals("Last round processed should be updated to 5th round",
                        expectedSixthRound, lastRoundProcessed);

                // Verify lastRoundInSync was also updated in SYNC state (after rewind and transition)
                ReplicationRound lastRoundInSync = discovery.getLastRoundInSync();
                assertNotNull("Last round in sync should not be null", lastRoundInSync);
                assertEquals("Last round in sync should be updated to match last round processed after SYNC transition",
                        expectedSixthRound, lastRoundInSync);

                // Verify state transitioned to SYNC
                assertEquals("State should transition to SYNC",
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC,
                        discovery.getReplicationReplayState());

                // Verify triggerFailover was not called
                assertEquals("triggerFailover should not be called", 0, discovery.getTriggerFailoverCallCount());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }

    /**
     * Tests state transition from SYNC to DEGRADED during replay processing.
     * Validates that lastRoundInSync is preserved at the last SYNC round.
     */
    @Test
    public void testReplay_StateTransition_SyncToDegradedDuringProcessing() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);

        try {
            long initialEndTime = 1704412800000L;
            long roundTimeMills = fileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;
            long bufferMillis = (long) (roundTimeMills * 0.15);
            long totalWaitTime = roundTimeMills + bufferMillis;

            // Create HAGroupStoreRecord for STANDBY state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.STANDBY, initialEndTime);

            // Set current time to allow processing 5 rounds
            long currentTime = initialEndTime + (5 * totalWaitTime);
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);

            try {
                TestableReplicationLogDiscoveryReplay discovery =
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);
                ReplicationRound initialRound = new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime);
                discovery.setLastRoundProcessed(initialRound);
                discovery.setLastRoundInSync(initialRound);
                discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC);

                // Simulate listener changing state to DEGRADED after 2 rounds
                discovery.setStateChangeAfterRounds(2, ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED);

                discovery.replay();

                // Verify processRound was called 5 times (2 in SYNC, 3 in DEGRADED)
                assertEquals("processRound should be called 5 times", 5, discovery.getProcessRoundCallCount());

                // Verify the rounds passed to processRound
                List<ReplicationRound> processedRounds = discovery.getProcessedRounds();

                // First 2 rounds in SYNC mode
                ReplicationRound expectedRound1 = new ReplicationRound(initialEndTime, initialEndTime + roundTimeMills);
                assertEquals("First round should match expected", expectedRound1, processedRounds.get(0));

                ReplicationRound expectedRound2 = new ReplicationRound(initialEndTime + roundTimeMills, initialEndTime + (2 * roundTimeMills));
                assertEquals("Second round should match expected", expectedRound2, processedRounds.get(1));

                // Remaining 3 rounds in DEGRADED mode
                ReplicationRound expectedRound3 = new ReplicationRound(initialEndTime + (2 * roundTimeMills), initialEndTime + (3 * roundTimeMills));
                assertEquals("Third round should match expected", expectedRound3, processedRounds.get(2));

                ReplicationRound expectedRound4 = new ReplicationRound(initialEndTime + (3 * roundTimeMills), initialEndTime + (4 * roundTimeMills));
                assertEquals("Fourth round should match expected", expectedRound4, processedRounds.get(3));

                ReplicationRound expectedRound5 = new ReplicationRound(initialEndTime + (4 * roundTimeMills), initialEndTime + (5 * roundTimeMills));
                assertEquals("Fifth round should match expected", expectedRound5, processedRounds.get(4));

                // Verify lastRoundProcessed was updated to 5th round
                ReplicationRound lastRoundAfterReplay = discovery.getLastRoundProcessed();
                assertEquals("Last round processed should be updated to 5th round",
                        expectedRound5, lastRoundAfterReplay);

                // Verify lastRoundInSync was updated only for first round (SYNC), then preserved
                // State changed to DEGRADED AFTER processing round 2, but BEFORE updating lastRoundInSync for round 2
                // So lastRoundInSync should remain at round 1 (the last round fully completed in SYNC state)
                ReplicationRound lastRoundInSync = discovery.getLastRoundInSync();
                assertNotNull("Last round in sync should not be null", lastRoundInSync);
                assertEquals("Last round in sync should be preserved at round 1 (last fully completed SYNC round)",
                        expectedRound1, lastRoundInSync);

                // Verify state is now DEGRADED
                assertEquals("State should be DEGRADED",
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED,
                        discovery.getReplicationReplayState());

                // Verify triggerFailover was not called
                assertEquals("triggerFailover should not be called", 0, discovery.getTriggerFailoverCallCount());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }

    /**
     * Tests state transition from DEGRADED to SYNCED_RECOVERY and then to SYNC.
     * Validates rewind behavior and lastRoundInSync update after SYNC transition.
     */
    @Test
    public void testReplay_StateTransition_DegradedToSyncedRecovery() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);

        try {
            long initialEndTime = 1704499200000L;
            long roundTimeMills = fileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;
            long bufferMillis = (long) (roundTimeMills * 0.15);

            // Create HAGroupStoreRecord for DEGRADED state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY, initialEndTime);

            // Set current time to allow processing 5 rounds
            long currentTime = initialEndTime + (5 * roundTimeMills) + bufferMillis;
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);

            try {
                TestableReplicationLogDiscoveryReplay discovery =
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);

                ReplicationRound lastInSyncRound = new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime);
                discovery.setLastRoundProcessed(new ReplicationRound(initialEndTime + roundTimeMills, initialEndTime + (2 * roundTimeMills)));
                discovery.setLastRoundInSync(lastInSyncRound);
                discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED);

                // Simulate listener changing state to SYNCED_RECOVERY after 2 rounds
                discovery.setStateChangeAfterRounds(2, ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNCED_RECOVERY);

                discovery.replay();

                // Verify processRound was called: 2 in DEGRADED, then 5 in SYNC
                // Total: 2 + 5 = 7 calls
                // First call uses getFirstRoundToProcess() which starts from lastRoundInSync.endTime = initialEndTime
                // After state change to SYNCED_RECOVERY, it rewinds and continues from lastRoundInSync.endTime
                assertEquals("processRound should be called 7 times", 7, discovery.getProcessRoundCallCount());

                // Verify the rounds passed to processRound
                List<ReplicationRound> processedRounds = discovery.getProcessedRounds();

                // First 2 rounds in DEGRADED mode (starting from lastRoundInSync.endTime = initialEndTime)
                // getFirstRoundToProcess() uses lastRoundInSync.endTime, not lastRoundProcessed.endTime
                ReplicationRound expectedRound1 = new ReplicationRound(initialEndTime, initialEndTime + roundTimeMills);
                assertEquals("First round should start from lastRoundInSync.endTime", expectedRound1, processedRounds.get(0));

                ReplicationRound expectedRound2 = new ReplicationRound(initialEndTime + roundTimeMills, initialEndTime + (2 * roundTimeMills));
                assertEquals("Second round should match expected", expectedRound2, processedRounds.get(1));

                // After 2 rounds, state changes to SYNCED_RECOVERY and rewinds to lastRoundInSync
                // Processing restarts from lastRoundInSync.endTime = initialEndTime (re-processing rounds 1-2 and continuing)
                ReplicationRound expectedRound3 = new ReplicationRound(initialEndTime, initialEndTime + roundTimeMills);
                assertEquals("Third round (after rewind) should restart from lastRoundInSync.endTime", expectedRound3, processedRounds.get(2));

                ReplicationRound expectedRound4 = new ReplicationRound(initialEndTime + roundTimeMills, initialEndTime + (2 * roundTimeMills));
                assertEquals("Fourth round should match expected", expectedRound4, processedRounds.get(3));

                ReplicationRound expectedRound5 = new ReplicationRound(initialEndTime + (2 * roundTimeMills), initialEndTime + (3 * roundTimeMills));
                assertEquals("Fifth round should match expected", expectedRound5, processedRounds.get(4));

                ReplicationRound expectedRound6 = new ReplicationRound(initialEndTime + (3 * roundTimeMills), initialEndTime + (4 * roundTimeMills));
                assertEquals("Sixth round should match expected", expectedRound6, processedRounds.get(5));

                ReplicationRound expectedRound7 = new ReplicationRound(initialEndTime + (4 * roundTimeMills), initialEndTime + (5 * roundTimeMills));
                assertEquals("Seventh round should match expected", expectedRound7, processedRounds.get(6));

                // Verify lastRoundProcessed was updated to 7th round
                ReplicationRound lastRoundAfterReplay = discovery.getLastRoundProcessed();
                assertEquals("Last round processed should be updated to 7th round",
                        expectedRound7, lastRoundAfterReplay);

                // Verify lastRoundInSync was preserved during DEGRADED, then updated during SYNC
                // After transition to SYNC (from SYNCED_RECOVERY), lastRoundInSync should match lastRoundProcessed
                ReplicationRound lastRoundInSync = discovery.getLastRoundInSync();
                assertNotNull("Last round in sync should not be null", lastRoundInSync);
                assertEquals("Last round in sync should be updated to match last round processed after SYNC transition",
                        expectedRound7, lastRoundInSync);

                // Verify state transitioned to SYNC (from SYNCED_RECOVERY)
                assertEquals("State should be SYNC after SYNCED_RECOVERY",
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC,
                        discovery.getReplicationReplayState());

                // Verify triggerFailover was not called
                assertEquals("triggerFailover should not be called", 0, discovery.getTriggerFailoverCallCount());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }

    /**
     * Tests state transition from SYNC to DEGRADED and back through SYNCED_RECOVERY to SYNC.
     * Validates lastRoundInSync preservation during DEGRADED, rewind in SYNCED_RECOVERY, and update in SYNC.
     */
    @Test
    public void testReplay_StateTransition_SyncToDegradedAndBackToSync() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);

        try {
            long initialEndTime = 1704672000000L;
            long roundTimeMills = fileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;
            long bufferMillis = (long) (roundTimeMills * 0.15);

            // Create HAGroupStoreRecord for STANDBY state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.STANDBY, initialEndTime);

            // Set current time to allow processing enough rounds (including rewind)
            long currentTime = initialEndTime + (10 * roundTimeMills) + bufferMillis;
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);

            try {
                TestableReplicationLogDiscoveryReplay discovery =
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);
                ReplicationRound initialRound = new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime);
                discovery.setLastRoundProcessed(initialRound);
                discovery.setLastRoundInSync(initialRound);
                discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC);

                // Simulate state transitions:
                // - After 2 rounds: SYNC -> DEGRADED
                // - After 5 rounds: DEGRADED -> SYNCED_RECOVERY (triggers rewind to lastRoundInSync)
                TestableReplicationLogDiscoveryReplay discoveryWithTransitions =
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord) {
                    private int roundCount = 0;

                    @Override
                    protected void processRound(ReplicationRound replicationRound) throws IOException {
                        super.processRound(replicationRound);
                        roundCount++;

                        // Transition to DEGRADED after 2 rounds
                        if (roundCount == 2) {
                            setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED);
                        }
                        // Transition to SYNCED_RECOVERY after 5 rounds (will trigger rewind)
                        else if (roundCount == 5) {
                            setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNCED_RECOVERY);
                        }
                    }
                };

                discoveryWithTransitions.setLastRoundProcessed(initialRound);
                discoveryWithTransitions.setLastRoundInSync(initialRound);
                discoveryWithTransitions.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC);

                discoveryWithTransitions.replay();

                // Verify processRound was called exactly 15 times:
                // - 2 rounds in SYNC (rounds 1-2)
                // - 3 rounds in DEGRADED (rounds 3-5)
                // - 1 round in SYNCED_RECOVERY (round 6, triggers rewind to lastRoundInSync)
                // - After rewind, continues from lastRoundInSync.endTime = firstRound
                // - 8 more rounds in SYNC (rounds 7-14, from initialEndTime to initialEndTime + 8*roundTimeMills)
                int totalRoundsProcessed = discoveryWithTransitions.getProcessRoundCallCount();
                assertEquals("processRound should be called exactly 14 times", 14, totalRoundsProcessed);

                // Verify the rounds passed to processRound
                List<ReplicationRound> processedRounds = discoveryWithTransitions.getProcessedRounds();

                // Rounds 1-2: SYNC mode (starting from initialEndTime)
                for (int i = 0; i < 2; i++) {
                    long startTime = initialEndTime + (i * roundTimeMills);
                    long endTime = initialEndTime + ((i + 1) * roundTimeMills);
                    ReplicationRound expectedRound = new ReplicationRound(startTime, endTime);
                    assertEquals("Round " + (i + 1) + " (SYNC) should match expected",
                            expectedRound, processedRounds.get(i));
                }

                // Rounds 3-5: DEGRADED mode (continuing from round 2)
                for (int i = 2; i < 5; i++) {
                    long startTime = initialEndTime + (i * roundTimeMills);
                    long endTime = initialEndTime + ((i + 1) * roundTimeMills);
                    ReplicationRound expectedRound = new ReplicationRound(startTime, endTime);
                    assertEquals("Round " + (i + 1) + " (DEGRADED) should match expected",
                            expectedRound, processedRounds.get(i));
                }

                // Round 6-14: SYNC mode after SYNCED_RECOVERY rewind
                // After rewind, starts from lastRoundInSync.endTime = initialEndTime + roundTimeMills
                for (int i = 5; i < 14; i++) {
                    // Offset by 1 because rewind goes back to lastRoundInSync.endTime
                    long startTime = initialEndTime + ((i - 4) * roundTimeMills);
                    long endTime = initialEndTime + ((i - 3) * roundTimeMills);
                    ReplicationRound expectedRound = new ReplicationRound(startTime, endTime);
                    assertEquals("Round " + (i + 1) + " (SYNC after rewind) should match expected",
                            expectedRound, processedRounds.get(i));
                }

                // Verify lastRoundProcessed was updated to the last processed round (round 14)
                ReplicationRound lastRoundAfterReplay = discoveryWithTransitions.getLastRoundProcessed();
                assertNotNull("Last round processed should not be null", lastRoundAfterReplay);
                ReplicationRound expectedLastRound = new ReplicationRound(
                        initialEndTime + (9 * roundTimeMills),
                        initialEndTime + (10 * roundTimeMills));
                assertEquals("Last round processed should be the 14th round",
                        expectedLastRound, lastRoundAfterReplay);

                // Verify lastRoundInSync behavior:
                // - Updated for round 1 (SYNC)
                // - State changed to DEGRADED after round 2, so lastRoundInSync stays at round 1
                // - Preserved during rounds 3-5 (DEGRADED)
                // - SYNCED_RECOVERY triggers rewind to lastRoundInSync
                // - After transition to SYNC, lastRoundInSync updates for rounds 6-14
                // - Final lastRoundInSync should match lastRoundProcessed (round 14)
                ReplicationRound lastRoundInSync = discoveryWithTransitions.getLastRoundInSync();
                assertNotNull("Last round in sync should not be null", lastRoundInSync);
                assertEquals("Last round in sync should match last round processed after returning to SYNC",
                        expectedLastRound, lastRoundInSync);

                // Verify state transitioned to SYNC (from SYNCED_RECOVERY)
                assertEquals("State should be SYNC after SYNCED_RECOVERY",
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC,
                        discoveryWithTransitions.getReplicationReplayState());

                // Verify triggerFailover was not called
                assertEquals("triggerFailover should not be called", 0, discoveryWithTransitions.getTriggerFailoverCallCount());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }

    /**
     * Tests replay when no rounds are ready to process.
     * Validates that lastRoundProcessed and lastRoundInSync remain unchanged.
     */
    @Test
    public void testReplay_NoRoundsToProcess() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);

        try {
            long initialEndTime = 1704585600000L;
            long roundTimeMills = fileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;

            // Create HAGroupStoreRecord for SYNC state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.STANDBY, initialEndTime);

            // Set current time to NOT allow processing any rounds (not enough time has passed)
            long currentTime = initialEndTime + 1000L; // Only 1 second after
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);

            try {
                TestableReplicationLogDiscoveryReplay discovery =
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);
                ReplicationRound initialRound = new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime);
                discovery.setLastRoundProcessed(initialRound);
                discovery.setLastRoundInSync(initialRound);
                discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC);

                ReplicationRound lastRoundBeforeReplay = discovery.getLastRoundProcessed();
                ReplicationRound lastRoundInSyncBeforeReplay = discovery.getLastRoundInSync();

                discovery.replay();

                // Verify processRound was not called
                assertEquals("processRound should not be called when no rounds to process",
                        0, discovery.getProcessRoundCallCount());

                // Verify lastRoundProcessed was not changed
                ReplicationRound lastRoundAfterReplay = discovery.getLastRoundProcessed();
                assertEquals("Last round processed should not change when no rounds to process",
                        lastRoundBeforeReplay, lastRoundAfterReplay);

                // Verify lastRoundInSync was not changed
                ReplicationRound lastRoundInSyncAfterReplay = discovery.getLastRoundInSync();
                assertEquals("Last round in sync should not change when no rounds to process",
                        lastRoundInSyncBeforeReplay, lastRoundInSyncAfterReplay);

                // Verify state remains SYNC
                assertEquals("State should remain SYNC",
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC,
                        discovery.getReplicationReplayState());

                // Verify triggerFailover was not called
                assertEquals("triggerFailover should not be called", 0, discovery.getTriggerFailoverCallCount());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }

    /**
     * Tests replay method when failoverPending becomes true during processing and triggers failover after all rounds.
     * Validates that triggerFailover is called exactly once when all conditions are met.
     */
    @Test
    public void testReplay_TriggerFailoverAfterProcessing() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);

        try {
            long initialEndTime = 1704153600000L; // 2024-01-02 00:00:00
            long roundTimeMills = fileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;
            long bufferMillis = (long) (roundTimeMills * 0.15);
            long totalWaitTime = roundTimeMills + bufferMillis;

            // Create HAGroupStoreRecord for STANDBY state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.STANDBY, initialEndTime);

            // Set current time to allow processing 3 rounds
            long currentTime = initialEndTime + (3 * totalWaitTime);
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);

            try {
                TestableReplicationLogDiscoveryReplay discovery =
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);
                ReplicationRound initialRound = new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime);
                discovery.setLastRoundProcessed(initialRound);
                discovery.setLastRoundInSync(initialRound);
                discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC);

                // Set up state change: after processing 2 rounds, set failoverPending to true
                discovery.setStateChangeAfterRounds(2, ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC);
                discovery.setFailoverPendingAfterRounds(2, true);

                discovery.replay();

                // Verify processRound was called 3 times
                assertEquals("processRound should be called 3 times", 3, discovery.getProcessRoundCallCount());

                // Verify the rounds passed to processRound
                List<ReplicationRound> processedRounds = discovery.getProcessedRounds();
                ReplicationRound expectedRound1 = new ReplicationRound(initialEndTime, initialEndTime + roundTimeMills);
                assertEquals("First round should match expected", expectedRound1, processedRounds.get(0));

                ReplicationRound expectedRound2 = new ReplicationRound(initialEndTime + roundTimeMills, initialEndTime + (2 * roundTimeMills));
                assertEquals("Second round should match expected", expectedRound2, processedRounds.get(1));

                ReplicationRound expectedRound3 = new ReplicationRound(initialEndTime + (2 * roundTimeMills), initialEndTime + (3 * roundTimeMills));
                assertEquals("Third round should match expected", expectedRound3, processedRounds.get(2));

                // Verify lastRoundProcessed was updated to 3rd round
                ReplicationRound lastRoundProcessed = discovery.getLastRoundProcessed();
                assertNotNull("Last round processed should not be null", lastRoundProcessed);
                assertEquals("Last round processed should be updated to 3rd round",
                        expectedRound3, lastRoundProcessed);

                // Verify lastRoundInSync was also updated in SYNC state
                ReplicationRound lastRoundInSync = discovery.getLastRoundInSync();
                assertNotNull("Last round in sync should not be null", lastRoundInSync);
                assertEquals("Last round in sync should be updated to match last round processed in SYNC state",
                        expectedRound3, lastRoundInSync);

                // Verify state remains SYNC
                assertEquals("State should remain SYNC",
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC,
                        discovery.getReplicationReplayState());

                // Verify triggerFailover was called exactly once
                assertEquals("triggerFailover should be called exactly once", 1, discovery.getTriggerFailoverCallCount());

                // Verify failoverPending is set to false after failover is triggered
                assertFalse("failoverPending should be set to false after failover is triggered",
                        discovery.getFailoverPending());

                // TODO: Ensure cluster state is updated to ACTIVE_IN_SYNC once failover is triggered.
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }

    private TestableReplicationLogTracker createReplicationLogTracker(final Configuration conf, final String haGroupName, final FileSystem fileSystem, final URI rootURI) throws IOException {
        Path newFilesDirectory = new Path(new Path(rootURI.getPath(), haGroupName), ReplicationLogReplay.IN_DIRECTORY_NAME);
        ReplicationShardDirectoryManager replicationShardDirectoryManager =
                new ReplicationShardDirectoryManager(conf, newFilesDirectory);
        TestableReplicationLogTracker testableReplicationLogTracker = new TestableReplicationLogTracker(conf, haGroupName, fileSystem, replicationShardDirectoryManager, METRICS_REPLICATION_LOG_TRACKER);
        testableReplicationLogTracker.init();
        return testableReplicationLogTracker;
    }

    /**
     * Testable implementation of ReplicationLogTracker for unit testing.
     * Exposes protected methods to allow test access.
     */
    private static class TestableReplicationLogTracker extends ReplicationLogTracker {
        public TestableReplicationLogTracker(Configuration conf, String haGroupName, FileSystem fileSystem, ReplicationShardDirectoryManager replicationShardDirectoryManager, MetricsReplicationLogTracker metrics) {
            super(conf, haGroupName, fileSystem, replicationShardDirectoryManager, metrics);
        }
        public Path getInProgressDirPath() {
            return super.getInProgressDirPath();
        }
    }

    /**
     * Tests the shouldTriggerFailover method with various combinations of failoverPending,
     * lastRoundInSync, lastRoundProcessed and in-progress files state.
     */
    @Test
    public void testShouldTriggerFailover() throws IOException {
        // Set up current time for consistent testing
        long currentTime = 1704153660000L; // 00:01:00
        EnvironmentEdge edge = () -> currentTime;
        EnvironmentEdgeManager.injectEdge(edge);

        // Initialize haGroupStoreRecord
        final ReplicationLogTracker tracker = Mockito.spy(createReplicationLogTracker(conf, haGroupName, localFs, standbyUri));
        long initialEndTime = currentTime - tracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;
        HAGroupStoreRecord haGroupStoreRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                HAGroupStoreRecord.HAGroupState.STANDBY, initialEndTime);

        try {
            // Create test rounds
            ReplicationRound testRound = new ReplicationRound(1704153600000L, 1704153660000L);
            ReplicationRound differentRound = new ReplicationRound(1704153540000L, 1704153600000L);

            // Test Case 1: All conditions true - should return true
            {
                when(tracker.getInProgressFiles()).thenReturn(Collections.emptyList());
                TestableReplicationLogDiscoveryReplay discovery = new TestableReplicationLogDiscoveryReplay(tracker, haGroupStoreRecord);
                discovery.setLastRoundInSync(testRound);
                discovery.setLastRoundProcessed(testRound);
                discovery.setFailoverPending(true);

                assertTrue("Should trigger failover when all conditions are met",
                        discovery.shouldTriggerFailover());
            }

            // Test Case 2: failoverPending is false - should return false
            {
                when(tracker.getInProgressFiles()).thenReturn(Collections.emptyList());
                TestableReplicationLogDiscoveryReplay discovery = new TestableReplicationLogDiscoveryReplay(tracker, haGroupStoreRecord);
                discovery.setLastRoundInSync(testRound);
                discovery.setLastRoundProcessed(testRound);
                discovery.setFailoverPending(false);

                assertFalse("Should not trigger failover when failoverPending is false",
                        discovery.shouldTriggerFailover());
            }

            // Test Case 3: lastRoundInSync not equals lastRoundProcessed - should return false
            {
                when(tracker.getInProgressFiles()).thenReturn(Collections.emptyList());
                TestableReplicationLogDiscoveryReplay discovery = new TestableReplicationLogDiscoveryReplay(tracker, haGroupStoreRecord);
                discovery.setLastRoundInSync(testRound);
                discovery.setLastRoundProcessed(differentRound);
                discovery.setFailoverPending(true);

                assertFalse("Should not trigger failover when lastRoundInSync != lastRoundProcessed",
                        discovery.shouldTriggerFailover());
            }

            // Test Case 4: in-progress files not empty - should return false
            {
                when(tracker.getInProgressFiles()).thenReturn(Collections.singletonList(new Path("test.plog")));
                TestableReplicationLogDiscoveryReplay discovery = new TestableReplicationLogDiscoveryReplay(tracker, haGroupStoreRecord);
                discovery.setLastRoundInSync(testRound);
                discovery.setLastRoundProcessed(testRound);
                discovery.setFailoverPending(true);

                assertFalse("Should not trigger failover when in-progress files are not empty",
                        discovery.shouldTriggerFailover());
            }

            // Test Case 5: failoverPending false AND lastRoundInSync != lastRoundProcessed - should return false
            {
                when(tracker.getInProgressFiles()).thenReturn(Collections.emptyList());
                TestableReplicationLogDiscoveryReplay discovery = new TestableReplicationLogDiscoveryReplay(tracker, haGroupStoreRecord);
                discovery.setLastRoundInSync(testRound);
                discovery.setLastRoundProcessed(differentRound);
                discovery.setFailoverPending(false);

                assertFalse("Should not trigger failover when failoverPending is false and rounds differ",
                        discovery.shouldTriggerFailover());
            }

            // Test Case 6: failoverPending false AND in-progress files not empty - should return false
            {
                when(tracker.getInProgressFiles()).thenReturn(Collections.singletonList(new Path("test.plog")));
                TestableReplicationLogDiscoveryReplay discovery = new TestableReplicationLogDiscoveryReplay(tracker, haGroupStoreRecord);
                discovery.setLastRoundInSync(testRound);
                discovery.setLastRoundProcessed(testRound);
                discovery.setFailoverPending(false);

                assertFalse("Should not trigger failover when failoverPending is false and files exist",
                        discovery.shouldTriggerFailover());
            }

            // Test Case 7: lastRoundInSync != lastRoundProcessed AND in-progress files not empty - should return false
            {
                when(tracker.getInProgressFiles()).thenReturn(Collections.singletonList(new Path("test.plog")));
                TestableReplicationLogDiscoveryReplay discovery = new TestableReplicationLogDiscoveryReplay(tracker, haGroupStoreRecord);
                discovery.setLastRoundInSync(testRound);
                discovery.setLastRoundProcessed(differentRound);
                discovery.setFailoverPending(true);

                assertFalse("Should not trigger failover when rounds differ and files exist",
                        discovery.shouldTriggerFailover());
            }

            // Test Case 8: All conditions false - should return false
            {
                when(tracker.getInProgressFiles()).thenReturn(Collections.singletonList(new Path("test.plog")));
                TestableReplicationLogDiscoveryReplay discovery = new TestableReplicationLogDiscoveryReplay(tracker, haGroupStoreRecord);
                discovery.setLastRoundInSync(testRound);
                discovery.setLastRoundProcessed(differentRound);
                discovery.setFailoverPending(false);

                assertFalse("Should not trigger failover when all conditions are false",
                        discovery.shouldTriggerFailover());
            }

        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    /**
     * Testable implementation of ReplicationLogDiscoveryReplay for unit testing.
     * Provides dependency injection for HAGroupStoreRecord, tracks processed rounds,
     * and supports simulating state changes during replay.
     */
    private static class TestableReplicationLogDiscoveryReplay extends ReplicationLogDiscoveryReplay {
        private final HAGroupStoreRecord haGroupStoreRecord;
        private int roundsProcessed = 0;
        private int stateChangeAfterRounds = -1;
        private ReplicationReplayState newStateAfterRounds = null;
        private int failoverPendingChangeAfterRounds = -1;
        private boolean failoverPendingValueAfterRounds = false;
        private final List<ReplicationRound> processedRounds = new java.util.ArrayList<>();

        public TestableReplicationLogDiscoveryReplay(ReplicationLogTracker replicationLogReplayFileTracker,
                                                     HAGroupStoreRecord haGroupStoreRecord) {
            super(replicationLogReplayFileTracker);
            this.haGroupStoreRecord = haGroupStoreRecord;
        }

        @Override
        protected HAGroupStoreRecord getHAGroupRecord() {
            return haGroupStoreRecord;
        }

        @Override
        protected void processRound(ReplicationRound replicationRound) throws IOException {
            LOG.info("Processing Round: {}", replicationRound);
            // Track processed rounds
            processedRounds.add(replicationRound);

            // Simulate state change by listener after certain number of rounds
            roundsProcessed++;
            if (stateChangeAfterRounds > 0 && roundsProcessed == stateChangeAfterRounds && newStateAfterRounds != null) {
                LOG.info("Rounds Processed: {}, newStateAfterRounds: {}", roundsProcessed, newStateAfterRounds);
                setReplicationReplayState(newStateAfterRounds);
            }

            // Simulate failover pending change by listener after certain number of rounds
            if (failoverPendingChangeAfterRounds > 0 && roundsProcessed == failoverPendingChangeAfterRounds) {
                LOG.info("Rounds Processed: {}, setting failoverPending to: {}", roundsProcessed, failoverPendingValueAfterRounds);
                setFailoverPending(failoverPendingValueAfterRounds);
            }
            // Don't actually process files in tests
        }

        public void setStateChangeAfterRounds(int afterRounds, ReplicationReplayState newState) {
            this.stateChangeAfterRounds = afterRounds;
            this.newStateAfterRounds = newState;
        }

        public void setFailoverPendingAfterRounds(int afterRounds, boolean failoverPendingValue) {
            this.failoverPendingChangeAfterRounds = afterRounds;
            this.failoverPendingValueAfterRounds = failoverPendingValue;
        }

        private int triggerFailoverCallCount = 0;

        @Override
        protected void triggerFailover() {
            // Track calls to triggerFailover for validation
            triggerFailoverCallCount++;

            // Simulate the real behavior: set failoverPending to false after triggering failover
            setFailoverPending(false);
        }

        public int getTriggerFailoverCallCount() {
            return triggerFailoverCallCount;
        }

        public int getProcessRoundCallCount() {
            return processedRounds.size();
        }

        public List<ReplicationRound> getProcessedRounds() {
            return new java.util.ArrayList<>(processedRounds);
        }
    }
}
