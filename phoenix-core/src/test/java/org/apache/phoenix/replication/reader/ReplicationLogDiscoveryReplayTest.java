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
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTrackerReplayImpl;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTracker;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ReplicationLogDiscoveryReplayTest {

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

    @Test
    public void testInitializeLastRoundProcessed_DegradedStateWithInProgressAndNewFiles() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        fileTracker.init();
        
        try {
            long currentTime = 1704153600000L; // 2024-01-02 00:00:00
            long inProgressFileTimestamp = 1704153540000L; // Earlier timestamp (00:59:00)
            long newFileTimestamp = 1704153570000L; // Middle timestamp (00:59:30)
            long lastSyncStateTime = 1704153550000L; // Between in-progress and new file
            
            // Create in-progress file
            Path inProgressDir = fileTracker.getInProgressDirPath();
            localFs.mkdirs(inProgressDir);
            Path inProgressFile = new Path(inProgressDir, inProgressFileTimestamp + "_rs-1_uuid.plog");
            localFs.create(inProgressFile, true).close();
            
            // Create new file
            long roundTimeMills = fileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;
            ReplicationRound newFileRound = new ReplicationRound(newFileTimestamp - roundTimeMills, newFileTimestamp);
            Path shardPath = fileTracker.getReplicationShardDirectoryManager().getShardDirectory(newFileRound.getStartTime());
            localFs.mkdirs(shardPath);
            Path newFile = new Path(shardPath, newFileTimestamp + "_rs-1.plog");
            localFs.create(newFile, true).close();
            
            // Create HAGroupStoreRecord for DEGRADED state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_WRITER, lastSyncStateTime);
            
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);
            
            try {
                TestableReplicationLogDiscoveryReplay discovery = 
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);
                discovery.initializeLastRoundProcessed();
                
                // Verify lastRoundProcessed uses minimum of in-progress, new files, and current time
                ReplicationRound lastRoundProcessed = discovery.getLastRoundProcessed();
                assertNotNull("Last round processed should not be null", lastRoundProcessed);
                long expectedEndTime = (inProgressFileTimestamp / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
                assertEquals("Should use minimum timestamp (in-progress file)", 
                        expectedEndTime, lastRoundProcessed.getEndTime());
                
                // Verify lastRoundInSync uses minimum of lastSyncStateTime and minimumTimestampFromFiles
                ReplicationRound lastRoundInSync = discovery.getLastRoundInSync();
                assertNotNull("Last round in sync should not be null", lastRoundInSync);
                long expectedSyncEndTime = (Math.min(lastSyncStateTime, inProgressFileTimestamp) / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
                assertEquals("Should use minimum of lastSyncStateTime and file timestamps", 
                        expectedSyncEndTime, lastRoundInSync.getEndTime());
                
                // Verify state is set to DEGRADED
                assertEquals("State should be DEGRADED", 
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED, 
                        discovery.getReplicationReplayState());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }
    
    @Test
    public void testInitializeLastRoundProcessed_DegradedStateWithOnlyNewFiles() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        fileTracker.init();
        
        try {
            long newFileTimestamp = 1704240060000L;
            long lastSyncStateTime = 1704240030000L;
            long currentTime = 1704240900000L;

            // Create new file
            long roundTimeMills = fileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;
            ReplicationRound newFileRound = new ReplicationRound(newFileTimestamp - roundTimeMills, newFileTimestamp);
            Path shardPath = fileTracker.getReplicationShardDirectoryManager().getShardDirectory(newFileRound.getStartTime());
            localFs.mkdirs(shardPath);
            Path newFile = new Path(shardPath, newFileTimestamp + "_rs-1.plog");
            localFs.create(newFile, true).close();
            
            // Create HAGroupStoreRecord for DEGRADED state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY, lastSyncStateTime);
            
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);
            
            try {
                TestableReplicationLogDiscoveryReplay discovery = 
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);
                discovery.initializeLastRoundProcessed();
                
                // Verify lastRoundProcessed uses minimum of new files and current time
                ReplicationRound lastRoundProcessed = discovery.getLastRoundProcessed();
                assertNotNull("Last round processed should not be null", lastRoundProcessed);
                long expectedEndTime = (newFileTimestamp / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
                assertEquals("Should use minimum timestamp (new file)", 
                        expectedEndTime, lastRoundProcessed.getEndTime());
                
                // Verify lastRoundInSync uses minimum of lastSyncStateTime and minimumTimestampFromFiles
                ReplicationRound lastRoundInSync = discovery.getLastRoundInSync();
                assertNotNull("Last round in sync should not be null", lastRoundInSync);
                long expectedSyncEndTime = (Math.min(lastSyncStateTime, newFileTimestamp) / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
                assertEquals("Should use minimum of lastSyncStateTime and file timestamps", 
                        expectedSyncEndTime, lastRoundInSync.getEndTime());
                
                // Verify state is DEGRADED
                assertEquals("State should be DEGRADED", 
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED, 
                        discovery.getReplicationReplayState());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }
    
    @Test
    public void testInitializeLastRoundProcessed_DegradedStateWithNoFiles() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        fileTracker.init();
        
        try {
            long currentTime = 1704326400000L;
            long lastSyncStateTime = 1704326300000L;
            
            // Create HAGroupStoreRecord for DEGRADED state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.DEGRADED_STANDBY_FOR_WRITER, lastSyncStateTime);
            
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);
            
            try {
                TestableReplicationLogDiscoveryReplay discovery = 
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);
                discovery.initializeLastRoundProcessed();
                
                // Verify lastRoundProcessed uses current time
                ReplicationRound lastRoundProcessed = discovery.getLastRoundProcessed();
                assertNotNull("Last round processed should not be null", lastRoundProcessed);
                long expectedEndTime = (currentTime / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
                assertEquals("Should use current time when no files exist", 
                        expectedEndTime, lastRoundProcessed.getEndTime());
                
                // Verify lastRoundInSync uses minimum of lastSyncStateTime and current time
                ReplicationRound lastRoundInSync = discovery.getLastRoundInSync();
                assertNotNull("Last round in sync should not be null", lastRoundInSync);
                long expectedSyncEndTime = (Math.min(lastSyncStateTime, currentTime) / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
                assertEquals("Should use minimum of lastSyncStateTime and current time", 
                        expectedSyncEndTime, lastRoundInSync.getEndTime());
                
                // Verify state is DEGRADED
                assertEquals("State should be DEGRADED", 
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED, 
                        discovery.getReplicationReplayState());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }
    
    @Test
    public void testInitializeLastRoundProcessed_SyncStateWithInProgressFiles() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        fileTracker.init();
        
        try {
            long currentTime = 1704412800000L;
            long inProgressTimestamp = 1704412740000L;
            
            // Create in-progress file
            Path inProgressDir = fileTracker.getInProgressDirPath();
            localFs.mkdirs(inProgressDir);
            Path inProgressFile = new Path(inProgressDir, inProgressTimestamp + "_rs-1_uuid.plog");
            localFs.create(inProgressFile, true).close();
            
            // Create HAGroupStoreRecord for STANDBY state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.STANDBY, currentTime);
            
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);
            
            try {
                TestableReplicationLogDiscoveryReplay discovery = 
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);
                discovery.initializeLastRoundProcessed();
                
                // Verify calls super.initializeLastRoundProcessed() and uses in-progress file
                ReplicationRound lastRoundProcessed = discovery.getLastRoundProcessed();
                assertNotNull("Last round processed should not be null", lastRoundProcessed);
                long expectedEndTime = (inProgressTimestamp / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
                assertEquals("Should use in-progress file timestamp", 
                        expectedEndTime, lastRoundProcessed.getEndTime());
                
                // Verify lastRoundInSync equals lastRoundProcessed
                ReplicationRound lastRoundInSync = discovery.getLastRoundInSync();
                assertNotNull("Last round in sync should not be null", lastRoundInSync);
                assertEquals("Last round in sync should equal last round processed", 
                        lastRoundProcessed, lastRoundInSync);
                
                // Verify state is SYNC
                assertEquals("State should be SYNC", 
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC, 
                        discovery.getReplicationReplayState());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }
    
    @Test
    public void testInitializeLastRoundProcessed_SyncStateWithNewFiles() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        fileTracker.init();
        
        try {
            long currentTime = 1704499200000L;
            long newFileTimestamp = 1704499140000L;
            
            // Create new file
            long roundTimeMills = fileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;
            ReplicationRound newFileRound = new ReplicationRound(newFileTimestamp - roundTimeMills, newFileTimestamp);
            Path shardPath = fileTracker.getReplicationShardDirectoryManager().getShardDirectory(newFileRound.getStartTime());
            localFs.mkdirs(shardPath);
            Path newFile = new Path(shardPath, newFileTimestamp + "_rs-1.plog");
            localFs.create(newFile, true).close();
            
            // Create HAGroupStoreRecord for STANDBY state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.STANDBY, currentTime);
            
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);
            
            try {
                TestableReplicationLogDiscoveryReplay discovery = 
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);
                discovery.initializeLastRoundProcessed();
                
                // Verify uses new file timestamp
                ReplicationRound lastRoundProcessed = discovery.getLastRoundProcessed();
                assertNotNull("Last round processed should not be null", lastRoundProcessed);
                long expectedEndTime = (newFileTimestamp / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
                assertEquals("Should use new file timestamp", 
                        expectedEndTime, lastRoundProcessed.getEndTime());
                
                // Verify lastRoundInSync equals lastRoundProcessed
                ReplicationRound lastRoundInSync = discovery.getLastRoundInSync();
                assertNotNull("Last round in sync should not be null", lastRoundInSync);
                assertEquals("Last round in sync should equal last round processed", 
                        lastRoundProcessed, lastRoundInSync);
                
                // Verify state is SYNC
                assertEquals("State should be SYNC", 
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC, 
                        discovery.getReplicationReplayState());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }
    
    @Test
    public void testInitializeLastRoundProcessed_SyncStateWithNoFiles() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        fileTracker.init();
        
        try {
            long currentTime = 1704585600000L;
            
            // Create HAGroupStoreRecord for STANDBY state
            HAGroupStoreRecord mockRecord = new HAGroupStoreRecord(HAGroupStoreRecord.DEFAULT_PROTOCOL_VERSION, haGroupName,
                    HAGroupStoreRecord.HAGroupState.STANDBY, currentTime);
            
            EnvironmentEdge edge = () -> currentTime;
            EnvironmentEdgeManager.injectEdge(edge);
            
            try {
                TestableReplicationLogDiscoveryReplay discovery = 
                        new TestableReplicationLogDiscoveryReplay(fileTracker, mockRecord);
                discovery.initializeLastRoundProcessed();
                
                // Verify uses current time
                ReplicationRound lastRoundProcessed = discovery.getLastRoundProcessed();
                assertNotNull("Last round processed should not be null", lastRoundProcessed);
                long expectedEndTime = (currentTime / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1);
                assertEquals("Should use current time when no files exist", 
                        expectedEndTime, lastRoundProcessed.getEndTime());
                
                // Verify lastRoundInSync equals lastRoundProcessed
                ReplicationRound lastRoundInSync = discovery.getLastRoundInSync();
                assertNotNull("Last round in sync should not be null", lastRoundInSync);
                assertEquals("Last round in sync should equal last round processed", 
                        lastRoundProcessed, lastRoundInSync);
                
                // Verify state is SYNC
                assertEquals("State should be SYNC", 
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC, 
                        discovery.getReplicationReplayState());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }

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
                discovery.setLastRoundProcessed(new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime));
                discovery.setLastRoundInSync(new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime));
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
                
                // Verify 3 rounds were processed
                ReplicationRound lastRoundProcessed = discovery.getLastRoundProcessed();
                assertNotNull("Last round processed should not be null", lastRoundProcessed);
                assertEquals("Should have processed 3 rounds", 
                        initialEndTime + (3 * roundTimeMills), lastRoundProcessed.getEndTime());
                
                // Verify state remains SYNC
                assertEquals("State should remain SYNC", 
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC, 
                        discovery.getReplicationReplayState());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }
    
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
                discovery.setLastRoundInSync(new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime));
                discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED);
                
                ReplicationRound lastRoundBeforeReplay = discovery.getLastRoundProcessed();
                
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
                
                // Verify lastRoundProcessed was updated (DEGRADED updates in memory, doesn't persist to store)
                ReplicationRound lastRoundAfterReplay = discovery.getLastRoundProcessed();
                assertEquals("Last round processed should be updated to 3rd round in DEGRADED state", 
                        expectedRound3, lastRoundAfterReplay);
                
                // Verify state remains DEGRADED
                assertEquals("State should remain DEGRADED", 
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED, 
                        discovery.getReplicationReplayState());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }
    
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
                
                // Verify it rewound to lastRoundInSync and then processed forward
                ReplicationRound lastRoundProcessed = discovery.getLastRoundProcessed();
                assertNotNull("Last round processed should not be null", lastRoundProcessed);
                
                // Should have rewound to lastInSyncRound.endTime and processed from there
                // With 5 rounds worth of time, it should process multiple rounds from lastInSyncRound
                assertEquals("Should have processed rounds starting from lastRoundInSync", 
                        initialEndTime + (5 * roundTimeMills), lastRoundProcessed.getEndTime());
                
                // Verify state transitioned to SYNC
                assertEquals("State should transition to SYNC", 
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC, 
                        discovery.getReplicationReplayState());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }
    
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
                discovery.setLastRoundProcessed(new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime));
                discovery.setLastRoundInSync(new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime));
                discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC);
                
                // Simulate listener changing state to DEGRADED after 2 rounds
                discovery.setStateChangeAfterRounds(2, ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED);
                
                ReplicationRound lastRoundBeforeReplay = discovery.getLastRoundProcessed();
                
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
                
                // Verify it processed 2 rounds in SYNC mode, then continued in DEGRADED mode
                ReplicationRound lastRoundAfterReplay = discovery.getLastRoundProcessed();
                
                // Both SYNC and DEGRADED modes update lastRoundProcessed (DEGRADED doesn't persist to store, but updates in memory)
                // So lastRoundProcessed should be updated to the 5th round
                assertEquals("Should have updated lastRoundProcessed to 5th round", 
                        initialEndTime + (5 * roundTimeMills), lastRoundAfterReplay.getEndTime());
                
                // Verify state is now DEGRADED
                assertEquals("State should be DEGRADED", 
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.DEGRADED, 
                        discovery.getReplicationReplayState());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }
    
    @Test
    public void testReplay_StateTransition_DegradedToSyncedRecovery() throws IOException {
        TestableReplicationLogTracker fileTracker = createReplicationLogTracker(conf, haGroupName, localFs, standbyUri);
        
        try {
            long initialEndTime = 1704499200000L;
            long roundTimeMills = fileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() * 1000L;
            long bufferMillis = (long) (roundTimeMills * 0.15);
            long totalWaitTime = roundTimeMills + bufferMillis;
            
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
                
                // Verify it processed in DEGRADED mode, then transitioned to SYNCED_RECOVERY and rewound
                ReplicationRound lastRoundAfterReplay = discovery.getLastRoundProcessed();
                
                // After SYNCED_RECOVERY rewind and re-processing, should end at initialEndTime + 5*round
                assertEquals("Should have rewound and processed from lastRoundInSync", 
                        initialEndTime + (5 * roundTimeMills), lastRoundAfterReplay.getEndTime());
                
                // Verify state transitioned to SYNC (from SYNCED_RECOVERY)
                assertEquals("State should be SYNC after SYNCED_RECOVERY", 
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC, 
                        discovery.getReplicationReplayState());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }
    
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
                discovery.setLastRoundProcessed(new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime));
                discovery.setLastRoundInSync(new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime));
                discovery.setReplicationReplayState(ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC);
                
                ReplicationRound lastRoundBeforeReplay = discovery.getLastRoundProcessed();
                
                discovery.replay();
                
                // Verify processRound was not called
                assertEquals("processRound should not be called when no rounds to process", 
                        0, discovery.getProcessRoundCallCount());
                
                // Verify no rounds were processed
                ReplicationRound lastRoundAfterReplay = discovery.getLastRoundProcessed();
                assertEquals("Last round processed should not change when no rounds to process", 
                        lastRoundBeforeReplay, lastRoundAfterReplay);
                
                // Verify state remains SYNC
                assertEquals("State should remain SYNC", 
                        ReplicationLogDiscoveryReplay.ReplicationReplayState.SYNC, 
                        discovery.getReplicationReplayState());
            } finally {
                EnvironmentEdgeManager.reset();
            }
        } finally {
            fileTracker.close();
        }
    }

    private TestableReplicationLogTracker createReplicationLogTracker(final Configuration conf, final String haGroupName, final FileSystem fileSystem, final URI rootURI) throws IOException {
        TestableReplicationLogTracker testableReplicationLogTracker = new TestableReplicationLogTracker(conf, haGroupName, fileSystem, rootURI, ReplicationLogTracker.DirectoryType.IN, METRICS_REPLICATION_LOG_TRACKER);
        testableReplicationLogTracker.init();
        return testableReplicationLogTracker;
    }
 
    private class TestableReplicationLogTracker extends ReplicationLogTracker {
        public TestableReplicationLogTracker(Configuration conf, String haGroupName, FileSystem fileSystem, URI rootURI, DirectoryType directoryType, MetricsReplicationLogTracker metrics) {
            super(conf, haGroupName, fileSystem, rootURI, directoryType, metrics);
        }
        public Path getInProgressDirPath() {
            return super.getInProgressDirPath();
        }
    }
    
    private static class TestableReplicationLogDiscoveryReplay extends ReplicationLogDiscoveryReplay {
        private final HAGroupStoreRecord haGroupStoreRecord;
        private int roundsProcessed = 0;
        private int stateChangeAfterRounds = -1;
        private ReplicationReplayState newStateAfterRounds = null;
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
            System.out.println("Processing Round: " + replicationRound);
            // Track processed rounds
            processedRounds.add(replicationRound);
            
            // Simulate state change by listener after certain number of rounds
            roundsProcessed++;
            if (stateChangeAfterRounds > 0 && roundsProcessed == stateChangeAfterRounds && newStateAfterRounds != null) {
                System.out.println("Rounds Processed: " + roundsProcessed + " - " + newStateAfterRounds);
                setReplicationReplayState(newStateAfterRounds);
            }
            // Don't actually process files in tests
        }
        
        public ReplicationRound getLastRoundInSync() {
            return super.getLastRoundInSync();
        }
 
        public ReplicationReplayState getReplicationReplayState() {
            return super.getReplicationReplayState();
        }
        
        public void setLastRoundProcessed(ReplicationRound round) {
            super.setLastRoundProcessed(round);
        }
        
        public void setStateChangeAfterRounds(int afterRounds, ReplicationReplayState newState) {
            this.stateChangeAfterRounds = afterRounds;
            this.newStateAfterRounds = newState;
        }
        
        public int getProcessRoundCallCount() {
            return processedRounds.size();
        }
        
        public List<ReplicationRound> getProcessedRounds() {
            return new java.util.ArrayList<>(processedRounds);
        }
    }
}
