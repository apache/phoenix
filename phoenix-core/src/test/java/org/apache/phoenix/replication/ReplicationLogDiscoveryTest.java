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
package org.apache.phoenix.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscovery;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTracker;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTrackerReplayImpl;
import org.apache.phoenix.replication.metrics.MetricsReplicationReplayLogDiscoveryReplayImpl;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationLogDiscoveryTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogDiscoveryTest.class);

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private TestableReplicationLogDiscovery discovery;
    private TestableReplicationLogTracker fileTracker;
    private Configuration conf;
    private FileSystem localFs;
    private URI rootURI;
    private Path testFolderPath;
    private static final String haGroupName = "testGroup";
    private static final MetricsReplicationLogTracker metricsLogTracker = new MetricsReplicationLogTrackerReplayImpl(haGroupName);
    private static final MetricsReplicationLogDiscovery metricsLogDiscovery = new MetricsReplicationReplayLogDiscoveryReplayImpl(haGroupName);

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        localFs = FileSystem.getLocal(conf);
        rootURI = new Path(testFolder.getRoot().toString()).toUri();
        testFolderPath = new Path(testFolder.getRoot().getAbsolutePath());
        fileTracker = Mockito.spy(new TestableReplicationLogTracker(conf, haGroupName, localFs, rootURI));
        fileTracker.init();

        discovery = Mockito.spy(new TestableReplicationLogDiscovery(fileTracker));
        Mockito.doReturn(metricsLogDiscovery).when(discovery).getMetrics();
    }

    @After
    public void tearDown() throws IOException {
        if (discovery != null) {
            discovery.stop();
            discovery.close();
        }
        if(fileTracker != null) {
            fileTracker.close();
        }
        localFs.delete(new Path(testFolder.getRoot().toURI()), true);
    }

    @Test
    public void testStartAndStop() throws IOException {
        // 1. Validate that it's not running initially
        assertFalse("Discovery should not be running initially", discovery.isRunning());

        // 2. Validate that scheduler is set to null initially
        assertNull("Scheduler should be null initially", discovery.getScheduler());

        // 3. Call the start method
        discovery.start();

        // 4. Ensure isRunning is set to true
        assertTrue("Discovery should be running after start", discovery.isRunning());

        // 5. Ensure scheduler is started with correct parameters
        assertNotNull("Scheduler should not be null after start", discovery.getScheduler());
        assertFalse("Scheduler should not be shutdown after start", discovery.getScheduler().isShutdown());

        // Verify thread name format
        String threadName = discovery.getExecutorThreadNameFormat();
        assertTrue("Thread name should contain ReplicationLogDiscovery", threadName.contains("ReplicationLogDiscovery"));

        // Verify replay interval
        long replayInterval = discovery.getReplayIntervalSeconds();
        assertEquals("Replay interval should be 10 seconds", 10L, replayInterval);

        // 6. Ensure starting again does not create a new scheduler (and also should not throw any exception)
        ScheduledExecutorService originalScheduler = discovery.getScheduler();
        discovery.start(); // Should not create new scheduler
        ScheduledExecutorService sameScheduler = discovery.getScheduler();
        assertSame("Should reuse the same scheduler instance", originalScheduler, sameScheduler);
        assertTrue("Discovery should still be running", discovery.isRunning());

        // 7. Call stop
        discovery.stop();

        // 8. Ensure scheduler is stopped
        assertTrue("Scheduler should be shutdown after stop", discovery.getScheduler().isShutdown());

        // 9. Ensure isRunning is false
        assertFalse("Discovery should not be running after stop", discovery.isRunning());
    }

    @Test
    public void testReplay() throws IOException {
        // Case 1: getRoundsToProcess returns non-empty list
        List<ReplicationRound> testRounds = new ArrayList<>();
        testRounds.add(new ReplicationRound(1704153600000L, 1704153660000L));
        testRounds.add(new ReplicationRound(1704153660000L, 1704153720000L));

        discovery.setMockRoundsToProcess(testRounds);
        discovery.replay();

        // Verify that processRound was called for each round
        List<ReplicationRound> processedRounds = discovery.getProcessedRounds();
        assertEquals("Should have processed 2 rounds", 2, processedRounds.size());
        assertEquals("First round should match", testRounds.get(0), processedRounds.get(0));
        assertEquals("Second round should match", testRounds.get(1), processedRounds.get(1));

        // Reset for next test
        discovery.resetProcessedRounds();

        // Case 2: getRoundsToProcess returns empty list
        discovery.setMockRoundsToProcess(new ArrayList<>());
        discovery.replay();

        // Verify that processRound was not called
        processedRounds = discovery.getProcessedRounds();
        assertEquals("Should not have processed any rounds when list is empty", 0, processedRounds.size());

        // Clean up
        discovery.resetProcessedRounds();
        discovery.setMockRoundsToProcess(null);
    }

    @Test
    public void testGetRoundsToProcess() throws IOException {
        // Case 1: getLastSuccessfullyProcessedRound returns a round with end time in the past
        // This should result in non-empty rounds to process
        ReplicationRound pastRound = new ReplicationRound(1704153600000L, 1704153660000L);
        Mockito.when(discovery.getLastRoundInSync()).thenReturn(pastRound);

        List<ReplicationRound> rounds = discovery.getRoundsToProcess();
        assertFalse("Should have found rounds to process when last processed round is in the past", rounds.isEmpty());

        // Case 2: getLastSuccessfullyProcessedRound returns a round end time in past (not far enough to account for buffer)
        // This should result in empty rounds to process
        long lastRoundEndTime = EnvironmentEdgeManager.currentTimeMillis() - discovery.getReplayIntervalSeconds() * 1000L; // 1 round before (but not far enough to account for buffer)
        ReplicationRound lastRound = new ReplicationRound(lastRoundEndTime - discovery.getReplayIntervalSeconds() * 1000L, lastRoundEndTime);
        Mockito.when(discovery.getLastRoundInSync()).thenReturn(lastRound);

        rounds = discovery.getRoundsToProcess();
        assertTrue("Should have empty rounds", rounds.isEmpty());

        // Case 3: getLastSuccessfullyProcessedRound returns a round very recent (less than round time) round
        // This should result in empty rounds to process
        lastRoundEndTime = EnvironmentEdgeManager.currentTimeMillis() - (discovery.getReplayIntervalSeconds()  * 1000L) / 5; // 12 seconds in past (less than round time)
        lastRound = new ReplicationRound(lastRoundEndTime - discovery.getReplayIntervalSeconds() * 1000L, lastRoundEndTime);
        Mockito.when(discovery.getLastRoundInSync()).thenReturn(lastRound);

        rounds = discovery.getRoundsToProcess();
        assertTrue("Should have empty rounds", rounds.isEmpty());
    }

    @Test
    public void testProcessRoundWithInProgressDirectoryProcessing() throws IOException {
        // 1. Create new files with start of the day round (00:00:00)
        ReplicationRound replicationRound = new ReplicationRound(1704153600000L, 1704153660000L); // 00:00:00 - 00:01:00
        List<Path> newFilesForRound = createNewFilesForRound(replicationRound, 3);

        // 2. Create file for shard count min round (which should also go to same shard)
        int shardCount = fileTracker.getReplicationShardDirectoryManager().getAllShardPaths().size();
        ReplicationRound differentRoundSameShard = new ReplicationRound(1704153600000L + (shardCount * 60 * 1000L), 1704153660000L + (shardCount * 60 * 1000L));
        List<Path> differentRoundSameShardFiles = createNewFilesForRound(differentRoundSameShard, 2);

        // 3. Create files for (00:01:00) and (00:02:00) start time of the rounds
        ReplicationRound round0100 = new ReplicationRound(1704153660000L, 1704153720000L); // 00:01:00 - 00:02:00
        List<Path> round0100NewFiles = createNewFilesForRound(round0100, 2);
        ReplicationRound round0200 = new ReplicationRound(1704153720000L, 1704153780000L); // 00:02:00 - 00:03:00
        List<Path> round0200NewFiles = createNewFilesForRound(round0200, 2);

        // 4. Create 2 in progress files for (00:00:04) timestamp
        long timestamp0004 = 1704153600000L + (4 * 1000L); // 00:00:04
        List<Path> inProgressFiles0004 = createInProgressFiles(timestamp0004, 2);

        // 5. Create 2 in progress files for (00:01:02) timestamp
        long timestamp0102 = 1704153660000L + (2 * 1000L); // 00:01:02
        List<Path> inProgressFiles0102 = createInProgressFiles(timestamp0102, 2);

        // 6. Mock shouldProcessInProgressDirectory to return true
        discovery.setMockShouldProcessInProgressDirectory(true);

        // Process the start of day round
        discovery.processRound(replicationRound);

        // 7. Ensure current round new files (3) are processed and in progress (4) are processed (Total 7)
        List<Path> processedFiles = discovery.getProcessedFiles();
        assertEquals("Invalid number of files processed", 7, processedFiles.size());

        // Create set of expected files that should be processed
        Set<String> expectedProcessedPaths = new HashSet<>();
        for (Path file : newFilesForRound) {
            expectedProcessedPaths.add(file.toUri().getPath());
        }
        for (Path file : inProgressFiles0004) {
            expectedProcessedPaths.add(file.toUri().getPath());
        }
        for (Path file : inProgressFiles0102) {
            expectedProcessedPaths.add(file.toUri().getPath());
        }

        // Create set of actually processed file paths
        Set<String> actualProcessedPaths = new HashSet<>();
        for (Path file : processedFiles) {
            actualProcessedPaths.add(file.toUri().getPath());
        }

        // Validate that sets are equal
        assertEquals("Expected and actual processed files should match", expectedProcessedPaths, actualProcessedPaths);

        // Verify that shouldProcessInProgressDirectory was called once
        Mockito.verify(discovery, Mockito.times(1)).shouldProcessInProgressDirectory();

        // Validate that files from other rounds were NOT processed
        for (Path unexpectedFile : differentRoundSameShardFiles) {
            assertFalse("Should NOT have processed shard count round file: " + unexpectedFile.getName(),
                processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }

        for (Path unexpectedFile : round0100NewFiles) {
            assertFalse("Should NOT have processed round 00:01:00 file: " + unexpectedFile.getName(),
                processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }

        for (Path unexpectedFile : round0200NewFiles) {
            assertFalse("Should NOT have processed round 00:02:00 file: " + unexpectedFile.getName(),
                processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }
    }

    @Test
    public void testProcessRoundWithoutInProgressDirectoryProcessing() throws IOException {
        // 1. Create new files with start of the day round (00:00:00)
        ReplicationRound replicationRound = new ReplicationRound(1704153600000L, 1704153660000L); // 00:00:00 - 00:01:00
        List<Path> newFilesForRound = createNewFilesForRound(replicationRound, 3);

        // 2. Create file for shard count min round (which should also go to same shard)
        int shardCount = fileTracker.getReplicationShardDirectoryManager().getAllShardPaths().size();
        ReplicationRound differentRoundSameShard = new ReplicationRound(1704153600000L + (shardCount * 60 * 1000L), 1704153660000L + (shardCount * 60 * 1000L));
        List<Path> differentRoundSameShardFiles = createNewFilesForRound(differentRoundSameShard, 2);

        // 3. Create files for (00:01:00) and (00:02:00) start time of the rounds
        ReplicationRound round0100 = new ReplicationRound(1704153660000L, 1704153720000L); // 00:01:00 - 00:02:00
        List<Path> round0100NewFiles = createNewFilesForRound(round0100, 2);
        ReplicationRound round0200 = new ReplicationRound(1704153720000L, 1704153780000L); // 00:02:00 - 00:03:00
        List<Path> round0200NewFiles = createNewFilesForRound(round0200, 2);

        // 4. Create 2 in progress files for (00:00:04) timestamp
        long timestamp0004 = 1704153600000L + (4 * 1000L); // 00:00:04
        List<Path> inProgressFiles0004 = createInProgressFiles(timestamp0004, 2);

        // 5. Create 2 in progress files for (00:01:02) timestamp
        long timestamp0102 = 1704153660000L + (2 * 1000L); // 00:01:02
        List<Path> inProgressFiles0102 = createInProgressFiles(timestamp0102, 2);

        // 6. Mock shouldProcessInProgressDirectory to return false
        discovery.setMockShouldProcessInProgressDirectory(false);

        // Process the start of day round
        discovery.processRound(replicationRound);

        // 7. Ensure only current round new files (3) are processed (Total 3, no in-progress files)
        List<Path> processedFiles = discovery.getProcessedFiles();
        assertEquals("Invalid number of files processed", 3, processedFiles.size());

        System.out.println("Processed files");
        for (Path file : processedFiles) {
            System.out.println(file);
        }

        // Create set of expected files that should be processed (only new files)
        Set<String> expectedProcessedPaths = new HashSet<>();
        for (Path file : newFilesForRound) {
            expectedProcessedPaths.add(file.toUri().getPath());
        }

        // Create set of actually processed file paths
        Set<String> actualProcessedPaths = new HashSet<>();
        for (Path file : processedFiles) {
            actualProcessedPaths.add(file.toUri().getPath());
        }

        // Validate that sets are equal
        assertEquals("Expected and actual processed files should match", expectedProcessedPaths, actualProcessedPaths);

        // Verify that shouldProcessInProgressDirectory was called once
        Mockito.verify(discovery, Mockito.times(1)).shouldProcessInProgressDirectory();

        // Validate that files from other rounds were NOT processed
        for (Path unexpectedFile : differentRoundSameShardFiles) {
            assertFalse("Should NOT have processed shard count round file: " + unexpectedFile.getName(),
                    processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }

        for (Path unexpectedFile : round0100NewFiles) {
            assertFalse("Should NOT have processed round 00:01:00 file: " + unexpectedFile.getName(),
                    processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }

        for (Path unexpectedFile : round0200NewFiles) {
            assertFalse("Should NOT have processed round 00:02:00 file: " + unexpectedFile.getName(),
                    processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }

        // Validate that in-progress files were NOT processed
        for (Path unexpectedFile : inProgressFiles0004) {
            assertFalse("Should NOT have processed in-progress file from 00:00:04: " + unexpectedFile.getName(),
                    processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }

        for (Path unexpectedFile : inProgressFiles0102) {
            assertFalse("Should NOT have processed in-progress file from 00:01:02: " + unexpectedFile.getName(),
                    processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }
    }

    @Test
    public void testShouldProcessInProgressDirectory() {
        // Test multiple times to verify probability-based behavior
        int totalTests = 1000;
        int trueCount = 0;

        for (int i = 0; i < totalTests; i++) {
            if (discovery.shouldProcessInProgressDirectory()) {
                trueCount++;
            }
        }

        // Calculate the actual probability
        double actualProbability = (double) trueCount / totalTests * 100.0;
        double expectedProbability = discovery.getInProgressDirectoryProcessProbability();

        // Verify that the actual probability is close to the expected probability
        // Allow for some variance due to randomness (within 2% of expected)
        double variance = Math.abs(actualProbability - expectedProbability);
        assertTrue("Actual probability (" + actualProbability + "%) should be close to expected probability (" +
            expectedProbability + "%), variance: " + variance + "%", variance < 2.0);

        // Verify that we have some true results (probability > 0)
        assertTrue("Should have some true results", trueCount > 0);

        // Verify that we don't have too many true results (probability < 100%)
        assertTrue("Should not have too many true results", trueCount < totalTests);

        LOG.info("ShouldProcessInProgressDirectory test results:");
        LOG.info("Total tests: " + totalTests);
        LOG.info("True count: " + trueCount);
        LOG.info("Actual probability: " + actualProbability + "%");
        LOG.info("Expected probability: " + expectedProbability + "%");
        LOG.info("Variance: " + variance + "%");
    }

    @Test
    public void testProcessNewFilesForRound() throws IOException {
        // 1. Create new files with start of the day round (00:00:00)
        ReplicationRound replicationRound = new ReplicationRound(1704153600000L, 1704153660000L); // 00:00:00 - 00:01:00
        List<Path> newFilesForRound = createNewFilesForRound(replicationRound, 3);

        // 2. Create file for shard count min round (which should also go to same shard)
        int shardCount = fileTracker.getReplicationShardDirectoryManager().getAllShardPaths().size();
        ReplicationRound differentRoundSameShard = new ReplicationRound(1704153600000L + (shardCount * 60 * 1000L), 1704153660000L + (shardCount * 60 * 1000L));
        List<Path> differentRoundSameShardFiles = createNewFilesForRound(differentRoundSameShard, 2);

        // 3. Create files for (00:01:00) and (00:02:00) start time of the rounds
        ReplicationRound round0100 = new ReplicationRound(1704153660000L, 1704153720000L); // 00:01:00 - 00:02:00
        List<Path> round0100NewFiles = createNewFilesForRound(round0100, 2);
        ReplicationRound round0200 = new ReplicationRound(1704153720000L, 1704153780000L); // 00:02:00 - 00:03:00
        List<Path> round0200NewFiles = createNewFilesForRound(round0200, 2);

        // 4. Create 2 in progress files for (00:00:04) timestamp
        long timestamp0004 = 1704153600000L + (4 * 1000L); // 00:00:04
        List<Path> inProgressFiles0004 = createInProgressFiles(timestamp0004, 2);

        // 5. Create 2 in progress files for (00:01:02) timestamp
        long timestamp0102 = 1704153660000L + (2 * 1000L); // 00:01:02
        List<Path> inProgressFiles0102 = createInProgressFiles(timestamp0102, 2);

        // Process new files for the round
        discovery.processNewFilesForRound(replicationRound);

        // 7. Ensure only current round new files (3) are processed
        List<Path> processedFiles = discovery.getProcessedFiles();
        assertEquals("Invalid number of files processed", 3, processedFiles.size());

        // Create set of expected files that should be processed (only new files)
        Set<String> expectedProcessedPaths = new HashSet<>();
        for (Path file : newFilesForRound) {
            expectedProcessedPaths.add(file.toUri().getPath());
        }

        // Create set of actually processed file paths
        Set<String> actualProcessedPaths = new HashSet<>();
        for (Path file : processedFiles) {
            actualProcessedPaths.add(file.toUri().getPath());
        }

        // Validate that sets are equal
        assertEquals("Expected and actual processed files should match", expectedProcessedPaths, actualProcessedPaths);

        Mockito.verify(fileTracker, Mockito.times(3)).markCompleted(Mockito.any(Path.class));

        // Verify that markCompleted was called for each processed file with correct paths
        for (Path expectedFile : newFilesForRound) {
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> path.getName().startsWith(expectedFile.getName().split("\\.")[0])));
        }

        // Validate that files from other rounds were NOT processed
        for (Path unexpectedFile : differentRoundSameShardFiles) {
            assertFalse("Should NOT have processed shard count round file: " + unexpectedFile.getName(),
                processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }

        for (Path unexpectedFile : round0100NewFiles) {
            assertFalse("Should NOT have processed round 00:01:00 file: " + unexpectedFile.getName(),
                processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }

        for (Path unexpectedFile : round0200NewFiles) {
            assertFalse("Should NOT have processed round 00:02:00 file: " + unexpectedFile.getName(),
                processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }

        // Validate that in-progress files were NOT processed (processNewFilesForRound only processes new files)
        for (Path unexpectedFile : inProgressFiles0004) {
            assertFalse("Should NOT have processed in-progress file from 00:00:04: " + unexpectedFile.getName(),
                processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }

        for (Path unexpectedFile : inProgressFiles0102) {
            assertFalse("Should NOT have processed in-progress file from 00:01:02: " + unexpectedFile.getName(),
                processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }
    }

    @Test
    public void testProcessNewFilesForRoundWithPartialFailure() throws IOException {
        // Create new files with start of the day round (00:00:00)
        ReplicationRound replicationRound = new ReplicationRound(1704153600000L, 1704153660000L); // 00:00:00 - 00:01:00
        List<Path> newFilesForRound = createNewFilesForRound(replicationRound, 5);

        // Mock processFile to throw exception for specific files (files 1 and 3)
        Mockito.doThrow(new IOException("Processing failed for file 1"))
            .when(discovery).processFile(Mockito.argThat(path -> path.toUri().getPath().equals(newFilesForRound.get(1).toUri().getPath())));
        Mockito.doThrow(new IOException("Processing failed for file 3"))
                .when(discovery).processFile(Mockito.argThat(path -> path.toUri().getPath().equals(newFilesForRound.get(3).toUri().getPath())));

        // Process new files for the round
        discovery.processNewFilesForRound(replicationRound);

        // Verify that processFile was called for each file in the round
        Mockito.verify(discovery, Mockito.times(5)).processFile(Mockito.any(Path.class));

        // Verify that processFile was called for each specific file
        for (Path expectedFile : newFilesForRound) {
            System.out.println("Checking for " + expectedFile);
            Mockito.verify(discovery, Mockito.times(1)).processFile(
                Mockito.argThat(path -> path.toUri().getPath().equals(expectedFile.toString())));
        }

        // Verify that markCompleted was called for each successfully processed file
        Mockito.verify(fileTracker, Mockito.times(3)).markCompleted(Mockito.any(Path.class));

        // Verify that markCompleted was called for each successfully processed file with correct paths
        System.out.println("Called for " + newFilesForRound.get(0).getName() + " but received");
        Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                Mockito.argThat(path -> path.getName().startsWith(newFilesForRound.get(0).getName().split("\\.")[0])));
        Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                Mockito.argThat(path -> path.getName().startsWith(newFilesForRound.get(2).getName().split("\\.")[0])));
        Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                Mockito.argThat(path -> path.getName().startsWith(newFilesForRound.get(4).getName().split("\\.")[0])));

        // Verify that markCompleted was NOT called for failed files
        Mockito.verify(fileTracker, Mockito.never()).markCompleted(
                Mockito.argThat(path -> path.getName().startsWith(newFilesForRound.get(1).getName().split("\\.")[0])));
        Mockito.verify(fileTracker, Mockito.never()).markCompleted(
                Mockito.argThat(path -> path.getName().startsWith(newFilesForRound.get(3).getName().split("\\.")[0])));

        // Verify that markFailed was called for failed files
        Mockito.verify(fileTracker, Mockito.times(1)).markFailed(
                Mockito.argThat(path -> path.getName().startsWith(newFilesForRound.get(1).getName().split("\\.")[0])));
        Mockito.verify(fileTracker, Mockito.times(1)).markFailed(
                Mockito.argThat(path -> path.getName().startsWith(newFilesForRound.get(3).getName().split("\\.")[0])));

        // Verify that markFailed was NOT called for successfully processed files
        Mockito.verify(fileTracker, Mockito.never()).markFailed(
                Mockito.argThat(path -> path.getName().startsWith(newFilesForRound.get(0).getName().split("\\.")[0])));
        Mockito.verify(fileTracker, Mockito.never()).markFailed(
                Mockito.argThat(path -> path.getName().startsWith(newFilesForRound.get(2).getName().split("\\.")[0])));
        Mockito.verify(fileTracker, Mockito.never()).markFailed(
                Mockito.argThat(path -> path.getName().startsWith(newFilesForRound.get(4).getName().split("\\.")[0])));
    }

    @Test
    public void testProcessNewFilesForRoundWithAllFailures() throws IOException {
        // Create new files with start of the day round (00:00:00)
        ReplicationRound replicationRound = new ReplicationRound(1704153600000L, 1704153660000L); // 00:00:00 - 00:01:00
        List<Path> newFilesForRound = createNewFilesForRound(replicationRound, 5);

        // Mock processFile to throw exception for all files
        for (Path file : newFilesForRound) {
            Mockito.doThrow(new IOException("Processing failed for file: " + file.getName()))
                .when(discovery).processFile(Mockito.argThat(path -> path.toUri().getPath().equals(file.toUri().getPath())));
        }

        // Process new files for the round
        discovery.processNewFilesForRound(replicationRound);

        // Verify that processFile was called for each file in the round
        Mockito.verify(discovery, Mockito.times(5)).processFile(Mockito.any(Path.class));

        // Verify that processFile was called for each specific file
        for (Path expectedFile : newFilesForRound) {
            Mockito.verify(discovery, Mockito.times(1)).processFile(
                Mockito.argThat(path -> path.toUri().getPath().equals(expectedFile.toString())));
        }

        // Verify that markCompleted was NOT called for any file (all failed)
        Mockito.verify(fileTracker, Mockito.never()).markCompleted(Mockito.any(Path.class));

        // Verify that markFailed was called for all files
        Mockito.verify(fileTracker, Mockito.times(5)).markFailed(Mockito.any(Path.class));

        // Verify that markFailed was called for each specific file with correct paths
        for (Path failedFile : newFilesForRound) {
            Mockito.verify(fileTracker, Mockito.times(1)).markFailed(
                Mockito.argThat(path -> {
                    System.out.println("Checking for " + fileTracker.getFilePrefix(path) + " and " + fileTracker.getFilePrefix(failedFile));
                    return fileTracker.getFilePrefix(path).equals(fileTracker.getFilePrefix(failedFile));
                }));
        }
    }

    @Test
    public void testProcessInProgressDirectory() throws IOException {
        // 1. Create in-progress files for different timestamps
        long timestamp0004 = 1704153600000L + (4 * 1000L); // 00:00:04
        List<Path> inProgressFiles0004 = createInProgressFiles(timestamp0004, 3);

        long timestamp0102 = 1704153660000L + (2 * 1000L); // 00:01:02
        List<Path> inProgressFiles0102 = createInProgressFiles(timestamp0102, 2);

        long timestamp0206 = 1704153720000L + (6 * 1000L); // 00:02:06
        List<Path> inProgressFiles0206 = createInProgressFiles(timestamp0206, 2);

        // 2. Create some new files to ensure they are NOT processed
        ReplicationRound replicationRound = new ReplicationRound(1704153600000L, 1704153660000L); // 00:00:00 - 00:01:00
        List<Path> newFilesForRound = createNewFilesForRound(replicationRound, 3);

        // Process in-progress directory
        discovery.processInProgressDirectory();

        // 3. Ensure all in-progress files (7 total) are processed
        List<Path> processedFiles = discovery.getProcessedFiles();
        assertEquals("Invalid number of files processed", 7, processedFiles.size());

        // Create set of expected files that should be processed (only in-progress files)
        Set<String> expectedProcessedPaths = new HashSet<>();
        for (Path file : inProgressFiles0004) {
            expectedProcessedPaths.add(file.toUri().getPath());
        }
        for (Path file : inProgressFiles0102) {
            expectedProcessedPaths.add(file.toUri().getPath());
        }
        for (Path file : inProgressFiles0206) {
            expectedProcessedPaths.add(file.toUri().getPath());
        }

        // Create set of actually processed file paths
        Set<String> actualProcessedPaths = new HashSet<>();
        for (Path file : processedFiles) {
            actualProcessedPaths.add(file.toUri().getPath());
        }

        // Validate that sets are equal
        assertEquals("Expected and actual processed files should match", expectedProcessedPaths, actualProcessedPaths);

        // Verify that markCompleted was called for each processed file
        Mockito.verify(fileTracker, Mockito.times(7)).markCompleted(Mockito.any(Path.class));

        // Verify that markCompleted was called for each processed file with correct paths
        for (Path expectedFile : inProgressFiles0004) {
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> path.getName().startsWith(fileTracker.getFilePrefix(expectedFile))));
        }
        for (Path expectedFile : inProgressFiles0102) {
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> path.getName().startsWith(fileTracker.getFilePrefix(expectedFile))));
        }
        for (Path expectedFile : inProgressFiles0206) {
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> path.getName().startsWith(fileTracker.getFilePrefix(expectedFile))));
        }

        // Validate that new files were NOT processed (processInProgressDirectory only processes in-progress files)
        for (Path unexpectedFile : newFilesForRound) {
            assertFalse("Should NOT have processed new file: " + unexpectedFile.getName(),
                processedFiles.stream().anyMatch(p -> p.toUri().getPath().equals(unexpectedFile.toUri().getPath())));
        }
    }

    @Test
    public void testProcessInProgressDirectoryWithIntermittentFailure() throws IOException {
        // Create in-progress files for different timestamps
        long timestamp0004 = 1704153600000L + (4 * 1000L); // 00:00:04
        List<Path> inProgressFiles0004 = createInProgressFiles(timestamp0004, 3);

        long timestamp0102 = 1704153660000L + (2 * 1000L); // 00:01:02
        List<Path> inProgressFiles0102 = createInProgressFiles(timestamp0102, 2);

        // Combine all in-progress files for easier access
        List<Path> allInProgressFiles = new ArrayList<>();
        allInProgressFiles.addAll(inProgressFiles0004);
        allInProgressFiles.addAll(inProgressFiles0102);

        // Mock processFile to throw exception for specific files (files 1 and 3)
        Mockito.doThrow(new IOException("Processing failed for file 1"))
            .when(discovery).processFile(Mockito.argThat(path -> path.toUri().getPath().equals(allInProgressFiles.get(1).toUri().getPath())));
        Mockito.doThrow(new IOException("Processing failed for file 3"))
                .when(discovery).processFile(Mockito.argThat(path -> path.toUri().getPath().equals(allInProgressFiles.get(3).toUri().getPath())));

        // Process in-progress directory
        discovery.processInProgressDirectory();

        // Verify that processFile was called for each file in the directory (i.e. 5 + 2 times for failed once that would succeed in next retry)
        Mockito.verify(discovery, Mockito.times(7)).processFile(Mockito.any(Path.class));

        // Verify that processFile was called for each specific file
        for (Path expectedFile : allInProgressFiles) {
            Mockito.verify(discovery, Mockito.times(1)).processFile(
                Mockito.argThat(path -> path.toUri().getPath().equals(expectedFile.toString())));
        }

        // Verify that markCompleted was called for each successfully processed file
        Mockito.verify(fileTracker, Mockito.times(5)).markCompleted(Mockito.any(Path.class));

        // Verify that markCompleted was called for 2 intermittent failed processed file
        Mockito.verify(fileTracker, Mockito.times(2)).markFailed(Mockito.any(Path.class));

        // Verify that markFailed was called once ONLY for failed files
        Mockito.verify(fileTracker, Mockito.times(1)).markFailed(
                Mockito.argThat(path -> fileTracker.getFilePrefix(path).equals(fileTracker.getFilePrefix(allInProgressFiles.get(1)))));
        Mockito.verify(fileTracker, Mockito.times(1)).markFailed(
                Mockito.argThat(path -> fileTracker.getFilePrefix(path).equals(fileTracker.getFilePrefix(allInProgressFiles.get(3)))));

        // Verify that markFailed was NOT called for files processed successfully in first iteration
        Mockito.verify(fileTracker, Mockito.never()).markFailed(
                Mockito.argThat(path -> fileTracker.getFilePrefix(path).equals(fileTracker.getFilePrefix(allInProgressFiles.get(0)))));
        Mockito.verify(fileTracker, Mockito.never()).markFailed(
                Mockito.argThat(path -> fileTracker.getFilePrefix(path).equals(fileTracker.getFilePrefix(allInProgressFiles.get(2)))));
        Mockito.verify(fileTracker, Mockito.never()).markFailed(
                Mockito.argThat(path -> fileTracker.getFilePrefix(path).equals(fileTracker.getFilePrefix(allInProgressFiles.get(4)))));

        // Verify that markCompleted was called for each successfully processed file with correct paths
        for (Path expectedFile : allInProgressFiles) {
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> fileTracker.getFilePrefix(path).equals(fileTracker.getFilePrefix(expectedFile))));
        }
    }

    @Test
    public void testGetMinTimestampFromInProgressFilesEmptyList() throws IOException {
        // Mock empty list of in-progress files
        doReturn(Collections.emptyList()).when(fileTracker).getInProgressFiles();

        // Call the method
        Optional<Long> result = discovery.getMinTimestampFromInProgressFiles();

        // Verify result is empty
        assertFalse("Result should be empty for empty file list", result.isPresent());

        // Verify getInProgressFiles was called once
        verify(fileTracker, times(1)).getInProgressFiles();
    }

    @Test
    public void testGetMinTimestampFromInProgressFilesSingleFile() throws IOException {
        // Create a single file path
        Path filePath = new Path("/test/1704153600000_rs1.plog");

        // Mock single file list
        doReturn(Collections.singletonList(filePath)).when(fileTracker).getInProgressFiles();
//        when(fileTracker.getInProgressFiles()).thenReturn(Collections.singletonList(filePath));
        when(fileTracker.getFileTimestamp(filePath)).thenReturn(1704153600000L);

        // Call the method
        Optional<Long> result = discovery.getMinTimestampFromInProgressFiles();

        // Verify result contains the timestamp
        assertTrue("Result should be present for single file", result.isPresent());
        assertEquals("Should return the timestamp of the single file",
                Long.valueOf(1704153600000L), result.get());

        // Verify method calls
        verify(fileTracker, times(1)).getInProgressFiles();
        verify(fileTracker, times(1)).getFileTimestamp(filePath);
    }

    @Test
    public void testGetMinTimestampFromInProgressFilesMultipleFiles() throws IOException {
        // Create multiple file paths with different timestamps
        Path file1 = new Path("/test/1704153660000_rs2.plog");
        Path file2 = new Path("/test/1704153600000_rs1.plog");
        Path file3 = new Path("/test/1704153720000_rs3.plog");

        List<Path> files = Arrays.asList(file1, file2, file3);

        // Mock file list and timestamps
        doReturn(files).when(fileTracker).getInProgressFiles();
        when(fileTracker.getFileTimestamp(file1)).thenReturn(1704153660000L);
        when(fileTracker.getFileTimestamp(file2)).thenReturn(1704153600000L);
        when(fileTracker.getFileTimestamp(file3)).thenReturn(1704153720000L);

        // Call the method
        Optional<Long> result = discovery.getMinTimestampFromInProgressFiles();

        // Verify result contains the minimum timestamp
        assertTrue("Result should be present for multiple files", result.isPresent());
        assertEquals("Should return the minimum timestamp",
                Long.valueOf(1704153600000L), result.get());

        // Verify method calls
        verify(fileTracker, times(1)).getInProgressFiles();
        verify(fileTracker, times(1)).getFileTimestamp(file1);
        verify(fileTracker, times(1)).getFileTimestamp(file2);
        verify(fileTracker, times(1)).getFileTimestamp(file3);
    }

    @Test
    public void testGetMinTimestampFromNewFilesEmptyList() throws IOException {
        // Mock empty list of new files
        when(fileTracker.getNewFiles()).thenReturn(Collections.emptyList());

        // Call the method
        Optional<Long> result = discovery.getMinTimestampFromNewFiles();

        // Verify result is empty
        assertFalse("Result should be empty for empty file list", result.isPresent());

        // Verify getNewFiles was called once
        verify(fileTracker, times(1)).getNewFiles();
    }

    @Test
    public void testGetMinTimestampFromNewFilesSingleFile() throws IOException {
        // Create a single file path
        Path filePath = new Path("/test/1704153600000_rs1.plog");

        // Mock single file list
        when(fileTracker.getNewFiles()).thenReturn(Arrays.asList(filePath));
        when(fileTracker.getFileTimestamp(filePath)).thenReturn(1704153600000L);

        // Call the method
        Optional<Long> result = discovery.getMinTimestampFromNewFiles();

        // Verify result contains the timestamp
        assertTrue("Result should be present for single file", result.isPresent());
        assertEquals("Should return the timestamp of the single file",
                Long.valueOf(1704153600000L), result.get());

        // Verify method calls
        verify(fileTracker, times(1)).getNewFiles();
        verify(fileTracker, times(1)).getFileTimestamp(filePath);
    }

    @Test
    public void testGetMinTimestampFromNewFilesMultipleFiles() throws IOException {
        // Create multiple file paths with different timestamps
        Path file1 = new Path("/test/1704153660000_rs2.plog");
        Path file2 = new Path("/test/1704153600000_rs1.plog");
        Path file3 = new Path("/test/1704153720000_rs3.plog");

        List<Path> files = Arrays.asList(file1, file2, file3);

        // Mock file list and timestamps
        when(fileTracker.getNewFiles()).thenReturn(files);
        when(fileTracker.getFileTimestamp(file1)).thenReturn(1704153660000L);
        when(fileTracker.getFileTimestamp(file2)).thenReturn(1704153600000L);
        when(fileTracker.getFileTimestamp(file3)).thenReturn(1704153720000L);

        // Call the method
        Optional<Long> result = discovery.getMinTimestampFromNewFiles();

        // Verify result contains the minimum timestamp
        assertTrue("Result should be present for multiple files", result.isPresent());
        assertEquals("Should return the minimum timestamp",
                Long.valueOf(1704153600000L), result.get());

        // Verify method calls
        verify(fileTracker, times(1)).getNewFiles();
        verify(fileTracker, times(1)).getFileTimestamp(file1);
        verify(fileTracker, times(1)).getFileTimestamp(file2);
        verify(fileTracker, times(1)).getFileTimestamp(file3);
    }

    @Test
    public void testInitializeLastRoundInSync_MultipleInProgressFiles_NoNewFiles() throws IOException {
        // Create multiple in-progress files with timestamps
        Path inProgressFile1 = new Path(testFolderPath, "1704067200000_rs1.plog"); // 2024-01-01 00:00:00
        Path inProgressFile2 = new Path(testFolderPath, "1704067260000_rs2.plog"); // 2024-01-01 00:01:00
        Path inProgressFile3 = new Path(testFolderPath, "1704067320000_rs3.plog"); // 2024-01-01 00:02:00

        List<Path> inProgressFiles = Arrays.asList(inProgressFile1, inProgressFile2, inProgressFile3);

        // Mock the tracker to return in-progress files and empty new files
        when(fileTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(fileTracker.getNewFiles()).thenReturn(Collections.emptyList());

        // Call the method
        discovery.initializeLastRoundInSync();

        // Verify the result
        ReplicationRound result = discovery.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);

        // Verify the round uses the minimum timestamp (1704067200000L)
        long expectedEndTime = (1704067200000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use minimum timestamp from in-progress files", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_MultipleInProgressFiles_OneNewFile() throws IOException {
        // Create multiple in-progress files with timestamps
        Path inProgressFile1 = new Path(testFolderPath, "1704153600000_rs1.plog"); // 2024-01-02 00:00:00
        Path inProgressFile2 = new Path(testFolderPath, "1704153660000_rs2.plog"); // 2024-01-02 00:01:00
        Path inProgressFile3 = new Path(testFolderPath, "1704153720000_rs3.plog"); // 2024-01-02 00:02:00

        List<Path> inProgressFiles = Arrays.asList(inProgressFile1, inProgressFile2, inProgressFile3);

        // Create one new file with later timestamp
        Path newFile = new Path(testFolderPath, "1704153780000_rs4.plog"); // 2024-01-02 00:03:00
        List<Path> newFiles = Arrays.asList(newFile);

        // Mock the tracker to return in-progress files and one new file
        when(fileTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(fileTracker.getNewFiles()).thenReturn(newFiles);

        // Call the method
        discovery.initializeLastRoundInSync();

        // Verify the result - should use minimum timestamp from in-progress files
        ReplicationRound result = discovery.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);

        long expectedEndTime = (1704153600000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use minimum timestamp from in-progress files", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_MultipleInProgressFiles_MultipleNewFiles() throws IOException {
        // Create multiple in-progress files with timestamps
        Path inProgressFile1 = new Path(testFolderPath, "1704240000000_rs1.plog"); // 2024-01-03 00:00:00
        Path inProgressFile2 = new Path(testFolderPath, "1704240060000_rs2.plog"); // 2024-01-03 00:01:00
        Path inProgressFile3 = new Path(testFolderPath, "1704240120000_rs3.plog"); // 2024-01-03 00:02:00

        List<Path> inProgressFiles = Arrays.asList(inProgressFile1, inProgressFile2, inProgressFile3);

        // Create multiple new files with later timestamps
        Path newFile1 = new Path(testFolderPath, "1704240180000_rs4.plog"); // 2024-01-03 00:03:00
        Path newFile2 = new Path(testFolderPath, "1704240240000_rs5.plog"); // 2024-01-03 00:04:00
        List<Path> newFiles = Arrays.asList(newFile1, newFile2);

        // Mock the tracker to return in-progress files and multiple new files
        when(fileTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(fileTracker.getNewFiles()).thenReturn(newFiles);

        // Call the method
        discovery.initializeLastRoundInSync();

        // Verify the result - should use minimum timestamp from in-progress files
        ReplicationRound result = discovery.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);

        long expectedEndTime = (1704240000000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use minimum timestamp from in-progress files", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_OneInProgressFile_NoNewFiles() throws IOException {
        // Create single in-progress file
        Path inProgressFile = new Path(testFolderPath, "1704326400000_rs1.plog"); // 2024-01-04 00:00:00
        List<Path> inProgressFiles = Arrays.asList(inProgressFile);

        // Mock the tracker to return single in-progress file and empty new files
        when(fileTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(fileTracker.getNewFiles()).thenReturn(Collections.emptyList());

        // Call the method
        discovery.initializeLastRoundInSync();

        // Verify the result
        ReplicationRound result = discovery.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);

        long expectedEndTime = (1704326400000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use timestamp from single in-progress file", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_OneInProgressFile_OneNewFile() throws IOException {
        // Create single in-progress file
        Path inProgressFile = new Path(testFolderPath, "1704412800000_rs1.plog"); // 2024-01-05 00:00:00
        List<Path> inProgressFiles = Arrays.asList(inProgressFile);

        // Create one new file with later timestamp
        Path newFile = new Path(testFolderPath, "1704412860000_rs2.plog"); // 2024-01-05 00:01:00
        List<Path> newFiles = Arrays.asList(newFile);

        // Mock the tracker to return single in-progress file and one new file
        when(fileTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(fileTracker.getNewFiles()).thenReturn(newFiles);

        // Call the method
        discovery.initializeLastRoundInSync();

        // Verify the result - should use timestamp from in-progress file
        ReplicationRound result = discovery.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);

        long expectedEndTime = (1704412800000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use timestamp from in-progress file", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_OneInProgressFile_MultipleNewFiles() throws IOException {
        // Create single in-progress file
        Path inProgressFile = new Path(testFolderPath, "1704499200000_rs1.plog"); // 2024-01-06 00:00:00
        List<Path> inProgressFiles = Arrays.asList(inProgressFile);

        // Create multiple new files with later timestamps
        Path newFile1 = new Path(testFolderPath, "1704499260000_rs2.plog"); // 2024-01-06 00:01:00
        Path newFile2 = new Path(testFolderPath, "1704499320000_rs3.plog"); // 2024-01-06 00:02:00
        List<Path> newFiles = Arrays.asList(newFile1, newFile2);

        // Mock the tracker to return single in-progress file and multiple new files
        when(fileTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(fileTracker.getNewFiles()).thenReturn(newFiles);

        // Call the method
        discovery.initializeLastRoundInSync();

        // Verify the result - should use timestamp from in-progress file
        ReplicationRound result = discovery.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);

        long expectedEndTime = (1704499200000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use timestamp from in-progress file", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_NoInProgressFiles_MultipleNewFiles() throws IOException {
        // Mock empty in-progress files
        when(fileTracker.getInProgressFiles()).thenReturn(Collections.emptyList());

        // Create multiple new files with timestamps
        Path newFile1 = new Path(testFolderPath, "1704585600000_rs1.plog"); // 2024-01-07 00:00:00
        Path newFile2 = new Path(testFolderPath, "1704585660000_rs2.plog"); // 2024-01-07 00:01:00
        Path newFile3 = new Path(testFolderPath, "1704585720000_rs3.plog"); // 2024-01-07 00:02:00
        List<Path> newFiles = Arrays.asList(newFile1, newFile2, newFile3);

        // Mock the tracker to return empty in-progress files and multiple new files
        when(fileTracker.getNewFiles()).thenReturn(newFiles);

        // Call the method
        discovery.initializeLastRoundInSync();

        // Verify the result
        ReplicationRound result = discovery.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);

        // Verify the round uses the minimum timestamp from new files (1704585600000L)
        long expectedEndTime = (1704585600000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use minimum timestamp from new files when no in-progress files", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_NoInProgressFiles_OneNewFile() throws IOException {
        // Mock empty in-progress files
        when(fileTracker.getInProgressFiles()).thenReturn(Collections.emptyList());

        // Create single new file
        Path newFile = new Path(testFolderPath, "1704672000000_rs1.plog"); // 2024-01-08 00:00:00
        List<Path> newFiles = Arrays.asList(newFile);

        // Mock the tracker to return empty in-progress files and one new file
        when(fileTracker.getNewFiles()).thenReturn(newFiles);

        // Call the method
        discovery.initializeLastRoundInSync();

        // Verify the result
        ReplicationRound result = discovery.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);

        long expectedEndTime = (1704672000000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use timestamp from single new file when no in-progress files", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_NoInProgressFiles_NoNewFiles() throws IOException {
        // Mock empty in-progress files
        when(fileTracker.getInProgressFiles()).thenReturn(Collections.emptyList());

        // Mock empty new files
        when(fileTracker.getNewFiles()).thenReturn(Collections.emptyList());

        // Set custom time using EnvironmentEdgeManager
        EnvironmentEdge customEdge = new EnvironmentEdge() {
            @Override
            public long currentTime() {
                return 1704758400000L; // 2024-01-09 00:00:00
            }
        };
        org.apache.hadoop.hbase.util.EnvironmentEdgeManager.injectEdge(customEdge);

        try {
            // Call the method
            discovery.initializeLastRoundInSync();

            // Verify the result
            ReplicationRound result = discovery.getLastRoundInSync();
            assertNotNull("Last successfully processed round should not be null", result);

            // Verify the round uses the custom time
            long expectedEndTime = (1704758400000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
            long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
            assertEquals("Should use custom time when no files are found", expectedStartTime, result.getStartTime());
            assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
        } finally {
            // Reset EnvironmentEdgeManager to default
            org.apache.hadoop.hbase.util.EnvironmentEdgeManager.reset();
        }
    }

    private List<Path> createNewFilesForRound(ReplicationRound replicationRound, int fileCount) throws IOException {
        // Create files for multiple rounds in the same shard with each file at gap of 2 seconds
        Preconditions.checkArgument(fileCount <= fileTracker.getReplicationShardDirectoryManager().getReplicationRoundDurationSeconds() / 2);
        ReplicationShardDirectoryManager shardManager = fileTracker.getReplicationShardDirectoryManager();
        Path shardPath = shardManager.getShardDirectory(replicationRound.getStartTime());
        localFs.mkdirs(shardPath);
        List<Path> newFiles = new ArrayList<>();
        for(int i = 0; i < fileCount; i++) {
            Path file = new Path(shardPath, replicationRound.getStartTime() + (2000L * i) + "_rs-" + i + ".plog");
            localFs.create(file, true).close();
            newFiles.add(file);
        }
        return newFiles;
    }

    private List<Path> createInProgressFiles(long timestamp, int count) throws IOException {
        // Create in-progress files
        Path inProgressDir = fileTracker.getInProgressDirPath();
        localFs.mkdirs(inProgressDir);
        List<Path> inProgressFiles = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String uuid = "12345678-1234-1234-1234-123456789abc" + i;
            Path inProgressFile = new Path(inProgressDir, timestamp + "_rs-" + i + "_" + uuid + ".plog");
            localFs.create(inProgressFile, true).close();
            inProgressFiles.add(inProgressFile);
        }
        return inProgressFiles;
    }



    private static class TestableReplicationLogTracker extends ReplicationLogTracker {
        public TestableReplicationLogTracker(final Configuration conf, final String haGroupName, final FileSystem fileSystem, final URI rootURI) {
            super(conf, haGroupName, fileSystem, rootURI, DirectoryType.IN, metricsLogTracker);
        }
    }

    private static class TestableReplicationLogDiscovery extends ReplicationLogDiscovery {
        private final List<Path> processedFiles = new ArrayList<>();
        private final List<ReplicationRound> processedRounds = new ArrayList<>();
        private List<ReplicationRound> mockRoundsToProcess = null;

        public TestableReplicationLogDiscovery(ReplicationLogTracker fileTracker) {
            super(fileTracker);
        }

        @Override
        protected void processFile(Path path) throws IOException {
            // Simulate file processing
            System.out.println("Simulating file processing");
            processedFiles.add(path);
        }

        @Override
        protected MetricsReplicationLogDiscovery createMetricsSource() {
            return metricsLogDiscovery;
        }

        @Override
        protected List<ReplicationRound> getRoundsToProcess() {
            if (mockRoundsToProcess != null) {
                return new ArrayList<>(mockRoundsToProcess);
            }
            return super.getRoundsToProcess();
        }

        @Override
        protected void processRound(ReplicationRound replicationRound) throws IOException {
            super.processRound(replicationRound);
            // Track processed rounds
            processedRounds.add(replicationRound);
        }

        public List<Path> getProcessedFiles() {
            return new ArrayList<>(processedFiles);
        }

        public List<ReplicationRound> getProcessedRounds() {
            return new ArrayList<>(processedRounds);
        }

        public void setMockRoundsToProcess(List<ReplicationRound> rounds) {
            this.mockRoundsToProcess = rounds;
        }

        public void resetProcessedRounds() {
            processedRounds.clear();
        }

        public ScheduledExecutorService getScheduler() {
            return super.scheduler;
        }

        private Boolean mockShouldProcessInProgressDirectory = null;

        @Override
        protected boolean shouldProcessInProgressDirectory() {
            if (mockShouldProcessInProgressDirectory != null) {
                return mockShouldProcessInProgressDirectory;
            }
            return super.shouldProcessInProgressDirectory();
        }

        public void setMockShouldProcessInProgressDirectory(boolean value) {
            this.mockShouldProcessInProgressDirectory = value;
        }

        public void resetProcessedFiles() {
            processedFiles.clear();
        }
    }
}
