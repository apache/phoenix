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
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscovery;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTracker;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTrackerReplayImpl;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogDiscoveryReplayImpl;
import org.apache.phoenix.replication.reader.ReplicationLogReplay;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
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
    private Path testFolderPath;
    private static final String haGroupName = "testGroup";
    private static final MetricsReplicationLogTracker metricsLogTracker = new MetricsReplicationLogTrackerReplayImpl(haGroupName);
    private static final MetricsReplicationLogDiscovery metricsLogDiscovery = new MetricsReplicationLogDiscoveryReplayImpl(haGroupName);

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        localFs = FileSystem.getLocal(conf);
        URI rootURI = new Path(testFolder.getRoot().toString()).toUri();
        testFolderPath = new Path(testFolder.getRoot().getAbsolutePath());
        Path newFilesDirectory = new Path(new Path(rootURI.getPath(), haGroupName), ReplicationLogReplay.IN_DIRECTORY_NAME);
        ReplicationShardDirectoryManager replicationShardDirectoryManager =
                new ReplicationShardDirectoryManager(conf, newFilesDirectory);
        fileTracker = Mockito.spy(new TestableReplicationLogTracker(conf, haGroupName, localFs, replicationShardDirectoryManager));
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

    /**
     * Tests the start and stop lifecycle of ReplicationLogDiscovery.
     * Validates scheduler initialization, thread naming, and proper cleanup.
     */
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

    /**
     * Tests processRound with in-progress directory processing enabled.
     * Validates that both new files and in-progress files are processed correctly.
     */
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

        // Create set of expected files that should be processed (by prefix, since UUIDs are added during markInProgress)
        Set<String> expectedProcessedFilePrefixes = new HashSet<>();
        for (Path file : newFilesForRound) {
            // Extract prefix before the file extension (for new files before markInProgress)
            // After markInProgress, files will have format: {timestamp}_{rs-n}_{UUID}.plog
            String fileName = file.getName();
            String prefix = fileName.substring(0, fileName.lastIndexOf("."));
            expectedProcessedFilePrefixes.add(prefix);
        }
        for (Path file : inProgressFiles0004) {
            // For in-progress files, they already have format: {timestamp}_{rs-n}_{UUID}.plog
            // Extract prefix before the UUID (everything before the last underscore)
            String fileName = file.getName();
            String prefix = fileName.substring(0, fileName.lastIndexOf("_"));
            expectedProcessedFilePrefixes.add(prefix);
        }
        for (Path file : inProgressFiles0102) {
            // For in-progress files, they already have format: {timestamp}_{rs-n}_{UUID}.plog
            // Extract prefix before the UUID (everything before the last underscore)
            String fileName = file.getName();
            String prefix = fileName.substring(0, fileName.lastIndexOf("_"));
            expectedProcessedFilePrefixes.add(prefix);
        }

        // Create set of actually processed file paths (extract prefixes)
        // Files after markInProgress will have format: {timestamp}_{rs-n}_{UUID}.plog
        Set<String> actualProcessedFilePrefixes = new HashSet<>();
        for (Path file : processedFiles) {
            String fileName = file.getName();
            // Extract prefix before UUID and extension (everything before the last underscore before .plog)
            // Remove the extension first
            String withoutExtension = fileName.substring(0, fileName.lastIndexOf("."));
            // Then get everything before the last underscore (which is the UUID)
            String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
            actualProcessedFilePrefixes.add(prefix);
        }

        // Validate that sets are equal
        assertEquals("Expected and actual processed files should match", expectedProcessedFilePrefixes, actualProcessedFilePrefixes);
        
        // Verify that markInProgress was called 7 times (3 new files + 4 in-progress files)
        // markInProgress is called before each file is processed
        Mockito.verify(fileTracker, Mockito.times(7)).markInProgress(Mockito.any(Path.class));
        
        // Verify that markInProgress was called for each expected file
        // For new files
        for (Path expectedFile : newFilesForRound) {
            Mockito.verify(fileTracker, Mockito.times(1)).markInProgress(
                    Mockito.argThat(path -> path.getName().startsWith(expectedFile.getName().split("\\.")[0])));
        }
        // For in-progress files
        for (Path expectedFile : inProgressFiles0004) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
            Mockito.verify(fileTracker, Mockito.times(1)).markInProgress(
                    Mockito.argThat(path -> path.getName().substring(0, path.getName().lastIndexOf("_")).equals(expectedPrefix)));
        }
        for (Path expectedFile : inProgressFiles0102) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
            Mockito.verify(fileTracker, Mockito.times(1)).markInProgress(
                    Mockito.argThat(path -> path.getName().substring(0, path.getName().lastIndexOf("_")).equals(expectedPrefix)));
        }
        
        // Verify that markCompleted was called 7 times (once for each successfully processed file)
        Mockito.verify(fileTracker, Mockito.times(7)).markCompleted(Mockito.any(Path.class));
        
        // Verify that markCompleted was called for each expected file with correct paths
        // For new files (they will have format: {timestamp}_{rs-n}_{UUID}.plog in in-progress dir)
        for (Path expectedFile : newFilesForRound) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("."));
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> {
                        String pathName = path.getName();
                        String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                        String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                        return prefix.equals(expectedPrefix);
                    }));
        }
        // For in-progress files (they will have updated UUIDs, but same prefix)
        for (Path expectedFile : inProgressFiles0004) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> {
                        String pathName = path.getName();
                        String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                        String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                        return prefix.equals(expectedPrefix);
                    }));
        }
        for (Path expectedFile : inProgressFiles0102) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> {
                        String pathName = path.getName();
                        String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                        String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                        return prefix.equals(expectedPrefix);
                    }));
        }

        // Verify that shouldProcessInProgressDirectory was called once
        Mockito.verify(discovery, Mockito.times(1)).shouldProcessInProgressDirectory();

        // Validate that files from other rounds were NOT processed (using prefix comparison)
        for (Path unexpectedFile : differentRoundSameShardFiles) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("."));
            assertFalse("Should NOT have processed shard count round file: " + unexpectedFile.getName(),
                actualProcessedFilePrefixes.contains(unexpectedPrefix));
        }

        for (Path unexpectedFile : round0100NewFiles) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("."));
            assertFalse("Should NOT have processed round 00:01:00 file: " + unexpectedFile.getName(),
                actualProcessedFilePrefixes.contains(unexpectedPrefix));
        }

        for (Path unexpectedFile : round0200NewFiles) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("."));
            assertFalse("Should NOT have processed round 00:02:00 file: " + unexpectedFile.getName(),
                actualProcessedFilePrefixes.contains(unexpectedPrefix));
        }
    }

    /**
     * Tests processRound with in-progress directory processing disabled.
     * Validates that only new files for the current round are processed.
     */
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

        // Create set of expected files that should be processed (by prefix, since UUIDs are added during markInProgress)
        Set<String> expectedProcessedFilePrefixes = new HashSet<>();
        for (Path file : newFilesForRound) {
            String fileName = file.getName();
            String prefix = fileName.substring(0, fileName.lastIndexOf("."));
            expectedProcessedFilePrefixes.add(prefix);
        }

        // Create set of actually processed file paths (extract prefixes)
        Set<String> actualProcessedFilePrefixes = new HashSet<>();
        for (Path file : processedFiles) {
            String fileName = file.getName();
            String withoutExtension = fileName.substring(0, fileName.lastIndexOf("."));
            String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
            actualProcessedFilePrefixes.add(prefix);
        }

        // Validate that sets are equal
        assertEquals("Expected and actual processed files should match", expectedProcessedFilePrefixes, actualProcessedFilePrefixes);

        // Verify that markInProgress was called 3 times (only new files, no in-progress files)
        Mockito.verify(fileTracker, Mockito.times(3)).markInProgress(Mockito.any(Path.class));
        
        // Verify that markInProgress was called for each expected file
        for (Path expectedFile : newFilesForRound) {
            Mockito.verify(fileTracker, Mockito.times(1)).markInProgress(
                    Mockito.argThat(path -> path.getName().startsWith(expectedFile.getName().split("\\.")[0])));
        }
        
        // Verify that markCompleted was called 3 times
        Mockito.verify(fileTracker, Mockito.times(3)).markCompleted(Mockito.any(Path.class));
        
        // Verify that markCompleted was called for each expected file with correct paths
        for (Path expectedFile : newFilesForRound) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("."));
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> {
                        String pathName = path.getName();
                        String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                        String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                        return prefix.equals(expectedPrefix);
                    }));
        }

        // Verify that shouldProcessInProgressDirectory was called once
        Mockito.verify(discovery, Mockito.times(1)).shouldProcessInProgressDirectory();

        // Validate that files from other rounds were NOT processed (using prefix comparison)
        for (Path unexpectedFile : differentRoundSameShardFiles) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("."));
            assertFalse("Should NOT have processed shard count round file: " + unexpectedFile.getName(),
                    actualProcessedFilePrefixes.contains(unexpectedPrefix));
        }

        for (Path unexpectedFile : round0100NewFiles) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("."));
            assertFalse("Should NOT have processed round 00:01:00 file: " + unexpectedFile.getName(),
                    actualProcessedFilePrefixes.contains(unexpectedPrefix));
        }

        for (Path unexpectedFile : round0200NewFiles) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("."));
            assertFalse("Should NOT have processed round 00:02:00 file: " + unexpectedFile.getName(),
                    actualProcessedFilePrefixes.contains(unexpectedPrefix));
        }

        // Validate that in-progress files were NOT processed
        for (Path unexpectedFile : inProgressFiles0004) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("_"));
            assertFalse("Should NOT have processed in-progress file from 00:00:04: " + unexpectedFile.getName(),
                    actualProcessedFilePrefixes.contains(unexpectedPrefix));
        }

        for (Path unexpectedFile : inProgressFiles0102) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("_"));
            assertFalse("Should NOT have processed in-progress file from 00:01:02: " + unexpectedFile.getName(),
                    actualProcessedFilePrefixes.contains(unexpectedPrefix));
        }
    }

    /**
     * Tests the probability-based in-progress directory processing decision.
     * Validates that the actual probability matches the configured probability.
     */
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

    /**
     * Tests processing of new files for a specific round.
     * Validates that only files belonging to the current round are processed.
     */
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

        // Create set of expected files that should be processed (by prefix, since UUIDs are added during markInProgress)
        Set<String> expectedProcessedFilePrefixes = new HashSet<>();
        for (Path file : newFilesForRound) {
            String fileName = file.getName();
            String prefix = fileName.substring(0, fileName.lastIndexOf("."));
            expectedProcessedFilePrefixes.add(prefix);
        }

        // Create set of actually processed file paths (extract prefixes)
        Set<String> actualProcessedFilePrefixes = new HashSet<>();
        for (Path file : processedFiles) {
            String fileName = file.getName();
            String withoutExtension = fileName.substring(0, fileName.lastIndexOf("."));
            String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
            actualProcessedFilePrefixes.add(prefix);
        }

        // Validate that sets are equal
        assertEquals("Expected and actual processed files should match", expectedProcessedFilePrefixes, actualProcessedFilePrefixes);

        // Verify that markInProgress was called 3 times
        Mockito.verify(fileTracker, Mockito.times(3)).markInProgress(Mockito.any(Path.class));
        
        // Verify that markInProgress was called for each expected file
        for (Path expectedFile : newFilesForRound) {
            Mockito.verify(fileTracker, Mockito.times(1)).markInProgress(
                    Mockito.argThat(path -> path.getName().startsWith(expectedFile.getName().split("\\.")[0])));
        }

        // Verify that markCompleted was called 3 times
        Mockito.verify(fileTracker, Mockito.times(3)).markCompleted(Mockito.any(Path.class));

        // Verify that markCompleted was called for each processed file with correct paths
        for (Path expectedFile : newFilesForRound) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("."));
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> {
                        String pathName = path.getName();
                        String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                        String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                        return prefix.equals(expectedPrefix);
                    }));
        }

        // Validate that files from other rounds were NOT processed (using prefix comparison)
        for (Path unexpectedFile : differentRoundSameShardFiles) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("."));
            assertFalse("Should NOT have processed shard count round file: " + unexpectedFile.getName(),
                actualProcessedFilePrefixes.contains(unexpectedPrefix));
        }

        for (Path unexpectedFile : round0100NewFiles) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("."));
            assertFalse("Should NOT have processed round 00:01:00 file: " + unexpectedFile.getName(),
                actualProcessedFilePrefixes.contains(unexpectedPrefix));
        }

        for (Path unexpectedFile : round0200NewFiles) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("."));
            assertFalse("Should NOT have processed round 00:02:00 file: " + unexpectedFile.getName(),
                actualProcessedFilePrefixes.contains(unexpectedPrefix));
        }

        // Validate that in-progress files were NOT processed (processNewFilesForRound only processes new files)
        for (Path unexpectedFile : inProgressFiles0004) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("_"));
            assertFalse("Should NOT have processed in-progress file from 00:00:04: " + unexpectedFile.getName(),
                actualProcessedFilePrefixes.contains(unexpectedPrefix));
        }

        for (Path unexpectedFile : inProgressFiles0102) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("_"));
            assertFalse("Should NOT have processed in-progress file from 00:01:02: " + unexpectedFile.getName(),
                actualProcessedFilePrefixes.contains(unexpectedPrefix));
        }
    }

    /**
     * Tests partial failure handling during new file processing.
     * Validates that successful files are marked completed while failed files are marked failed.
     */
    @Test
    public void testProcessNewFilesForRoundWithPartialFailure() throws IOException {
        // Create new files with start of the day round (00:00:00)
        ReplicationRound replicationRound = new ReplicationRound(1704153600000L, 1704153660000L); // 00:00:00 - 00:01:00
        List<Path> newFilesForRound = createNewFilesForRound(replicationRound, 5);

        // Mock processFile to throw exception for specific files (files 1 and 3) - using prefix matching
        String file1Prefix = newFilesForRound.get(1).getName().substring(0, newFilesForRound.get(1).getName().lastIndexOf("."));
        Mockito.doThrow(new IOException("Processing failed for file 1"))
            .when(discovery).processFile(Mockito.argThat(path -> {
                String pathName = path.getName();
                String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                return prefix.equals(file1Prefix);
            }));
        String file3Prefix = newFilesForRound.get(3).getName().substring(0, newFilesForRound.get(3).getName().lastIndexOf("."));
        Mockito.doThrow(new IOException("Processing failed for file 3"))
                .when(discovery).processFile(Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(file3Prefix);
                }));

        // Process new files for the round
        discovery.processNewFilesForRound(replicationRound);

        // Verify that markInProgress was called 5 times (for all files)
        Mockito.verify(fileTracker, Mockito.times(5)).markInProgress(Mockito.any(Path.class));
        
        // Verify that markInProgress was called for each expected file
        for (Path expectedFile : newFilesForRound) {
            Mockito.verify(fileTracker, Mockito.times(1)).markInProgress(
                    Mockito.argThat(path -> path.getName().startsWith(expectedFile.getName().split("\\.")[0])));
        }

        // Verify that processFile was called for each file in the round
        Mockito.verify(discovery, Mockito.times(5)).processFile(Mockito.any(Path.class));

        // Verify that processFile was called for each specific file (using prefix matching)
        for (Path expectedFile : newFilesForRound) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("."));
            Mockito.verify(discovery, Mockito.times(1)).processFile(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(expectedPrefix);
                }));
        }

        // Verify that markCompleted was called for each successfully processed file
        Mockito.verify(fileTracker, Mockito.times(3)).markCompleted(Mockito.any(Path.class));

        // Verify that markCompleted was called for each successfully processed file with correct paths
        String expectedPrefix0 = newFilesForRound.get(0).getName().substring(0, newFilesForRound.get(0).getName().lastIndexOf("."));
        Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(expectedPrefix0);
                }));
        String expectedPrefix2 = newFilesForRound.get(2).getName().substring(0, newFilesForRound.get(2).getName().lastIndexOf("."));
        Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(expectedPrefix2);
                }));
        String expectedPrefix4 = newFilesForRound.get(4).getName().substring(0, newFilesForRound.get(4).getName().lastIndexOf("."));
        Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(expectedPrefix4);
                }));

        // Verify that markCompleted was NOT called for failed files
        String unexpectedPrefix1 = newFilesForRound.get(1).getName().substring(0, newFilesForRound.get(1).getName().lastIndexOf("."));
        Mockito.verify(fileTracker, Mockito.never()).markCompleted(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(unexpectedPrefix1);
                }));
        String unexpectedPrefix3 = newFilesForRound.get(3).getName().substring(0, newFilesForRound.get(3).getName().lastIndexOf("."));
        Mockito.verify(fileTracker, Mockito.never()).markCompleted(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(unexpectedPrefix3);
                }));

        // Verify that markFailed was called for failed files
        Mockito.verify(fileTracker, Mockito.times(1)).markFailed(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(unexpectedPrefix1);
                }));
        Mockito.verify(fileTracker, Mockito.times(1)).markFailed(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(unexpectedPrefix3);
                }));

        // Verify that markFailed was NOT called for successfully processed files
        Mockito.verify(fileTracker, Mockito.never()).markFailed(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(expectedPrefix0);
                }));
        Mockito.verify(fileTracker, Mockito.never()).markFailed(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(expectedPrefix2);
                }));
        Mockito.verify(fileTracker, Mockito.never()).markFailed(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(expectedPrefix4);
                }));
    }

    /**
     * Tests complete failure handling during new file processing.
     * Validates that all failed files are marked as failed with no completed files.
     */
    @Test
    public void testProcessNewFilesForRoundWithAllFailures() throws IOException {
        // Create new files with start of the day round (00:00:00)
        ReplicationRound replicationRound = new ReplicationRound(1704153600000L, 1704153660000L); // 00:00:00 - 00:01:00
        List<Path> newFilesForRound = createNewFilesForRound(replicationRound, 5);

        // Mock processFile to throw exception for all files (using prefix matching since files are moved to in-progress)
        for (Path file : newFilesForRound) {
            String filePrefix = file.getName().substring(0, file.getName().lastIndexOf("."));
            Mockito.doThrow(new IOException("Processing failed for file: " + file.getName()))
                .when(discovery).processFile(Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(filePrefix);
                }));
        }

        // Process new files for the round
        discovery.processNewFilesForRound(replicationRound);

        // Verify that processFile was called for each file in the round
        Mockito.verify(discovery, Mockito.times(5)).processFile(Mockito.any(Path.class));

        // Verify that markInProgress was called 5 times (before processing fails)
        Mockito.verify(fileTracker, Mockito.times(5)).markInProgress(Mockito.any(Path.class));
        
        // Verify that markInProgress was called for each expected file
        for (Path expectedFile : newFilesForRound) {
            Mockito.verify(fileTracker, Mockito.times(1)).markInProgress(
                    Mockito.argThat(path -> path.getName().startsWith(expectedFile.getName().split("\\.")[0])));
        }

        // Verify that processFile was called for each specific file (using prefix matching)
        for (Path expectedFile : newFilesForRound) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("."));
            Mockito.verify(discovery, Mockito.times(1)).processFile(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(expectedPrefix);
                }));
        }

        // Verify that markCompleted was NOT called for any file (all failed)
        Mockito.verify(fileTracker, Mockito.never()).markCompleted(Mockito.any(Path.class));

        // Verify that markFailed was called for all files
        Mockito.verify(fileTracker, Mockito.times(5)).markFailed(Mockito.any(Path.class));

        // Verify that markFailed was called for each specific file with correct paths
        for (Path failedFile : newFilesForRound) {
            String expectedPrefix = failedFile.getName().substring(0, failedFile.getName().lastIndexOf("."));
            Mockito.verify(fileTracker, Mockito.times(1)).markFailed(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(expectedPrefix);
                }));
        }
    }

    /**
     * Tests processing of all files in the in-progress directory.
     * Validates that only in-progress files are processed, not new files.
     */
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

        // Create set of expected files that should be processed (by prefix, since UUIDs are updated during markInProgress)
        Set<String> expectedProcessedFilePrefixes = new HashSet<>();
        for (Path file : inProgressFiles0004) {
            String fileName = file.getName();
            String prefix = fileName.substring(0, fileName.lastIndexOf("_"));
            expectedProcessedFilePrefixes.add(prefix);
        }
        for (Path file : inProgressFiles0102) {
            String fileName = file.getName();
            String prefix = fileName.substring(0, fileName.lastIndexOf("_"));
            expectedProcessedFilePrefixes.add(prefix);
        }
        for (Path file : inProgressFiles0206) {
            String fileName = file.getName();
            String prefix = fileName.substring(0, fileName.lastIndexOf("_"));
            expectedProcessedFilePrefixes.add(prefix);
        }

        // Create set of actually processed file paths (extract prefixes)
        Set<String> actualProcessedFilePrefixes = new HashSet<>();
        for (Path file : processedFiles) {
            String fileName = file.getName();
            String withoutExtension = fileName.substring(0, fileName.lastIndexOf("."));
            String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
            actualProcessedFilePrefixes.add(prefix);
        }

        // Validate that sets are equal
        assertEquals("Expected and actual processed files should match", expectedProcessedFilePrefixes, actualProcessedFilePrefixes);

        // Verify that markInProgress was called 7 times
        Mockito.verify(fileTracker, Mockito.times(7)).markInProgress(Mockito.any(Path.class));
        
        // Verify that markInProgress was called for each expected file
        for (Path expectedFile : inProgressFiles0004) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
            Mockito.verify(fileTracker, Mockito.times(1)).markInProgress(
                    Mockito.argThat(path -> {
                        String pathName = path.getName();
                        String prefix = pathName.substring(0, pathName.lastIndexOf("_"));
                        return prefix.equals(expectedPrefix);
                    }));
        }
        for (Path expectedFile : inProgressFiles0102) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
            Mockito.verify(fileTracker, Mockito.times(1)).markInProgress(
                    Mockito.argThat(path -> {
                        String pathName = path.getName();
                        String prefix = pathName.substring(0, pathName.lastIndexOf("_"));
                        return prefix.equals(expectedPrefix);
                    }));
        }
        for (Path expectedFile : inProgressFiles0206) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
            Mockito.verify(fileTracker, Mockito.times(1)).markInProgress(
                    Mockito.argThat(path -> {
                        String pathName = path.getName();
                        String prefix = pathName.substring(0, pathName.lastIndexOf("_"));
                        return prefix.equals(expectedPrefix);
                    }));
        }

        // Verify that markCompleted was called for each processed file
        Mockito.verify(fileTracker, Mockito.times(7)).markCompleted(Mockito.any(Path.class));

        // Verify that markCompleted was called for each processed file with correct paths
        for (Path expectedFile : inProgressFiles0004) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> {
                        String pathName = path.getName();
                        String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                        String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                        return prefix.equals(expectedPrefix);
                    }));
        }
        for (Path expectedFile : inProgressFiles0102) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> {
                        String pathName = path.getName();
                        String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                        String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                        return prefix.equals(expectedPrefix);
                    }));
        }
        for (Path expectedFile : inProgressFiles0206) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> {
                        String pathName = path.getName();
                        String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                        String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                        return prefix.equals(expectedPrefix);
                    }));
        }

        // Validate that new files were NOT processed (processInProgressDirectory only processes in-progress files)
        for (Path unexpectedFile : newFilesForRound) {
            String unexpectedPrefix = unexpectedFile.getName().substring(0, unexpectedFile.getName().lastIndexOf("."));
            assertFalse("Should NOT have processed new file: " + unexpectedFile.getName(),
                actualProcessedFilePrefixes.contains(unexpectedPrefix));
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

        // Mock processFile to throw exception for specific files (files 1 and 3) only on first call, succeed on retry
        String file1Prefix = allInProgressFiles.get(1).getName().substring(0, allInProgressFiles.get(1).getName().lastIndexOf("_"));
        Mockito.doThrow(new IOException("Processing failed for file 1"))
            .doCallRealMethod()
            .when(discovery).processFile(Mockito.argThat(path -> {
                String pathName = path.getName();
                String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                return prefix.equals(file1Prefix);
            }));
        String file3Prefix = allInProgressFiles.get(3).getName().substring(0, allInProgressFiles.get(3).getName().lastIndexOf("_"));
        Mockito.doThrow(new IOException("Processing failed for file 3"))
                .doCallRealMethod()
                .when(discovery).processFile(Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(file3Prefix);
                }));

        // Process in-progress directory
        discovery.processInProgressDirectory();

        // Verify that markInProgress was called 7 times (5 initially + 2 for retries)
        Mockito.verify(fileTracker, Mockito.times(7)).markInProgress(Mockito.any(Path.class));
        
        // Verify that markInProgress was called for each expected file
        // Files 1 and 3 are called twice (initial attempt + retry), others once
        for (int i = 0; i < allInProgressFiles.size(); i++) {
            Path expectedFile = allInProgressFiles.get(i);
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
            int expectedTimes = (i == 1 || i == 3) ? 2 : 1; // Files 1 and 3 are retried
            Mockito.verify(fileTracker, Mockito.times(expectedTimes)).markInProgress(
                    Mockito.argThat(path -> {
                        String pathName = path.getName();
                        String prefix = pathName.substring(0, pathName.lastIndexOf("_"));
                        return prefix.equals(expectedPrefix);
                    }));
        }

        // Verify that processFile was called for each file in the directory (i.e. 5 + 2 times for failed once that would succeed in next retry)
        Mockito.verify(discovery, Mockito.times(7)).processFile(Mockito.any(Path.class));

        // Verify that processFile was called for each specific file (using prefix matching)
        // Files 1 and 3 should be called twice (fail once, succeed on retry), others once
        for (int i = 0; i < allInProgressFiles.size(); i++) {
            Path expectedFile = allInProgressFiles.get(i);
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
            int expectedTimes = (i == 1 || i == 3) ? 2 : 1; // Files 1 and 3 are called twice (fail + retry success)
            Mockito.verify(discovery, Mockito.times(expectedTimes)).processFile(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(expectedPrefix);
                }));
        }

        // Verify that markCompleted was called for each successfully processed file
        Mockito.verify(fileTracker, Mockito.times(5)).markCompleted(Mockito.any(Path.class));

        // Verify that markCompleted was called for 2 intermittent failed processed file
        Mockito.verify(fileTracker, Mockito.times(2)).markFailed(Mockito.any(Path.class));

        // Verify that markFailed was called once ONLY for failed files
        String failedPrefix1 = allInProgressFiles.get(1).getName().substring(0, allInProgressFiles.get(1).getName().lastIndexOf("_"));
        Mockito.verify(fileTracker, Mockito.times(1)).markFailed(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(failedPrefix1);
                }));
        String failedPrefix3 = allInProgressFiles.get(3).getName().substring(0, allInProgressFiles.get(3).getName().lastIndexOf("_"));
        Mockito.verify(fileTracker, Mockito.times(1)).markFailed(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(failedPrefix3);
                }));

        // Verify that markFailed was NOT called for files processed successfully in first iteration
        String successPrefix0 = allInProgressFiles.get(0).getName().substring(0, allInProgressFiles.get(0).getName().lastIndexOf("_"));
        Mockito.verify(fileTracker, Mockito.never()).markFailed(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(successPrefix0);
                }));
        String successPrefix2 = allInProgressFiles.get(2).getName().substring(0, allInProgressFiles.get(2).getName().lastIndexOf("_"));
        Mockito.verify(fileTracker, Mockito.never()).markFailed(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(successPrefix2);
                }));
        String successPrefix4 = allInProgressFiles.get(4).getName().substring(0, allInProgressFiles.get(4).getName().lastIndexOf("_"));
        Mockito.verify(fileTracker, Mockito.never()).markFailed(
                Mockito.argThat(path -> {
                    String pathName = path.getName();
                    String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                    String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                    return prefix.equals(successPrefix4);
                }));

        // Verify that markCompleted was called for each successfully processed file with correct paths
        for (Path expectedFile : allInProgressFiles) {
            String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
            Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                    Mockito.argThat(path -> {
                        String pathName = path.getName();
                        String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                        String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                        return prefix.equals(expectedPrefix);
                    }));
        }
    }

    /**
     * Tests processing of in-progress directory when no files meet the timestamp criteria.
     * Validates that no files are processed when all files are too recent.
     */
    @Test
    public void testProcessInProgressDirectoryWithNoOldFiles() throws IOException {
        // Set up current time for consistent testing
        long currentTime = 1704153660000L; // 00:01:00
        EnvironmentEdge edge = () -> currentTime;
        EnvironmentEdgeManager.injectEdge(edge);

        try {
            // Create only recent files (all within the threshold)
            long recentTimestamp1 = 1704153655000L; // 00:00:55 (5 seconds old)
            long recentTimestamp2 = 1704153658000L; // 00:00:58 (2 seconds old)

            List<Path> recentFiles1 = createInProgressFiles(recentTimestamp1, 2);
            List<Path> recentFiles2 = createInProgressFiles(recentTimestamp2, 2);

            // Process in-progress directory
            discovery.processInProgressDirectory();

            // Get processed files
            List<Path> processedFiles = discovery.getProcessedFiles();

            // Verify that no files were processed (all files are too recent)
            assertEquals("Should not process any files when all files are too recent", 0, processedFiles.size());

        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    /**
     * Tests processing of in-progress directory with timestamp filtering using getOlderInProgressFiles.
     * Validates that only files older than the calculated threshold are processed, excluding recent files.
     */
    @Test
    public void testProcessInProgressDirectoryWithTimestampFiltering() throws IOException {
        // Set up current time for consistent testing
        long currentTime = 1704153660000L; // 00:01:00
        EnvironmentEdge edge = () -> currentTime;
        EnvironmentEdgeManager.injectEdge(edge);

        try {
            // Create files with various ages
            long veryOldTimestamp = 1704153600000L; // 00:00:00 (1 minute old) - should be processed
            long oldTimestamp = 1704153630000L; // 00:00:30 (30 seconds old) - should be processed
            long recentTimestamp = 1704153655000L; // 00:00:55 (5 seconds old) - should NOT be processed
            long veryRecentTimestamp = 1704153658000L; // 00:00:58 (2 seconds old) - should NOT be processed

            List<Path> veryOldFiles = createInProgressFiles(veryOldTimestamp, 1);
            List<Path> oldFiles = createInProgressFiles(oldTimestamp, 1);
            List<Path> recentFiles = createInProgressFiles(recentTimestamp, 1);
            List<Path> veryRecentFiles = createInProgressFiles(veryRecentTimestamp, 1);

            // Process in-progress directory
            discovery.processInProgressDirectory();

            // Get processed files
            List<Path> processedFiles = discovery.getProcessedFiles();

            // Verify that only old files were processed (2 old files, 0 recent files)
            assertEquals("Should process only old files based on timestamp filtering", 2, processedFiles.size());

            // Create set of expected processed files (by prefix, since UUIDs are updated during markInProgress)
            Set<String> expectedProcessedFilePrefixes = new HashSet<>();
            for (Path file : veryOldFiles) {
                String fileName = file.getName();
                String prefix = fileName.substring(0, fileName.lastIndexOf("_"));
                expectedProcessedFilePrefixes.add(prefix);
            }
            for (Path file : oldFiles) {
                String fileName = file.getName();
                String prefix = fileName.substring(0, fileName.lastIndexOf("_"));
                expectedProcessedFilePrefixes.add(prefix);
            }

            // Create set of actually processed file paths (extract prefixes)
            Set<String> actualProcessedFilePrefixes = new HashSet<>();
            for (Path file : processedFiles) {
                String fileName = file.getName();
                String withoutExtension = fileName.substring(0, fileName.lastIndexOf("."));
                String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                actualProcessedFilePrefixes.add(prefix);
            }

            // Validate that only old files were processed
            assertEquals("Expected and actual processed files should match (only old files)",
                    expectedProcessedFilePrefixes, actualProcessedFilePrefixes);

            // Verify that markInProgress was called 2 times (only old files)
            Mockito.verify(fileTracker, Mockito.times(2)).markInProgress(Mockito.any(Path.class));
            
            // Verify that markInProgress was called for each expected file
            for (Path expectedFile : veryOldFiles) {
                String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
                Mockito.verify(fileTracker, Mockito.times(1)).markInProgress(
                        Mockito.argThat(path -> {
                            String pathName = path.getName();
                            String prefix = pathName.substring(0, pathName.lastIndexOf("_"));
                            return prefix.equals(expectedPrefix);
                        }));
            }
            for (Path expectedFile : oldFiles) {
                String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
                Mockito.verify(fileTracker, Mockito.times(1)).markInProgress(
                        Mockito.argThat(path -> {
                            String pathName = path.getName();
                            String prefix = pathName.substring(0, pathName.lastIndexOf("_"));
                            return prefix.equals(expectedPrefix);
                        }));
            }
            
            // Verify that markCompleted was called 2 times
            Mockito.verify(fileTracker, Mockito.times(2)).markCompleted(Mockito.any(Path.class));
            
            // Verify that markCompleted was called for each expected file with correct paths
            for (Path expectedFile : veryOldFiles) {
                String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
                Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                        Mockito.argThat(path -> {
                            String pathName = path.getName();
                            String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                            String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                            return prefix.equals(expectedPrefix);
                        }));
            }
            for (Path expectedFile : oldFiles) {
                String expectedPrefix = expectedFile.getName().substring(0, expectedFile.getName().lastIndexOf("_"));
                Mockito.verify(fileTracker, Mockito.times(1)).markCompleted(
                        Mockito.argThat(path -> {
                            String pathName = path.getName();
                            String withoutExtension = pathName.substring(0, pathName.lastIndexOf("."));
                            String prefix = withoutExtension.substring(0, withoutExtension.lastIndexOf("_"));
                            return prefix.equals(expectedPrefix);
                        }));
            }

            // Verify that recent files were NOT processed
            for (Path file : recentFiles) {
                String unexpectedPrefix = file.getName().substring(0, file.getName().lastIndexOf("_"));
                assertFalse("Recent files should not be processed due to timestamp filtering: " + file.getName(),
                        actualProcessedFilePrefixes.contains(unexpectedPrefix));
            }
            for (Path file : veryRecentFiles) {
                String unexpectedPrefix = file.getName().substring(0, file.getName().lastIndexOf("_"));
                assertFalse("Recent files should not be processed due to timestamp filtering: " + file.getName(),
                        actualProcessedFilePrefixes.contains(unexpectedPrefix));
            }

        } finally {
            EnvironmentEdgeManager.reset();
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
        when(fileTracker.getNewFiles()).thenReturn(Collections.singletonList(filePath));
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
        discovery.initializeLastRoundProcessed();

        // Verify the result
        ReplicationRound result = discovery.getLastRoundProcessed();
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
        List<Path> newFiles = Collections.singletonList(newFile);

        // Mock the tracker to return in-progress files and one new file
        when(fileTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(fileTracker.getNewFiles()).thenReturn(newFiles);

        // Call the method
        discovery.initializeLastRoundProcessed();

        // Verify the result - should use minimum timestamp from in-progress files
        ReplicationRound result = discovery.getLastRoundProcessed();
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
        discovery.initializeLastRoundProcessed();

        // Verify the result - should use minimum timestamp from in-progress files
        ReplicationRound result = discovery.getLastRoundProcessed();
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
        List<Path> inProgressFiles = Collections.singletonList(inProgressFile);

        // Mock the tracker to return single in-progress file and empty new files
        when(fileTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(fileTracker.getNewFiles()).thenReturn(Collections.emptyList());

        // Call the method
        discovery.initializeLastRoundProcessed();

        // Verify the result
        ReplicationRound result = discovery.getLastRoundProcessed();
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
        List<Path> inProgressFiles = Collections.singletonList(inProgressFile);

        // Create one new file with later timestamp
        Path newFile = new Path(testFolderPath, "1704412860000_rs2.plog"); // 2024-01-05 00:01:00
        List<Path> newFiles = Collections.singletonList(newFile);

        // Mock the tracker to return single in-progress file and one new file
        when(fileTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(fileTracker.getNewFiles()).thenReturn(newFiles);

        // Call the method
        discovery.initializeLastRoundProcessed();

        // Verify the result - should use timestamp from in-progress file
        ReplicationRound result = discovery.getLastRoundProcessed();
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
        List<Path> inProgressFiles = Collections.singletonList(inProgressFile);

        // Create multiple new files with later timestamps
        Path newFile1 = new Path(testFolderPath, "1704499260000_rs2.plog"); // 2024-01-06 00:01:00
        Path newFile2 = new Path(testFolderPath, "1704499320000_rs3.plog"); // 2024-01-06 00:02:00
        List<Path> newFiles = Arrays.asList(newFile1, newFile2);

        // Mock the tracker to return single in-progress file and multiple new files
        when(fileTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(fileTracker.getNewFiles()).thenReturn(newFiles);

        // Call the method
        discovery.initializeLastRoundProcessed();

        // Verify the result - should use timestamp from in-progress file
        ReplicationRound result = discovery.getLastRoundProcessed();
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
        discovery.initializeLastRoundProcessed();

        // Verify the result
        ReplicationRound result = discovery.getLastRoundProcessed();
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
        List<Path> newFiles = Collections.singletonList(newFile);

        // Mock the tracker to return empty in-progress files and one new file
        when(fileTracker.getNewFiles()).thenReturn(newFiles);

        // Call the method
        discovery.initializeLastRoundProcessed();

        // Verify the result
        ReplicationRound result = discovery.getLastRoundProcessed();
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
        EnvironmentEdge customEdge = () -> {
            return 1704758400000L; // 2024-01-09 00:00:00
        };
        EnvironmentEdgeManager.injectEdge(customEdge);

        try {
            // Call the method
            discovery.initializeLastRoundProcessed();

            // Verify the result
            ReplicationRound result = discovery.getLastRoundProcessed();
            assertNotNull("Last successfully processed round should not be null", result);

            // Verify the round uses the custom time
            long expectedEndTime = (1704758400000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
            long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
            assertEquals("Should use custom time when no files are found", expectedStartTime, result.getStartTime());
            assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
        } finally {
            // Reset EnvironmentEdgeManager to default
            EnvironmentEdgeManager.reset();
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
            Path file = new Path(shardPath, replicationRound.getStartTime() + (1234L * i) + "_rs-" + i + ".plog");
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

    @Test
    public void testReplay() throws IOException {
        long roundTimeMills = discovery.getRoundTimeMills();
        long bufferMillis = discovery.getBufferMillis();
        long totalWaitTime = roundTimeMills + bufferMillis;

        // Test Case 1: No rounds to process (not enough time has passed)
        long initialEndTime = 1704153600000L; // 2024-01-02 00:00:00
        discovery.setLastRoundProcessed(new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime));
        discovery.resetProcessedRounds();

        EnvironmentEdge edge1 = () -> {
            return initialEndTime + 1000L; // Only 1 second after last round
        };
        EnvironmentEdgeManager.injectEdge(edge1);

        try {
            discovery.replay();
            List<ReplicationRound> processedRounds = discovery.getProcessedRounds();
            assertEquals("Should not process any rounds when not enough time has passed", 0, processedRounds.size());
        } finally {
            EnvironmentEdgeManager.reset();
        }

        // Test Case 2: Exactly one round to process
        discovery.setLastRoundProcessed(new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime));
        discovery.resetProcessedRounds();

        EnvironmentEdge edge2 = () -> {
            return initialEndTime + totalWaitTime; // Exactly threshold for one round
        };
        EnvironmentEdgeManager.injectEdge(edge2);

        try {
            discovery.replay();
            List<ReplicationRound> processedRounds = discovery.getProcessedRounds();
            assertEquals("Should process exactly one round", 1, processedRounds.size());

            ReplicationRound expectedRound = new ReplicationRound(initialEndTime, initialEndTime + roundTimeMills);
            assertEquals("Processed round should match expected round",
                    expectedRound, processedRounds.get(0));
            assertEquals("Last round processed should be updated",
                    expectedRound, discovery.getLastRoundProcessed());
        } finally {
            EnvironmentEdgeManager.reset();
        }

        // Test Case 3: Multiple rounds to process (3 rounds worth of time)
        discovery.setLastRoundProcessed(new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime));
        discovery.resetProcessedRounds();

        EnvironmentEdge edge3 = () -> {
            return initialEndTime + (3 * totalWaitTime); // 3 rounds worth of time
        };
        EnvironmentEdgeManager.injectEdge(edge3);

        try {
            discovery.replay();
            List<ReplicationRound> processedRounds = discovery.getProcessedRounds();
            assertEquals("Should process 3 rounds", 3, processedRounds.size());

            // Verify first round
            ReplicationRound expectedRound1 = new ReplicationRound(initialEndTime, initialEndTime + roundTimeMills);
            assertEquals("First round should match expected", expectedRound1, processedRounds.get(0));

            // Verify second round
            ReplicationRound expectedRound2 = new ReplicationRound(initialEndTime + roundTimeMills, initialEndTime + (2 * roundTimeMills));
            assertEquals("Second round should match expected", expectedRound2, processedRounds.get(1));

            // Verify third round
            ReplicationRound expectedRound3 = new ReplicationRound(initialEndTime + (2 * roundTimeMills), initialEndTime + (3 * roundTimeMills));
            assertEquals("Third round should match expected", expectedRound3, processedRounds.get(2));

            // Verify last round processed was updated to the last round
            assertEquals("Last round processed should be updated to third round",
                    expectedRound3, discovery.getLastRoundProcessed());
        } finally {
            EnvironmentEdgeManager.reset();
        }

        // Test Case 4: Exception during processing (should stop and not update last round)
        discovery.setLastRoundProcessed(new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime));
        discovery.resetProcessedRounds();

        // Create files for multiple rounds
        ReplicationRound round1 = new ReplicationRound(initialEndTime, initialEndTime + roundTimeMills);
        ReplicationRound round2 = new ReplicationRound(initialEndTime + roundTimeMills, initialEndTime + (2 * roundTimeMills));
        ReplicationRound round3 = new ReplicationRound(initialEndTime + (2 * roundTimeMills), initialEndTime + (3 * roundTimeMills));

        createNewFilesForRound(round1, 2);
        createNewFilesForRound(round2, 2);
        createNewFilesForRound(round3, 2);

        // Mock shouldProcessInProgressDirectory to return false (only process new files)
        discovery.setMockShouldProcessInProgressDirectory(false);

        // Mock processRound to throw exception on second round
        Mockito.doCallRealMethod().doThrow(new IOException("Simulated failure on round 2"))
                .when(discovery).processRound(Mockito.any(ReplicationRound.class));

        EnvironmentEdge edge4 = () -> {
            return initialEndTime + (3 * totalWaitTime); // 3 rounds worth of time
        };
        EnvironmentEdgeManager.injectEdge(edge4);

        try {
            discovery.replay();

            // Should have processed only the first round before exception
            List<ReplicationRound> processedRounds = discovery.getProcessedRounds();
            assertEquals("Should process only 1 round before exception", 1, processedRounds.size());

            ReplicationRound expectedRound1 = new ReplicationRound(initialEndTime, initialEndTime + roundTimeMills);
            assertEquals("First round should be processed", expectedRound1, processedRounds.get(0));

            // Last round processed should be updated to first round (before exception)
            assertEquals("Last round processed should be updated to first round only",
                    expectedRound1, discovery.getLastRoundProcessed());
        } finally {
            EnvironmentEdgeManager.reset();
            // Reset the mock
            Mockito.doCallRealMethod().when(discovery).processRound(Mockito.any(ReplicationRound.class));
        }

        // Test Case 5: Resume after exception (should continue from last successful round)
        discovery.resetProcessedRounds();

        EnvironmentEdge edge5 = () -> {
            return initialEndTime + (3 * totalWaitTime); // Still 3 rounds worth from original
        };
        EnvironmentEdgeManager.injectEdge(edge5);

        try {
            discovery.replay();

            // Should process the remaining 2 rounds
            List<ReplicationRound> processedRounds = discovery.getProcessedRounds();
            assertEquals("Should process remaining 2 rounds", 2, processedRounds.size());

            // Verify second round (continuing from where we left off)
            ReplicationRound expectedRound2 = new ReplicationRound(initialEndTime + roundTimeMills, initialEndTime + (2 * roundTimeMills));
            assertEquals("Second round should match expected", expectedRound2, processedRounds.get(0));

            // Verify third round
            ReplicationRound expectedRound3 = new ReplicationRound(initialEndTime + (2 * roundTimeMills), initialEndTime + (3 * roundTimeMills));
            assertEquals("Third round should match expected", expectedRound3, processedRounds.get(1));

            // Last round processed should now be updated to third round
            assertEquals("Last round processed should be updated to third round",
                    expectedRound3, discovery.getLastRoundProcessed());
        } finally {
            EnvironmentEdgeManager.reset();
        }

        // Test Case 6: Boundary condition - exactly at buffer threshold
        discovery.setLastRoundProcessed(new ReplicationRound(initialEndTime - roundTimeMills, initialEndTime));
        discovery.resetProcessedRounds();

        EnvironmentEdge edge6 = () -> {
            return initialEndTime + totalWaitTime - 1L; // 1ms before threshold
        };
        EnvironmentEdgeManager.injectEdge(edge6);

        try {
            discovery.replay();
            List<ReplicationRound> processedRounds = discovery.getProcessedRounds();
            assertEquals("Should not process any rounds when 1ms before threshold", 0, processedRounds.size());
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    @Test
    public void testGetNextRoundToProcess() {
        // Setup: roundTimeMills = 60000ms (1 minute), bufferMillis = 15% of 60000 = 9000ms
        // Total wait time needed = 69000ms
        long roundTimeMills = discovery.getRoundTimeMills();
        long bufferMillis = discovery.getBufferMillis();
        long totalWaitTime = roundTimeMills + bufferMillis;

        // Test Case 1: Not enough time has passed (just started)
        long lastRoundEndTimestamp = 1704153600000L; // 2024-01-02 00:00:00
        discovery.setLastRoundProcessed(new ReplicationRound(lastRoundEndTimestamp - roundTimeMills, lastRoundEndTimestamp));

        long currentTime1 = lastRoundEndTimestamp + 1000L; // Only 1 second later

        EnvironmentEdge edge1 = () -> currentTime1;
        EnvironmentEdgeManager.injectEdge(edge1);

        try {
            Optional<ReplicationRound> result1 = discovery.getNextRoundToProcess();
            assertFalse("Should return empty when not enough time has passed (1 second)",
                    result1.isPresent());
        } finally {
            EnvironmentEdgeManager.reset();
        }

        // Test Case 2: Not enough time has passed (just before threshold)
        discovery.setLastRoundProcessed(new ReplicationRound(lastRoundEndTimestamp - roundTimeMills, lastRoundEndTimestamp));

        long currentTime2 = lastRoundEndTimestamp + totalWaitTime - 1L; // 1ms before threshold

        EnvironmentEdge edge2 = () -> currentTime2;
        EnvironmentEdgeManager.injectEdge(edge2);

        try {
            Optional<ReplicationRound> result2 = discovery.getNextRoundToProcess();
            assertFalse("Should return empty when not enough time has passed (1ms before threshold)",
                    result2.isPresent());
        } finally {
            EnvironmentEdgeManager.reset();
        }

        // Test Case 3: Exactly at threshold - should return next round
        discovery.setLastRoundProcessed(new ReplicationRound(lastRoundEndTimestamp - roundTimeMills, lastRoundEndTimestamp));

        long currentTime3 = lastRoundEndTimestamp + totalWaitTime; // Exactly at threshold

        EnvironmentEdge edge3 = () -> currentTime3;
        EnvironmentEdgeManager.injectEdge(edge3);

        try {
            Optional<ReplicationRound> result3 = discovery.getNextRoundToProcess();
            assertTrue("Should return next round when exactly at threshold", result3.isPresent());

            ReplicationRound expectedRound = new ReplicationRound(lastRoundEndTimestamp, lastRoundEndTimestamp + roundTimeMills);
            assertEquals("Returned round should match expected", expectedRound, result3.get());
        } finally {
            EnvironmentEdgeManager.reset();
        }

        // Test Case 4: Time has passed beyond threshold (1ms after)
        discovery.setLastRoundProcessed(new ReplicationRound(lastRoundEndTimestamp - roundTimeMills, lastRoundEndTimestamp));

        long currentTime4 = lastRoundEndTimestamp + totalWaitTime + 1L; // 1ms after threshold

        EnvironmentEdge edge4 = () -> currentTime4;
        EnvironmentEdgeManager.injectEdge(edge4);

        try {
            Optional<ReplicationRound> result4 = discovery.getNextRoundToProcess();
            assertTrue("Should return next round when 1ms after threshold", result4.isPresent());

            ReplicationRound expectedRound = new ReplicationRound(lastRoundEndTimestamp, lastRoundEndTimestamp + roundTimeMills);
            assertEquals("Returned round should match expected", expectedRound, result4.get());
        } finally {
            EnvironmentEdgeManager.reset();
        }

        // Test Case 5: Much time has passed (multiple rounds worth)
        discovery.setLastRoundProcessed(new ReplicationRound(lastRoundEndTimestamp - roundTimeMills, lastRoundEndTimestamp));

        long currentTime5 = lastRoundEndTimestamp + (3 * totalWaitTime); // 3 rounds worth of time

        EnvironmentEdge edge5 = () -> currentTime5;
        EnvironmentEdgeManager.injectEdge(edge5);

        try {
            Optional<ReplicationRound> result5 = discovery.getNextRoundToProcess();
            assertTrue("Should return next round when multiple rounds worth of time has passed",
                    result5.isPresent());

            ReplicationRound expectedRound = new ReplicationRound(lastRoundEndTimestamp, lastRoundEndTimestamp + roundTimeMills);
            assertEquals("Returned round should match expected", expectedRound, result5.get());
        } finally {
            EnvironmentEdgeManager.reset();
        }

        // Test Case 6: Halfway through buffer time
        discovery.setLastRoundProcessed(new ReplicationRound(lastRoundEndTimestamp - roundTimeMills, lastRoundEndTimestamp));

        long currentTime8 = lastRoundEndTimestamp + roundTimeMills + (bufferMillis / 2);

        EnvironmentEdge edge8 = () -> currentTime8;
        EnvironmentEdgeManager.injectEdge(edge8);

        try {
            Optional<ReplicationRound> result8 = discovery.getNextRoundToProcess();
            assertFalse("Should return empty when halfway through buffer time",
                    result8.isPresent());
        } finally {
            EnvironmentEdgeManager.reset();
        }
    }

    /**
     * Testable implementation of ReplicationLogTracker for unit testing.
     * Exposes protected methods and provides minimal implementation for testing.
     */
    private static class TestableReplicationLogTracker extends ReplicationLogTracker {
        public TestableReplicationLogTracker(final Configuration conf, final String haGroupName, final FileSystem fileSystem, final ReplicationShardDirectoryManager replicationShardDirectoryManager) {
            super(conf, haGroupName, fileSystem, replicationShardDirectoryManager, metricsLogTracker);
        }
    }

    /**
     * Testable implementation of ReplicationLogDiscovery for unit testing.
     * Tracks processed files and rounds, and provides access to protected methods.
     */
    private static class TestableReplicationLogDiscovery extends ReplicationLogDiscovery {
        private final List<Path> processedFiles = new ArrayList<>();
        private final List<ReplicationRound> processedRounds = new ArrayList<>();

        public TestableReplicationLogDiscovery(ReplicationLogTracker fileTracker) {
            super(fileTracker);
        }

        public long getRoundTimeMills() {
            return roundTimeMills;
        }

        public long getBufferMillis() {
            return bufferMillis;
        }

        @Override
        protected void processFile(Path path) throws IOException {
            // Simulate file processing
            processedFiles.add(path);
        }

        @Override
        protected MetricsReplicationLogDiscovery createMetricsSource() {
            return metricsLogDiscovery;
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

        public void resetProcessedRounds() {
            processedRounds.clear();
        }

        public void setLastRoundProcessed(ReplicationRound round) {
            super.setLastRoundProcessed(round);
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
    }
}
