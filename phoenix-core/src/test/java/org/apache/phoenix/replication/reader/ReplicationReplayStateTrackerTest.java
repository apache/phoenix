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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.replication.ReplicationRound;
import org.apache.phoenix.replication.ReplicationShardDirectoryManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;

public class ReplicationReplayStateTrackerTest {

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private ReplicationReplayStateTracker stateTracker;
    private TestableReplicationLogReplayFileTracker mockTracker;
    private Path rootPath;

    @Before
    public void setUp() {
        stateTracker = new ReplicationReplayStateTracker();
        
        // Create real shard manager with default configuration
        Configuration conf = HBaseConfiguration.create();
        this.rootPath = new Path(testFolder.getRoot().getAbsolutePath());
        ReplicationShardDirectoryManager realShardManager = new ReplicationShardDirectoryManager(conf, rootPath);
        
        // Create mock of ReplicationLogFileTracker
        mockTracker = mock(TestableReplicationLogReplayFileTracker.class);
        
        // Return real ReplicationShardDirectoryManager when getReplicationShardDirectoryManager is called
        when(mockTracker.getReplicationShardDirectoryManager()).thenReturn(realShardManager);
        
        // Setup getFileTimestamp to return timestamp from filename
        doAnswer(invocation -> {
            Path path = invocation.getArgument(0);
            String[] parts = path.getName().split("_");
            return Long.parseLong(parts[0]);
        }).when(mockTracker).getFileTimestamp(any(Path.class));
    }

    @Test
    public void testInit_MultipleInProgressFiles_NoNewFiles() throws IOException {
        // Create multiple in-progress files with timestamps
        Path inProgressFile1 = new Path(rootPath, "1704067200000_rs1.plog"); // 2024-01-01 00:00:00
        Path inProgressFile2 = new Path(rootPath, "1704067260000_rs2.plog"); // 2024-01-01 00:01:00
        Path inProgressFile3 = new Path(rootPath, "1704067320000_rs3.plog"); // 2024-01-01 00:02:00
        
        List<Path> inProgressFiles = Arrays.asList(inProgressFile1, inProgressFile2, inProgressFile3);
        
        // Mock the tracker to return in-progress files and empty new files
        when(mockTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(mockTracker.getNewFiles()).thenReturn(Collections.emptyList());
        
        // Call the method
        stateTracker.init(mockTracker);
        
        // Verify the result
        ReplicationRound result = stateTracker.getLastRoundInSync();
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
        Path inProgressFile1 = new Path(rootPath, "1704153600000_rs1.plog"); // 2024-01-02 00:00:00
        Path inProgressFile2 = new Path(rootPath, "1704153660000_rs2.plog"); // 2024-01-02 00:01:00
        Path inProgressFile3 = new Path(rootPath, "1704153720000_rs3.plog"); // 2024-01-02 00:02:00
        
        List<Path> inProgressFiles = Arrays.asList(inProgressFile1, inProgressFile2, inProgressFile3);
        
        // Create one new file with later timestamp
        Path newFile = new Path(rootPath, "1704153780000_rs4.plog"); // 2024-01-02 00:03:00
        List<Path> newFiles = Arrays.asList(newFile);
        
        // Mock the tracker to return in-progress files and one new file
        when(mockTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(mockTracker.getNewFiles()).thenReturn(newFiles);
        
        // Call the method
        stateTracker.init(mockTracker);
        
        // Verify the result - should use minimum timestamp from in-progress files
        ReplicationRound result = stateTracker.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);
        
        long expectedEndTime = (1704153600000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use minimum timestamp from in-progress files", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_MultipleInProgressFiles_MultipleNewFiles() throws IOException {
        // Create multiple in-progress files with timestamps
        Path inProgressFile1 = new Path(rootPath, "1704240000000_rs1.plog"); // 2024-01-03 00:00:00
        Path inProgressFile2 = new Path(rootPath, "1704240060000_rs2.plog"); // 2024-01-03 00:01:00
        Path inProgressFile3 = new Path(rootPath, "1704240120000_rs3.plog"); // 2024-01-03 00:02:00
        
        List<Path> inProgressFiles = Arrays.asList(inProgressFile1, inProgressFile2, inProgressFile3);
        
        // Create multiple new files with later timestamps
        Path newFile1 = new Path(rootPath, "1704240180000_rs4.plog"); // 2024-01-03 00:03:00
        Path newFile2 = new Path(rootPath, "1704240240000_rs5.plog"); // 2024-01-03 00:04:00
        List<Path> newFiles = Arrays.asList(newFile1, newFile2);
        
        // Mock the tracker to return in-progress files and multiple new files
        when(mockTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(mockTracker.getNewFiles()).thenReturn(newFiles);
        
        // Call the method
        stateTracker.init(mockTracker);
        
        // Verify the result - should use minimum timestamp from in-progress files
        ReplicationRound result = stateTracker.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);
        
        long expectedEndTime = (1704240000000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use minimum timestamp from in-progress files", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_OneInProgressFile_NoNewFiles() throws IOException {
        // Create single in-progress file
        Path inProgressFile = new Path(rootPath, "1704326400000_rs1.plog"); // 2024-01-04 00:00:00
        List<Path> inProgressFiles = Arrays.asList(inProgressFile);
        
        // Mock the tracker to return single in-progress file and empty new files
        when(mockTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(mockTracker.getNewFiles()).thenReturn(Collections.emptyList());
        
        // Call the method
        stateTracker.init(mockTracker);
        
        // Verify the result
        ReplicationRound result = stateTracker.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);
        
        long expectedEndTime = (1704326400000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use timestamp from single in-progress file", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_OneInProgressFile_OneNewFile() throws IOException {
        // Create single in-progress file
        Path inProgressFile = new Path(rootPath, "1704412800000_rs1.plog"); // 2024-01-05 00:00:00
        List<Path> inProgressFiles = Arrays.asList(inProgressFile);
        
        // Create one new file with later timestamp
        Path newFile = new Path(rootPath, "1704412860000_rs2.plog"); // 2024-01-05 00:01:00
        List<Path> newFiles = Arrays.asList(newFile);
        
        // Mock the tracker to return single in-progress file and one new file
        when(mockTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(mockTracker.getNewFiles()).thenReturn(newFiles);
        
        // Call the method
        stateTracker.init(mockTracker);
        
        // Verify the result - should use timestamp from in-progress file
        ReplicationRound result = stateTracker.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);
        
        long expectedEndTime = (1704412800000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use timestamp from in-progress file", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_OneInProgressFile_MultipleNewFiles() throws IOException {
        // Create single in-progress file
        Path inProgressFile = new Path(rootPath, "1704499200000_rs1.plog"); // 2024-01-06 00:00:00
        List<Path> inProgressFiles = Arrays.asList(inProgressFile);
        
        // Create multiple new files with later timestamps
        Path newFile1 = new Path(rootPath, "1704499260000_rs2.plog"); // 2024-01-06 00:01:00
        Path newFile2 = new Path(rootPath, "1704499320000_rs3.plog"); // 2024-01-06 00:02:00
        List<Path> newFiles = Arrays.asList(newFile1, newFile2);
        
        // Mock the tracker to return single in-progress file and multiple new files
        when(mockTracker.getInProgressFiles()).thenReturn(inProgressFiles);
        when(mockTracker.getNewFiles()).thenReturn(newFiles);
        
        // Call the method
        stateTracker.init(mockTracker);
        
        // Verify the result - should use timestamp from in-progress file
        ReplicationRound result = stateTracker.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);
        
        long expectedEndTime = (1704499200000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use timestamp from in-progress file", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_NoInProgressFiles_MultipleNewFiles() throws IOException {
        // Mock empty in-progress files
        when(mockTracker.getInProgressFiles()).thenReturn(Collections.emptyList());
        
        // Create multiple new files with timestamps
        Path newFile1 = new Path(rootPath, "1704585600000_rs1.plog"); // 2024-01-07 00:00:00
        Path newFile2 = new Path(rootPath, "1704585660000_rs2.plog"); // 2024-01-07 00:01:00
        Path newFile3 = new Path(rootPath, "1704585720000_rs3.plog"); // 2024-01-07 00:02:00
        List<Path> newFiles = Arrays.asList(newFile1, newFile2, newFile3);
        
        // Mock the tracker to return empty in-progress files and multiple new files
        when(mockTracker.getNewFiles()).thenReturn(newFiles);
        
        // Call the method
        stateTracker.init(mockTracker);
        
        // Verify the result
        ReplicationRound result = stateTracker.getLastRoundInSync();
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
        when(mockTracker.getInProgressFiles()).thenReturn(Collections.emptyList());
        
        // Create single new file
        Path newFile = new Path(rootPath, "1704672000000_rs1.plog"); // 2024-01-08 00:00:00
        List<Path> newFiles = Arrays.asList(newFile);
        
        // Mock the tracker to return empty in-progress files and one new file
        when(mockTracker.getNewFiles()).thenReturn(newFiles);
        
        // Call the method
        stateTracker.init(mockTracker);
        
        // Verify the result
        ReplicationRound result = stateTracker.getLastRoundInSync();
        assertNotNull("Last successfully processed round should not be null", result);
        
        long expectedEndTime = (1704672000000L / TimeUnit.MINUTES.toMillis(1)) * TimeUnit.MINUTES.toMillis(1); // Round down to nearest 60-second boundary
        long expectedStartTime = expectedEndTime - TimeUnit.MINUTES.toMillis(1); // Start time is 60 seconds less than end time
        assertEquals("Should use timestamp from single new file when no in-progress files", expectedStartTime, result.getStartTime());
        assertEquals("End time must be rounded down to nearest 60-second boundary", expectedEndTime, result.getEndTime());
    }

    @Test
    public void testInit_NoInProgressFiles_NoNewFiles() throws IOException {
        // Mock empty in-progress files
        when(mockTracker.getInProgressFiles()).thenReturn(Collections.emptyList());
        
        // Mock empty new files
        when(mockTracker.getNewFiles()).thenReturn(Collections.emptyList());
        
        // Set custom time using EnvironmentEdgeManager
        EnvironmentEdge customEdge = new EnvironmentEdge() {
            @Override
            public long currentTime() {
                return 1704758400000L; // 2024-01-09 00:00:00
            }
        };
        EnvironmentEdgeManager.injectEdge(customEdge);
        
        try {
            // Call the method
            stateTracker.init(mockTracker);
            
            // Verify the result
            ReplicationRound result = stateTracker.getLastRoundInSync();
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

    private static class TestableReplicationLogReplayFileTracker extends ReplicationLogReplayFileTracker {

        public TestableReplicationLogReplayFileTracker(Configuration conf, String haGroupName, FileSystem fileSystem, URI rootURI) {
            super(conf, haGroupName, fileSystem, rootURI);
        }

        // Expose the protected method for testing
        public List<Path> getInProgressFiles() throws IOException {
            return super.getInProgressFiles();
        }

        // Expose the protected method for testing
        public List<Path> getNewFiles() throws IOException {
            return super.getNewFiles();
        }
    }
}
