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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.ClassRule;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ReplicationStateTrackerTest {

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private TestableReplicationStateTracker stateTracker;
    private ReplicationLogFileTracker mockTracker;
    private Configuration conf;

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        mockTracker = Mockito.mock(ReplicationLogFileTracker.class);
        stateTracker = new TestableReplicationStateTracker();
    }

    @Test
    public void testGetMinTimestampFromInProgressFilesEmptyList() throws IOException {
        // Mock empty list of in-progress files
        when(mockTracker.getInProgressFiles()).thenReturn(Collections.emptyList());

        // Call the method
        Optional<Long> result = stateTracker.getMinTimestampFromInProgressFiles(mockTracker);

        // Verify result is empty
        assertFalse("Result should be empty for empty file list", result.isPresent());

        // Verify getInProgressFiles was called once
        verify(mockTracker, times(1)).getInProgressFiles();
    }

    @Test
    public void testGetMinTimestampFromInProgressFilesSingleFile() throws IOException {
        // Create a single file path
        Path filePath = new Path("/test/1704153600000_rs1.plog");
        
        // Mock single file list
        when(mockTracker.getInProgressFiles()).thenReturn(Arrays.asList(filePath));
        when(mockTracker.getFileTimestamp(filePath)).thenReturn(1704153600000L);

        // Call the method
        Optional<Long> result = stateTracker.getMinTimestampFromInProgressFiles(mockTracker);

        // Verify result contains the timestamp
        assertTrue("Result should be present for single file", result.isPresent());
        assertEquals("Should return the timestamp of the single file", 
            Long.valueOf(1704153600000L), result.get());

        // Verify method calls
        verify(mockTracker, times(1)).getInProgressFiles();
        verify(mockTracker, times(1)).getFileTimestamp(filePath);
    }

    @Test
    public void testGetMinTimestampFromInProgressFilesMultipleFiles() throws IOException {
        // Create multiple file paths with different timestamps
        Path file1 = new Path("/test/1704153660000_rs2.plog");
        Path file2 = new Path("/test/1704153600000_rs1.plog");
        Path file3 = new Path("/test/1704153720000_rs3.plog");
        
        List<Path> files = Arrays.asList(file1, file2, file3);
        
        // Mock file list and timestamps
        when(mockTracker.getInProgressFiles()).thenReturn(files);
        when(mockTracker.getFileTimestamp(file1)).thenReturn(1704153660000L);
        when(mockTracker.getFileTimestamp(file2)).thenReturn(1704153600000L);
        when(mockTracker.getFileTimestamp(file3)).thenReturn(1704153720000L);

        // Call the method
        Optional<Long> result = stateTracker.getMinTimestampFromInProgressFiles(mockTracker);

        // Verify result contains the minimum timestamp
        assertTrue("Result should be present for multiple files", result.isPresent());
        assertEquals("Should return the minimum timestamp", 
            Long.valueOf(1704153600000L), result.get());

        // Verify method calls
        verify(mockTracker, times(1)).getInProgressFiles();
        verify(mockTracker, times(1)).getFileTimestamp(file1);
        verify(mockTracker, times(1)).getFileTimestamp(file2);
        verify(mockTracker, times(1)).getFileTimestamp(file3);
    }

    @Test
    public void testGetMinTimestampFromNewFilesEmptyList() throws IOException {
        // Mock empty list of new files
        when(mockTracker.getNewFiles()).thenReturn(Collections.emptyList());

        // Call the method
        Optional<Long> result = stateTracker.getMinTimestampFromNewFiles(mockTracker);

        // Verify result is empty
        assertFalse("Result should be empty for empty file list", result.isPresent());

        // Verify getNewFiles was called once
        verify(mockTracker, times(1)).getNewFiles();
    }

    @Test
    public void testGetMinTimestampFromNewFilesSingleFile() throws IOException {
        // Create a single file path
        Path filePath = new Path("/test/1704153600000_rs1.plog");
        
        // Mock single file list
        when(mockTracker.getNewFiles()).thenReturn(Arrays.asList(filePath));
        when(mockTracker.getFileTimestamp(filePath)).thenReturn(1704153600000L);

        // Call the method
        Optional<Long> result = stateTracker.getMinTimestampFromNewFiles(mockTracker);

        // Verify result contains the timestamp
        assertTrue("Result should be present for single file", result.isPresent());
        assertEquals("Should return the timestamp of the single file", 
            Long.valueOf(1704153600000L), result.get());

        // Verify method calls
        verify(mockTracker, times(1)).getNewFiles();
        verify(mockTracker, times(1)).getFileTimestamp(filePath);
    }

    @Test
    public void testGetMinTimestampFromNewFilesMultipleFiles() throws IOException {
        // Create multiple file paths with different timestamps
        Path file1 = new Path("/test/1704153660000_rs2.plog");
        Path file2 = new Path("/test/1704153600000_rs1.plog");
        Path file3 = new Path("/test/1704153720000_rs3.plog");
        
        List<Path> files = Arrays.asList(file1, file2, file3);
        
        // Mock file list and timestamps
        when(mockTracker.getNewFiles()).thenReturn(files);
        when(mockTracker.getFileTimestamp(file1)).thenReturn(1704153660000L);
        when(mockTracker.getFileTimestamp(file2)).thenReturn(1704153600000L);
        when(mockTracker.getFileTimestamp(file3)).thenReturn(1704153720000L);

        // Call the method
        Optional<Long> result = stateTracker.getMinTimestampFromNewFiles(mockTracker);

        // Verify result contains the minimum timestamp
        assertTrue("Result should be present for multiple files", result.isPresent());
        assertEquals("Should return the minimum timestamp", 
            Long.valueOf(1704153600000L), result.get());

        // Verify method calls
        verify(mockTracker, times(1)).getNewFiles();
        verify(mockTracker, times(1)).getFileTimestamp(file1);
        verify(mockTracker, times(1)).getFileTimestamp(file2);
        verify(mockTracker, times(1)).getFileTimestamp(file3);
    }


    /**
     * Testable implementation of ReplicationStateTracker for testing
     */
    private static class TestableReplicationStateTracker extends ReplicationStateTracker {
        
        @Override
        public void init(ReplicationLogFileTracker replicationLogFileTracker) throws IOException {
            // No-op implementation for testing
        }
        
        // Expose the protected method for testing
        public Optional<Long> getMinTimestampFromInProgressFiles(ReplicationLogFileTracker replicationLogFileTracker) throws IOException {
            return super.getMinTimestampFromInProgressFiles(replicationLogFileTracker);
        }
        
        // Expose the protected method for testing
        public Optional<Long> getMinTimestampFromNewFiles(ReplicationLogFileTracker replicationLogFileTracker) throws IOException {
            return super.getMinTimestampFromNewFiles(replicationLogFileTracker);
        }
    }
}
