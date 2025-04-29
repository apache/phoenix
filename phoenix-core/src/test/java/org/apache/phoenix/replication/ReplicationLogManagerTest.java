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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.phoenix.replication.log.LogFile;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReplicationLogManagerTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private Configuration conf;
    private RegionServerServices mockRsServices;
    private FileSystem mockFs;
    private Path standbyLogDir;
    private ManualEnvironmentEdge injectEdge;
    private TestableReplicationLogManager logManager;

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        // Use a temporary folder for the standby HDFS URL
        standbyLogDir = new Path(testFolder.newFolder("standby").toURI());
        conf.set(ReplicationLogManager.REPLICATION_STANDBY_HDFS_URL_KEY, standbyLogDir.toString());

        // Set short rotation time/size for testing
        conf.setLong(ReplicationLogManager.REPLICATION_LOG_ROTATION_TIME_MS_KEY, 100);
        conf.setLong(ReplicationLogManager.REPLICATION_LOG_ROTATION_SIZE_BYTES_KEY, 100);
        conf.setDouble(ReplicationLogManager.REPLICATION_LOG_ROTATION_SIZE_PERCENTAGE_KEY, 1.0);

        // Mock RegionServerServices
        mockRsServices = mock(RegionServerServices.class);
        when(mockRsServices.getServerName()).thenReturn(ServerName.valueOf("testserver", 60010,
            EnvironmentEdgeManager.currentTimeMillis()));
        mockFs = mock(FileSystem.class);
        // Mock FileSystem methods
        when(mockFs.getUri()).thenReturn(standbyLogDir.toUri());
        when(mockFs.getConf()).thenReturn(conf);
        when(mockFs.exists(standbyLogDir)).thenReturn(true);

        // Use a ManualEnvironmentEdge to control the time
        injectEdge = new ManualEnvironmentEdge();
        injectEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
        EnvironmentEdgeManager.injectEdge(injectEdge);

        // Create the testable manager instance, mocking the standby FileSystem
        logManager = new TestableReplicationLogManager(conf, mockRsServices, mockFs) {
            @Override
            protected void initializeFileSystems() throws IOException {
                this.standbyFs = mockFs;
                this.standbyUrl = standbyLogDir.toUri();
            }
        };
        logManager.init();
    }

    @After
    public void tearDown() {
        logManager.close();
        EnvironmentEdgeManager.reset(); // Reset to default clock
    }

    @Test
    public void testTimeBasedRotation() throws Exception {
        long initialRotationTime = logManager.getLastRotationTime();
        long initialGeneration = logManager.getWriterGeneration();
        LogFile.Writer writer1 = logManager.getWriter();
        LogFile.Writer internalWriter1 = logManager.getCurrentInternalWriter();
        assertNotNull("Writer should not be null", writer1);
        assertNotNull("Writer should not be null", internalWriter1);
        // Advance time just before rotation threshold
        injectEdge.incrementValue(90);
        writer1 = logManager.getWriter();
        // Should still be the same writer instance (internal delegate) and same generation
        assertSame("Writer should not rotate before time threshold", internalWriter1,
            logManager.getCurrentInternalWriter());
        assertEquals("Generation should not change", initialGeneration,
            logManager.getWriterGeneration());
        // Advance time past rotation threshold
        injectEdge.incrementValue(20); // 110ms > 100ms
        LogFile.Writer writer2 = logManager.getWriter();
        LogFile.Writer internalWriter2 = logManager.getCurrentInternalWriter();
        assertNotNull("Writer should not be null", writer2);
        assertNotNull("Writer should not be null", internalWriter2);
        assertNotEquals("Writer should rotate after time threshold", internalWriter1,
            internalWriter2);
        assertEquals("Should have created two writers", 2, logManager.getCreatedWriters().size());
        assertEquals("Generation should increment", initialGeneration + 1,
            logManager.getWriterGeneration());
        assertTrue("Last rotation time should update",
            logManager.getLastRotationTime() > initialRotationTime);
        assertEquals("Should have closed a writer", 1, logManager.getClosedWriters().size());
        assertTrue("Old writer should be closed",
            logManager.getClosedWriters().contains(writer1));
        // Verify close was called on the internal writer
        verify(internalWriter1, times(1)).close();
    }

    @Test
    public void testSizeBasedRotation() throws Exception {
        long initialRotationTime = logManager.getLastRotationTime();
        long initialGeneration = logManager.getWriterGeneration();
        LogFile.Writer writer1 = logManager.getWriter();
        LogFile.Writer internalWriter1 = logManager.getCurrentInternalWriter();
        assertNotNull("Writer should not be null", writer1);
        assertNotNull("Writer should not be null", internalWriter1);
        // Simulate size below threshold
        when(internalWriter1.getLength()).thenReturn(50L);
        writer1 = logManager.getWriter();
        assertSame("Writer should not rotate before size threshold", internalWriter1,
            logManager.getCurrentInternalWriter());
        assertEquals("Generation should not change", initialGeneration,
            logManager.getWriterGeneration());
        // Simulate size at/above threshold
        when(internalWriter1.getLength()).thenReturn(100L);
        injectEdge.incrementValue(10); // We need to increment the manual edge
        LogFile.Writer writer2 = logManager.getWriter();
        LogFile.Writer internalWriter2 = logManager.getCurrentInternalWriter();
        assertNotNull("Writer should not be null", writer2);
        assertNotNull("Writer should not be null", internalWriter2);
        assertNotEquals("Writer should rotate after size threshold", internalWriter1,
            internalWriter2);
        assertEquals("Should have created two writers", 2, logManager.getCreatedWriters().size());
        assertEquals("Generation should increment", initialGeneration + 1,
            logManager.getWriterGeneration());
        assertTrue("Last rotation time should update",
            logManager.getLastRotationTime() > initialRotationTime);
        assertEquals("Should have closed a writer", 1, logManager.getClosedWriters().size());
        assertTrue("Old writer should be closed",
            logManager.getClosedWriters().contains(writer1));
        // Verify close was called on the internal writer
        verify(internalWriter1, times(1)).close();
    }

    @Test(expected = ReplicationLogManager.StaleLogWriterException.class)
    public void testStaleWriterExceptionOnSync() throws Exception {
        LogFile.Writer writer1 = logManager.getWriter();
        LogFile.Writer internalWriter1 = logManager.getCurrentInternalWriter();
        assertNotNull("Writer should not be null", writer1);
        assertNotNull("Writer should not be null", internalWriter1);
        // Force rotation
        injectEdge.incrementValue(110); // Ensure time-based rotation happens
        LogFile.Writer writer2 = logManager.getWriter();
        assertNotNull("Writer should not be null", writer2);
        // This should throw StaleLogWriterException
        writer1.sync();
        // Verify close was called on the old internal writer
        verify(internalWriter1, times(1)).close();
        // Verify sync was never called on the old delegate
        verify(internalWriter1, never()).sync();
    }

    @Test(expected = ReplicationLogManager.StaleLogWriterException.class)
    public void testStaleWriterExceptionOnAppend() throws Exception {
        LogFile.Writer writer1 = logManager.getWriter();
        LogFile.Writer internalWriter1 = logManager.getCurrentInternalWriter();
        assertNotNull("Writer should not be null", writer1);
        assertNotNull("Writer should not be null", internalWriter1);
        // Force rotation
        injectEdge.incrementValue(110); // Ensure time-based rotation happens
        LogFile.Writer writer2 = logManager.getWriter();
        assertNotNull("Writer should not be null", writer2);
        // Try to append using the old wrapper
        Mutation mockMutation = mock(Mutation.class);
        // This should throw StaleLogWriterException
        writer1.append("TABLE", 1L, mockMutation);
        // Verify close was called on the old internal writer
        verify(internalWriter1, times(1)).close();
        // Verify append was never called on the old delegate
        verify(internalWriter1, never()).append("TABLE", 1L, mockMutation);
    }

    @Test
    public void testRotationTask() throws Exception {
        // Test that the background rotation task works
        long initialRotationTime = logManager.getLastRotationTime();
        long initialGeneration = logManager.getWriterGeneration();
        LogFile.Writer writer1 = logManager.getWriter();
        LogFile.Writer internalWriter1 = logManager.getCurrentInternalWriter();
        assertNotNull("Writer should not be null", writer1);
        assertNotNull("Writer should not be null", internalWriter1);
        // Advance time significantly past the rotation interval
        injectEdge.incrementValue(200);
        // Wait for the rotation task to potentially run (it runs every rotationTimeMs / 4)
        // Give it more than enough time
        Thread.sleep(200);
        // Get the writer again, it should have been rotated by the background task
        LogFile.Writer writer2 = logManager.getWriter();
        LogFile.Writer internalWriter2 = logManager.getCurrentInternalWriter();
        assertNotNull("Writer should not be null", writer2);
        assertNotNull("Writer should not be null", internalWriter2);
        assertNotEquals("Writer should have been rotated by the background task", internalWriter1,
            internalWriter2);
        assertEquals("Generation should increment", initialGeneration + 1,
            logManager.getWriterGeneration());
        assertTrue("Last rotation time should update",
            logManager.getLastRotationTime() > initialRotationTime);
        assertEquals("Should have closed a writer", 1, logManager.getClosedWriters().size());
        assertTrue("Old writer should be closed",
            logManager.getClosedWriters().contains(writer1));
        // Verify close was called on the old internal writer
        verify(internalWriter1, times(1)).close();
    }

    @Test
    public void testClose() throws Exception {
         LogFile.Writer writer1 = logManager.getWriter();
         LogFile.Writer internalWriter1 = logManager.getCurrentInternalWriter();
         assertNotNull("Writer should not be null", writer1);
         assertNotNull("Writer should not be null", internalWriter1);
         logManager.close();
         assertTrue("Writer should be closed", logManager.getClosedWriters().contains(writer1));
         // Verify close was called on the old internal writer
         verify(internalWriter1, times(1)).close();
         // Verify further calls fail
         try {
             logManager.getWriter();
             fail("Should have thrown IOException after close");
         } catch (IOException e) {
             // expected
             assertTrue(e.getMessage().contains("Closed"));
         }
    }

    // Test subclass to override writer creation and allow access to internal state
    public static class TestableReplicationLogManager extends ReplicationLogManager {

        private final FileSystem mockFs;
        private final Set<LogFile.Writer> createdWriters = new HashSet<>();
        private final Set<LogFile.Writer> closedWriters = new HashSet<>();

        public TestableReplicationLogManager(Configuration conf, RegionServerServices rsServices,
                FileSystem mockFs) {
            super(conf, rsServices);
            this.mockFs = mockFs;
        }

        @Override
        protected LogFile.Writer createNewWriter(FileSystem fs, URI url) throws IOException {
            // Return a mock writer instead of creating a real one
            LogFile.Writer mockWriter = mock(LogFile.Writer.class);
            // Configure basic behavior needed for tests
            when(mockWriter.getLength()).thenReturn(0L); // Default length is 0
            createdWriters.add(mockWriter);
            return mockWriter;
        }

        @Override
        protected void closeWriter(LogFile.Writer writer) {
            if (writer == null) {
                return;
            }
            // Track closed writers
            closedWriters.add(writer);
            try {
                // Still call close for verification purposes
                writer.close();
            } catch (IOException e) {
                // Ignore in test
            }
        }

        // Expose internal state for testing
        long getLastRotationTime() {
            return lastRotationTime;
        }

        long getWriterGeneration() {
            return writerGeneration;
        }

        LogFile.Writer getCurrentInternalWriter() {
            ReplicationLogManager.Writer writer = (ReplicationLogManager.Writer) currentWriter;
            if (writer == null) {
                return null;
            }
            return writer.delegate;
        }

        Collection<LogFile.Writer> getCreatedWriters() {
            return createdWriters;
        }

        Collection<LogFile.Writer> getClosedWriters() {
            return closedWriters;
        }

    }

}
