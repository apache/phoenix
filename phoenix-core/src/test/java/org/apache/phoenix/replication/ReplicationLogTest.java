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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.logging.Log4jUtils;
import org.apache.phoenix.replication.log.LogFileTestUtil;
import org.apache.phoenix.replication.log.LogFileWriter;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class ReplicationLogTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private Configuration conf;
    private ServerName serverName;
    private FileSystem localFs;
    private URI standbyUri;
    private ReplicationLog logWriter;

    static final int TEST_RINGBUFFER_SIZE = 32;
    static final int TEST_SYNC_TIMEOUT = 1000;
    static final int TEST_ROTATION_TIME = 5000;
    static final int TEST_ROTATION_SIZE_BYTES = 10 * 1024;

    static {
        Log4jUtils.setLogLevel("org.apache.phoenix.replication.ReplicationLog", "TRACE");
    }

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        localFs = FileSystem.getLocal(conf);
        standbyUri = new Path(testFolder.toString()).toUri();
        serverName = ServerName.valueOf("test", 60010, EnvironmentEdgeManager.currentTimeMillis());
        conf.set(ReplicationLog.REPLICATION_STANDBY_HDFS_URL_KEY, standbyUri.toString());
        // Small ring buffer size for testing
        conf.setInt(ReplicationLog.REPLICATION_LOG_RINGBUFFER_SIZE_KEY, TEST_RINGBUFFER_SIZE);
        // Set a short sync timeout for testing
        conf.setLong(ReplicationLog.REPLICATION_LOG_SYNC_TIMEOUT_KEY, TEST_SYNC_TIMEOUT);
        // Set rotation time to 10 seconds
        conf.setLong(ReplicationLog.REPLICATION_LOG_ROTATION_TIME_MS_KEY, TEST_ROTATION_TIME);
        // Small size threshold for testing
        conf.setLong(ReplicationLog.REPLICATION_LOG_ROTATION_SIZE_BYTES_KEY, TEST_ROTATION_SIZE_BYTES);

        logWriter = spy(new TestableReplicationLogWriter(conf, serverName));
        logWriter.init();
    }

    @After
    public void tearDown() throws IllegalArgumentException, IOException {
        if (logWriter != null) {
            logWriter.close();
        }
        localFs.delete(new Path(standbyUri.getPath()), true);
    }

    @Test
    public void testAppendAndSync() throws Exception {
        final String tableName = "TESTTBL";
        final long commitId1 = 1L;
        final long commitId2 = 2L;
        final long commitId3 = 3L;
        final long commitId4 = 4L;
        final long commitId5 = 5L;
        final Mutation put1 = LogFileTestUtil.newPut("row1", 1, 1);
        final Mutation put2 = LogFileTestUtil.newPut("row2", 2, 1);
        final Mutation put3 = LogFileTestUtil.newPut("row3", 3, 1);
        final Mutation put4 = LogFileTestUtil.newPut("row4", 4, 1);
        final Mutation put5 = LogFileTestUtil.newPut("row5", 5, 1);

        // Get the inner writer
        LogFileWriter writer = logWriter.getWriter();
        assertNotNull("Writer should not be null", writer);
        InOrder inOrder = Mockito.inOrder(writer);

        logWriter.append(tableName, commitId1, put1);
        logWriter.append(tableName, commitId2, put2);
        logWriter.append(tableName, commitId3, put3);
        logWriter.append(tableName, commitId4, put4);
        logWriter.append(tableName, commitId5, put5);

        logWriter.sync();

        // Happens-before ordering verification, using Mockito's inOrder. Verify that the appends
        // happen before sync, and sync happened after appends.
        inOrder.verify(writer, times(1)).append(eq(tableName), eq(commitId1), eq(put1));
        inOrder.verify(writer, times(1)).append(eq(tableName), eq(commitId2), eq(put2));
        inOrder.verify(writer, times(1)).append(eq(tableName), eq(commitId3), eq(put3));
        inOrder.verify(writer, times(1)).append(eq(tableName), eq(commitId4), eq(put4));
        inOrder.verify(writer, times(1)).append(eq(tableName), eq(commitId5), eq(put5));
        inOrder.verify(writer, times(1)).sync();
    }

    @Test
    public void testAppendFailureAndRetry() throws Exception {
        final String tableName = "TBLAFR";
        final long commitId = 1L;
        final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

        // Get the inner writer
        LogFileWriter writerBeforeRoll = logWriter.getWriter();
        assertNotNull("Initial writer should not be null", writerBeforeRoll);

        // Configure writerBeforeRoll to fail on the first append call
        doThrow(new IOException("Simulated append failure"))
            .when(writerBeforeRoll).append(anyString(), anyLong(), any(Mutation.class));

        // Append data
        logWriter.append(tableName, commitId, put);
        logWriter.sync();

        // Get the inner writer we rolled to.
        LogFileWriter writerAfterRoll = logWriter.getWriter();
        assertNotNull("Initial writer should not be null", writerBeforeRoll);

        // Verify the sequence: append (fail), rotate, append (succeed), sync
        InOrder inOrder = Mockito.inOrder(writerBeforeRoll, writerAfterRoll);
        inOrder.verify(writerBeforeRoll, times(1)).append(eq(tableName), eq(commitId), eq(put));
        inOrder.verify(writerBeforeRoll, times(0)).sync(); // We failed append, did not try
        inOrder.verify(writerAfterRoll, times(1))
            .append(eq(tableName), eq(commitId), eq(put)); // Retry
        inOrder.verify(writerAfterRoll, times(1)).sync();
    }

    @Test
    public void testSyncFailureAndRetry() throws Exception {
      final String tableName = "TBLSFR";
      final long commitId = 1L;
      final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

      // Get the inner writer
      LogFileWriter writerBeforeRoll = logWriter.getWriter();
      assertNotNull("Initial writer should not be null", writerBeforeRoll);

      // Configure writerBeforeRoll to fail on the first sync call
      doThrow(new IOException("Simulated sync failure")).when(writerBeforeRoll).sync();

      // Append data
      logWriter.append(tableName, commitId, put);
      logWriter.sync();

      // Get the inner writer we rolled to.
      LogFileWriter writerAfterRoll = logWriter.getWriter();
      assertNotNull("Initial writer should not be null", writerBeforeRoll);

      // Verify the sequence: append, sync (fail), rotate, append (retry), sync (succeed)
      InOrder inOrder = Mockito.inOrder(writerBeforeRoll, writerAfterRoll);
      inOrder.verify(writerBeforeRoll, times(1)).append(eq(tableName), eq(commitId), eq(put));
      inOrder.verify(writerBeforeRoll, times(1)).sync(); // Failed
      inOrder.verify(writerAfterRoll, times(1))
          .append(eq(tableName), eq(commitId), eq(put)); // Replay
      inOrder.verify(writerAfterRoll, times(1)).sync(); // Succeeded
    }

    @Test
    public void testBlockingWhenRingFull() throws Exception {
        final String tableName = "TBLBWRF";
        final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
        long commitId = 0;

        // Get the inner writer
        LogFileWriter innerWriter = logWriter.getWriter();
        assertNotNull("Inner writer should not be null", innerWriter);

        // Create a slow consumer to fill up the ring buffer.
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(50); // Simulate slow processing
                return invocation.callRealMethod();
            }
        }).when(innerWriter).append(anyString(), anyLong(), any(Mutation.class));

        // Fill up the ring buffer by sending enough events.
        for (int i = 0; i < TEST_RINGBUFFER_SIZE; i++) {
            logWriter.append(tableName, commitId++, put);
        }

        // Now try to append when the ring is full. This should block until space becomes
        // available.
        long myCommitId = commitId++;
        CompletableFuture<Void> startFuture = new CompletableFuture<>();
        CompletableFuture<Void> appendFuture = new CompletableFuture<>();
        Thread appendThread = new Thread(() -> {
            try {
                startFuture.complete(null);
                logWriter.append(tableName, myCommitId, put);
                appendFuture.complete(null);
            } catch (IOException e) {
                appendFuture.completeExceptionally(e);
            }
        });
        appendThread.start();

        // Wait for the append thread.
        startFuture.get();

        // Verify the append is still blocked
        assertFalse("Append should be blocked when ring is full", appendFuture.isDone());

        // Let some events process to free up space.
        Thread.sleep(100);

        // Now the append should complete. Any issues and we will time out here.
        appendFuture.get();
        assertTrue("Append should have completed", appendFuture.isDone());

        // Verify the append eventually happens on the writer.
        verify(innerWriter, timeout(10000).times(1)).append(eq(tableName), eq(myCommitId), any());
    }

    @Test
    public void testSyncTimeout() throws Exception {
        final String tableName = "TBLST";
        final long commitId = 1L;
        final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

        // Get the inner writer
        LogFileWriter innerWriter = logWriter.getWriter();
        assertNotNull("Inner writer should not be null", innerWriter);

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                // Pause long enough to cause a timeout.
                Thread.sleep((long)(TEST_SYNC_TIMEOUT * 1.25));
                return invocation.callRealMethod();
            }
        }).when(innerWriter).sync();

        // Append some data
        logWriter.append(tableName, commitId, put);

        // Try to sync and expect it to timeout
        try {
            logWriter.sync();
            fail("Expected sync to timeout");
        } catch (IOException e) {
            assertTrue("Expected timeout exception", e.getCause() instanceof TimeoutException);
        }
    }

    @Test
    public void testConcurrentProducers() throws Exception {
        final String tableName = "TBLCP";
        final int APPENDS_PER_THREAD = 1000;
        // Create a latch to coordinate thread starts
        final CountDownLatch startLatch = new CountDownLatch(1);
        // Create a latch to track completion of all appends
        final CountDownLatch completionLatch = new CountDownLatch(2);

        // Get the inner writer
        LogFileWriter innerWriter = logWriter.getWriter();
        assertNotNull("Inner writer should not be null", innerWriter);

        // Thread 1: Append mutations with even commit IDs
        Thread producerEven = new Thread(() -> {
            try {
                startLatch.await(); // Wait for start signal
                for (int i = 0; i < APPENDS_PER_THREAD; i++) {
                    final long commitId = i * 2;
                    final Mutation put = LogFileTestUtil.newPut("row" + commitId, commitId, 1);
                    logWriter.append(tableName, commitId, put);
                }
            } catch (Exception e) {
                fail("Producer 1 failed: " + e.getMessage());
            } finally {
                completionLatch.countDown();
            }
        });

        // Thread 2: Append mutations with odd commit IDs
        Thread producerOdd = new Thread(() -> {
            try {
                startLatch.await(); // Wait for start signal
                for (int i = 0; i < APPENDS_PER_THREAD; i++) {
                    final long commitId = i * 2 + 1;
                    final Mutation put = LogFileTestUtil.newPut("row" + commitId, commitId, 1);
                    logWriter.append(tableName, commitId, put);
                }
            } catch (Exception e) {
                fail("Producer 2 failed: " + e.getMessage());
            } finally {
                completionLatch.countDown();
            }
        });

        // Start both threads.
        producerEven.start();
        producerOdd.start();
        // Signal threads to start.
        startLatch.countDown();
        // Wait for all appends to complete
        completionLatch.await();

        // Perform a sync to ensure all appends are processed.
        InOrder inOrder = Mockito.inOrder(innerWriter); // To verify the below sync.
        logWriter.sync();

        // Verify that all of appends were processed by the internal writer.
        for (int i = 0; i < APPENDS_PER_THREAD * 2; i++) {
            final long commitId = i;
            verify(innerWriter, times(1)).append(eq(tableName), eq(commitId), any());
        }

        // Verify the final sync was called.
        inOrder.verify(innerWriter, times(1)).sync();
    }

    @Test
    public void testTimeBasedRotation() throws Exception {
        final String tableName = "TBLTBR";
        final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
        final long commitId = 1L;

        // Get the initial writer
        LogFileWriter writerBeforeRotation = logWriter.getWriter();
        assertNotNull("Initial writer should not be null", writerBeforeRotation);

        // Append some data
        logWriter.append(tableName, commitId, put);
        logWriter.sync();

        // Wait for rotation time to elapse
        Thread.sleep((long)(TEST_ROTATION_TIME * 1.25));

        // Append more data to trigger rotation check
        logWriter.append(tableName, commitId + 1, put);
        logWriter.sync();

        // Get the new writer after rotation
        LogFileWriter writerAfterRotation = logWriter.getWriter();
        assertNotNull("New writer should not be null", writerAfterRotation);
        assertTrue("Writer should have been rotated", writerAfterRotation != writerBeforeRotation);

        // Verify the sequence of operations
        InOrder inOrder = Mockito.inOrder(writerBeforeRotation, writerAfterRotation);
        inOrder.verify(writerBeforeRotation, times(1))
            .append(eq(tableName), eq(commitId), eq(put));     // First append to initial writer
        inOrder.verify(writerBeforeRotation, times(1)).sync();
        inOrder.verify(writerAfterRotation, times(0))
            .append(eq(tableName), eq(commitId), eq(put));     // First append is not replayed
        inOrder.verify(writerAfterRotation, times(1))
            .append(eq(tableName), eq(commitId + 1), eq(put)); // Second append to new writer
        inOrder.verify(writerAfterRotation, times(1)).sync();
    }

    @Test
    public void testSizeBasedRotation() throws Exception {
        final String tableName = "TBLSBR";
        final Mutation put = LogFileTestUtil.newPut("row", 1, 10);
        long commitId = 1L;

        LogFileWriter writerBeforeRotation = logWriter.getWriter();
        assertNotNull("Initial writer should not be null", writerBeforeRotation);

        // Append enough data so that we exceed the size threshold.
        for (int i = 0; i < 100; i++) {
            logWriter.append(tableName, commitId++, put);
        }
        logWriter.sync(); // Should trigger a sized based rotation

        // Get the new writer after the expected rotation.
        LogFileWriter writerAfterRotation = logWriter.getWriter();
        assertNotNull("New writer should not be null", writerAfterRotation);
        assertTrue("Writer should have been rotated", writerAfterRotation != writerBeforeRotation);

        // Append one more mutation to verify we're using the new writer.
        logWriter.append(tableName, commitId, put);
        logWriter.sync();

        // Verify the sequence of operations
        InOrder inOrder = Mockito.inOrder(writerBeforeRotation, writerAfterRotation);
        
        // Verify all appends before rotation went to the first writer.
        for (int i = 1; i < commitId; i++) {
            inOrder.verify(writerBeforeRotation, times(1))
                .append(eq(tableName), eq((long)i), eq(put));
        }
        inOrder.verify(writerBeforeRotation, times(1)).sync();

        // Verify the final append went to the new writer.
        inOrder.verify(writerAfterRotation, times(1))
            .append(eq(tableName), eq(commitId), eq(put));
        inOrder.verify(writerAfterRotation, times(1)).sync();
    }

    @Test
    public void testClose() throws Exception {
        final String tableName = "TBLCLOSE";
        final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
        final long commitId = 1L;

        // Get the inner writer
        LogFileWriter innerWriter = logWriter.getWriter();
        assertNotNull("Inner writer should not be null", innerWriter);

        // Append some data
        logWriter.append(tableName, commitId, put);

        // Close the log writer
        logWriter.close();

        // Verify the inner writer was closed
        verify(innerWriter, times(1)).close();

        // Verify we can't append after close
        try {
            logWriter.append(tableName, commitId + 1, put);
            fail("Expected append to fail after close");
        } catch (IOException e) {
            // Expected
        }

        // Verify we can't sync after close
        try {
            logWriter.sync();
            fail("Expected sync to fail after close");
        } catch (IOException e) {
            // Expected
        }

        // Verify we can close multiple times without error
        logWriter.close();
    }

    @Test
    public void testRotationTask() throws Exception {
        final String tableName = "TBLRT";
        final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
        long commitId = 1L;

        LogFileWriter writerBeforeRotation = logWriter.getWriter();
        assertNotNull("Initial writer should not be null", writerBeforeRotation);

        // Append some data and wait for the rotation time to elapse plus a small buffer.
        logWriter.append(tableName, commitId, put);
        logWriter.sync();
        Thread.sleep((long)(TEST_ROTATION_TIME * 1.25));

        // Get the new writer after the rotation.
        LogFileWriter writerAfterRotation = logWriter.getWriter();
        assertNotNull("New writer should not be null", writerAfterRotation);
        assertTrue("Writer should have been rotated", writerAfterRotation != writerBeforeRotation);

        // Verify first append and sync went to initial writer
        verify(writerBeforeRotation, times(1)).append(eq(tableName), eq(1L), eq(put));
        verify(writerBeforeRotation, times(1)).sync();
        // Verify the initial writer was closed
        verify(writerBeforeRotation, times(1)).close();
    }

    @Test
    public void testFailedRotation() throws Exception {
        final String tableName = "TBLFR";
        final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
        long commitId = 1L;

        // Get the initial writer
        LogFileWriter initialWriter = logWriter.getWriter();
        assertNotNull("Initial writer should not be null", initialWriter);

        // Append some data
        logWriter.append(tableName, commitId, put);
        logWriter.sync();

        // Now configure the log writer to fail when creating new writers
        doThrow(new IOException("Simulated failure to create new writer"))
            .when(logWriter).createNewWriter(any(FileSystem.class), any(URI.class));

        // Wait for rotation time to elapse plus a small buffer
        Thread.sleep((long)(TEST_ROTATION_TIME * 1.25));

        // Try to append more data - this should still work with the original writer
        logWriter.append(tableName, commitId + 1, put);
        logWriter.sync();

        // Verify we're still using the original writer
        LogFileWriter currentWriter = logWriter.getWriter();
        assertTrue("Should still be using original writer", currentWriter == initialWriter);

        // Verify all operations went to the original writer
        verify(initialWriter, times(1)).append(eq(tableName), eq(commitId), eq(put));
        verify(initialWriter, times(1)).append(eq(tableName), eq(commitId + 1), eq(put));
        verify(initialWriter, times(2)).sync();
    }

    @Test
    public void testEventProcessingException() throws Exception {
        final String tableName = "TBLEPE";
        final long commitId = 1L;
        final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

        // Get the inner writer
        LogFileWriter innerWriter = logWriter.getWriter();
        assertNotNull("Writer should not be null", innerWriter);

        // Configure writer to throw a RuntimeException on append
        doThrow(new RuntimeException("Simulated critical error"))
            .when(innerWriter).append(anyString(), anyLong(), any(Mutation.class));

        // Append data. This should trigger the LogExceptionHandler, which will close logWriter.
        logWriter.append(tableName, commitId, put);
        try {
            logWriter.sync();
            fail("Should have thrown IOException because sync timed out");
        } catch (IOException e) {
            assertTrue("Expected timeout exception", e.getCause() instanceof TimeoutException);
        }

        // Verify that subsequent operations fail because the log is closed
        try {
            logWriter.append(tableName, commitId + 1, put);
            fail("Should have thrown IOException because log is closed");
        } catch (IOException e) {
          assertTrue("Expected an IOException because log is closed",
              e.getMessage().contains("Closed"));
        }

        // Verify that the inner writer was closed by the LogExceptionHandler
        verify(innerWriter, times(1)).close();
    }

    @Test
    public void testSyncFailureAllRetriesExhausted() throws Exception {
        final String tableName = "TBLSAFR";
        final long commitId = 1L;
        final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

        // Get the initial writer
        LogFileWriter initialWriter = logWriter.getWriter();
        assertNotNull("Initial writer should not be null", initialWriter);

        // Configure initial writer to fail on sync
        doThrow(new IOException("Simulated sync failure"))
            .when(initialWriter).sync();

        // createNewWriter should keep returning the bad writer
        doAnswer(invocation -> initialWriter).when(logWriter)
            .createNewWriter(any(FileSystem.class), any(URI.class));

        // Append data
        logWriter.append(tableName, commitId, put);

        // Try to sync. Should fail after exhausting retries.
        try {
            logWriter.sync();
            fail("Expected sync to fail after exhausting retries");
        } catch (IOException e) {
            assertTrue("Expected timeout exception", e.getCause() instanceof TimeoutException);
        }

        // Each retry creates a new writer, so that is 1 + 5 retries.
        verify(logWriter, times(6)).createNewWriter(any(FileSystem.class), any(URI.class));
    }

    static class TestableReplicationLogWriter extends ReplicationLog {

        protected TestableReplicationLogWriter(Configuration conf, ServerName serverName) {
            super(conf, serverName);
        }

        @Override
        protected LogFileWriter createNewWriter(FileSystem fs,
                URI url) throws IOException {
            return spy(super.createNewWriter(fs, url));
        }

    }

}
