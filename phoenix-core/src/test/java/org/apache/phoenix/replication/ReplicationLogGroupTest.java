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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.replication.ReplicationLogGroupWriter.RotationReason;
import org.apache.phoenix.replication.log.LogFile;
import org.apache.phoenix.replication.log.LogFileReader;
import org.apache.phoenix.replication.log.LogFileReaderContext;
import org.apache.phoenix.replication.log.LogFileTestUtil;
import org.apache.phoenix.replication.log.LogFileWriter;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationLogGroupTest {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogGroupTest.class);

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  private Configuration conf;
  private ServerName serverName;
  private FileSystem localFs;
  private URI standbyUri;
  private ReplicationLogGroup logGroup;

  static final int TEST_RINGBUFFER_SIZE = 32;
  static final int TEST_SYNC_TIMEOUT = 1000;
  static final int TEST_ROTATION_TIME = 5000;
  static final int TEST_ROTATION_SIZE_BYTES = 10 * 1024;

  @Before
  public void setUp() throws IOException {
    conf = HBaseConfiguration.create();
    localFs = FileSystem.getLocal(conf);
    standbyUri = new Path(testFolder.toString()).toUri();
    serverName = ServerName.valueOf("test", 60010, EnvironmentEdgeManager.currentTimeMillis());
    conf.set(ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY, standbyUri.toString());
    // Small ring buffer size for testing
    conf.setInt(ReplicationLogGroup.REPLICATION_LOG_RINGBUFFER_SIZE_KEY, TEST_RINGBUFFER_SIZE);
    // Set a short sync timeout for testing
    conf.setLong(ReplicationLogGroup.REPLICATION_LOG_SYNC_TIMEOUT_KEY, TEST_SYNC_TIMEOUT);
    // Set rotation time to 10 seconds
    conf.setLong(ReplicationLogGroup.REPLICATION_LOG_ROTATION_TIME_MS_KEY, TEST_ROTATION_TIME);
    // Small size threshold for testing
    conf.setLong(ReplicationLogGroup.REPLICATION_LOG_ROTATION_SIZE_BYTES_KEY,
      TEST_ROTATION_SIZE_BYTES);

    logGroup = new TestableLogGroup(conf, serverName, "testHAGroup");
    logGroup.init();
  }

  @After
  public void tearDown() throws Exception {
    if (logGroup != null) {
      logGroup.close();
    }
  }

  /**
   * Tests basic append and sync functionality of the replication log. Verifies that mutations are
   * correctly appended to the log and that sync operations properly commit the changes to disk.
   */
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
    LogFileWriter writer = logGroup.getActiveWriter().getWriter();
    assertNotNull("Writer should not be null", writer);
    InOrder inOrder = Mockito.inOrder(writer);

    logGroup.append(tableName, commitId1, put1);
    logGroup.append(tableName, commitId2, put2);
    logGroup.append(tableName, commitId3, put3);
    logGroup.append(tableName, commitId4, put4);
    logGroup.append(tableName, commitId5, put5);

    logGroup.sync();

    // Happens-before ordering verification, using Mockito's inOrder. Verify that the appends
    // happen before sync, and sync happened after appends.
    inOrder.verify(writer, times(1)).append(eq(tableName), eq(commitId1), eq(put1));
    inOrder.verify(writer, times(1)).append(eq(tableName), eq(commitId2), eq(put2));
    inOrder.verify(writer, times(1)).append(eq(tableName), eq(commitId3), eq(put3));
    inOrder.verify(writer, times(1)).append(eq(tableName), eq(commitId4), eq(put4));
    inOrder.verify(writer, times(1)).append(eq(tableName), eq(commitId5), eq(put5));
    inOrder.verify(writer, times(1)).sync();
  }

  /**
   * Tests the behavior when a sync operation fails. Verifies that the system properly handles sync
   * failures by rolling to a new writer and retrying the operation.
   */
  @Test
  public void testSyncFailureAndRetry() throws Exception {
    final String tableName = "TBLSFR";
    final long commitId = 1L;
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

    // Get the inner writer
    LogFileWriter writerBeforeRoll = logGroup.getActiveWriter().getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRoll);

    // Configure writerBeforeRoll to fail on the first sync call
    doThrow(new IOException("Simulated sync failure")).when(writerBeforeRoll).sync();

    // Append data
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Get the inner writer we rolled to.
    LogFileWriter writerAfterRoll = logGroup.getActiveWriter().getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRoll);

    // Verify the sequence: append, sync (fail), rotate, append (retry), sync (succeed)
    InOrder inOrder = Mockito.inOrder(writerBeforeRoll, writerAfterRoll);
    inOrder.verify(writerBeforeRoll, times(1)).append(eq(tableName), eq(commitId), eq(put));
    inOrder.verify(writerBeforeRoll, times(1)).sync(); // Failed
    inOrder.verify(writerAfterRoll, times(1)).append(eq(tableName), eq(commitId), eq(put)); // Replay
    inOrder.verify(writerAfterRoll, times(1)).sync(); // Succeeded
  }

  /**
   * Tests the blocking behavior when the ring buffer is full. Verifies that append operations block
   * when the ring buffer is full and resume as soon as space becomes available again.
   */
  @Test
  public void testBlockingWhenRingFull() throws Exception {
    final String tableName = "TBLBWRF";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    long commitId = 0;

    // Get the inner writer
    LogFileWriter innerWriter = logGroup.getActiveWriter().getWriter();
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
      logGroup.append(tableName, commitId++, put);
    }

    // Now try to append when the ring is full. This should block until space becomes
    // available.
    long myCommitId = commitId++;
    CompletableFuture<Void> startFuture = new CompletableFuture<>();
    CompletableFuture<Void> appendFuture = new CompletableFuture<>();
    Thread appendThread = new Thread(() -> {
      try {
        startFuture.complete(null);
        logGroup.append(tableName, myCommitId, put);
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

  /**
   * Tests the behavior when an append operation fails. Verifies that the system properly handles
   * append failures by rolling to a new writer and retrying the operation.
   */
  @Test
  public void testAppendFailureAndRetry() throws Exception {
    final String tableName = "TBLAFR";
    final long commitId = 1L;
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

    // Get the inner writer
    LogFileWriter writerBeforeRoll = logGroup.getActiveWriter().getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRoll);

    // Configure writerBeforeRoll to fail on the first append call
    doThrow(new IOException("Simulated append failure")).when(writerBeforeRoll).append(anyString(),
      anyLong(), any(Mutation.class));

    // Append data
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Get the inner writer we rolled to.
    LogFileWriter writerAfterRoll = logGroup.getActiveWriter().getWriter();
    assertNotNull("Rolled writer should not be null", writerAfterRoll);

    // Verify the sequence: append (fail), rotate, append (succeed), sync
    InOrder inOrder = Mockito.inOrder(writerBeforeRoll, writerAfterRoll);
    inOrder.verify(writerBeforeRoll, times(1)).append(eq(tableName), eq(commitId), eq(put));
    inOrder.verify(writerBeforeRoll, times(0)).sync(); // We failed append, did not try
    inOrder.verify(writerAfterRoll, times(1)).append(eq(tableName), eq(commitId), eq(put)); // Retry
    inOrder.verify(writerAfterRoll, times(1)).sync();
  }

  /**
   * Tests the sync timeout behavior. Verifies that sync operations time out after the configured
   * interval if they cannot complete.
   */
  @Test
  public void testSyncTimeout() throws Exception {
    final String tableName = "TBLST";
    final long commitId = 1L;
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

    // Get the inner writer
    LogFileWriter innerWriter = logGroup.getActiveWriter().getWriter();
    assertNotNull("Inner writer should not be null", innerWriter);

    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        // Pause long enough to cause a timeout.
        Thread.sleep((long) (TEST_SYNC_TIMEOUT * 1.25));
        return invocation.callRealMethod();
      }
    }).when(innerWriter).sync();

    // Append some data
    logGroup.append(tableName, commitId, put);

    // Try to sync and expect it to timeout
    try {
      logGroup.sync();
      fail("Expected sync to timeout");
    } catch (IOException e) {
      assertTrue("Expected timeout exception", e.getCause() instanceof TimeoutException);
    }
  }

  /**
   * Tests concurrent append operations from multiple producers. Verifies that the system correctly
   * handles concurrent appends from multiple threads and maintains data consistency.
   */
  @Test
  public void testConcurrentProducers() throws Exception {
    final String tableName = "TBLCP";
    final int APPENDS_PER_THREAD = 1000;
    // Create a latch to coordinate thread starts
    final CountDownLatch startLatch = new CountDownLatch(1);
    // Create a latch to track completion of all appends
    final CountDownLatch completionLatch = new CountDownLatch(2);

    // Get the inner writer
    LogFileWriter innerWriter = logGroup.getActiveWriter().getWriter();
    assertNotNull("Inner writer should not be null", innerWriter);

    // Thread 1: Append mutations with even commit IDs
    Thread producerEven = new Thread(() -> {
      try {
        startLatch.await(); // Wait for start signal
        for (int i = 0; i < APPENDS_PER_THREAD; i++) {
          final long commitId = i * 2;
          final Mutation put = LogFileTestUtil.newPut("row" + commitId, commitId, 1);
          logGroup.append(tableName, commitId, put);
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
          logGroup.append(tableName, commitId, put);
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
    logGroup.sync();
    // Verify the final sync was called.
    inOrder.verify(innerWriter, times(1)).sync();

    // Verify that all of appends were processed by the internal writer.
    for (int i = 0; i < APPENDS_PER_THREAD * 2; i++) {
      final long commitId = i;
      verify(innerWriter, times(1)).append(eq(tableName), eq(commitId), any());
    }
  }

  /**
   * Tests time-based log rotation. Verifies that the log file is rotated after the configured
   * rotation time period and that operations continue correctly with the new log file.
   */
  @Test
  public void testTimeBasedRotation() throws Exception {
    final String tableName = "TBLTBR";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    final long commitId = 1L;

    // Get the initial writer
    LogFileWriter writerBeforeRotation = logGroup.getActiveWriter().getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRotation);

    // Append some data
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Wait for rotation time to elapse
    Thread.sleep((long) (TEST_ROTATION_TIME * 1.25));

    // Append more data to trigger rotation check
    logGroup.append(tableName, commitId + 1, put);
    logGroup.sync();

    // Get the new writer after rotation
    LogFileWriter writerAfterRotation = logGroup.getActiveWriter().getWriter();
    assertNotNull("New writer should not be null", writerAfterRotation);
    assertTrue("Writer should have been rotated", writerAfterRotation != writerBeforeRotation);

    // Verify the sequence of operations
    InOrder inOrder = Mockito.inOrder(writerBeforeRotation, writerAfterRotation);
    inOrder.verify(writerBeforeRotation, times(1)).append(eq(tableName), eq(commitId), eq(put)); // First
                                                                                                 // append
                                                                                                 // to
                                                                                                 // initial
                                                                                                 // writer
    inOrder.verify(writerBeforeRotation, times(1)).sync();
    inOrder.verify(writerAfterRotation, times(0)).append(eq(tableName), eq(commitId), eq(put)); // First
                                                                                                // append
                                                                                                // is
                                                                                                // not
                                                                                                // replayed
    inOrder.verify(writerAfterRotation, times(1)).append(eq(tableName), eq(commitId + 1), eq(put)); // Second
                                                                                                    // append
                                                                                                    // to
                                                                                                    // new
                                                                                                    // writer
    inOrder.verify(writerAfterRotation, times(1)).sync();
  }

  /**
   * Tests size-based log rotation. Verifies that the log file is rotated when it exceeds the
   * configured size threshold and that operations continue correctly with the new log file.
   */
  @Test
  public void testSizeBasedRotation() throws Exception {
    final String tableName = "TBLSBR";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 10);
    long commitId = 1L;

    LogFileWriter writerBeforeRotation = logGroup.getActiveWriter().getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRotation);

    // Append enough data so that we exceed the size threshold.
    for (int i = 0; i < 100; i++) {
      logGroup.append(tableName, commitId++, put);
    }
    logGroup.sync(); // Should trigger a sized based rotation

    // Get the new writer after the expected rotation.
    LogFileWriter writerAfterRotation = logGroup.getActiveWriter().getWriter();
    assertNotNull("New writer should not be null", writerAfterRotation);
    assertTrue("Writer should have been rotated", writerAfterRotation != writerBeforeRotation);

    // Append one more mutation to verify we're using the new writer.
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Verify the sequence of operations
    InOrder inOrder = Mockito.inOrder(writerBeforeRotation, writerAfterRotation);
    // Verify all appends before rotation went to the first writer.
    for (int i = 1; i < commitId; i++) {
      inOrder.verify(writerBeforeRotation, times(1)).append(eq(tableName), eq((long) i), eq(put));
    }
    inOrder.verify(writerBeforeRotation, times(1)).sync();
    // Verify the final append went to the new writer.
    inOrder.verify(writerAfterRotation, times(1)).append(eq(tableName), eq(commitId), eq(put));
    inOrder.verify(writerAfterRotation, times(1)).sync();
  }

  /**
   * Tests the close operation of the replication log. Verifies that the log properly closes its
   * resources and prevents further operations after being closed.
   */
  @Test
  public void testClose() throws Exception {
    final String tableName = "TBLCLOSE";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    final long commitId = 1L;

    // Get the inner writer
    LogFileWriter innerWriter = logGroup.getActiveWriter().getWriter();
    assertNotNull("Inner writer should not be null", innerWriter);

    // Append some data
    logGroup.append(tableName, commitId, put);

    // Close the log writer
    logGroup.close();

    // Verify the inner writer was closed
    verify(innerWriter, times(1)).close();

    // Verify we can't append after close
    try {
      logGroup.append(tableName, commitId + 1, put);
      fail("Expected append to fail after close");
    } catch (IOException e) {
      // Expected
    }

    // Verify we can't sync after close
    try {
      logGroup.sync();
      fail("Expected sync to fail after close");
    } catch (IOException e) {
      // Expected
    }

    // Verify we can close multiple times without error
    logGroup.close();
  }

  /**
   * Tests the automatic rotation task. Verifies that the background rotation task correctly rotates
   * log files based on the configured rotation time.
   */
  @Test
  public void testRotationTask() throws Exception {
    final String tableName = "TBLRT";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    long commitId = 1L;

    LogFileWriter writerBeforeRotation = logGroup.getActiveWriter().getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRotation);

    // Append some data and wait for the rotation time to elapse plus a small buffer.
    logGroup.append(tableName, commitId, put);
    logGroup.sync();
    Thread.sleep((long) (TEST_ROTATION_TIME * 1.25));

    // Get the new writer after the rotation.
    LogFileWriter writerAfterRotation = logGroup.getActiveWriter().getWriter();
    assertNotNull("New writer should not be null", writerAfterRotation);
    assertTrue("Writer should have been rotated", writerAfterRotation != writerBeforeRotation);

    // Verify first append and sync went to initial writer
    verify(writerBeforeRotation, times(1)).append(eq(tableName), eq(1L), eq(put));
    verify(writerBeforeRotation, times(1)).sync();
    // Verify the initial writer was closed
    verify(writerBeforeRotation, times(1)).close();
  }

  /**
   * Tests behavior when log rotation fails temporarily but eventually succeeds. Verifies that:
   * <ul>
   * <li>The system can handle temporary rotation failures</li>
   * <li>After failing twice, the third rotation attempt succeeds</li>
   * <li>Operations continue correctly with the new writer after successful rotation</li>
   * <li>The metrics for rotation failures are properly tracked</li>
   * <li>Operations can continue with the current writer while rotation attempts are failing</li>
   * </ul>
   * <p>
   * This test simulates a scenario where the first two rotation attempts fail (e.g., due to
   * temporary HDFS issues) but the third attempt succeeds. This is a common real-world scenario
   * where transient failures occur but the system eventually recovers. During the failed rotation
   * attempts, the system should continue to operate normally with the current writer.
   */
  @Test
  public void testFailedRotation() throws Exception {
    final String tableName = "TBLFR";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    long commitId = 1L;

    ReplicationLogGroupWriter logGroupWriter = logGroup.getActiveWriter();

    // Get the initial writer
    LogFileWriter initialWriter = logGroupWriter.getWriter();
    assertNotNull("Initial writer should not be null", initialWriter);

    // Configure the log writer to fail only the first time when creating new writers.
    AtomicBoolean shouldFail = new AtomicBoolean(true);
    doAnswer(invocation -> {
      if (shouldFail.getAndSet(false)) {
        throw new IOException("Simulated failure to create new writer");
      }
      return invocation.callRealMethod();
    }).when(logGroupWriter).createNewWriter();

    // Append some data
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Rotate the log.
    LogFileWriter writerAfterFailedRotate = logGroupWriter.rotateLog(RotationReason.TIME);
    assertEquals("Should still be using the initial writer", initialWriter,
      writerAfterFailedRotate);

    // While rotation is failing, verify we can continue to use the current writer.
    logGroup.append(tableName, commitId + 1, put);
    logGroup.sync();

    LogFileWriter writerAfterRotate = logGroupWriter.rotateLog(RotationReason.TIME);
    assertNotEquals("Should be using a new writer", initialWriter, writerAfterRotate);

    // Try to append more data. This should work with the new writer after successful rotation.
    logGroup.append(tableName, commitId + 2, put);
    logGroup.sync();

    // Verify operations went to the writers in the correct order
    InOrder inOrder = Mockito.inOrder(initialWriter, writerAfterRotate);
    // First append and sync on initial writer.
    inOrder.verify(initialWriter).append(eq(tableName), eq(commitId), eq(put));
    inOrder.verify(initialWriter).sync();
    // Second append and sync on initial writer after failed rotation.
    inOrder.verify(initialWriter).append(eq(tableName), eq(commitId + 1), eq(put));
    inOrder.verify(initialWriter).sync();
    // Final append and sync on new writer after successful rotation.
    inOrder.verify(writerAfterRotate).append(eq(tableName), eq(commitId + 2), eq(put));
    inOrder.verify(writerAfterRotate).sync();
  }

  /**
   * This test simulates a scenario where rotation consistently fails and verifies that the system
   * properly propagates an exception after exhausting all retry attempts.
   */
  @Test
  public void testTooManyRotationFailures() throws Exception {
    final String tableName = "TBLTMRF";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    long commitId = 1L;

    ReplicationLogGroupWriter logGroupWriter = logGroup.getActiveWriter();

    // Get the initial writer
    LogFileWriter initialWriter = logGroupWriter.getWriter();
    assertNotNull("Initial writer should not be null", initialWriter);

    // Configure the log writer to always fail when creating new writers
    doThrow(new IOException("Simulated failure to create new writer")).when(logGroupWriter)
      .createNewWriter();

    // Append some data
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Try to rotate the log multiple times until we exceed the retry limit
    for (int i = 0; i <= ReplicationLogGroup.DEFAULT_REPLICATION_LOG_ROTATION_RETRIES; i++) {
      try {
        logGroupWriter.rotateLog(RotationReason.TIME);
      } catch (IOException e) {
        if (i < ReplicationLogGroup.DEFAULT_REPLICATION_LOG_ROTATION_RETRIES) {
          // Not the last attempt yet, continue
          continue;
        }
        // This was the last attempt, verify the exception
        assertTrue("Expected IOException", e instanceof IOException);
        assertTrue("Expected our mocked failure cause",
          e.getMessage().contains("Simulated failure"));

      }
    }

    // Verify subsequent operations fail because the log is closed
    try {
      logGroup.append(tableName, commitId + 1, put);
      logGroup.sync();
      fail("Expected append to fail because log is closed");
    } catch (IOException e) {
      assertTrue("Expected an IOException because log is closed",
        e.getMessage().contains("Closed"));
    }
  }

  /**
   * Tests handling of critical exceptions during event processing. Verifies that the system
   * properly handles critical errors by closing the log and preventing further operations.
   */
  @Test
  public void testEventProcessingException() throws Exception {
    final String tableName = "TBLEPE";
    final long commitId = 1L;
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

    // Get the inner writer
    LogFileWriter innerWriter = logGroup.getActiveWriter().getWriter();
    assertNotNull("Writer should not be null", innerWriter);

    // Configure writer to throw a RuntimeException on append
    doThrow(new RuntimeException("Simulated critical error")).when(innerWriter).append(anyString(),
      anyLong(), any(Mutation.class));

    // Append data. This should trigger the LogExceptionHandler, which will close logWriter.
    logGroup.append(tableName, commitId, put);
    try {
      logGroup.sync();
      fail("Should have thrown IOException because sync timed out");
    } catch (IOException e) {
      assertTrue("Expected timeout exception", e.getCause() instanceof TimeoutException);
    }

    // Verify that subsequent operations fail because the log is closed
    try {
      logGroup.append(tableName, commitId + 1, put);
      fail("Should have thrown IOException because log is closed");
    } catch (IOException e) {
      assertTrue("Expected an IOException because log is closed",
        e.getMessage().contains("Closed"));
    }

    // Verify that the inner writer was closed by the LogExceptionHandler
    verify(innerWriter, times(1)).close();
  }

  /**
   * Tests behavior when all sync retry attempts are exhausted. Verifies that the system properly
   * handles the case where sync operations fail repeatedly and eventually timeout.
   */
  @Test
  public void testSyncFailureAllRetriesExhausted() throws Exception {
    final String tableName = "TBLSAFR";
    final long commitId = 1L;
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

    ReplicationLogGroupWriter logGroupWriter = logGroup.getActiveWriter();

    // Get the initial writer
    LogFileWriter initialWriter = logGroupWriter.getWriter();
    assertNotNull("Initial writer should not be null", initialWriter);

    // Configure initial writer to fail on sync
    doThrow(new IOException("Simulated sync failure")).when(initialWriter).sync();

    // createNewWriter should keep returning the bad writer
    doAnswer(invocation -> initialWriter).when(logGroupWriter).createNewWriter();

    // Append data
    logGroup.append(tableName, commitId, put);

    // Try to sync. Should fail after exhausting retries.
    try {
      logGroup.sync();
      fail("Expected sync to fail after exhausting retries");
    } catch (IOException e) {
      assertTrue("Expected timeout exception", e.getCause() instanceof TimeoutException);
    }

    // Each retry creates a new writer, so that is at least 1 create + 5 retries.
    verify(logGroupWriter, atLeast(6)).createNewWriter();
  }

  /**
   * Tests log rotation behavior during batch operations. Verifies that the system correctly handles
   * rotation when there are pending batch operations, ensuring no data loss.
   */
  @Test
  public void testRotationDuringBatch() throws Exception {
    final String tableName = "TBLRDB";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    long commitId = 1L;

    // Get the initial writer
    LogFileWriter writerBeforeRotation = logGroup.getActiveWriter().getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRotation);

    // Append several items to fill currentBatch but don't sync yet
    for (int i = 0; i < 5; i++) {
      logGroup.append(tableName, commitId + i, put);
    }

    // Force a rotation by waiting for rotation time to elapse
    Thread.sleep((long) (TEST_ROTATION_TIME * 1.25));

    // Get the new writer after rotation
    LogFileWriter writerAfterRotation = logGroup.getActiveWriter().getWriter();
    assertNotNull("New writer should not be null", writerAfterRotation);
    assertTrue("Writer should have been rotated", writerAfterRotation != writerBeforeRotation);

    // Now trigger a sync which should replay the currentBatch to the new writer
    logGroup.sync();

    // Verify the sequence of operations
    InOrder inOrder = Mockito.inOrder(writerBeforeRotation, writerAfterRotation);

    // Verify all appends before rotation went to the first writer
    for (int i = 0; i < 5; i++) {
      inOrder.verify(writerBeforeRotation, times(1)).append(eq(tableName), eq(commitId + i),
        eq(put));
    }

    // Verify the currentBatch was replayed to the new writer
    for (int i = 0; i < 5; i++) {
      inOrder.verify(writerAfterRotation, times(1)).append(eq(tableName), eq(commitId + i),
        eq(put));
    }

    // Verify sync happened on the new writer
    inOrder.verify(writerAfterRotation, times(1)).sync();

    // Verify the initial writer was closed
    verify(writerBeforeRotation, times(1)).close();
  }

  /**
   * Tests reading records after writing them to the log. Verifies that records written to the log
   * can be correctly read back and match the original data.
   */
  @Test
  public void testReadAfterWrite() throws Exception {
    final String tableName = "TBLRAW";
    final int NUM_RECORDS = 100;
    List<LogFile.Record> originalRecords = new ArrayList<>();

    ReplicationLogGroupWriter logGroupWriter = logGroup.getActiveWriter();

    // Get the path of the log file.
    Path logPath = logGroupWriter.getWriter().getContext().getFilePath();

    for (int i = 0; i < NUM_RECORDS; i++) {
      LogFile.Record record = LogFileTestUtil.newPutRecord(tableName, i, "row" + i, i, 1);
      originalRecords.add(record);
      logGroup.append(record.getHBaseTableName(), record.getCommitId(), record.getMutation());
    }
    logGroup.sync(); // Sync to commit the appends to the current writer.

    // Force a rotation to close the current writer.
    logGroupWriter.rotateLog(RotationReason.SIZE);

    assertTrue("Log file should exist", localFs.exists(logPath));

    // Read and verify all records
    LogFileReader reader = new LogFileReader();
    LogFileReaderContext readerContext =
      new LogFileReaderContext(conf).setFileSystem(localFs).setFilePath(logPath);
    reader.init(readerContext);

    List<LogFile.Record> readRecords = new ArrayList<>();
    LogFile.Record record;
    while ((record = reader.next()) != null) {
      readRecords.add(record);
    }

    reader.close();

    // Verify we have the expected number of records.
    assertEquals("Number of records mismatch", NUM_RECORDS, readRecords.size());

    // Verify each record matches the original.
    for (int i = 0; i < NUM_RECORDS; i++) {
      LogFileTestUtil.assertRecordEquals("Record mismatch at index " + i, originalRecords.get(i),
        readRecords.get(i));
    }
  }

  /**
   * Tests reading records after multiple log rotations. Verifies that records can be correctly read
   * across multiple log files after several rotations, maintaining data consistency.
   */
  @Test
  public void testReadAfterMultipleRotations() throws Exception {
    final String tableName = "TBLRAMR";
    final int NUM_RECORDS_PER_ROTATION = 100;
    final int NUM_ROTATIONS = 10;
    final int TOTAL_RECORDS = NUM_RECORDS_PER_ROTATION * NUM_ROTATIONS;
    List<LogFile.Record> originalRecords = new ArrayList<>();
    List<Path> logPaths = new ArrayList<>();

    ReplicationLogGroupWriter logGroupWriter = logGroup.getActiveWriter();

    // Write records across multiple rotations.
    for (int rotation = 0; rotation < NUM_ROTATIONS; rotation++) {
      // Get the path of the current log file.
      Path logPath = logGroupWriter.getWriter().getContext().getFilePath();
      logPaths.add(logPath);

      for (int i = 0; i < NUM_RECORDS_PER_ROTATION; i++) {
        int commitId = (rotation * NUM_RECORDS_PER_ROTATION) + i;
        LogFile.Record record =
          LogFileTestUtil.newPutRecord(tableName, commitId, "row" + commitId, commitId, 1);
        originalRecords.add(record);
        logGroup.append(record.getHBaseTableName(), record.getCommitId(), record.getMutation());
      }
      logGroup.sync(); // Sync to commit the appends to the current writer.
      // Force a rotation to close the current writer.
      logGroupWriter.rotateLog(RotationReason.SIZE);
    }

    // Verify all log files exist
    for (Path logPath : logPaths) {
      assertTrue("Log file should exist: " + logPath, localFs.exists(logPath));
    }

    // Read and verify all records from each log file, in the order in which the log files
    // were written.
    List<LogFile.Record> readRecords = new ArrayList<>();
    for (Path logPath : logPaths) {
      LogFileReader reader = new LogFileReader();
      LogFileReaderContext readerContext =
        new LogFileReaderContext(conf).setFileSystem(localFs).setFilePath(logPath);
      reader.init(readerContext);

      LogFile.Record record;
      while ((record = reader.next()) != null) {
        readRecords.add(record);
      }
      reader.close();
    }

    // Verify we have the expected number of records.
    assertEquals("Total number of records mismatch", TOTAL_RECORDS, readRecords.size());

    // Verify each record matches the original. This confirms the total ordering of all records
    // in all files.
    for (int i = 0; i < TOTAL_RECORDS; i++) {
      LogFileTestUtil.assertRecordEquals("Record mismatch at index " + i, originalRecords.get(i),
        readRecords.get(i));
    }
  }

  /**
   * Tests reading records after multiple rotations with intermittent syncs. If we do not sync when
   * we roll a file, the in-flight batch is replayed into the new writer when we do finally sync
   * (with the new writer). Verifies that records can be correctly read even when syncs are not
   * performed before each rotation, ensuring data consistency.
   */
  @Test
  public void testReadAfterMultipleRotationsWithReplay() throws Exception {
    final String tableName = "TBLRAMRIS";
    final int NUM_RECORDS_PER_ROTATION = 100;
    final int NUM_ROTATIONS = 10;
    final int TOTAL_RECORDS = NUM_RECORDS_PER_ROTATION * NUM_ROTATIONS;
    List<LogFile.Record> originalRecords = new ArrayList<>();
    List<Path> logPaths = new ArrayList<>();

    ReplicationLogGroupWriter logGroupWriter = logGroup.getActiveWriter();

    // Write records across multiple rotations, only syncing 50% of the time.
    for (int rotation = 0; rotation < NUM_ROTATIONS; rotation++) {
      // Get the path of the current log file.
      Path logPath = logGroupWriter.getWriter().getContext().getFilePath();
      logPaths.add(logPath);

      for (int i = 0; i < NUM_RECORDS_PER_ROTATION; i++) {
        int commitId = (rotation * NUM_RECORDS_PER_ROTATION) + i;
        LogFile.Record record =
          LogFileTestUtil.newPutRecord(tableName, commitId, "row" + commitId, commitId, 1);
        originalRecords.add(record);
        logGroup.append(record.getHBaseTableName(), record.getCommitId(), record.getMutation());
      }

      // Only sync 50% of the time before rotation. To ensure we sync on the last file
      // we are going to write, use 'rotation % 2 == 1' instead of 'rotation % 2 == 0'.
      if (rotation % 2 == 1) {
        logGroup.sync(); // Sync to commit the appends to the current writer.
      }
      // Force a rotation to close the current writer.
      logGroupWriter.rotateLog(RotationReason.SIZE);
    }

    // Verify all log files exist
    for (Path logPath : logPaths) {
      assertTrue("Log file should exist: " + logPath, localFs.exists(logPath));
    }

    // Read and verify all records from each log file, tracking unique records and duplicates.
    Set<LogFile.Record> uniqueRecords = new HashSet<>();
    List<LogFile.Record> allReadRecords = new ArrayList<>();

    for (Path logPath : logPaths) {
      LogFileReader reader = new LogFileReader();
      LogFileReaderContext readerContext =
        new LogFileReaderContext(conf).setFileSystem(localFs).setFilePath(logPath);
      reader.init(readerContext);
      LogFile.Record record;
      while ((record = reader.next()) != null) {
        allReadRecords.add(record);
        uniqueRecords.add(record);
      }
      reader.close();
    }

    // Print statistics about duplicates for informational purposes.
    LOG.info("{} total records across all files", allReadRecords.size());
    LOG.info("{} unique records", uniqueRecords.size());
    LOG.info("{} duplicate records", allReadRecords.size() - uniqueRecords.size());

    // Verify we have all the expected unique records
    assertEquals("Number of unique records mismatch", TOTAL_RECORDS, uniqueRecords.size());
  }

  /**
   * Tests behavior when a RuntimeException occurs during writer.getLength() in shouldRotate().
   * Verifies that the system properly handles critical errors by closing the log and preventing
   * further operations.
   */
  @Test
  public void testRuntimeExceptionDuringLengthCheck() throws Exception {
    final String tableName = "TBLRDL";
    final long commitId = 1L;
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

    // Get the initial writer
    LogFileWriter innerWriter = logGroup.getActiveWriter().getWriter();
    assertNotNull("Inner writer should not be null", innerWriter);

    // Configure writer to throw RuntimeException on getLength()
    doThrow(new RuntimeException("Simulated critical error")).when(innerWriter).getLength();

    // Append data. This should trigger the LogExceptionHandler, which will close logWriter.
    logGroup.append(tableName, commitId, put);
    try {
      logGroup.sync();
      fail("Should have thrown IOException because sync timed out");
    } catch (IOException e) {
      assertTrue("Expected timeout exception", e.getCause() instanceof TimeoutException);
    }

    // Verify that subsequent operations fail because the log is closed
    try {
      logGroup.append(tableName, commitId + 1, put);
      fail("Should have thrown IOException because log is closed");
    } catch (IOException e) {
      assertTrue("Expected an IOException because log is closed",
        e.getMessage().contains("Closed"));
    }

    // Verify that the inner writer was closed by the LogExceptionHandler
    verify(innerWriter, times(1)).close();
  }

  /**
   * Tests behavior when a RuntimeException occurs during append() after closeOnError() has been
   * called. Verifies that the system properly rejects sync operations after being closed.
   */
  @Test
  public void testAppendAfterCloseOnError() throws Exception {
    final String tableName = "TBLAAE";
    final long commitId = 1L;
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

    // Get the inner writer
    LogFileWriter innerWriter = logGroup.getActiveWriter().getWriter();
    assertNotNull("Writer should not be null", innerWriter);

    // Configure writer to throw RuntimeException on append
    doThrow(new RuntimeException("Simulated critical error")).when(innerWriter).append(anyString(),
      anyLong(), any(Mutation.class));

    // Append data to trigger closeOnError()
    logGroup.append(tableName, commitId, put);
    try {
      logGroup.sync();
      fail("Should have thrown IOException because sync timed out");
    } catch (IOException e) {
      assertTrue("Expected timeout exception", e.getCause() instanceof TimeoutException);
    }

    // Verify that subsequent append operations fail because the log is closed
    try {
      logGroup.append(tableName, commitId, put);
      fail("Should have thrown IOException because log is closed");
    } catch (IOException e) {
      assertTrue("Expected an IOException because log is closed",
        e.getMessage().contains("Closed"));
    }

    // Verify that the inner writer was closed by the LogExceptionHandler
    verify(innerWriter, times(1)).close();
  }

  /**
   * Tests behavior when a RuntimeException occurs during sync() after closeOnError() has been
   * called. Verifies that the system properly rejects sync operations after being closed.
   */
  @Test
  public void testSyncAfterCloseOnError() throws Exception {
    final String tableName = "TBLSAE";
    final long commitId = 1L;
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

    // Get the inner writer
    LogFileWriter innerWriter = logGroup.getActiveWriter().getWriter();
    assertNotNull("Writer should not be null", innerWriter);

    // Configure writer to throw RuntimeException on append
    doThrow(new RuntimeException("Simulated critical error")).when(innerWriter).append(anyString(),
      anyLong(), any(Mutation.class));

    // Append data to trigger closeOnError()
    logGroup.append(tableName, commitId, put);
    try {
      logGroup.sync();
      fail("Should have thrown IOException because sync timed out");
    } catch (IOException e) {
      assertTrue("Expected timeout exception", e.getCause() instanceof TimeoutException);
    }

    // Verify that subsequent sync operations fail because the log is closed
    try {
      logGroup.sync();
      fail("Should have thrown IOException because log is closed");
    } catch (IOException e) {
      assertTrue("Expected an IOException because log is closed",
        e.getMessage().contains("Closed"));
    }

    // Verify that the inner writer was closed by the LogExceptionHandler
    verify(innerWriter, times(1)).close();
  }

  /**
   * Tests that multiple sync requests are consolidated into a single sync operation on the inner
   * writer when they occur in quick succession. Verifies that the Disruptor batching and
   * LogEventHandler processing correctly consolidates multiple sync requests into a single sync
   * operation, while still completing all sync futures successfully.
   */
  @Test
  public void testSyncConsolidation() throws Exception {
    final String tableName = "TBLSC";
    final Mutation put1 = LogFileTestUtil.newPut("row1", 1, 1);
    final long commitId1 = 1L;
    final Mutation put2 = LogFileTestUtil.newPut("row2", 2, 1);
    final long commitId2 = 2L;
    final Mutation put3 = LogFileTestUtil.newPut("row3", 3, 1);
    final long commitId3 = 3L;

    LogFileWriter innerWriter = logGroup.getActiveWriter().getWriter();
    assertNotNull("Inner writer should not be null", innerWriter);

    // Configure writer to briefly hold up the LogEventHandler upon first append.
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Thread.sleep(50); // Delay to allow multiple events to be posted
        return invocation.callRealMethod();
      }
    }).when(innerWriter).append(eq(tableName), eq(commitId1), eq(put1));

    // Post appends and three syncs in quick succession. The first append will be delayed long
    // enough for the three syncs to appear in a single Disruptor batch. Then they should all
    // be consolidated into a single sync.
    logGroup.append(tableName, commitId1, put1);
    logGroup.sync();
    logGroup.append(tableName, commitId2, put2);
    logGroup.sync();
    logGroup.append(tableName, commitId3, put3);
    logGroup.sync();

    // Verify the sequence of operations on the inner writer: the three appends, then exactly
    // one sync.
    InOrder inOrder = Mockito.inOrder(innerWriter);
    inOrder.verify(innerWriter, times(1)).append(eq(tableName), eq(commitId1), eq(put1));
    inOrder.verify(innerWriter, times(1)).append(eq(tableName), eq(commitId2), eq(put2));
    inOrder.verify(innerWriter, times(1)).append(eq(tableName), eq(commitId3), eq(put3));
    inOrder.verify(innerWriter, times(1)).sync(); // Only one sync should be called
  }

  /**
   * Tests that ReplicationLogGroup.get() returns the same instance for the same haGroupId. Verifies
   * that multiple calls with the same parameters return the cached instance.
   */
  @Test
  public void testReplicationLogGroupCaching() throws Exception {
    final String haGroupId1 = "testHAGroup1";
    final String haGroupId2 = "testHAGroup2";

    // Get instances for the first HA group
    ReplicationLogGroup g1_1 = ReplicationLogGroup.get(conf, serverName, haGroupId1);
    ReplicationLogGroup g1_2 = ReplicationLogGroup.get(conf, serverName, haGroupId1);

    // Verify same instance is returned for same haGroupId
    assertNotNull("ReplicationLogGroup should not be null", g1_1);
    assertNotNull("ReplicationLogGroup should not be null", g1_2);
    assertTrue("Same instance should be returned for same haGroupId", g1_2 == g1_1);
    assertEquals("HA Group name should match", haGroupId1, g1_1.getHaGroupName());

    // Get instance for a different HA group
    ReplicationLogGroup g2_1 = ReplicationLogGroup.get(conf, serverName, haGroupId2);
    assertNotNull("ReplicationLogGroup should not be null", g2_1);
    assertTrue("Different instance should be returned for different haGroupId", g2_1 != g1_1);
    assertEquals("HA Group name should match", haGroupId2, g2_1.getHaGroupName());

    // Verify multiple calls still return cached instances
    ReplicationLogGroup g1_3 = ReplicationLogGroup.get(conf, serverName, haGroupId1);
    ReplicationLogGroup g2_2 = ReplicationLogGroup.get(conf, serverName, haGroupId2);
    assertTrue("Cached instance should be returned", g1_3 == g1_1);
    assertTrue("Cached instance should be returned", g2_2 == g2_1);

    // Clean up
    g1_1.close();
    g2_1.close();
  }

  /**
   * Tests that close() removes the instance from the cache. Verifies that after closing, a new call
   * to get() creates a new instance.
   */
  @Test
  public void testReplicationLogGroupCacheRemovalOnClose() throws Exception {
    final String haGroupId = "testHAGroupCacheRemoval";

    // Get initial instance
    ReplicationLogGroup g1_1 = ReplicationLogGroup.get(conf, serverName, haGroupId);
    assertNotNull("ReplicationLogGroup should not be null", g1_1);
    assertFalse("Group should not be closed initially", g1_1.isClosed());

    // Verify cached instance is returned
    ReplicationLogGroup g1_2 = ReplicationLogGroup.get(conf, serverName, haGroupId);
    assertTrue("Same instance should be returned before close", g1_2 == g1_1);

    // Close the group
    g1_1.close();
    assertTrue("Group should be closed", g1_1.isClosed());

    // Get instance after close - should be a new instance
    ReplicationLogGroup g1_3 = ReplicationLogGroup.get(conf, serverName, haGroupId);
    assertNotNull("ReplicationLogGroup should not be null after close", g1_3);
    assertFalse("New group should not be closed", g1_3.isClosed());
    assertTrue("New instance should be created after close", g1_1 != g1_3);
    assertEquals("HA Group name should match", haGroupId, g1_3.getHaGroupName());

    // Clean up
    g1_3.close();
  }

  static class TestableLogGroup extends ReplicationLogGroup {

    public TestableLogGroup(Configuration conf, ServerName serverName, String haGroupName) {
      super(conf, serverName, haGroupName);
    }

    @Override
    protected ReplicationLogGroupWriter createRemoteWriter() throws IOException {
      ReplicationLogGroupWriter writer = spy(new TestableStandbyLogGroupWriter(this));
      writer.init();
      return writer;
    }

  }

  /**
   * Testable version of StandbyLogGroupWriter that allows spying on writers.
   */
  static class TestableStandbyLogGroupWriter extends StandbyLogGroupWriter {

    protected TestableStandbyLogGroupWriter(ReplicationLogGroup logGroup) {
      super(logGroup);
    }

    @Override
    protected LogFileWriter createNewWriter() throws IOException {
      LogFileWriter writer = super.createNewWriter();
      return spy(writer);
    }
  }
}
