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

import static java.lang.Thread.sleep;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.STORE_AND_FORWARD;
import static org.apache.phoenix.replication.ReplicationShardDirectoryManager.PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY;
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
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.replication.log.LogFile;
import org.apache.phoenix.replication.log.LogFileReader;
import org.apache.phoenix.replication.log.LogFileReaderContext;
import org.apache.phoenix.replication.log.LogFileTestUtil;
import org.apache.phoenix.replication.log.LogFileWriter;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationLogGroupTest extends ReplicationLogBaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogGroupTest.class);

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
    LogFileWriter writer = logGroup.getActiveLog().getWriter();
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
   * Tests the behavior when a sync operation fails transiently. Verifies that the system retries
   * with the same writer and succeeds on the next attempt.
   */
  @Test
  public void testSyncFailureAndRetry() throws Exception {
    final String tableName = "TBLSFR";
    final long commitId = 1L;
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

    ReplicationLog activeLog = logGroup.getActiveLog();
    // Get the inner writer
    LogFileWriter writer = activeLog.getWriter();
    assertNotNull("Initial writer should not be null", writer);

    // Keep returning the same writer so a rotation tick can't swap in a different one
    doAnswer(invocation -> writer).when(activeLog).createNewWriter();

    // Configure writer to fail on the first sync call, then succeed
    doThrow(new IOException("Simulated sync failure")).doCallRealMethod().when(writer).sync();

    // Append data
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Verify the sequence: append, sync (fail), sync (succeed on retry with same writer)
    InOrder inOrder = Mockito.inOrder(writer);
    inOrder.verify(writer, times(1)).append(eq(tableName), eq(commitId), eq(put));
    inOrder.verify(writer, times(2)).sync(); // First fails, second succeeds
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
    LogFileWriter innerWriter = logGroup.getActiveLog().getWriter();
    assertNotNull("Inner writer should not be null", innerWriter);

    // Create a slow consumer to fill up the ring buffer.
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        sleep(50); // Simulate slow processing
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
    sleep(100);

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
    LogFileWriter writerBeforeRoll = logGroup.getActiveLog().getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRoll);

    // Configure writerBeforeRoll to fail on the first append call
    doThrow(new IOException("Simulated append failure")).when(writerBeforeRoll).append(anyString(),
      anyLong(), any(Mutation.class));

    // Append data
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Get the inner writer we rolled to.
    LogFileWriter writerAfterRoll = logGroup.getActiveLog().getWriter();
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
    LogFileWriter innerWriter = logGroup.getActiveLog().getWriter();
    assertNotNull("Inner writer should not be null", innerWriter);

    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        // Pause long enough to cause a timeout.
        sleep((long) (TEST_SYNC_TIMEOUT * 1.25));
        LOG.info("Waking up from sleep");
        return invocation.callRealMethod();
      }
    }).when(innerWriter).sync();

    // Append some data
    logGroup.append(tableName, commitId, put);

    // sync on the writer will timeout
    try {
      logGroup.sync();
      fail("Should have thrown RuntimeException because sync timed out");
    } catch (RuntimeException e) {
      assertTrue("Expected timeout exception", e.getCause() instanceof TimeoutException);
    }
    // reset
    doNothing().when(innerWriter).sync();
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
    LogFileWriter innerWriter = logGroup.getActiveLog().getWriter();
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
   * rotation time period and that operations continue correctly with the new log file. The writer
   * swap happens pre-action in apply(), so the second append+sync go directly to the new writer.
   */
  @Test
  public void testTimeBasedRotation() throws Exception {
    final String tableName = "TBLTBR";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    final long commitId = 1L;
    final int roundDurationSeconds = 5;

    conf.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, roundDurationSeconds);
    recreateLogGroup();

    // Get the initial writer
    LogFileWriter writerBeforeRotation = logGroup.getActiveLog().getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRotation);

    // Append some data
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Wait for rotation time to elapse so LogRotationTask stages a pendingWriter
    waitForRotationTick(roundDurationSeconds);

    // This append's apply() drains pendingWriter before the append, so it goes to new writer
    logGroup.append(tableName, commitId + 1, put);
    logGroup.sync();

    // Verify we have a new writer
    LogFileWriter writerAfterRotation = logGroup.getActiveLog().getWriter();
    assertNotNull("New writer should not be null", writerAfterRotation);
    assertTrue("Writer should have been rotated", writerAfterRotation != writerBeforeRotation);

    // Verify the sequence of operations
    InOrder inOrder = Mockito.inOrder(writerBeforeRotation, writerAfterRotation);
    inOrder.verify(writerBeforeRotation, times(1)).append(eq(tableName), eq(commitId), eq(put));
    inOrder.verify(writerBeforeRotation, times(1)).sync();
    inOrder.verify(writerAfterRotation, times(1)).append(eq(tableName), eq(commitId + 1), eq(put));
    inOrder.verify(writerAfterRotation, times(1)).sync();
  }

  /**
   * Tests size-based log rotation. Verifies that the log file is rotated when it exceeds the
   * configured size threshold and that operations continue correctly with the new log file. After
   * the first sync, requestRotationIfNeeded submits an on-demand LogRotationTask which creates the
   * new writer immediately on the executor thread.
   */
  @Test
  public void testSizeBasedRotation() throws Exception {
    final String tableName = "TBLSBR";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 10);
    long commitId = 1L;

    ReplicationLog activeLog = logGroup.getActiveLog();
    LogFileWriter writerBeforeRotation = activeLog.getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRotation);

    // Append enough data so that we exceed the size threshold.
    for (int i = 0; i < 100; i++) {
      logGroup.append(tableName, commitId++, put);
    }
    // Sync: data goes to old writer. requestRotationIfNeeded submits on-demand rotation task.
    logGroup.sync();

    // Wait for the on-demand rotation task to create a new writer on the background thread
    Thread.sleep(100);

    // Next append's apply() drains pending writer → goes to new writer
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    LogFileWriter writerAfterRotation = activeLog.getWriter();
    assertNotNull("New writer should not be null", writerAfterRotation);
    assertTrue("Writer should have been rotated", writerAfterRotation != writerBeforeRotation);

    // Verify the final append went to the new writer
    verify(writerAfterRotation, times(1)).append(eq(tableName), eq(commitId), eq(put));
    verify(writerAfterRotation, times(1)).sync();
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
    LogFileWriter innerWriter = logGroup.getActiveLog().getWriter();
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
   * log files based on the configured rotation time. The writer swap happens pre-action in apply(),
   * and the old writer is closed asynchronously via closeExecutor.
   */
  @Test
  public void testRotationTask() throws Exception {
    final String tableName = "TBLRT";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    long commitId = 1L;
    final int roundDurationSeconds = 5;

    conf.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, roundDurationSeconds);
    recreateLogGroup();

    LogFileWriter writerBeforeRotation = logGroup.getActiveLog().getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRotation);

    // Append some data and wait for the rotation time to elapse plus a small buffer.
    logGroup.append(tableName, commitId, put);
    logGroup.sync();
    waitForRotationTick(roundDurationSeconds);

    // The LogRotationTask has staged a pendingWriter. Next append's apply() drains it.
    logGroup.append(tableName, commitId + 1, put);
    logGroup.sync();

    // Get the new writer after the rotation.
    LogFileWriter writerAfterRotation = logGroup.getActiveLog().getWriter();
    assertNotNull("New writer should not be null", writerAfterRotation);
    assertTrue("Writer should have been rotated", writerAfterRotation != writerBeforeRotation);

    // Verify first batch went to initial writer
    verify(writerBeforeRotation, times(1)).append(eq(tableName), eq(1L), eq(put));
    verify(writerBeforeRotation, times(1)).sync();
    // Verify second batch went to new writer (swap happened before append)
    verify(writerAfterRotation, times(1)).append(eq(tableName), eq(commitId + 1), eq(put));
    verify(writerAfterRotation, times(1)).sync();
    // Verify the initial writer was closed asynchronously
    verify(writerBeforeRotation, timeout(5000).times(1)).close();
  }

  /**
   * Tests behavior when log rotation fails temporarily but eventually succeeds. The rotation task
   * fails to create a new writer on the first attempt, but succeeds on the second. During the
   * failure, operations continue normally with the current writer.
   */
  @Test
  public void testFailedRotation() throws Exception {
    final String tableName = "TBLFR";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    long commitId = 1L;

    ReplicationLog activeLog = logGroup.getActiveLog();

    LogFileWriter initialWriter = activeLog.getWriter();
    assertNotNull("Initial writer should not be null", initialWriter);

    // Configure the log writer to fail only the first time when creating new writers.
    AtomicBoolean shouldFail = new AtomicBoolean(true);
    doAnswer(invocation -> {
      if (shouldFail.getAndSet(false)) {
        throw new IOException("Simulated failure to create new writer");
      }
      return invocation.callRealMethod();
    }).when(activeLog).createNewWriter();

    // Append some data
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Force rotation — first attempt will fail
    activeLog.forceRotation();

    // Verify we can still use the current writer after the failed rotation
    logGroup.append(tableName, commitId + 1, put);
    logGroup.sync();
    assertEquals("Should still be using the initial writer", initialWriter, activeLog.getWriter());

    // Force rotation again — this one should succeed
    activeLog.forceRotation();

    // Trigger swap by appending and syncing
    logGroup.append(tableName, commitId + 2, put);
    logGroup.sync();

    LogFileWriter writerAfterRotate = activeLog.getWriter();
    assertNotEquals("Should be using a new writer", initialWriter, writerAfterRotate);

    // Verify operations went to the writers in the correct order
    InOrder inOrder = Mockito.inOrder(initialWriter, writerAfterRotate);
    inOrder.verify(initialWriter).append(eq(tableName), eq(commitId), eq(put));
    inOrder.verify(initialWriter).sync();
    inOrder.verify(initialWriter).append(eq(tableName), eq(commitId + 1), eq(put));
    inOrder.verify(initialWriter).sync();
    inOrder.verify(writerAfterRotate).append(eq(tableName), eq(commitId + 2), eq(put));
    inOrder.verify(writerAfterRotate).sync();
  }

  /**
   * Tests that too many consecutive rotation failures cause the log to close via closeOnError().
   * The LogRotationTask tracks failures across attempts and fail-stops after maxRotationRetries.
   */
  @Test
  public void testTooManyRotationFailures() throws Exception {
    final String tableName = "TBLTMRF";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    long commitId = 1L;

    ReplicationLog activeLog = logGroup.getActiveLog();

    LogFileWriter initialWriter = activeLog.getWriter();
    assertNotNull("Initial writer should not be null", initialWriter);

    // Configure the log writer to always fail when creating new writers
    doThrow(new IOException("Simulated failure to create new writer")).when(activeLog)
      .createNewWriter();

    // Append some data
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Force rotation repeatedly until it exceeds maxRotationRetries and calls closeOnError
    for (int i = 0; i <= ReplicationLogGroup.DEFAULT_REPLICATION_LOG_ROTATION_RETRIES; i++) {
      activeLog.forceRotation();
    }

    assertTrue("Log should be closed after too many rotation failures", activeLog.isClosed());

    // Verify subsequent operations trigger a mode switch to STORE_AND_FORWARD
    logGroup.append(tableName, commitId + 1, put);
    logGroup.sync();
    assertEquals(STORE_AND_FORWARD, logGroup.getMode());
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
    LogFileWriter innerWriter = logGroup.getActiveLog().getWriter();
    assertNotNull("Writer should not be null", innerWriter);

    // Configure writer to throw a RuntimeException on append
    doThrow(new RuntimeException("Simulated critical error")).when(innerWriter).append(anyString(),
      anyLong(), any(Mutation.class));

    // Append data. This should trigger the LogExceptionHandler, which will close logWriter.
    logGroup.append(tableName, commitId, put);
    try {
      logGroup.sync();
      fail("Should have thrown Runtime because sync timed out");
    } catch (RuntimeException e) {
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
   * Tests the behavior when a sync operation fails multiple times until all the attempts are
   * exhausted on the remote cluster and then we switch to the STORE_AND_FORWARD mode and
   * successfully complete the sync
   */
  @Test
  public void testSwitchToStoreAndForwardOnSyncFailure() throws Exception {
    final String tableName = "TBLSAFR";
    final long commitId = 1L;
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

    ReplicationLog activeLog = logGroup.getActiveLog();

    // Get the initial writer
    LogFileWriter initialWriter = activeLog.getWriter();
    assertNotNull("Initial writer should not be null", initialWriter);

    // Configure initial writer to always fail on sync
    doThrow(new IOException("Simulated sync failure")).when(initialWriter).sync();

    // Make any new writers also fail on sync so a rotation can't rescue the retry loop
    doAnswer(invocation -> {
      LogFileWriter w = (LogFileWriter) invocation.callRealMethod();
      doThrow(new IOException("Simulated sync failure")).when(w).sync();
      return w;
    }).when(activeLog).createNewWriter();

    // Append data
    logGroup.append(tableName, commitId, put);
    // Try to sync. Should fail after exhausting retries and then switch to STORE_AND_FORWARD
    logGroup.sync();

    // All retries use the same writer — verify sync was attempted maxAttempts times
    verify(initialWriter, atLeast(2)).sync();
    assertEquals(STORE_AND_FORWARD, logGroup.getMode());
  }

  /**
   * Tests the behavior when we fail to update the HAGroup store status when we switch to the
   * STORE_AND_FORWARD mode and abort
   */
  @Test
  public void testFailToUpdateHAGroupStatusOnSwitchToStoreAndForward() throws Exception {
    final String tableName = "TBLSAFR";
    final long commitId = 1L;
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

    ReplicationLog activeLog = logGroup.getActiveLog();

    // Get the initial writer
    LogFileWriter initialWriter = activeLog.getWriter();
    assertNotNull("Initial writer should not be null", initialWriter);

    // Configure initial writer to always fail on sync
    doThrow(new IOException("Simulated sync failure")).when(initialWriter).sync();

    // Make any new writers also fail on sync so a rotation can't rescue the retry loop
    doAnswer(invocation -> {
      LogFileWriter w = (LogFileWriter) invocation.callRealMethod();
      doThrow(new IOException("Simulated sync failure")).when(w).sync();
      return w;
    }).when(activeLog).createNewWriter();

    doThrow(new IOException("Simulated failure to update HAGroupStore state"))
      .when(haGroupStoreManager).setHAGroupStatusToStoreAndForward(haGroupName);

    // Append data
    logGroup.append(tableName, commitId, put);
    // Try to sync. Should fail after exhausting retries and then switch to STORE_AND_FORWARD
    try {
      logGroup.sync();
      fail("Should have thrown exception because of failure to update mode");
    } catch (RuntimeException ex) {
      assertTrue(ex.getMessage().contains("Simulated sync failure"));
    }
    // wait for the event processor thread to clean up
    Thread.sleep(3);
  }

  /**
   * Tests rotation during an in-flight batch. When a pending writer is staged by the rotation task
   * while appends are in flight, the sync's apply() drains the pending writer, replays the 5
   * unsynced records into the new writer, then syncs the new writer.
   */
  @Test
  public void testRotationDuringBatch() throws Exception {
    final String tableName = "TBLRDB";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    long commitId = 1L;

    // Get the initial writer
    LogFileWriter writerBeforeRotation = logGroup.getActiveLog().getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRotation);

    // Append several items but don't sync yet
    for (int i = 0; i < 5; i++) {
      logGroup.append(tableName, commitId + i, put);
    }

    // Stage a pending writer via forced rotation
    logGroup.getActiveLog().forceRotation();

    // Sync — apply() drains pendingWriter, replays 5 records into new writer, then syncs
    logGroup.sync();

    // The swap happened before sync action
    LogFileWriter writerAfterRotation = logGroup.getActiveLog().getWriter();
    assertNotNull("New writer should not be null", writerAfterRotation);
    assertTrue("Writer should have been rotated", writerAfterRotation != writerBeforeRotation);

    InOrder inOrder = Mockito.inOrder(writerBeforeRotation, writerAfterRotation);
    // 5 appends went to old writer (processed before rotation task fired)
    for (int i = 0; i < 5; i++) {
      inOrder.verify(writerBeforeRotation, times(1)).append(eq(tableName), eq(commitId + i),
        eq(put));
    }
    // Swap happens before sync action: 5 records replayed into new writer
    for (int i = 0; i < 5; i++) {
      inOrder.verify(writerAfterRotation, times(1)).append(eq(tableName), eq(commitId + i),
        eq(put));
    }
    // Sync goes to new writer
    inOrder.verify(writerAfterRotation, times(1)).sync();

    // Verify the initial writer was closed asynchronously
    verify(writerBeforeRotation, timeout(5000).times(1)).close();
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

    ReplicationLog activeLog = logGroup.getActiveLog();

    // Get the path of the log file.
    Path logPath = activeLog.getWriter().getContext().getFilePath();

    for (int i = 0; i < NUM_RECORDS; i++) {
      LogFile.Record record = LogFileTestUtil.newPutRecord(tableName, i, "row" + i, i, 1);
      originalRecords.add(record);
      logGroup.append(record.getHBaseTableName(), record.getCommitId(), record.getMutation());
    }
    logGroup.sync(); // Sync to commit the appends to the current writer.

    // Force a rotation to close the current writer.
    activeLog.forceRotation();

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

    ReplicationLog activeLog = logGroup.getActiveLog();

    // Write records across multiple rotations.
    for (int rotation = 0; rotation < NUM_ROTATIONS; rotation++) {
      // Get the path of the current log file.
      Path logPath = activeLog.getWriter().getContext().getFilePath();
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
      activeLog.forceRotation();
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
   * Tests reading records after multiple rotations with varying batch sizes. The writer swap
   * happens inside {@code apply} before each attempt, so unsynced appends are replayed into the new
   * writer. This test verifies data consistency across many rotations with different record counts
   * per file.
   */
  @Test
  public void testReadAfterMultipleRotationsWithVaryingBatchSizes() throws Exception {
    final String tableName = "TBLRAMRVBS";
    final int NUM_RECORDS_PER_ROTATION = 100;
    final int NUM_ROTATIONS = 10;
    final int TOTAL_RECORDS = NUM_RECORDS_PER_ROTATION * NUM_ROTATIONS;
    List<LogFile.Record> originalRecords = new ArrayList<>();
    List<Path> logPaths = new ArrayList<>();

    ReplicationLog activeLog = logGroup.getActiveLog();

    for (int rotation = 0; rotation < NUM_ROTATIONS; rotation++) {
      Path logPath = activeLog.getWriter().getContext().getFilePath();
      logPaths.add(logPath);

      for (int i = 0; i < NUM_RECORDS_PER_ROTATION; i++) {
        int commitId = (rotation * NUM_RECORDS_PER_ROTATION) + i;
        LogFile.Record record =
          LogFileTestUtil.newPutRecord(tableName, commitId, "row" + commitId, commitId, 1);
        originalRecords.add(record);
        logGroup.append(record.getHBaseTableName(), record.getCommitId(), record.getMutation());
      }

      logGroup.sync();
      activeLog.forceRotation();
    }

    // Verify all log files exist
    for (Path logPath : logPaths) {
      assertTrue("Log file should exist: " + logPath, localFs.exists(logPath));
    }

    // Read and verify all records from each log file
    List<LogFile.Record> allReadRecords = new ArrayList<>();
    for (Path logPath : logPaths) {
      LogFileReader reader = new LogFileReader();
      LogFileReaderContext readerContext =
        new LogFileReaderContext(conf).setFileSystem(localFs).setFilePath(logPath);
      reader.init(readerContext);
      LogFile.Record record;
      while ((record = reader.next()) != null) {
        allReadRecords.add(record);
      }
      reader.close();
    }

    assertEquals("Total number of records mismatch", TOTAL_RECORDS, allReadRecords.size());

    for (int i = 0; i < TOTAL_RECORDS; i++) {
      LogFileTestUtil.assertRecordEquals("Record mismatch at index " + i, originalRecords.get(i),
        allReadRecords.get(i));
    }
  }

  /**
   * Tests behavior when a RuntimeException occurs during writer.getLength() in
   * shouldRotateForSize(). Verifies that the system properly handles critical errors by closing the
   * log and preventing further operations.
   */
  @Test
  public void testRuntimeExceptionDuringLengthCheck() throws Exception {
    final String tableName = "TBLRDL";
    final long commitId = 1L;
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);

    // Get the initial writer
    LogFileWriter innerWriter = logGroup.getActiveLog().getWriter();
    assertNotNull("Inner writer should not be null", innerWriter);

    // Configure writer to throw RuntimeException on getLength()
    doThrow(new RuntimeException("Simulated critical error")).when(innerWriter).getLength();

    // Append data. This should trigger the LogExceptionHandler, which will close logWriter.
    logGroup.append(tableName, commitId, put);
    try {
      logGroup.sync();
      fail("Should have thrown RuntimeException because sync timed out");
    } catch (RuntimeException e) {
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
    LogFileWriter innerWriter = logGroup.getActiveLog().getWriter();
    assertNotNull("Writer should not be null", innerWriter);

    // Configure writer to throw RuntimeException on append
    doThrow(new RuntimeException("Simulated critical error")).when(innerWriter).append(anyString(),
      anyLong(), any(Mutation.class));

    // Append data to trigger closeOnError()
    logGroup.append(tableName, commitId, put);
    try {
      logGroup.sync();
      fail("Should have thrown RuntimeException because sync timed out");
    } catch (RuntimeException e) {
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
    LogFileWriter innerWriter = logGroup.getActiveLog().getWriter();
    assertNotNull("Writer should not be null", innerWriter);

    // Configure writer to throw RuntimeException on append
    doThrow(new RuntimeException("Simulated critical error")).when(innerWriter).append(anyString(),
      anyLong(), any(Mutation.class));

    // Append data to trigger closeOnError()
    logGroup.append(tableName, commitId, put);
    try {
      logGroup.sync();
      fail("Should have thrown RuntimeException because sync timed out");
    } catch (RuntimeException e) {
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
   * Tests that an undrained pendingWriter is closed and replaced by the rotation task. This
   * simulates an idle system where no events flow to drain the staged writer before the next
   * rotation tick fires. Verifies the undrained writer is actually closed by the second tick.
   */
  @Test
  public void testUndrainedPendingWriterReplaced() throws Exception {
    final String tableName = "TBLUPWR";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    final long commitId = 1L;
    final int roundDurationSeconds = 5;

    conf.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, roundDurationSeconds);
    recreateLogGroup();

    ReplicationLog activeLog = logGroup.getActiveLog();
    LogFileWriter initialWriter = activeLog.getWriter();
    assertNotNull("Initial writer should not be null", initialWriter);

    // Capture writers created by rotation ticks
    List<LogFileWriter> rotationWriters = new ArrayList<>();
    doAnswer(invocation -> {
      LogFileWriter w = (LogFileWriter) invocation.callRealMethod();
      rotationWriters.add(w);
      return w;
    }).when(activeLog).createNewWriter();

    // Append and sync to establish baseline
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Wait for rotation tick — this stages a pending writer (W2)
    waitForRotationTick(roundDurationSeconds);

    // The pending writer is staged but not drained (no events to trigger checkAndReplaceWriter).
    // Wait for the next rotation tick — it should close the undrained writer and stage a new one.
    waitForRotationTick(roundDurationSeconds);

    // Now drain by appending (apply() calls checkAndReplaceWriter)
    logGroup.append(tableName, commitId + 1, put);
    logGroup.sync();

    LogFileWriter writerAfterRotation = activeLog.getWriter();
    assertTrue("Writer should have been rotated", writerAfterRotation != initialWriter);

    // W2 was created by first tick, W3 by second tick
    assertTrue("Expected at least 2 rotation writers", rotationWriters.size() >= 2);
    LogFileWriter undrainedWriter = rotationWriters.get(0);
    LogFileWriter finalWriter = rotationWriters.get(rotationWriters.size() - 1);

    // Undrained W2 was closed synchronously by the second rotation tick
    verify(undrainedWriter, times(1)).close();

    // Final writer is the one we're using
    assertEquals("Active writer should be the last rotation writer", finalWriter,
      writerAfterRotation);
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

    LogFileWriter innerWriter = logGroup.getActiveLog().getWriter();
    assertNotNull("Inner writer should not be null", innerWriter);

    // Configure writer to briefly hold up the LogEventHandler upon first append.
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        sleep(50); // Delay to allow multiple events to be posted
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
    ReplicationLogGroup g1_1 =
      ReplicationLogGroup.get(conf, serverName, haGroupId1, haGroupStoreManager);
    ReplicationLogGroup g1_2 =
      ReplicationLogGroup.get(conf, serverName, haGroupId1, haGroupStoreManager);

    // Verify same instance is returned for same haGroupId
    assertNotNull("ReplicationLogGroup should not be null", g1_1);
    assertNotNull("ReplicationLogGroup should not be null", g1_2);
    assertTrue("Same instance should be returned for same haGroupId", g1_2 == g1_1);
    assertEquals("HA Group name should match", haGroupId1, g1_1.getHAGroupName());

    // Get instance for a different HA group
    ReplicationLogGroup g2_1 =
      ReplicationLogGroup.get(conf, serverName, haGroupId2, haGroupStoreManager);
    assertNotNull("ReplicationLogGroup should not be null", g2_1);
    assertTrue("Different instance should be returned for different haGroupId", g2_1 != g1_1);
    assertEquals("HA Group name should match", haGroupId2, g2_1.getHAGroupName());

    // Verify multiple calls still return cached instances
    ReplicationLogGroup g1_3 =
      ReplicationLogGroup.get(conf, serverName, haGroupId1, haGroupStoreManager);
    ReplicationLogGroup g2_2 =
      ReplicationLogGroup.get(conf, serverName, haGroupId2, haGroupStoreManager);
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
    ReplicationLogGroup g1_1 =
      ReplicationLogGroup.get(conf, serverName, haGroupId, haGroupStoreManager);
    assertNotNull("ReplicationLogGroup should not be null", g1_1);
    assertFalse("Group should not be closed initially", g1_1.isClosed());

    // Verify cached instance is returned
    ReplicationLogGroup g1_2 =
      ReplicationLogGroup.get(conf, serverName, haGroupId, haGroupStoreManager);
    assertTrue("Same instance should be returned before close", g1_2 == g1_1);

    // Close the group
    g1_1.close();
    assertTrue("Group should be closed", g1_1.isClosed());

    // Get instance after close - should be a new instance
    ReplicationLogGroup g1_3 =
      ReplicationLogGroup.get(conf, serverName, haGroupId, haGroupStoreManager);
    assertNotNull("ReplicationLogGroup should not be null after close", g1_3);
    assertFalse("New group should not be closed", g1_3.isClosed());
    assertTrue("New instance should be created after close", g1_1 != g1_3);
    assertEquals("HA Group name should match", haGroupId, g1_3.getHAGroupName());

    // Clean up
    g1_3.close();
  }

  @Test
  public void testInFlightAppendsReplayAfterModeSwitch() throws Exception {
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
    ReplicationLog activeLog = logGroup.getActiveLog();
    LogFileWriter writer = activeLog.getWriter();
    assertNotNull("Writer should not be null", writer);
    // keep returning the same writer
    doAnswer(invocation -> writer).when(activeLog).createNewWriter();

    logGroup.append(tableName, commitId1, put1);
    logGroup.append(tableName, commitId2, put2);
    logGroup.append(tableName, commitId3, put3);
    logGroup.append(tableName, commitId4, put4);

    // configure writer to throw IOException on the 5th append
    doThrow(new IOException("Simulate append failure")).when(writer).append(tableName, commitId5,
      put5);

    logGroup.append(tableName, commitId5, put5);
    logGroup.sync();

    LogFileWriter storeAndForwardWriter = logGroup.getActiveLog().getWriter();
    assertTrue("After switching mode we should have a new writer", writer != storeAndForwardWriter);
    InOrder inOrder = Mockito.inOrder(storeAndForwardWriter);

    // verify that all the in-flight appends and syncs are replayed on the new store and forward
    // writer
    inOrder.verify(storeAndForwardWriter, times(1)).append(eq(tableName), eq(commitId1), eq(put1));
    inOrder.verify(storeAndForwardWriter, times(1)).append(eq(tableName), eq(commitId2), eq(put2));
    inOrder.verify(storeAndForwardWriter, times(1)).append(eq(tableName), eq(commitId3), eq(put3));
    inOrder.verify(storeAndForwardWriter, times(1)).append(eq(tableName), eq(commitId4), eq(put4));
    inOrder.verify(storeAndForwardWriter, times(1)).append(eq(tableName), eq(commitId5), eq(put5));
    inOrder.verify(storeAndForwardWriter, times(1)).sync();
  }

  /**
   * Tests that multiple instances of the same HA group don't attempt to register the same jmx
   * metrics instance which is not allowed.
   */
  @Test
  public void testMetricsCaching() throws Exception {
    String hagroup1 = "group1";
    // create a HA group which creates the metrics jmx instance
    ReplicationLogGroup group1 =
      spy(new ReplicationLogGroup(conf, serverName, hagroup1, haGroupStoreManager));
    // HA group initialization fails so the HA group is not cached
    doThrow(new IOException("Simulate HAGroup initialization error")).when(group1).init();
    try {
      group1.init();
      fail("HAGroup initialization should have failed");
    } catch (IOException e) {
      // expected
    }
    // retry creating another instance of the same HA Group
    ReplicationLogGroup group2 =
      new ReplicationLogGroup(conf, serverName, hagroup1, haGroupStoreManager);
    // this time initialization succeeds
    group2.init();
    // metrics instance is the same in both the instances
    assertEquals(group1.metrics, group2.metrics);
    group2.close();
  }

  /**
   * Tests that unsynced appends are replayed into the new writer when a swap happens mid-batch.
   * Append 3 records (no sync), stage a pending writer, append a 4th record which triggers the swap
   * + replay, then sync. Old writer gets 3 appends, new writer gets 3 replayed + 4th append + sync.
   */
  @Test
  public void testReplayOnMidBatchSwap() throws Exception {
    final String tableName = "TBLRMBS";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    long commitId = 1L;

    ReplicationLog activeLog = logGroup.getActiveLog();
    LogFileWriter writerBeforeRotation = activeLog.getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRotation);

    // Append 3 records without syncing — they accumulate in currentBatch
    for (int i = 0; i < 3; i++) {
      logGroup.append(tableName, commitId + i, put);
    }

    // Stage a pending writer via forced rotation
    activeLog.forceRotation();

    // 4th append triggers checkAndReplaceWriter + generation mismatch → replays 3 records
    logGroup.append(tableName, commitId + 3, put);
    logGroup.sync();

    LogFileWriter writerAfterRotation = activeLog.getWriter();
    assertTrue("Writer should have been rotated", writerAfterRotation != writerBeforeRotation);

    InOrder inOrder = Mockito.inOrder(writerBeforeRotation, writerAfterRotation);
    // 3 appends went to old writer
    for (int i = 0; i < 3; i++) {
      inOrder.verify(writerBeforeRotation, times(1)).append(eq(tableName), eq(commitId + i),
        eq(put));
    }
    // 3 records replayed into new writer
    for (int i = 0; i < 3; i++) {
      inOrder.verify(writerAfterRotation, times(1)).append(eq(tableName), eq(commitId + i),
        eq(put));
    }
    // 4th append goes to new writer
    inOrder.verify(writerAfterRotation, times(1)).append(eq(tableName), eq(commitId + 3), eq(put));
    inOrder.verify(writerAfterRotation, times(1)).sync();

    // Old writer closed async
    verify(writerBeforeRotation, timeout(5000).times(1)).close();
  }

  /**
   * Tests the lease-recovery scenario: when the current writer's stream is broken (simulated by
   * failing sync), a staged pending writer is picked up before the action, replays unsynced
   * appends, and succeeds without ever calling sync on the broken writer.
   */
  @Test
  public void testRetryPicksUpStagedWriter() throws Exception {
    final String tableName = "TBLRPSW";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    final long commitId = 1L;

    ReplicationLog activeLog = logGroup.getActiveLog();
    LogFileWriter initialWriter = activeLog.getWriter();
    assertNotNull("Initial writer should not be null", initialWriter);

    // Append a record — goes to initialWriter
    logGroup.append(tableName, commitId, put);

    // Configure initial writer's sync to fail (simulating broken stream after lease recovery)
    doThrow(new IOException("Simulated broken stream")).when(initialWriter).sync();

    // Stage a pending writer via forced rotation
    activeLog.forceRotation();

    // Sync: checkAndReplaceWriter drains pending writer before the sync action,
    // replays 1 record into new writer, then syncs new writer. Old writer's sync is
    // never called because the swap happens before the action.
    logGroup.sync();

    LogFileWriter newWriter = activeLog.getWriter();
    assertTrue("Should be using new writer", newWriter != initialWriter);

    // Old writer: received the append only
    verify(initialWriter, times(1)).append(eq(tableName), eq(commitId), eq(put));

    // New writer: received replayed append + successful sync
    verify(newWriter, times(1)).append(eq(tableName), eq(commitId), eq(put));
    verify(newWriter, times(1)).sync();
  }

  /**
   * Tests the idle-then-lease-recovery scenario: after a sync clears currentBatch, the system goes
   * idle. A rotation tick stages a pending writer. The reader performs HDFS lease recovery,
   * breaking the old writer's stream. When events resume, apply() drains the healthy staged writer
   * before the action — the broken writer is never touched. No replay needed (empty batch).
   */
  @Test
  public void testIdleLeaseRecoveryDrainsStagedWriter() throws Exception {
    final String tableName = "TBLILRDSW";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    final long commitId = 1L;

    ReplicationLog activeLog = logGroup.getActiveLog();
    LogFileWriter initialWriter = activeLog.getWriter();
    assertNotNull("Initial writer should not be null", initialWriter);

    // Append + sync to establish baseline and clear currentBatch
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Stage W2 in pendingWriter via forced rotation
    activeLog.forceRotation();

    // Simulate HDFS lease recovery breaking the old writer's stream
    doThrow(new IOException("Simulated broken stream after lease recovery")).when(initialWriter)
      .append(anyString(), anyLong(), any(Mutation.class));
    doThrow(new IOException("Simulated broken stream after lease recovery")).when(initialWriter)
      .sync();

    // Events resume — apply() drains W2 before the action, so broken writer is never touched
    logGroup.append(tableName, commitId + 1, put);
    logGroup.sync();

    LogFileWriter newWriter = activeLog.getWriter();
    assertTrue("Should be using new writer after idle + lease recovery",
      newWriter != initialWriter);

    // New writer received the new append + sync (no replay — currentBatch was empty)
    verify(newWriter, times(1)).append(eq(tableName), eq(commitId + 1), eq(put));
    verify(newWriter, times(1)).sync();

    // Old writer: only the pre-idle append + sync, nothing after the break
    verify(initialWriter, times(1)).append(eq(tableName), eq(commitId), eq(put));
    verify(initialWriter, times(1)).sync();
  }

  /**
   * Tests that a failed replay is retried on the next attempt. The new writer's first append fails
   * during replay, so the generation stays stale. On retry, the generation mismatch is detected
   * again and replay succeeds.
   */
  @Test
  public void testReplayFailureRetries() throws Exception {
    final String tableName = "TBLRFR";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    long commitId = 1L;

    ReplicationLog activeLog = logGroup.getActiveLog();
    LogFileWriter initialWriter = activeLog.getWriter();
    assertNotNull("Initial writer should not be null", initialWriter);

    // Intercept createNewWriter to capture the new writer and make its first append fail
    final AtomicBoolean failFirstAppend = new AtomicBoolean(true);
    doAnswer(invocation -> {
      LogFileWriter w = (LogFileWriter) invocation.callRealMethod();
      doAnswer(appendInvocation -> {
        if (failFirstAppend.getAndSet(false)) {
          throw new IOException("Simulated transient HDFS error during replay");
        }
        return appendInvocation.callRealMethod();
      }).when(w).append(anyString(), anyLong(), any(Mutation.class));
      return w;
    }).when(activeLog).createNewWriter();

    // Append 2 records without syncing — they accumulate in currentBatch
    logGroup.append(tableName, commitId, put);
    logGroup.append(tableName, commitId + 1, put);

    // Stage a pending writer via forced rotation (whose first append will fail)
    activeLog.forceRotation();

    // 3rd append triggers swap + replay of [r1, r2].
    // Attempt 1: replay fails on first record → IOException → generation stays stale
    // Attempt 2: generation mismatch still → replay retries → succeeds → r3 appended
    logGroup.append(tableName, commitId + 2, put);
    logGroup.sync();

    LogFileWriter newWriter = activeLog.getWriter();
    assertTrue("Should be using new writer", newWriter != initialWriter);

    // New writer: attempt 1 replay failed on r1 (1 call). Attempt 2 replayed r1+r2 (2 calls)
    // then appended r3. Total: r1 called 2x, r2 called 1x, r3 called 1x.
    verify(newWriter, times(2)).append(eq(tableName), eq(commitId), eq(put));
    verify(newWriter, times(1)).append(eq(tableName), eq(commitId + 1), eq(put));
    verify(newWriter, times(1)).append(eq(tableName), eq(commitId + 2), eq(put));
    verify(newWriter, times(1)).sync();
  }

  /**
   * Tests error-recovery rotation: when the current writer's stream is broken, the second failure
   * in apply() triggers requestRotation() which submits an on-demand LogRotationTask. During the
   * retry sleep, the background thread creates a new writer. The next attempt drains it and
   * succeeds.
   */
  @Test
  public void testErrorRecoveryRequestsNewWriter() throws Exception {
    final String tableName = "TBLERNW";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    final long commitId = 1L;

    ReplicationLog activeLog = logGroup.getActiveLog();
    LogFileWriter initialWriter = activeLog.getWriter();
    assertNotNull("Initial writer should not be null", initialWriter);

    // Configure initial writer's append to always fail (simulating broken HDFS stream)
    doThrow(new IOException("Simulated broken stream")).when(initialWriter).append(anyString(),
      anyLong(), any(Mutation.class));

    // Append — attempt 1 fails, attempt 2 fails + requestRotation(), during sleep the
    // background thread creates a new writer, attempt 3 drains it and succeeds
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    LogFileWriter newWriter = activeLog.getWriter();
    assertTrue("Should be using a new writer after error recovery", newWriter != initialWriter);

    // Old writer received failed attempts
    verify(initialWriter, atLeast(2)).append(eq(tableName), eq(commitId), eq(put));
    // New writer received the successful append
    verify(newWriter, times(1)).append(eq(tableName), eq(commitId), eq(put));
    verify(newWriter, times(1)).sync();
  }

  /**
   * Tests that an on-demand size rotation mid-interval does not suppress the next scheduled tick.
   * After size rotation creates a writer early, the scheduled tick still fires and creates another.
   */
  @Test
  public void testOnDemandRotationDoesNotSuppressScheduledTick() throws Exception {
    final String tableName = "TBLODRNST";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 10);
    long commitId = 1L;
    final int roundDurationSeconds = 5;

    conf.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, roundDurationSeconds);
    recreateLogGroup();

    ReplicationLog activeLog = logGroup.getActiveLog();
    LogFileWriter initialWriter = activeLog.getWriter();
    assertNotNull("Initial writer should not be null", initialWriter);

    // Append enough data to trigger size rotation → requestRotation() fires
    for (int i = 0; i < 100; i++) {
      logGroup.append(tableName, commitId++, put);
    }
    logGroup.sync();

    // Wait for the on-demand rotation task to create a new writer on the background thread
    Thread.sleep(100);

    // Drain W2 via next append
    logGroup.append(tableName, commitId++, put);
    logGroup.sync();

    LogFileWriter writerAfterSizeRotation = activeLog.getWriter();
    assertTrue("Writer should have been rotated for size",
      writerAfterSizeRotation != initialWriter);

    // Wait for the scheduled rotation tick
    waitForRotationTick(roundDurationSeconds);

    // Drain W3
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    LogFileWriter writerAfterScheduledTick = activeLog.getWriter();
    assertTrue("Scheduled tick should have created a new writer (not suppressed)",
      writerAfterScheduledTick != writerAfterSizeRotation);
  }

  /**
   * Tests that the rotation executor fires at the round boundary by verifying that after waiting
   * for slightly more than a round, a rotation has occurred.
   */
  @Test
  public void testRotationScheduleAlignsWithRoundBoundary() throws Exception {
    final String tableName = "TBLRSARB";
    final Mutation put = LogFileTestUtil.newPut("row", 1, 1);
    final long commitId = 1L;
    final int roundDurationSeconds = 5;

    conf.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, roundDurationSeconds);
    recreateLogGroup();

    LogFileWriter writerBeforeRotation = logGroup.getActiveLog().getWriter();
    assertNotNull("Initial writer should not be null", writerBeforeRotation);

    // Append and sync so we have data
    logGroup.append(tableName, commitId, put);
    logGroup.sync();

    // Wait for slightly more than one round duration — the rotation task should have fired
    waitForRotationTick(roundDurationSeconds);

    // Trigger drain
    logGroup.append(tableName, commitId + 1, put);
    logGroup.sync();

    LogFileWriter writerAfterRotation = logGroup.getActiveLog().getWriter();
    assertTrue("Rotation should have happened at round boundary",
      writerAfterRotation != writerBeforeRotation);
  }

  // @Test
  public void testAppendTimeoutWhileSyncPending() throws Exception {
    final String tableName = "TESTTBL";
    final long commitId1 = 1L;
    final Mutation put1 = LogFileTestUtil.newPut("row1", 1, 1);

    // Get the inner writer
    ReplicationLog activeLog = logGroup.getActiveLog();
    LogFileWriter writer = activeLog.getWriter();
    assertNotNull("Writer should not be null", writer);
    // keep returning the same writer
    // doAnswer(invocation -> writer).when(activeLog).createNewWriter();
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        // Thread.sleep((long)(TEST_SYNC_TIMEOUT * 1.25)); // Simulate slow append processing
        // throw new CallTimeoutException("Simulate append timeout");
        Object result = invocation.callRealMethod();
        sleep((long) (TEST_SYNC_TIMEOUT * 1.25)); // Simulate slow but successful append
        return result;
      }
    }).when(writer).append(anyString(), anyLong(), any(Mutation.class));

    logGroup.append(tableName, commitId1, put1);
    logGroup.sync();

    LogFileWriter storeAndForwardWriter = logGroup.getActiveLog().getWriter();
    assertTrue("After switching mode we should have a new writer", writer != storeAndForwardWriter);
    InOrder inOrder = Mockito.inOrder(storeAndForwardWriter);
    // verify that all the in-flight appends and syncs are replayed on the new store and forward
    // writer
    inOrder.verify(storeAndForwardWriter, times(1)).append(eq(tableName), eq(commitId1), eq(put1));
    inOrder.verify(storeAndForwardWriter, times(1)).sync();
  }
}
