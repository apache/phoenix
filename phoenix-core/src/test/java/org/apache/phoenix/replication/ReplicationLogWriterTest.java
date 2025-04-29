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

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.phoenix.replication.ReplicationLogManagerTest.TestableReplicationLogManager;
import org.apache.phoenix.replication.log.LogFile;
import org.apache.phoenix.replication.log.LogFileTestUtil;
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

public class ReplicationLogWriterTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private Configuration conf;
    private RegionServerServices mockRsServices;
    private FileSystem mockFs;
    private Path standbyLogDir;
    private TestableReplicationLogManager logManager;
    private LogFile.Writer internalWriter;

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        // Use a temporary folder for the standby HDFS URL
        standbyLogDir = new Path(testFolder.newFolder("standby").toURI());
        conf.set(ReplicationLogManager.REPLICATION_STANDBY_HDFS_URL_KEY, standbyLogDir.toString());

        // Mock RegionServerServices
        mockRsServices = mock(RegionServerServices.class);
        when(mockRsServices.getServerName()).thenReturn(ServerName.valueOf("testserver", 60010,
              EnvironmentEdgeManager.currentTimeMillis()));

        // Mock FileSystem used by the manager
        mockFs = mock(FileSystem.class);
        when(mockFs.getUri()).thenReturn(standbyLogDir.toUri());
        when(mockFs.getConf()).thenReturn(conf);
        when(mockFs.exists(standbyLogDir)).thenReturn(true);

        // Create the testable manager instance, mocking the standby FileSystem
        logManager = spy(new TestableReplicationLogManager(conf, mockRsServices, mockFs));
        logManager.init(); // Initialize the manager first

        // Get the initial mock writer created by the manager during its init
        internalWriter = logManager.getCurrentInternalWriter();
        assertNotNull("Initial mock writer should not be null", internalWriter);
    }

    @After
    public void tearDown() throws IOException {
        logManager.close();
    }

    @Test(timeout=10000)
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

        // Create the ReplicationLogWriter instance to be tested
        ReplicationLogWriter logWriter = new ReplicationLogWriter(conf, logManager);
        logWriter.init();

        // Capture the future passed to the mock writer's sync
        final AtomicReference<CompletableFuture<Void>> syncFutureRef = new AtomicReference<>();

        // Mock the log writer. Append should just succeed (void return). Sync should capture the
        // future and complete successfully after a short delay to simulate some work and ensure
        // the sync call blocks appropriately.
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                CompletableFuture<Void> future = syncFutureRef.get();
                if (future != null) {
                    // Simulate successful sync completion after a delay
                    new Thread(() -> {
                        try {
                            Thread.sleep(100); // A short delay, enough to be interesting
                            future.complete(null);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            future.completeExceptionally(e);
                        }
                    }).start();
                }
                return null;
            }
        }).when(internalWriter).sync();

        // Write the appends to the Disruptor. The handling will be async with respect to the test
        // executor.

        logWriter.append(tableName, commitId1, put1);
        logWriter.append(tableName, commitId2, put2);
        logWriter.append(tableName, commitId3, put3);
        logWriter.append(tableName, commitId4, put4);
        logWriter.append(tableName, commitId5, put5);

        // "Write" the sync to the Disruptor.
        // Sync in a different thread so we don't block the test executor, which must now do the
        // inOrder verification below.

        CompletableFuture<Void> syncResultFuture = new CompletableFuture<>();
        Thread syncThread = new Thread(() -> {
            try {
                logWriter.sync();
                syncResultFuture.complete(null);
            } catch (IOException e) {
                syncResultFuture.completeExceptionally(e);
            }
        });
        syncThread.start();

        // Happens-before ordering verification, using Mockito's inOrder.

        // Verify that the appends happen before sync, and sync happened after appends
        // Use timeout with verify to wait for the async event handler to process
        InOrder inOrder = Mockito.inOrder(internalWriter);

        // Wait for appends (these might happen very quickly)
        inOrder.verify(internalWriter, timeout(1000).times(1)).append(eq(tableName), eq(commitId1),
            eq(put1));
        inOrder.verify(internalWriter, timeout(1000).times(1)).append(eq(tableName), eq(commitId2),
            eq(put2));
        inOrder.verify(internalWriter, timeout(1000).times(1)).append(eq(tableName), eq(commitId3),
            eq(put3));
        inOrder.verify(internalWriter, timeout(1000).times(1)).append(eq(tableName), eq(commitId4),
            eq(put4));
        inOrder.verify(internalWriter, timeout(1000).times(1)).append(eq(tableName), eq(commitId5),
            eq(put5));

        // Wait for sync (this depends on the syncFuture completion)
        inOrder.verify(internalWriter, timeout(1000).times(1)).sync();

        // Wait for the sync() call itself to complete. Any issues here will cause the test to
        // stall and eventually time out.
        syncResultFuture.get();

        // Close the log writer.
        logWriter.close();

        // Since close calls sync again, we expect another sync call
        inOrder.verify(internalWriter, timeout(6000).times(1)).sync();
    }

    @Test(timeout=15000)
    public void testSyncFailureAndRetry() throws Exception {
        final String tableName = "RETRYTBL";
        final long commitId1 = 10L;
        final Mutation put1 = LogFileTestUtil.newPut("retryRow1", 10, 1);

        // Create the ReplicationLogWriter instance to be tested
        ReplicationLogWriter logWriter = new ReplicationLogWriter(conf, logManager);
        logWriter.init();

        // Get the initial internal writer
        LogFile.Writer writerBeforeRoll = logManager.getCurrentInternalWriter();
        assertNotNull("Initial mock writer should not be null", writerBeforeRoll);

        // Configure writerBeforeRoll to fail on the first sync call
        doThrow(new IOException("Simulated sync failure"))
            .doNothing() // Succeed on subsequent calls (though it won't be called again)
            .when(writerBeforeRoll).sync();

        // Prepare a second mock writer to be returned after rotation
        LogFile.Writer writerAfterRoll = mock(LogFile.Writer.class);
        when(writerAfterRoll.getLength()).thenReturn(0L); // Needed for shouldRotate check

        // Configure the logManager's rotateLog method to return the second writer
        doAnswer(invocation -> {
            // Simulate the actual rotation behavior: close old, create new
            logManager.closeWriter(writerBeforeRoll); // Close the failing writer
            logManager.currentWriter = logManager.new Writer(writerAfterRoll,
                logManager.writerGeneration + 1);
            logManager.writerGeneration++;
            logManager.lastRotationTime = EnvironmentEdgeManager.currentTimeMillis();
            return logManager.currentWriter;
        }).when(logManager).rotateLog();

        // Configure internalWriter2 to succeed on sync
        doAnswer(invocation -> null).when(writerAfterRoll).sync();

        // Append data
        logWriter.append(tableName, commitId1, put1);

        // Call sync and expect it to eventually succeed after retry. Do it in another thread so we
        // continue to InOrder behavioral verification.
        CompletableFuture<Void> syncResultFuture = new CompletableFuture<>();
        Thread syncThread = new Thread(() -> {
            try {
                logWriter.sync();
                syncResultFuture.complete(null);
            } catch (IOException e) {
                syncResultFuture.completeExceptionally(e);
            }
        });
        syncThread.start();

        // Verify the sequence: append, sync (fail), rotate, append (retry), sync (succeed)
        InOrder inOrder = Mockito.inOrder(writerBeforeRoll, writerAfterRoll, logManager);

        // 1. Append to the first writer
        inOrder.verify(writerBeforeRoll, times(1)).append(eq(tableName), eq(commitId1), eq(put1));
        // 2. First sync attempt fails
        inOrder.verify(writerBeforeRoll, times(1)).sync();
        // 3. Log rotation is triggered due to failure
        inOrder.verify(logManager, times(1)).rotateLog(); // Verify rotation was called

        // Wait for the sync() call itself to complete successfully
        syncResultFuture.get();

        // 4. Append is retried on the second writer
        inOrder.verify(writerAfterRoll, times(1)).append(eq(tableName), eq(commitId1), eq(put1));
        // 5. Second sync attempt succeeds
        inOrder.verify(writerAfterRoll, times(1)).sync();

        logWriter.close();
    }

}
