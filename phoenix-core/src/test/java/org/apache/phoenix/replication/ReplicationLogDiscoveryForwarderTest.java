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
package org.apache.phoenix.replication;

import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.STORE_AND_FORWARD;
import static org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode.SYNC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.apache.phoenix.replication.ReplicationLogGroup.ReplicationMode;
import org.apache.phoenix.replication.log.LogFileTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationLogDiscoveryForwarderTest extends ReplicationLogBaseTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(ReplicationLogDiscoveryForwarderTest.class);

    public ReplicationLogDiscoveryForwarderTest() {
        // we want to start in STORE_AND_FORWARD mode
        super(HAGroupState.ACTIVE_NOT_IN_SYNC);
    }

    @Before
    public void setUp() throws IOException {
        ReplicationMode mode = logGroup.getMode();
        Assert.assertTrue(mode.equals(STORE_AND_FORWARD));
    }

    @After
    public void tearDown() throws IOException {}

    @Test
    public void testLogForwardingAndTransitionBackToSyncMode() throws Exception {
        final String tableName = "TESTTBL";
        final long count = 100L;
        int roundDurationSeconds =
                logGroup.getFallbackShardManager().getReplicationRoundDurationSeconds();

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                // explicitly set the replication mode to SYNC
                logGroup.setMode(SYNC);
                try {
                    logGroup.sync();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return 0L;
            }
        }).when(haGroupStoreManager).setHAGroupStatusToSync(haGroupName);

        for (long id = 1; id <=count; ++id) {
            Mutation put = LogFileTestUtil.newPut("row_" + id, id, 2);
            logGroup.append(tableName, id, put);
        }
        logGroup.sync();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> future = executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    ReplicationLogTracker logTracker =
                            logGroup.getLogForwarder().getReplicationLogTracker();
                    while (true) {
                        try {
                            if (Thread.currentThread().isInterrupted()) {
                                LOG.info("Task interrupted, exiting");
                                return false;
                            }
                            int newFileCount = logTracker.getNewFiles().size();
                            int inProgressCount = logTracker.getInProgressFiles().size();
                            if (newFileCount == 0 && inProgressCount == 0) {
                                // wait for the mode transition to finish
                                Thread.sleep(2000);
                                LOG.info("All files processed");
                                return true;
                            }
                            LOG.info("New files = {} In-progress files = {}",
                                    newFileCount, inProgressCount);
                            Thread.sleep(roundDurationSeconds * 1000);
                        } catch (InterruptedException e) {
                            LOG.info("Task received InterruptedException, exiting");
                            Thread.currentThread().interrupt(); // Re-interrupt the thread
                            return false;
                        }
                    }
                }});
            try {
                Boolean ret = future.get(120, TimeUnit.SECONDS);
                assertTrue(ret);
                // we should have switched back to the SYNC mode
                assertEquals(SYNC, logGroup.getMode());
                // the log forwarder should not be running since we are in SYNC mode
                assertFalse(logGroup.getLogForwarder().isRunning);
            } catch (TimeoutException e) {
                LOG.info("Task timed out, cancelling it");
                future.cancel(true);
                fail("Task timed out");
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Task failed", e);
                fail("Task failed");
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testSyncModeUpdateWaitTime() throws Exception {
        final long[] waitTime = {8L};
        int roundDurationSeconds =
                logGroup.getFallbackShardManager().getReplicationRoundDurationSeconds();

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                long ret = 0L;
                if (waitTime[0] > 0) {
                    ret = waitTime[0];
                    // reset to 0
                    waitTime[0] = 0;
                } else {
                    // explicitly set the replication mode to SYNC
                    logGroup.setMode(SYNC);
                    try {
                        logGroup.sync();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return ret;
            }
        }).when(haGroupStoreManager).setHAGroupStatusToSync(haGroupName);
        Thread.sleep(roundDurationSeconds * 4 * 1000);
        // we should have switched back to the SYNC mode
        assertEquals(SYNC, logGroup.getMode());
    }
}
