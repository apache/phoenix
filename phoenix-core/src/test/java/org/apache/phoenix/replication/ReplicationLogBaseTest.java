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

import static org.apache.phoenix.replication.ReplicationLogDiscoveryForwarder.REPLICATION_FORWARDER_WAITING_BUFFER_PERCENTAGE_KEY;
import static org.apache.phoenix.replication.ReplicationShardDirectoryManager.PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.jdbc.HAGroupStoreRecord;
import org.apache.phoenix.jdbc.HAGroupStoreRecord.HAGroupState;
import org.apache.phoenix.jdbc.HighAvailabilityPolicy;
import org.apache.phoenix.replication.log.LogFileWriter;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationLogBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogBaseTest.class);

    @ClassRule
    public static TemporaryFolder standbyFolder = new TemporaryFolder();
    @ClassRule
    public static TemporaryFolder localFolder = new TemporaryFolder();
    @Rule
    public TestName name = new TestName();

    protected String haGroupName;
    protected Configuration conf;
    protected ServerName serverName;
    protected FileSystem localFs;
    protected URI standbyUri;
    protected URI fallbackUri;
    @Mock
    protected HAGroupStoreManager haGroupStoreManager;
    protected HAGroupStoreRecord storeRecord;
    protected HAGroupState initialState;
    protected ReplicationLogGroup logGroup;

    static final int TEST_RINGBUFFER_SIZE = 32;
    static final int TEST_SYNC_TIMEOUT = 1000;
    static final int TEST_ROTATION_TIME = 5000;
    static final int TEST_ROTATION_SIZE_BYTES = 10 * 1024;
    static final int TEST_REPLICATION_ROUND_DURATION_SECONDS = 20;

    protected ReplicationLogBaseTest() {
        this(HAGroupState.ACTIVE_IN_SYNC);
    }

    protected ReplicationLogBaseTest(HAGroupState initialState) {
        this.initialState = initialState;
    }

    @Before
    public void setUpBase() throws IOException {
        MockitoAnnotations.initMocks(this);
        haGroupName = name.getMethodName();
        conf = HBaseConfiguration.create();
        localFs = FileSystem.getLocal(conf);
        standbyUri = new Path(standbyFolder.getRoot().toString()).toUri();
        fallbackUri = new Path(localFolder.getRoot().toString()).toUri();
        serverName = ServerName.valueOf("test", 60010, EnvironmentEdgeManager.currentTimeMillis());
        conf.set(ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY, standbyUri.toString());
        conf.set(ReplicationLogGroup.REPLICATION_FALLBACK_HDFS_URL_KEY, fallbackUri.toString());
        // Small ring buffer size for testing
        conf.setInt(ReplicationLogGroup.REPLICATION_LOG_RINGBUFFER_SIZE_KEY, TEST_RINGBUFFER_SIZE);
        // Set a short sync timeout for testing
        conf.setLong(ReplicationLogGroup.REPLICATION_LOG_SYNC_TIMEOUT_KEY, TEST_SYNC_TIMEOUT);
        // Set rotation time to 10 seconds
        conf.setLong(ReplicationLogGroup.REPLICATION_LOG_ROTATION_TIME_MS_KEY, TEST_ROTATION_TIME);
        // Small size threshold for testing
        conf.setLong(ReplicationLogGroup.REPLICATION_LOG_ROTATION_SIZE_BYTES_KEY,
            TEST_ROTATION_SIZE_BYTES);
        // small value of replication round duration
        conf.setInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY,
            TEST_REPLICATION_ROUND_DURATION_SECONDS);
        conf.setDouble(REPLICATION_FORWARDER_WAITING_BUFFER_PERCENTAGE_KEY, 0.0);

        // initialize the group store record
        storeRecord = initHAGroupStoreRecord();
        doReturn(Optional.of(storeRecord)).when(haGroupStoreManager)
                .getHAGroupStoreRecord(anyString());

        logGroup = new TestableLogGroup(conf, serverName, haGroupName, haGroupStoreManager);
        logGroup.init();
    }

    @After
    public void tearDown() throws Exception {
        if (logGroup != null) {
            logGroup.close();
        }
    }

    private HAGroupStoreRecord initHAGroupStoreRecord() {
        return new HAGroupStoreRecord(null, haGroupName, initialState, 0,
                HighAvailabilityPolicy.FAILOVER.toString(), "peerZKUrl", "clusterUrl",
                "peerClusterUrl", 0L);
    }

    static class TestableLogGroup extends ReplicationLogGroup {

        public TestableLogGroup(Configuration conf,
                                ServerName serverName,
                                String haGroupName,
                                HAGroupStoreManager haGroupStoreManager) {
            super(conf, serverName, haGroupName, haGroupStoreManager);
        }

        @Override
        protected ReplicationLog createStandbyLog() throws IOException {
            return spy(new TestableLog(this, standbyShardManager));
        }

        @Override
        protected ReplicationLog createFallbackLog() throws IOException {
            return spy(new TestableLog(this, fallbackShardManager));
        }

    }

    /**
     * Testable version of ReplicationLog that allows spying on the log
     */
    static class TestableLog extends ReplicationLog {

        public TestableLog(ReplicationLogGroup logGroup, ReplicationShardDirectoryManager shardManager) {
            super(logGroup, shardManager);
        }

        @Override
        protected LogFileWriter createNewWriter() throws IOException {
            LogFileWriter writer = super.createNewWriter();
            return spy(writer);
        }
    }
}
