package org.apache.phoenix.replication.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.phoenix.end2end.ParallelStatsDisabledIT;
import org.apache.phoenix.replication.ReplicationLogFileTracker;
import org.apache.phoenix.replication.ReplicationLogGroup;
import org.apache.phoenix.replication.ReplicationShardDirectoryManager;
import org.apache.phoenix.replication.ReplicationStateTracker;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class ReplicationReplayLogDiscoveryTest {

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private Configuration conf;
    private FileSystem localFs;
    private URI standbyUri;

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        localFs = FileSystem.getLocal(conf);
        standbyUri = new Path(testFolder.toString()).toUri();
        conf.set(ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY, standbyUri.toString());
    }

    @After
    public void tearDown() throws IOException {
        localFs.delete(new Path(testFolder.getRoot().toURI()), true);
    }

    @Test
    public void testGetExecutorThreadNameFormat() {
        // Create ReplicationReplayLogDiscovery instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, "testGroup", localFs, standbyUri);
        ReplicationStateTracker stateTracker = new ReplicationStateTracker();
        ReplicationReplayLogDiscovery discovery = new ReplicationReplayLogDiscovery(fileTracker, stateTracker);
        
        // Test that it returns the expected constant value
        String result = discovery.getExecutorThreadNameFormat();
        assertEquals("Should return the expected thread name format", 
            "Phoenix-Replication-Replay-%d", result);
    }

    @Test
    public void testGetReplayIntervalSeconds() {
        // Create ReplicationReplayLogDiscovery instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, "testGroup", localFs, standbyUri);
        ReplicationStateTracker stateTracker = new ReplicationStateTracker();
        ReplicationReplayLogDiscovery discovery = new ReplicationReplayLogDiscovery(fileTracker, stateTracker);
        
        // Test default value when no custom config is set
        long defaultResult = discovery.getReplayIntervalSeconds();
        assertEquals("Should return default value when no custom config is set", 
            ReplicationReplayLogDiscovery.DEFAULT_REPLAY_INTERVAL_SECONDS, defaultResult);
        
        // Test custom value when config is set
        conf.setLong(ReplicationReplayLogDiscovery.REPLICATION_REPLAY_INTERVAL_SECONDS_KEY, 120L);
        long customResult = discovery.getReplayIntervalSeconds();
        assertEquals("Should return custom value when config is set", 
            120L, customResult);
    }

    @Test
    public void testGetShutdownTimeoutSeconds() {
        // Create ReplicationReplayLogDiscovery instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, "testGroup", localFs, standbyUri);
        ReplicationStateTracker stateTracker = new ReplicationStateTracker();
        ReplicationReplayLogDiscovery discovery = new ReplicationReplayLogDiscovery(fileTracker, stateTracker);
        
        // Test default value when no custom config is set
        long defaultResult = discovery.getShutdownTimeoutSeconds();
        assertEquals("Should return default value when no custom config is set", 
            ReplicationReplayLogDiscovery.DEFAULT_SHUTDOWN_TIMEOUT_SECONDS, defaultResult);
        
        // Test custom value when config is set
        conf.setLong(ReplicationReplayLogDiscovery.REPLICATION_REPLAY_SHUTDOWN_TIMEOUT_SECONDS_KEY, 45L);
        long customResult = discovery.getShutdownTimeoutSeconds();
        assertEquals("Should return custom value when config is set", 
            45L, customResult);
    }

    @Test
    public void testGetExecutorThreadCount() {
        // Create ReplicationReplayLogDiscovery instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, "testGroup", localFs, standbyUri);
        ReplicationStateTracker stateTracker = new ReplicationStateTracker();
        ReplicationReplayLogDiscovery discovery = new ReplicationReplayLogDiscovery(fileTracker, stateTracker);
        
        // Test default value when no custom config is set
        int defaultResult = discovery.getExecutorThreadCount();
        assertEquals("Should return default value when no custom config is set", 
            ReplicationReplayLogDiscovery.DEFAULT_EXECUTOR_THREAD_COUNT, defaultResult);
        
        // Test custom value when config is set
        conf.setInt(ReplicationReplayLogDiscovery.REPLICATION_REPLAY_EXECUTOR_THREAD_COUNT_KEY, 3);
        int customResult = discovery.getExecutorThreadCount();
        assertEquals("Should return custom value when config is set", 
            3, customResult);
    }

    @Test
    public void testGetInProgressDirectoryProcessProbability() {
        // Create ReplicationReplayLogDiscovery instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, "testGroup", localFs, standbyUri);
        ReplicationStateTracker stateTracker = new ReplicationStateTracker();
        ReplicationReplayLogDiscovery discovery = new ReplicationReplayLogDiscovery(fileTracker, stateTracker);
        
        // Test default value when no custom config is set
        double defaultResult = discovery.getInProgressDirectoryProcessProbability();
        assertEquals("Should return default value when no custom config is set", 
            ReplicationReplayLogDiscovery.DEFAULT_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY, defaultResult, 0.001);
        
        // Test custom value when config is set
        conf.setDouble(ReplicationReplayLogDiscovery.REPLICATION_REPLAY_IN_PROGRESS_DIRECTORY_PROCESSING_PROBABILITY_KEY, 10.5);
        double customResult = discovery.getInProgressDirectoryProcessProbability();
        assertEquals("Should return custom value when config is set", 
            10.5, customResult, 0.001);
    }

    @Test
    public void testGetWaitingBufferPercentage() {
        // Create ReplicationReplayLogDiscovery instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, "testGroup", localFs, standbyUri);
        ReplicationStateTracker stateTracker = new ReplicationStateTracker();
        ReplicationReplayLogDiscovery discovery = new ReplicationReplayLogDiscovery(fileTracker, stateTracker);
        
        // Test default value when no custom config is set
        double defaultResult = discovery.getWaitingBufferPercentage();
        assertEquals("Should return default value when no custom config is set", 
            ReplicationReplayLogDiscovery.DEFAULT_WAITING_BUFFER_PERCENTAGE, defaultResult, 0.001);
        
        // Test custom value when config is set
        conf.setDouble(ReplicationReplayLogDiscovery.REPLICATION_REPLAY_WAITING_BUFFER_PERCENTAGE_KEY, 20.0);
        double customResult = discovery.getWaitingBufferPercentage();
        assertEquals("Should return custom value when config is set", 
            20.0, customResult, 0.001);
    }

}
