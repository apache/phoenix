package org.apache.phoenix.replication.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ReplicationReplayTest {

    @ClassRule
    public static TemporaryFolder testFolder = new TemporaryFolder();

    private Configuration conf;
    private FileSystem localFs;
    private URI rootURI;
    private String haGroupName;

    @Before
    public void setUp() throws IOException {
        conf = HBaseConfiguration.create();
        localFs = FileSystem.getLocal(conf);
        rootURI = new Path(testFolder.getRoot().toString()).toUri();
        haGroupName = "testGroup";
        
        // Set the required configuration for ReplicationReplay
        conf.set(ReplicationReplay.REPLICATION_LOG_REPLAY_HDFS_URL_KEY, rootURI.toString());
    }

    @After
    public void tearDown() throws IOException {
        localFs.delete(new Path(testFolder.getRoot().toURI()), true);
    }

    @Test
    public void testInit() throws IOException {
        // Create TestableReplicationReplay instance
        ReplicationReplay replicationReplay = new ReplicationReplay(conf, haGroupName);
        
        // Call init method
        replicationReplay.init();
        
        // 1. Ensure filesystem and rootURI are initialized correctly
        assertNotNull("FileSystem should be initialized", replicationReplay.getFileSystem());
        assertNotNull("RootURI should be initialized", replicationReplay.getRootURI());
        assertEquals("RootURI should match the configured URI", rootURI, replicationReplay.getRootURI());
        
        // 2. Ensure expected haGroupFilesPath is created
        Path expectedHaGroupFilesPath = new Path(rootURI.getPath(), haGroupName);
        assertTrue("HA group files path should be created", 
            replicationReplay.getFileSystem().exists(expectedHaGroupFilesPath));
        
        // 3. Ensure replicationReplayLogDiscovery is initialized correctly
        assertNotNull("ReplicationReplayLogDiscovery should be initialized", 
            replicationReplay.getReplicationReplayLogDiscovery());
    }

    @Test
    public void testReplicationReplayInstanceCaching() {
        final String haGroupName1 = "testHAGroup_1";
        final String haGroupName2 = "testHAGroup_2";

        // Get instances for the first HA group
        ReplicationReplay group1Instance1 = ReplicationReplay.get(conf, haGroupName1);
        ReplicationReplay group1Instance2 = ReplicationReplay.get(conf, haGroupName1);

        // Verify same instance is returned for same haGroupName
        assertNotNull("ReplicationReplay should not be null", group1Instance1);
        assertNotNull("ReplicationReplay should not be null", group1Instance2);
        assertSame("Same instance should be returned for same haGroup", group1Instance1, group1Instance2);

        // Get instance for a different HA group
        ReplicationReplay group2Instance1 = ReplicationReplay.get(conf, haGroupName2);
        assertNotNull("ReplicationReplay should not be null", group2Instance1);
        assertNotSame("Different instance should be returned for different haGroup", group2Instance1, group1Instance1);

        // Verify multiple calls still return cached instances
        ReplicationReplay group1Instance3 = ReplicationReplay.get(conf, haGroupName1);
        ReplicationReplay group2Instance2 = ReplicationReplay.get(conf, haGroupName2);
        assertSame("Cached instance should be returned", group1Instance3, group1Instance1);
        assertSame("Cached instance should be returned", group2Instance2, group2Instance1);
    }

    @Test
    public void testReplicationReplayCacheRemovalOnClose() {
        final String haGroupName = "testHAGroup";

        // Get initial instance
        ReplicationReplay group1Instance1 = ReplicationReplay.get(conf, haGroupName);
        assertNotNull("ReplicationReplay should not be null", group1Instance1);

        // Verify cached instance is returned
        ReplicationReplay group1Instance2 = ReplicationReplay.get(conf, haGroupName);
        assertSame("Same instance should be returned before close", group1Instance2, group1Instance1);

        // Close the replay instance
        group1Instance1.close();

        // Get instance after close - should be a new instance
        ReplicationReplay group1Instance3 = ReplicationReplay.get(conf, haGroupName);
        assertNotNull("ReplicationReplay should not be null after close", group1Instance3);
        assertNotSame("New instance should be created after close", group1Instance1, group1Instance3);
        assertEquals("HA Group ID should match", haGroupName, group1Instance3.getHaGroupName());

        // Clean up
        group1Instance3.close();
    }

}
