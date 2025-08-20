package org.apache.phoenix.replication.reader;

import static org.junit.Assert.assertEquals;

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

public class ReplicationLogReplayFileTrackerTest {

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
    }

    @After
    public void tearDown() throws IOException {
        localFs.delete(new Path(testFolder.getRoot().toURI()), true);
    }

    @Test
    public void testGetNewLogSubDirectoryName() {
        // Create ReplicationLogReplayFileTracker instance
        ReplicationLogReplayFileTracker fileTracker = new ReplicationLogReplayFileTracker(conf, haGroupName, localFs, rootURI);
        
        // Test that it returns the same string as IN_SUBDIRECTORY constant
        String result = fileTracker.getNewLogSubDirectoryName();
        assertEquals("Should return the same string as IN_SUBDIRECTORY constant", 
            ReplicationLogReplayFileTracker.IN_SUBDIRECTORY, result);
    }

}
