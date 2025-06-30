package org.apache.phoenix.replication.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.ReplicationStateTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;
public class ReplicationReplay {

    private static final Logger LOG = LoggerFactory.getLogger(ReplicationReplay.class);

    /** The path on the HDFS where log files are to be read. */
    public static final String REPLICATION_LOG_REPLAY_HDFS_URL_KEY =
            "phoenix.replication.log.replay.hdfs.url";

    // Singleton instances per group name
    private static final ConcurrentHashMap<String, ReplicationReplay> instances = new ConcurrentHashMap<>();

    private final Configuration conf;
    private final String haGroupName;
    private FileSystem fileSystem;
    private URI rootURI;
    private ReplicationReplayLogDiscovery replicationReplayLogDiscovery;

    private ReplicationReplay(final Configuration conf, final String haGroupName) {
        this.conf = conf;
        this.haGroupName = haGroupName;
    }

    /**
     * Gets or creates a singleton instance of ReplicationReplay for the specified group name.
     * @param conf The configuration
     * @param haGroupName The HA group name
     * @return The singleton instance for the group
     */
    public static ReplicationReplay get(final Configuration conf, final String haGroupName) {
        return instances.computeIfAbsent(haGroupName, groupName -> {
            try {
                ReplicationReplay instance = new ReplicationReplay(conf, groupName);
                instance.init();
                return instance;
            } catch (IOException e) {
                LOG.error("Failed to initialize ReplicationReplay for group: " + groupName, e);
                throw new RuntimeException("Failed to initialize ReplicationReplay", e);
            }
        });
    }

    public void startReplay() throws IOException {
        replicationReplayLogDiscovery.start();
    }

    public void stopReplay() throws IOException {
        replicationReplayLogDiscovery.stop();
    }

    protected void init() throws IOException {
        initializeFileSystem();
        ReplicationLogReplayFileTracker replicationLogReplayFileTracker = new ReplicationLogReplayFileTracker(conf, haGroupName, fileSystem, rootURI);
        replicationLogReplayFileTracker.init();
        ReplicationStateTracker replicationStateTracker = new ReplicationStateTracker();
        replicationStateTracker.init(replicationLogReplayFileTracker);
        this.replicationReplayLogDiscovery = new ReplicationReplayLogDiscovery(replicationLogReplayFileTracker, replicationStateTracker);
    }

    /** Initializes the filesystem and creates root log directory. */
    protected void initializeFileSystem() throws IOException {
        String uriString = conf.get(REPLICATION_LOG_REPLAY_HDFS_URL_KEY);
        if (uriString == null) {
            throw new IOException(REPLICATION_LOG_REPLAY_HDFS_URL_KEY + " is not configured");
        }
        try {
            this.rootURI = new URI(uriString);
            this.fileSystem = FileSystem.get(rootURI, conf);
            Path logDirectoryPath = new Path(rootURI.getPath());
            if (!fileSystem.exists(logDirectoryPath)) {
                LOG.info("Creating directory {}", logDirectoryPath);
                if (!fileSystem.mkdirs(logDirectoryPath)) {
                    throw new IOException("Failed to create directory: " + uriString);
                }
            }
        } catch (URISyntaxException e) {
            throw new IOException(REPLICATION_LOG_REPLAY_HDFS_URL_KEY + " is not valid", e);
        }
    }

    protected ReplicationReplayLogDiscovery getReplicationReplayLogDiscovery() {
        return this.replicationReplayLogDiscovery;
    }
}
