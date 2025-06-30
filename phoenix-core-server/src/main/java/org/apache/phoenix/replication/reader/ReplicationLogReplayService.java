package org.apache.phoenix.replication.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.ReplicationLogDiscovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class ReplicationLogReplayService {

    private static volatile ReplicationLogReplayService instance;
    private final Configuration conf;

    private ReplicationLogReplayService(final Configuration conf) {
        // TODO Check if replication replay service is enabled via config
        this.conf = conf;
    }

    /**
     * Gets the singleton instance of the ReplicationLogReplayService using the lazy initializer pattern.
     * Initializes the instance if it hasn't been created yet.
     * @param conf Configuration object.
     * @return The singleton ReplicationLogManager instance.
     * @throws IOException If initialization fails.
     */
    public static ReplicationLogReplayService getInstance(Configuration conf)
            throws IOException {
        if (instance == null) {
            synchronized (ReplicationLogReplayService.class) {
                if (instance == null) {
                    instance = new ReplicationLogReplayService(conf);
                }
            }
        }
        return instance;
    }

    public void start() throws IOException {
        // TODO: Ensure service is not already started
        List<String> replicationGroups = getReplicationGroups();
        for(String replicationGroup : replicationGroups) {
            ReplicationReplay.get(conf, replicationGroup).startReplay();
        }
    }

    public void stop() throws IOException {
        // Stop log replay for all groups
        List<String> replicationGroups = getReplicationGroups();
        for(String replicationGroup : replicationGroups) {
            ReplicationReplay.get(conf, replicationGroup).stopReplay();
        }
    }

    protected List<String> getReplicationGroups() {
        // TODO: Return list of replication groups using HAGroupStoreClient
        return new ArrayList<>();
    }
}
