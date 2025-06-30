package org.apache.phoenix.replication.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.phoenix.replication.ReplicationLogFileTracker;

import java.net.URI;

public class ReplicationLogReplayFileTracker extends ReplicationLogFileTracker {

    public ReplicationLogReplayFileTracker(final Configuration conf, final String haGroupName, final FileSystem fileSystem, final URI rootURI) {
        super(conf, haGroupName, fileSystem, rootURI);
    }

    public static final String IN_SUBDIRECTORY = "in";

    @Override
    protected String getNewLogSubDirectoryName() {
        return IN_SUBDIRECTORY;
    }
}
