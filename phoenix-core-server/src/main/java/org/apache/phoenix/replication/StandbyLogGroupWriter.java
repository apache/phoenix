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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.log.LogFileWriter;
import org.apache.phoenix.replication.log.LogFileWriterContext;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Synchronous replication implementation of ReplicationLogGroupWriter.
 * <p>
 * This class implements synchronous replication to a standby cluster's HDFS. It writes replication
 * logs directly to the standby cluster in synchronous mode, providing immediate consistency for
 * failover scenarios.
 */
public class StandbyLogGroupWriter extends ReplicationLogGroupWriter {

    private static final Logger LOG = LoggerFactory.getLogger(StandbyLogGroupWriter.class);

    private FileSystem standbyFs;
    private URI standbyUrl;
    protected int numShards;
    protected final ConcurrentHashMap<Path, Object> shardMap = new ConcurrentHashMap<>();

    /**
     * Constructor for StandbyLogGroupWriter.
     */
    public StandbyLogGroupWriter(ReplicationLogGroup logGroup) {
        super(logGroup);
        Configuration conf = logGroup.getConfiguration();
        this.numShards = conf.getInt(ReplicationLogGroup.REPLICATION_NUM_SHARDS_KEY,
            ReplicationLogGroup.DEFAULT_REPLICATION_NUM_SHARDS);
        LOG.debug("Created StandbyLogGroupWriter for HA Group: {}", logGroup.getHaGroupName());
    }

    @Override
    protected void initializeFileSystems() throws IOException {
        if (numShards > ReplicationLogGroup.MAX_REPLICATION_NUM_SHARDS) {
            throw new IllegalArgumentException(ReplicationLogGroup.REPLICATION_NUM_SHARDS_KEY
                + " is " + numShards + ", but the limit is "
                + ReplicationLogGroup.MAX_REPLICATION_NUM_SHARDS);
        }
        Configuration conf = logGroup.getConfiguration();
        String standbyUrlString = conf.get(ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY);
        if (standbyUrlString == null || standbyUrlString.trim().isEmpty()) {
            throw new IOException("Standby HDFS URL not configured: "
                + ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY);
        }
        try {
            standbyUrl = new URI(standbyUrlString);
            standbyFs = getFileSystem(standbyUrl);
            LOG.info("Initialized standby filesystem: {}", standbyUrl);
        } catch (URISyntaxException e) {
            throw new IOException("Invalid standby HDFS URL: " + standbyUrlString, e);
        }
    }

    /**
     * Creates a new log file path in a sharded directory structure based on server name and
     * timestamp. The resulting path structure is
     * <pre>
     * [url]/[haGroupId]/[shard]/[timestamp]-[servername].plog
     * </pre>
     */
    protected Path makeWriterPath(FileSystem fs, URI url) throws IOException {
        Path haGroupPath = new Path(url.getPath(), logGroup.getHaGroupName());
        long timestamp = EnvironmentEdgeManager.currentTimeMillis();
        // To have all logs for a given regionserver appear in the same shard, hash only the
        // serverName. However we expect some regionservers will have significantly more load than
        // others so we instead distribute the logs over all of the shards randomly for a more even
        // overall distribution by also hashing the timestamp.
        int shard = Math.floorMod(logGroup.getServerName().hashCode() ^ Long.hashCode(timestamp),
            numShards);
        Path shardPath = new Path(haGroupPath,
            String.format(ReplicationLogGroup.SHARD_DIR_FORMAT, shard));
        // Ensure the shard directory exists. We track which shard directories we have probed or
        // created to avoid a round trip to the namenode for repeats.
        IOException[] exception = new IOException[1];
        shardMap.computeIfAbsent(shardPath, p -> {
            try {
                if (!fs.exists(p)) {
                    fs.mkdirs(haGroupPath); // This probably exists, but just in case.
                    if (!fs.mkdirs(shardPath)) {
                        throw new IOException("Could not create path: " + p);
                    }
                }
            } catch (IOException e) {
                exception[0] = e;
                return null; // Don't cache the path if we can't create it.
            }
            return p;
        });
        // If we faced an exception in computeIfAbsent, throw it
        if (exception[0] != null) {
            throw exception[0];
        }
        Path filePath = new Path(shardPath, String.format(ReplicationLogGroup.FILE_NAME_FORMAT,
            timestamp, logGroup.getServerName()));
        return filePath;
    }

    /** Creates and initializes a new LogFileWriter. */
    protected LogFileWriter createNewWriter() throws IOException {
        Path filePath = makeWriterPath(standbyFs, standbyUrl);
        LogFileWriterContext writerContext = new LogFileWriterContext(logGroup.getConfiguration())
            .setFileSystem(standbyFs)
            .setFilePath(filePath).setCompression(compression);
        LogFileWriter newWriter = new LogFileWriter();
        newWriter.init(writerContext);
        newWriter.setGeneration(writerGeneration.incrementAndGet());
        return newWriter;
    }
}
