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
package org.apache.phoenix.replication.reader;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.ReplicationLogTracker;
import org.apache.phoenix.replication.ReplicationShardDirectoryManager;
import org.apache.phoenix.replication.metrics.MetricsReplicationLogTrackerReplayImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages replication replay operations for a specific HA group. Provides singleton instances per
 * group name and orchestrates the initialization of file system, file tracker, state tracker, and
 * log discovery components. It also handles starting and stopping replay operations through the log
 * discovery service.
 */
public class ReplicationLogReplay {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogReplay.class);

  /**
   * The path on the HDFS where log files are to be read.
   */
  public static final String REPLICATION_LOG_REPLAY_HDFS_URL_KEY =
    "phoenix.replication.log.replay.hdfs.url";

  public static final String IN_DIRECTORY_NAME = "in";
  /**
   * Singleton instances per group name
   */
  private static final ConcurrentHashMap<String, ReplicationLogReplay> INSTANCES =
    new ConcurrentHashMap<>();

  private final Configuration conf;
  private final String haGroupName;
  private FileSystem fileSystem;
  private URI rootURI;
  private ReplicationLogDiscoveryReplay replicationLogDiscoveryReplay;

  protected ReplicationLogReplay(final Configuration conf, final String haGroupName) {
    this.conf = conf;
    this.haGroupName = haGroupName;
  }

  /**
   * Gets or creates a singleton instance of ReplicationLogReplay for the specified group name.
   * @param conf        The configuration
   * @param haGroupName The HA group name
   * @return The singleton instance for the group
   */
  public static ReplicationLogReplay get(final Configuration conf, final String haGroupName) {
    return INSTANCES.computeIfAbsent(haGroupName, groupName -> {
      try {
        ReplicationLogReplay instance = new ReplicationLogReplay(conf, groupName);
        instance.init();
        return instance;
      } catch (IOException e) {
        LOG.error("Failed to initialize ReplicationLogReplay for group: " + groupName, e);
        throw new RuntimeException("Failed to initialize ReplicationLogReplay", e);
      }
    });
  }

  /**
   * Delegate the start replay task to the {@link ReplicationLogDiscoveryReplay}
   * @throws IOException - in case the start operation fails
   */
  public void startReplay() throws IOException {
    replicationLogDiscoveryReplay.start();
  }

  /**
   * Delegate the stop replay task to the {@link ReplicationLogDiscoveryReplay}
   * @throws IOException - in case the stop operation fails
   */
  public void stopReplay() throws IOException {
    replicationLogDiscoveryReplay.stop();
  }

  /**
   * Initializes the replication replay components including file system, file tracker, state
   * tracker, and log discovery service. Sets up the complete replay components for the HA group.
   * @throws IOException if there's an error during initialization
   */
  protected void init() throws IOException {
    LOG.info("Initializing ReplicationLogReplay for haGroup: {}", haGroupName);
    initializeFileSystem();
    Path newFilesDirectory =
      new Path(new Path(rootURI.getPath(), haGroupName), ReplicationLogReplay.IN_DIRECTORY_NAME);
    ReplicationShardDirectoryManager replicationShardDirectoryManager =
      new ReplicationShardDirectoryManager(conf, newFilesDirectory);
    ReplicationLogTracker replicationLogReplayFileTracker =
      new ReplicationLogTracker(conf, haGroupName, fileSystem, replicationShardDirectoryManager,
        new MetricsReplicationLogTrackerReplayImpl(haGroupName));
    replicationLogReplayFileTracker.init();
    this.replicationLogDiscoveryReplay =
      new ReplicationLogDiscoveryReplay(replicationLogReplayFileTracker);
    this.replicationLogDiscoveryReplay.init();
  }

  public void close() {
    LOG.info("Closing ReplicationLogReplay for haGroup: {}", haGroupName);
    replicationLogDiscoveryReplay.getReplicationLogFileTracker().close();
    replicationLogDiscoveryReplay.close();
    // Remove the instance from cache
    INSTANCES.remove(haGroupName);
  }

  /** Initializes the filesystem and creates root log directory. */
  private void initializeFileSystem() throws IOException {
    String uriString = conf.get(REPLICATION_LOG_REPLAY_HDFS_URL_KEY);
    if (uriString == null || uriString.isEmpty()) {
      throw new IOException(REPLICATION_LOG_REPLAY_HDFS_URL_KEY + " is not configured");
    }
    try {
      this.rootURI = new URI(uriString);
      this.fileSystem = FileSystem.get(rootURI, conf);
      Path haGroupFilesPath = new Path(rootURI.getPath(), haGroupName);
      if (!fileSystem.exists(haGroupFilesPath)) {
        LOG.info("Creating directory {}", haGroupFilesPath);
        if (!fileSystem.mkdirs(haGroupFilesPath)) {
          throw new IOException("Failed to create directory: " + uriString);
        }
      }
    } catch (URISyntaxException e) {
      throw new IOException(REPLICATION_LOG_REPLAY_HDFS_URL_KEY + " is not valid", e);
    }
  }

  protected ReplicationLogDiscoveryReplay getReplicationReplayLogDiscovery() {
    return this.replicationLogDiscoveryReplay;
  }

  protected FileSystem getFileSystem() {
    return this.fileSystem;
  }

  protected URI getRootURI() {
    return this.rootURI;
  }

  protected String getHaGroupName() {
    return this.haGroupName;
  }
}
