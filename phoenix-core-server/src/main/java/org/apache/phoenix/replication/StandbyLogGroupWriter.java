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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.phoenix.replication.log.LogFileWriter;
import org.apache.phoenix.replication.log.LogFileWriterContext;
import org.apache.phoenix.replication.reader.ReplicationLogReplay;
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
  private Path haGroupLogFilesPath;
  protected final ConcurrentHashMap<Path, Object> shardMap = new ConcurrentHashMap<>();

  /**
   * Constructor for StandbyLogGroupWriter.
   */
  public StandbyLogGroupWriter(ReplicationLogGroup logGroup) {
    super(logGroup);
    LOG.debug("Created StandbyLogGroupWriter for HA Group: {}", logGroup.getHaGroupName());
  }

  @Override
  protected void initializeFileSystems() throws IOException {
    Configuration conf = logGroup.getConfiguration();
    String standbyUrlString = conf.get(ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY);
    if (standbyUrlString == null || standbyUrlString.trim().isEmpty()) {
      throw new IOException(
        "Standby HDFS URL not configured: " + ReplicationLogGroup.REPLICATION_STANDBY_HDFS_URL_KEY);
    }
    try {
      standbyUrl = new URI(standbyUrlString);
      standbyFs = getFileSystem(standbyUrl);
      LOG.info("Initialized standby filesystem: {}", standbyUrl);
    } catch (URISyntaxException e) {
      throw new IOException("Invalid standby HDFS URL: " + standbyUrlString, e);
    }
  }

  @Override
  protected void initializeReplicationShardDirectoryManager() {
    this.haGroupLogFilesPath = new Path(new Path(standbyUrl.getPath(), logGroup.getHaGroupName()),
      ReplicationLogReplay.IN_DIRECTORY_NAME);
    this.replicationShardDirectoryManager =
      new ReplicationShardDirectoryManager(logGroup.getConfiguration(), haGroupLogFilesPath);
  }

  /**
   * Creates a new log file path in a sharded directory structure using
   * {@link ReplicationShardDirectoryManager}. Directory Structure:
   * [root_path]/[ha_group_name]/in/shard/[shard_directory]/[file_name]
   */
  protected Path makeWriterPath(FileSystem fs) throws IOException {
    long timestamp = EnvironmentEdgeManager.currentTimeMillis();
    Path shardPath = replicationShardDirectoryManager.getShardDirectory(timestamp);
    // Ensure the shard directory exists. We track which shard directories we have probed or
    // created to avoid a round trip to the namenode for repeats.
    IOException[] exception = new IOException[1];
    shardMap.computeIfAbsent(shardPath, p -> {
      try {
        if (!fs.exists(p)) {
          fs.mkdirs(haGroupLogFilesPath); // This probably exists, but just in case.
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
    Path filePath = new Path(shardPath,
      String.format(ReplicationLogGroup.FILE_NAME_FORMAT, timestamp, logGroup.getServerName()));
    return filePath;
  }

  /** Creates and initializes a new LogFileWriter. */
  protected LogFileWriter createNewWriter() throws IOException {
    Path filePath = makeWriterPath(standbyFs);
    LogFileWriterContext writerContext = new LogFileWriterContext(logGroup.getConfiguration())
      .setFileSystem(standbyFs).setFilePath(filePath).setCompression(compression);
    LogFileWriter newWriter = new LogFileWriter();
    newWriter.init(writerContext);
    newWriter.setGeneration(writerGeneration.incrementAndGet());
    return newWriter;
  }
}
