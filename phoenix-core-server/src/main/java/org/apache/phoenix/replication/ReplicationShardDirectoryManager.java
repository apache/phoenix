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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Manages shard-based directory structure for Phoenix replication log files. This class manages
 * mapping between replication log files and different shard directories based on timestamp. The
 * root directory could be IN (on standby cluster) or OUT(on active cluster) and it manages shard
 * interaction within given root directory.
 * <p>
 * <strong>Directory Structure:</strong>
 * </p>
 *
 * <pre>
 * /phoenix/replication/<group-id>/in/shard/
 * ├── 000/  (files from 00:00:00-00:01:00)
 * ├── 001/  (files from 00:01:00-00:02:00)
 * ├── 002/  (files from 00:02:00-00:03:00)
 * └── ...   (continues for numShards directories)
 * </pre>
 */
public class ReplicationShardDirectoryManager {

  /**
   * The number of shards (subfolders) to maintain in the "IN" / "OUT" directory.
   */
  public static final String REPLICATION_NUM_SHARDS_KEY = "phoenix.replication.log.shards";

  /**
   * Default number of shard directories. Assuming 400 workers on standby writing replication log
   * files every 1 min, and a lag of 2 days, number of files would be 400 * 2 * 24 * 60 = 1152000
   * files. Each shard will have (1152000 / 128) = 9000 files which is very well manageable for
   * single HDFS directory
   */
  public static final int DEFAULT_REPLICATION_NUM_SHARDS = 128;

  /**
   * Format string for shard directory names. Uses 3-digit zero-padded format (e.g., "000", "001",
   * "002").
   */
  public static final String SHARD_DIR_FORMAT = "%03d";

  /** File extension for replication log files. */
  public static final String LOG_FILE_EXTENSION = ".plog";

  /**
   * Name of the staging subdirectory created inside each shard directory. The forwarder copies an
   * in-flight forwarded log file into &lt;shard&gt;/.staging/&lt;ts&gt;_&lt;server&gt;.plog and
   * then atomically renames it up to &lt;shard&gt;/&lt;ts&gt;_&lt;server&gt;.plog once fully
   * written. Because every replay/tracker listing gates on
   * {@link org.apache.hadoop.fs.FileStatus#isFile()}, this subdirectory (and any mid-copy file
   * within it) is invisible to replay.
   */
  public static final String STAGING_SUB_DIRECTORY_NAME = ".staging";

  /**
   * Format string for log file names. <timestamp>_<servername>.plog Example
   * 1762470665995_localhost,54575,1762470584502.plog
   */
  public static final String FILE_NAME_FORMAT = "%d_%s" + LOG_FILE_EXTENSION;

  /**
   * Configuration key for the duration of each replication round in seconds.
   */
  public static final String PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY =
    "phoenix.replication.round.duration.seconds";

  /**
   * Default duration of each replication round in seconds. Files with timestamps within the same
   * 60-second window will be placed in the same shard directory. This provides a good balance
   * between file distribution and processing efficiency.
   */
  public static final int DEFAULT_REPLICATION_ROUND_DURATION_SECONDS = 60;

  private static final String REPLICATION_SHARD_SUB_DIRECTORY_NAME = "shard";

  private final int numShards;

  private final int replicationRoundDurationSeconds;

  private final Path shardDirectoryPath;

  private final Path rootDirectoryPath;

  private final FileSystem shardFS;

  private final ConcurrentHashMap<Path, Object> shardMap = new ConcurrentHashMap<>();

  public ReplicationShardDirectoryManager(Configuration conf, FileSystem fs, Path rootPath) {
    this.shardFS = fs;
    this.rootDirectoryPath = rootPath;
    this.shardDirectoryPath =
      new Path(rootPath.toUri().getPath(), REPLICATION_SHARD_SUB_DIRECTORY_NAME);
    this.numShards = conf.getInt(REPLICATION_NUM_SHARDS_KEY, DEFAULT_REPLICATION_NUM_SHARDS);
    this.replicationRoundDurationSeconds = conf.getInt(
      PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, DEFAULT_REPLICATION_ROUND_DURATION_SECONDS);
  }

  /**
   * Returns the shard directory to which file with given timestamp belongs to based on round time
   * period
   * @param fileTimestamp The timestamp in milliseconds since epoch
   * @return The shard directory path for the given timestamp
   */
  public Path getShardDirectory(long fileTimestamp) {
    // 1. Figure out how many seconds have passed from start of the day for this file
    // Convert timestamp to seconds since epoch
    long secondsSinceEpoch = fileTimestamp / 1000L;

    // Calculate seconds since start of the day (00:00:00)
    // Get the number of seconds since the start of the current day
    long secondsSinceStartOfDay = secondsSinceEpoch % TimeUnit.DAYS.toSeconds(1);

    // 2. Calculate which shard this timestamp belongs to
    // Each shard represents a time range: 0 to roundTimeSeconds = shard 0,
    // roundTimeSeconds to 2*roundTimeSeconds = shard 1, etc.
    int shardIndex = (int) (secondsSinceStartOfDay / replicationRoundDurationSeconds);

    // Apply modulo to ensure shard index doesn't exceed numShards
    shardIndex = shardIndex % numShards;

    // Create the shard directory path with formatted shard number
    String shardDirName = String.format(SHARD_DIR_FORMAT, shardIndex);
    return new Path(shardDirectoryPath, shardDirName);
  }

  /**
   * Returns the shard directory to which file with given replication round belongs to.
   * @param replicationRound The replication round for which to get the shard directory
   * @return The shard directory path for the given replication round
   */
  public Path getShardDirectory(ReplicationRound replicationRound) {
    return getShardDirectory(replicationRound.getStartTime());
  }

  /**
   * Creates a new log file path in a sharded directory structure File path:
   * [root_path]/[ha_group_name]/[in|out]/shard/[shard_directory]/[file_name]
   * @param timestamp  current time
   * @param serverName name of the server creating the log file
   * @return Path to the replication log file
   */
  public Path getWriterPath(long timestamp, String serverName) throws IOException {
    Path shardPath = getShardDirectory(timestamp);
    // Ensure the shard directory exists. We track which shard directories we have probed or
    // created to avoid a round trip to the namenode for repeats.
    IOException[] exception = new IOException[1];
    shardMap.computeIfAbsent(shardPath, p -> {
      try {
        if (!shardFS.exists(p)) {
          if (!shardFS.mkdirs(shardPath)) {
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
    return new Path(shardPath, String.format(FILE_NAME_FORMAT, timestamp, serverName));
  }

  /**
   * Returns the staging path for a fully-resolved final writer path: {@code
   * <shard>/.staging/<file-name>}. A file staged here keeps its real {@code .plog} name but is
   * invisible to replay because it lives under a subdirectory that every replay/tracker listing
   * skips via {@code FileStatus.isFile()}. The forwarder copies bytes here and then atomically
   * renames up to {@code finalPath} (same shard directory, same FileSystem) to publish. The staging
   * directory is created implicitly by the copy ({@code create()} makes parent dirs), so this is a
   * pure path computation with no namenode round trip.
   * @param finalPath the final replay-eligible path (as returned by
   *                  {@link #getWriterPath(long, String)})
   * @return the staging path {@code <shard>/.staging/<file-name>}
   */
  public Path getStagingPath(Path finalPath) {
    Path stagingDir = new Path(finalPath.getParent(), STAGING_SUB_DIRECTORY_NAME);
    return new Path(stagingDir, finalPath.getName());
  }

  /**
   * Returns a ReplicationRound object based on the given round start time, calculating the end time
   * as start time + round duration.
   * @param roundStartTime - start time of the given round.
   * @return The round to which input roundStartTime belongs to
   */
  public ReplicationRound getReplicationRoundFromStartTime(long roundStartTime) {
    long validRoundStartTime = getNearestRoundStartTimestamp(roundStartTime);
    long validRoundEndTime = validRoundStartTime + replicationRoundDurationSeconds * 1000L;
    return new ReplicationRound(validRoundStartTime, validRoundEndTime);
  }

  /**
   * Returns a ReplicationRound object based on the given round end time, calculating the start time
   * as end time - round duration.
   * @param roundEndTime - end time of the given round.
   * @return The round to which input roundEndTime belongs to
   */
  public ReplicationRound getReplicationRoundFromEndTime(long roundEndTime) {
    long validRoundEndTime = getNearestRoundStartTimestamp(roundEndTime);
    long validRoundStartTime =
      Math.max(0L, validRoundEndTime - replicationRoundDurationSeconds * 1000L);
    return new ReplicationRound(validRoundStartTime, validRoundEndTime);
  }

  /**
   * Returns a list of all shard directory paths, formatted with 3-digit zero-padded shard numbers.
   */
  public List<Path> getAllShardPaths() {
    List<Path> shardPaths = new ArrayList<>();
    for (int i = 0; i < numShards; i++) {
      String shardDirName = String.format(SHARD_DIR_FORMAT, i);
      Path shardPath = new Path(shardDirectoryPath, shardDirName);
      shardPaths.add(shardPath);
    }
    return shardPaths;
  }

  /**
   * Returns the nearest replication round start timestamp for the given timestamp.
   * @param timestamp The timestamp in milliseconds since epoch
   * @return The nearest replication round start timestamp
   */
  public long getNearestRoundStartTimestamp(long timestamp) {
    // Convert round time from seconds to milliseconds
    long roundTimeMs = replicationRoundDurationSeconds * 1000L;

    // Calculate the nearest round start timestamp
    // This rounds down to the nearest multiple of round time
    return (timestamp / roundTimeMs) * roundTimeMs;
  }

  public ReplicationRound getPreviousRound(final ReplicationRound replicationRound) {
    return getReplicationRoundFromEndTime(replicationRound.getStartTime());
  }

  public ReplicationRound getNextRound(final ReplicationRound replicationRound) {
    return getReplicationRoundFromStartTime(replicationRound.getEndTime());
  }

  public int getReplicationRoundDurationSeconds() {
    return this.replicationRoundDurationSeconds;
  }

  public Path getShardDirectoryPath() {
    return this.shardDirectoryPath;
  }

  public Path getRootDirectoryPath() {
    return this.rootDirectoryPath;
  }

  public int getNumShards() {
    return this.numShards;
  }

  public FileSystem getFileSystem() {
    return this.shardFS;
  }
}
