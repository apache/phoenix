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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Manages shard-based directory structure for Phoenix replication log files.
 *
 * This class manages mapping between replication log files and different shard directories based on timestamp.
 * The root directory could be IN (on standby cluster) or OUT(on active cluster) and it manages shard interaction within given root directory.
 * <p><strong>Directory Structure:</strong></p>
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
     * Default number of shard directories. Assuming 400 workers on standby writing replication log files every 1 min,
     * and a lag of 2 days, number of files would be 400 * 2 * 24 * 60 = 1152000 files. Each shard will have
     * (1152000 / 128) = 9000 files which is very well manageable for single HDFS directory
     */
    public static final int DEFAULT_REPLICATION_NUM_SHARDS = 128;

    /**
     * Format string for shard directory names. Uses 3-digit zero-padded format (e.g., "000", "001", "002").
     */
    public static final String SHARD_DIR_FORMAT = "%03d";

    /**
     * Configuration key for the duration of each replication round in seconds.
     */
    public static final String PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY = "phoenix.replication.round.duration.seconds";

    /**
     * Default duration of each replication round in seconds.
     * Files with timestamps within the same 60-second window will be placed in the same shard directory.
     * This provides a good balance between file distribution and processing efficiency.
     */
    public static final int DEFAULT_REPLICATION_ROUND_DURATION_SECONDS = 60;

    private static final String REPLICATION_SHARD_SUB_DIRECTORY_NAME = "shard";

    private final int numShards;

    private final int replicationRoundDurationSeconds;

    private final Path shardDirectoryPath;

    public ReplicationShardDirectoryManager(final Configuration conf, final Path rootPath) {
        this.shardDirectoryPath = new Path(rootPath.toUri().getPath(), REPLICATION_SHARD_SUB_DIRECTORY_NAME);
        this.numShards = conf.getInt(REPLICATION_NUM_SHARDS_KEY, DEFAULT_REPLICATION_NUM_SHARDS);
        this.replicationRoundDurationSeconds = conf.getInt(PHOENIX_REPLICATION_ROUND_DURATION_SECONDS_KEY, DEFAULT_REPLICATION_ROUND_DURATION_SECONDS);
    }

    /**
     * Returns the shard directory to which file with given timestamp belongs to based on round time period
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
     * Returns a ReplicationRound object based on the given round start time, calculating the end time as start time + round duration.
     * @param roundStartTime - start time of the given round.
     * @return The round to which input roundStartTime belongs to
     */
    public ReplicationRound getReplicationRoundFromStartTime(long roundStartTime) {
        long validRoundStartTime = getNearestRoundStartTimestamp(roundStartTime);
        long validRoundEndTime = validRoundStartTime + replicationRoundDurationSeconds * 1000L;
        return new ReplicationRound(validRoundStartTime, validRoundEndTime);
    }

    /**
     * Returns a ReplicationRound object based on the given round end time, calculating the start time as end time - round duration.
     * @param roundEndTime - end time of the given round.
     * @return The round to which input roundEndTime belongs to
     */
    public ReplicationRound getReplicationRoundFromEndTime(long roundEndTime) {
        long validRoundEndTime = getNearestRoundStartTimestamp(roundEndTime);
        long validRoundStartTime = validRoundEndTime - replicationRoundDurationSeconds * 1000L;
        return new ReplicationRound(validRoundStartTime, validRoundEndTime);
    }

    /** Returns a list of all shard directory paths, formatted with 3-digit zero-padded shard numbers. */
    public List<Path> getAllShardPaths() {
        List<Path> shardPaths = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            String shardDirName = String.format(SHARD_DIR_FORMAT, i);
            Path shardPath = new Path(shardDirectoryPath, shardDirName);
            shardPaths.add(shardPath);
        }
        return shardPaths;
    }

    public int getReplicationRoundDurationSeconds() {
        return this.replicationRoundDurationSeconds;
    }

    public Path getShardDirectoryPath() {
        return this.shardDirectoryPath;
    }

    public int getNumShards() {
        return this.numShards;
    }

    /**
     * Returns the nearest replication round start timestamp for the given timestamp.
     * @param timestamp The timestamp in milliseconds since epoch
     * @return The nearest replication round start timestamp
     */
    protected long getNearestRoundStartTimestamp(long timestamp) {
        // Convert round time from seconds to milliseconds
        long roundTimeMs = replicationRoundDurationSeconds * 1000L;

        // Calculate the nearest round start timestamp
        // This rounds down to the nearest multiple of round time
        return (timestamp / roundTimeMs) * roundTimeMs;
    }
}
