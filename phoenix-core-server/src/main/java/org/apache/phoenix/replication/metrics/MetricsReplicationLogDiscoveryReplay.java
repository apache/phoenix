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
package org.apache.phoenix.replication.metrics;

/**
 * Interface for metrics specific to ReplicationLogDiscoveryReplay operations. Extends the base
 * MetricsReplicationLogDiscovery with replay-specific metrics.
 */
public interface MetricsReplicationLogDiscoveryReplay extends MetricsReplicationLogDiscovery {

  String CONSISTENCY_POINT = "consistencyPoint";
  String CONSISTENCY_POINT_DESC =
    "Consistency point timestamp in milliseconds for the HA Group during replay";
  String END_TO_END_REPLAY_LAG = "endToEndReplayLagMs";
  String END_TO_END_REPLAY_LAG_DESC =
    "Histogram of end-to-end replay lag, from when a file's round became eligible for processing "
      + "to when the file finished replaying, in milliseconds. Sampled only on successful replay, "
      + "so a file that keeps failing contributes no sample";
  String PICKUP_LAG = "pickupLagMs";
  String PICKUP_LAG_DESC =
    "Histogram of pickup lag, from when a file's round became eligible for processing to when the "
      + "file was claimed (renamed into the in-progress directory), in milliseconds. Sampled only "
      + "on a file's first claim";

  /**
   * Updates the consistency point metric. The consistency point represents the timestamp up to
   * which all mutations have been replayed and the data is consistent for failover or read
   * operations.
   * @param consistencyPointMs The consistency point timestamp in milliseconds
   */
  void updateConsistencyPoint(long consistencyPointMs);

  /**
   * Records a sample into the end-to-end replay lag histogram: the elapsed time from when a file's
   * round became eligible for processing to when the file finished replaying.
   * <p>
   * Recorded only after a file replays successfully, so it is survivorship-biased: a file that
   * keeps failing on every reclaim contributes no sample even though that is the clearest replay
   * stall. Read it alongside {@code logFileReplayFailureCount}, {@code roundsExceedingRoundTime},
   * and {@code consistencyPoint} staleness, which a healthy-looking lag histogram would otherwise
   * mask.
   * @param lagMs The end-to-end replay lag in milliseconds
   */
  void updateEndToEndReplayLag(long lagMs);

  /**
   * Records a sample into the pickup lag histogram: the elapsed time from when a file's round
   * became eligible for processing to when the file was claimed (renamed into the in-progress
   * directory).
   * <p>
   * Recorded only on a file's first claim (never on a reclaim), so like
   * {@link #updateEndToEndReplayLag(long)} it cannot see a file that is claimed once and then never
   * replays; read it alongside the failure and staleness signals noted there.
   * @param lagMs The pickup lag in milliseconds
   */
  void updatePickupLag(long lagMs);
}
