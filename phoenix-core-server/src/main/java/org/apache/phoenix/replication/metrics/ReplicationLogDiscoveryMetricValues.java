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
 * Class to hold the values of all metrics tracked by the ReplicationLogDiscovery metrics source.
 */
public class ReplicationLogDiscoveryMetricValues {

  private final long numRoundsProcessed;
  private final long numInProgressDirectoryProcessed;
  private final long timeToProcessNewFilesMs;
  private final long timeToProcessInProgressFilesMs;

  public ReplicationLogDiscoveryMetricValues(long numRoundsProcessed,
    long numInProgressDirectoryProcessed, long timeToProcessNewFilesMs,
    long timeToProcessInProgressFilesMs) {
    this.numRoundsProcessed = numRoundsProcessed;
    this.numInProgressDirectoryProcessed = numInProgressDirectoryProcessed;
    this.timeToProcessNewFilesMs = timeToProcessNewFilesMs;
    this.timeToProcessInProgressFilesMs = timeToProcessInProgressFilesMs;
  }

  public long getNumRoundsProcessed() {
    return numRoundsProcessed;
  }

  public long getNumInProgressDirectoryProcessed() {
    return numInProgressDirectoryProcessed;
  }

  public long getTimeToProcessNewFilesMs() {
    return timeToProcessNewFilesMs;
  }

  public long getTimeToProcessInProgressFilesMs() {
    return timeToProcessInProgressFilesMs;
  }

}
