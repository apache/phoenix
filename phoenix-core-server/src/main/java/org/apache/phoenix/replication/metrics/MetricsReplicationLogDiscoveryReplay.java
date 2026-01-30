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

  /**
   * Updates the consistency point metric. The consistency point represents the timestamp up to
   * which all mutations have been replayed and the data is consistent for failover or read
   * operations.
   * @param consistencyPointMs The consistency point timestamp in milliseconds
   */
  void updateConsistencyPoint(long consistencyPointMs);
}
