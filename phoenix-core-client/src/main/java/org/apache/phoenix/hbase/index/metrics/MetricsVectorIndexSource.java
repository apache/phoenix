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
package org.apache.phoenix.hbase.index.metrics;

import org.apache.hadoop.hbase.metrics.BaseSource;

public interface MetricsVectorIndexSource extends BaseSource {
  // Names of the metrics source for Metrics2 and JMX
  String METRICS_NAME = "VectorIndex";
  String METRICS_CONTEXT = "phoenix";
  String METRICS_DESCRIPTION = "Metrics about Phoenix Vector Index Operations";
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  String VECTOR_CENTROID_ASSIGNMENTS = "vectorCentroidAssignments";
  String VECTOR_CENTROID_ASSIGNMENTS_DESC =
    "The number of vectors written to a centroid's posting list by index maintenance";

  String VECTOR_CENTROID_REASSIGNMENTS = "vectorCentroidReassignments";
  String VECTOR_CENTROID_REASSIGNMENTS_DESC =
    "The number of updated vectors index maintenance moved to a different centroid";

  String VECTOR_SCORECARD_FLUSH_TIME = "vectorScorecardFlushTime";
  String VECTOR_SCORECARD_FLUSH_TIME_DESC =
    "Histogram for the time in milliseconds for vector scorecard flushes";

  String VECTOR_SCORECARD_RECONCILE_TIME = "vectorScorecardReconcileTime";
  String VECTOR_SCORECARD_RECONCILE_TIME_DESC =
    "Histogram for the time in milliseconds for vector scorecard reconciliations";

  /**
   * Adds to the count of vectors that index maintenance wrote to a centroid posting list.
   * @param indexName Name of the index
   * @param count     Number of assignments to add
   */
  void incrementVectorCentroidAssignments(String indexName, long count);

  /**
   * Adds to the count of updated vectors that index maintenance moved to a different centroid.
   * @param indexName Name of the index
   * @param count     Number of reassignments to add
   */
  void incrementVectorCentroidReassignments(String indexName, long count);

  /**
   * Records the duration of one scorecard flush in the flush time histogram.
   * @param indexName Name of the index
   * @param timeMs    Duration of the flush in milliseconds
   */
  void updateVectorScorecardFlushTime(String indexName, long timeMs);

  /**
   * Records the duration of one scorecard reconciliation in the reconciliation time histogram.
   * @param indexName Name of the index
   * @param timeMs    Duration of the reconciliation in milliseconds
   */
  void updateVectorScorecardReconcileTime(String indexName, long timeMs);
}
