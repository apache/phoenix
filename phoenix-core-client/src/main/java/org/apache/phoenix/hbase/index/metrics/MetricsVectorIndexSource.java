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

  /** The kinds of the HNSW metrics. */
  enum HnswMetricKind {
    COUNTER,
    GAUGE,
    HISTOGRAM
  }

  /**
   * The metrics of the local HNSW indexes. The name of each metric is {@code vectorHnsw} and the
   * name of the constant. An update adds its value to a counter, adds its value to a gauge as a
   * signed change, or records its value in a histogram of milliseconds. Each metric has a value for
   * the server. Each metric that is not for the server only also has a value for each index, with
   * the name {@code <metric>.<index>}.
   */
  enum HnswMetric {
    CHANGES_APPLIED("ChangesApplied", HnswMetricKind.COUNTER, false,
      "The number of row changes that HNSW indexes applied to their mutable graphs"),
    MUTABLE_ENTRIES("MutableEntries", HnswMetricKind.GAUGE, false,
      "The number of entries in the mutable graphs of the open HNSW index managers"),
    SPLIT_REQUESTS("SplitRequests", HnswMetricKind.COUNTER, false,
      "The number of region split requests that HNSW indexes sent because writes outpace flushes"),
    REGIONS("Regions", HnswMetricKind.GAUGE, false,
      "The number of regions with an open HNSW index manager"),
    REGIONS_UNCOVERED("RegionsUncovered", HnswMetricKind.GAUGE, false,
      "The number of regions whose HNSW base segments do not cover the region"),
    REGIONS_UNRESTRICTED("RegionsUnrestricted", HnswMetricKind.GAUGE, false,
      "The number of regions that read HNSW scans without candidate restriction"),
    REBUILD_QUEUE("RebuildQueue", HnswMetricKind.GAUGE, true,
      "The number of HNSW full rebuilds that wait in the rebuild queue of the server"),
    REBUILDS("Rebuilds", HnswMetricKind.COUNTER, false,
      "The number of HNSW full rebuilds that cut over to a new base segment"),
    REBUILD_TIME("RebuildTime", HnswMetricKind.HISTOGRAM, false,
      "Histogram for the time in milliseconds from the start of an HNSW full rebuild to its cutover"),
    REBUILD_WAIT_TIME("RebuildWaitTime", HnswMetricKind.HISTOGRAM, false,
      "Histogram for the time in milliseconds that an HNSW full rebuild waits in the queue"),
    OPEN_TIME("OpenTime", HnswMetricKind.HISTOGRAM, false,
      "Histogram for the time in milliseconds to open an HNSW index manager"),
    REPLAYED_ROWS("ReplayedRows", HnswMetricKind.COUNTER, false,
      "The number of changed rows that HNSW index managers replayed at open"),
    SEGMENTS("Segments", HnswMetricKind.GAUGE, false,
      "The number of HNSW segments that the open index managers search"),
    DELTA_FLUSHES("DeltaFlushes", HnswMetricKind.COUNTER, false,
      "The number of HNSW delta segments that cut over"),
    DELTA_FLUSH_TIME("DeltaFlushTime", HnswMetricKind.HISTOGRAM, false,
      "Histogram for the time in milliseconds from the start of an HNSW delta flush to its cutover"),
    SEGMENT_BYTES_WRITTEN("SegmentBytesWritten", HnswMetricKind.COUNTER, false,
      "The number of payload and tombstone bytes of the HNSW segments that regions wrote"),
    STORE_FLUSH_WAIT_TIME("StoreFlushWaitTime", HnswMetricKind.HISTOGRAM, false,
      "Histogram for the time in milliseconds that an HNSW base write waits for its V# flush"),
    OFFHEAP_MAX_BYTES("OffheapMaxBytes", HnswMetricKind.GAUGE, true,
      "The off-heap budget in bytes of the HNSW segments of the server"),
    OFFHEAP_ALLOCATED_BYTES("OffheapAllocatedBytes", HnswMetricKind.GAUGE, true,
      "The number of off-heap bytes that the HNSW segment budget of the server counts"),
    OFFHEAP_RESIDENT_SEGMENTS("OffheapResidentSegments", HnswMetricKind.GAUGE, true,
      "The number of HNSW segment graphs that are resident off-heap"),
    OFFHEAP_EVICTIONS("OffheapEvictions", HnswMetricKind.COUNTER, true,
      "The number of HNSW segment graphs that the off-heap allocator evicted"),
    OFFHEAP_EVICTED_BYTES("OffheapEvictedBytes", HnswMetricKind.COUNTER, true,
      "The number of bytes of the HNSW segment graphs that the off-heap allocator evicted"),
    SEGMENT_LOADS("SegmentLoads", HnswMetricKind.COUNTER, true,
      "The number of HNSW segment graphs that the server loaded off-heap"),
    SEGMENT_LOAD_TIME("SegmentLoadTime", HnswMetricKind.HISTOGRAM, true,
      "Histogram for the time in milliseconds to read and load an HNSW segment graph off-heap"),
    OFFHEAP_REFUSALS("OffheapRefusals", HnswMetricKind.COUNTER, true,
      "The number of off-heap allocations for HNSW segments that failed"),
    SEARCHES("Searches", HnswMetricKind.COUNTER, false,
      "The number of HNSW scans that searched for candidate rows"),
    SEARCH_TIME("SearchTime", HnswMetricKind.HISTOGRAM, false,
      "Histogram for the time in milliseconds of the candidate search of an HNSW scan"),
    NODES_VISITED("NodesVisited", HnswMetricKind.COUNTER, false,
      "The number of graph nodes that HNSW candidate searches visited"),
    PROBE_ROUNDS("ProbeRounds", HnswMetricKind.COUNTER, false,
      "The number of HNSW candidate probes that read candidate rows"),
    PROBE_ROWS_READ("ProbeRowsRead", HnswMetricKind.COUNTER, false,
      "The number of candidate rows that HNSW candidate probes read"),
    UNRESTRICTED_UNCOVERED("UnrestrictedUncovered", HnswMetricKind.COUNTER, false,
      "The number of HNSW scans without candidate restriction because the region is uncovered"),
    UNRESTRICTED_LIMIT("UnrestrictedLimit", HnswMetricKind.COUNTER, false,
      "The number of HNSW scans without candidate restriction because of the candidate limits"),
    MAINTENANCE_FAILURES("MaintenanceFailures", HnswMetricKind.COUNTER, false,
      "The number of HNSW delta flushes and full rebuilds that failed"),
    MAINTENANCE_STALLED("MaintenanceStalled", HnswMetricKind.COUNTER, false,
      "The number of failed HNSW delta flushes and full rebuilds that are not retried"),
    SEGMENTS_LEFT_OUT("SegmentsLeftOut", HnswMetricKind.COUNTER, false,
      "The number of HNSW segments that regions left out because they cannot read them"),
    OPEN_FAILURES("OpenFailures", HnswMetricKind.COUNTER, false,
      "The number of HNSW index manager opens that failed"),
    REPAIR_REQUESTS("RepairRequests", HnswMetricKind.COUNTER, false,
      "The number of HNSW full rebuilds that regions requested to repair their segments");

    private final String metricName;
    private final HnswMetricKind kind;
    private final boolean serverOnly;
    private final String description;

    HnswMetric(String name, HnswMetricKind kind, boolean serverOnly, String description) {
      this.metricName = "vectorHnsw" + name;
      this.kind = kind;
      this.serverOnly = serverOnly;
      this.description = description;
    }

    /** Returns the name of the server value. The value of an index adds "." and the index. */
    public String metricName() {
      return metricName;
    }

    public HnswMetricKind kind() {
      return kind;
    }

    /** Returns true if the metric has no value for each index. */
    public boolean serverOnly() {
      return serverOnly;
    }

    public String description() {
      return description;
    }
  }

  /**
   * The HNSW metrics of one index. A caller gets them once, so that updates do no registry lookup.
   */
  interface HnswIndexMetrics {
    /**
     * Updates the value of the index and the value of the server. A metric for the server only has
     * no value for the index.
     * @param metric the metric
     * @param value  the count to add, the signed change of a gauge, or the time in milliseconds
     */
    void update(HnswMetric metric, long value);
  }

  /**
   * Returns the HNSW metrics of an index. The first call registers the metrics of the index, which
   * stay registered while the server runs.
   * @param indexName Name of the index, without view prefixes
   */
  HnswIndexMetrics getHnswIndexMetrics(String indexName);

  /**
   * Updates the value of the server of an HNSW metric.
   * @param metric the metric
   * @param value  the count to add, the signed change of a gauge, or the time in milliseconds
   */
  void updateHnsw(HnswMetric metric, long value);
}
