/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.hbase.index.metrics;

import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * Interface for metrics from GlobalIndexChecker
 */
public interface GlobalIndexCheckerSource extends BaseSource {
    // Metrics2 and JMX constants
    String METRICS_NAME = "GlobalIndexChecker";
    String METRICS_CONTEXT = "phoenix";
    String METRICS_DESCRIPTION = "Metrics about the Phoenix Global Index Checker";
    String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

    String INDEX_REPAIR = "indexRepairs";
    String INDEX_REPAIR_DESC = "The number of index row repairs";

    String INDEX_REPAIR_FAILURE = "indexRepairFailures";
    String INDEX_REPAIR_FAILURE_DESC = "The number of index row repair failures";

    String INDEX_REPAIR_TIME = "indexRepairTime";
    String INDEX_REPAIR_TIME_DESC = "Histogram for the time in milliseconds for index row repairs";

    String INDEX_REPAIR_FAILURE_TIME = "indexRepairFailureTime";
    String INDEX_REPAIR_FAILURE_TIME_DESC = "Histogram for the time in milliseconds for index row repair failures";

    /**
     * Increments the number of index repairs
     */
    void incrementIndexRepairs();

    /**
     * Increments the number of index repair failures
     */
    void incrementIndexRepairFailures();

    /**
     * Updates the index repair time histogram
     *
     * @param t time taken in milliseconds
     */
    void updateIndexRepairTime(long t);

    /**
     * Updates the index repair failure time histogram
     *
     * @param t time taken in milliseconds
     */
    void updateIndexRepairFailureTime(long t);
}