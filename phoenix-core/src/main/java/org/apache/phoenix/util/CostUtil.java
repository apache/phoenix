/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util;

import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.optimize.Cost;
import org.apache.phoenix.query.QueryServices;

/**
 * Utilities for computing costs.
 *
 * Some of the methods here should eventually be replaced by a metadata framework which
 * estimates output metrics for each QueryPlan or operation, e.g. row count, byte count,
 * etc.
 */
public class CostUtil {

    // An estimate of the ratio of result data from group-by against the input data.
    private final static double GROUPING_FACTOR = 0.1;

    // Io operations conducted in intermediate evaluations like sorting or aggregation
    // should be counted twice since they usually involve both read and write.
    private final static double IO_COST_MULTIPLIER = 2.0;

    /**
     * Estimate the number of output bytes of an aggregate.
     * @param byteCount the number of input bytes
     * @param groupBy the compiled GroupBy object
     * @param aggregatorsSize the byte size of aggregators
     * @return the output byte count
     */
    public static double estimateAggregateOutputBytes(
            double byteCount, GroupBy groupBy, int aggregatorsSize) {
        if (groupBy.isUngroupedAggregate()) {
            return aggregatorsSize;
        }
        return byteCount * GROUPING_FACTOR;
    }

    /**
     * Estimate the cost of an aggregate.
     * @param byteCount the number of input bytes
     * @param groupBy the compiled GroupBy object
     * @param aggregatorsSize the byte size of aggregators
     * @param parallelLevel number of parallel workers or threads
     * @return the cost
     */
    public static Cost estimateAggregateCost(
            double byteCount, GroupBy groupBy, int aggregatorsSize, int parallelLevel) {
        double outputBytes = estimateAggregateOutputBytes(byteCount, groupBy, aggregatorsSize);
        double orderedFactor = groupBy.isOrderPreserving() ? 0.2 : 1.0;
        return new Cost(0, 0, outputBytes * orderedFactor * IO_COST_MULTIPLIER / parallelLevel);
    }

    /**
     * Estimate the cost of an order-by
     * @param byteCount the number of input bytes
     * @param parallelLevel number of parallel workers or threads
     * @return the cost
     */
    public static Cost estimateOrderByCost(double byteCount, int parallelLevel) {
        return new Cost(0, 0, byteCount * IO_COST_MULTIPLIER / parallelLevel);
    }

    /**
     * Estimate the parallel level of an operation
     * @param runningOnServer if the operation will be running on server side
     * @param services the QueryServices object
     * @return the parallel level
     */
    public static int estimateParallelLevel(boolean runningOnServer, QueryServices services) {
        // TODO currently return constants for simplicity, should derive from cluster config.
        return runningOnServer ? 10 : 1;
    }
}
