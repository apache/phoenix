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

import org.apache.phoenix.optimize.Cost;

/**
 * Utilities for computing costs
 *
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
     * @param isGrouping if this is a grouping aggregate
     * @param aggregatorsSize the byte size of aggregators
     * @return the output byte count
     */
    public static double estimateAggregateOutputBytes(
            double byteCount, boolean isGrouping, int aggregatorsSize) {
        if (!isGrouping) {
            return aggregatorsSize;
        }
        return byteCount * GROUPING_FACTOR;
    }

    /**
     * Estimate the cost of an aggregate.
     * @param byteCount the number of input bytes
     * @param isGrouping if this is a grouping aggregate
     * @param aggregatorsSize the byte size of aggregators
     * @param parallelLevel number of parallel workers or threads
     * @return the cost
     */
    public static Cost estimateAggregateCost(
            double byteCount, boolean isGrouping, int aggregatorsSize, int parallelLevel) {
        double outputBytes = estimateAggregateOutputBytes(
                byteCount, isGrouping, aggregatorsSize);
        return new Cost(0, 0, outputBytes * IO_COST_MULTIPLIER / parallelLevel);
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
}
