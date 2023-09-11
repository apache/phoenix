/**
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
package org.apache.phoenix.monitoring.connectionqueryservice;

import static org.apache.phoenix.query.QueryServices.CONNECTION_QUERY_SERVICE_HISTOGRAM_SIZE_RANGES;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtilHelper;
import org.apache.phoenix.monitoring.RangeHistogram;
import org.apache.phoenix.query.QueryServices;

/**
 * Histogram for calculating phoenix connection. We read ranges using
 * config property {@link QueryServices#PHOENIX_HISTOGRAM_SIZE_RANGES}.
 * If this property is not set then it will default to DEFAULT_RANGE values.
 */
public class ConnectionQueryServicesHistogram extends RangeHistogram {
    static final long[] DEFAULT_RANGE = {1, 10, 100, 500, 1000};
    public ConnectionQueryServicesHistogram(String name, String description, Configuration conf) {
        super(initializeRanges(conf), name, description);
    }

    private static long[] initializeRanges(Configuration conf) {
        long[] ranges = PhoenixConfigurationUtilHelper.getLongs(
                conf, CONNECTION_QUERY_SERVICE_HISTOGRAM_SIZE_RANGES);
        return ranges != null ? ranges : DEFAULT_RANGE;
    }
}
