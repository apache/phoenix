/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
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
package org.apache.phoenix.monitoring;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.QueryServices;

/**
 * Histogram for calculating sizes (for eg: bytes read, bytes scanned). We read ranges using
 * config property {@link QueryServices#PHOENIX_HISTOGRAM_SIZE_RANGES}. If this property is not set
 * then it will default to {@link org.apache.hadoop.metrics2.lib.MutableSizeHistogram#RANGES}
 * values.
 */
public class SizeHistogram extends RangeHistogram {

    //default range of bins for size Histograms
    protected static long[]
            DEFAULT_RANGE =
            { 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000 };
    public SizeHistogram(String name, String description, Configuration conf) {
        super(initializeRanges(conf), name, description);
        initializeRanges(conf);
    }

    private static long[] initializeRanges(Configuration conf) {
        long[] ranges = PhoenixConfigurationUtil.getLongs(conf,
                QueryServices.PHOENIX_HISTOGRAM_SIZE_RANGES);
        return ranges != null ? ranges : DEFAULT_RANGE;
    }

}