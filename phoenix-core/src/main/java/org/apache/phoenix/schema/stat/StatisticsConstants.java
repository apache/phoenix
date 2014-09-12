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
package org.apache.phoenix.schema.stat;

public class StatisticsConstants {

    public static final String HISTOGRAM_BYTE_DEPTH_CONF_KEY = "org.apache.phoenix.guidepost.width";

    // TODO : What should be the configuration here
    public static final long HISTOGRAM_DEFAULT_BYTE_DEPTH = 512;

    public static final String MIN_STATS_FREQ_UPDATION = "phoenix.query.minStatsFrequencyUpdation";

    public static final long DEFAULT_STATS_FREQ_UPDATION = 2 * 60000;

}
    