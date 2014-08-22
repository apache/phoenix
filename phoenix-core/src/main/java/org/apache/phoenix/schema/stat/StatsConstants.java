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

import org.apache.hadoop.hbase.util.Bytes;

public class StatsConstants {
    public static final byte[] MAX_SUFFIX = Bytes.toBytes("max_region_key");
    public static final byte[] MIN_SUFFIX = Bytes.toBytes("min_region_key");
    public final static byte[] MIN_MAX_STAT = Bytes.toBytes("min_max_stat");
    public static final String HISTOGRAM_BYTE_DEPTH_CONF_KEY = "org.apache.phoenix.guidepost.width";
    public final static byte[] EQUAL_DEPTH_HISTOGRAM = Bytes.toBytes("equal_depth_histogram");
    public static final long HISTOGRAM_DEFAULT_BYTE_DEPTH = 100;
    public static final byte[] GUIDE_POSTS_STATS = Bytes.toBytes("guide_posts_stats");
    public static final byte[] GUIDE_POST_SUFFIX = Bytes.toBytes("guide_posts_key");
}

