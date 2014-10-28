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
package org.apache.phoenix.schema.stats;

import java.util.SortedMap;

import com.google.common.collect.ImmutableSortedMap;


/*
 * The table is defined on the client side, but it is populated on the server side. The client should not populate any data to the
 * statistics object.
 */
public interface PTableStats {
    public static final PTableStats EMPTY_STATS = new PTableStats() {
        @Override
        public SortedMap<byte[], GuidePostsInfo> getGuidePosts() {
            return ImmutableSortedMap.of();
        }

        @Override
        public int getEstimatedSize() {
            return 0;
        }

        @Override
        public long getTimestamp() {
            return StatisticsCollector.NO_TIMESTAMP;
        }
    };

    /**
     * TODO: Change from TreeMap to Map
     * Returns a tree map of the guide posts collected against a column family
     * @return
     */
    SortedMap<byte[], GuidePostsInfo> getGuidePosts();

    int getEstimatedSize();
    
    long getTimestamp();
}
