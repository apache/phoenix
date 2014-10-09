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

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;
 
 /**
 * Implementation for PTableStats.
 */
public class PTableStatsImpl implements PTableStats {
    private final SortedMap<byte[], List<byte[]>> guidePosts;

    public PTableStatsImpl() {
        this(new TreeMap<byte[], List<byte[]>>(Bytes.BYTES_COMPARATOR));
    }

    public PTableStatsImpl(SortedMap<byte[], List<byte[]>> guidePosts) {
        this.guidePosts = guidePosts;
    }

    @Override
    public SortedMap<byte[], List<byte[]>> getGuidePosts() {
        return guidePosts;
    }
}
