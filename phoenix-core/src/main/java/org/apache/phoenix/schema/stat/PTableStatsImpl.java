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

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.collect.ImmutableMap;


/**
 * Implementation for PTableStats.
 */
public class PTableStatsImpl implements PTableStats {

    // The map for guide posts should be immutable. We only take the current snapshot from outside
    // method call and store it.
    private Map<String, byte[][]> regionGuidePosts;

    public PTableStatsImpl() { }

    public PTableStatsImpl(Map<String, byte[][]> stats) {
        regionGuidePosts = ImmutableMap.copyOf(stats);
    }

    @Override
    public byte[][] getRegionGuidePosts(HRegionInfo region) {
        return regionGuidePosts.get(region.getRegionNameAsString());
    }

    @Override
    public void write(DataOutput output) throws IOException {
        if (regionGuidePosts == null) {
            WritableUtils.writeVInt(output, 0);
            return;
        }
        WritableUtils.writeVInt(output, regionGuidePosts.size());
        for (Entry<String, byte[][]> entry : regionGuidePosts.entrySet()) {
            WritableUtils.writeString(output, entry.getKey());
            byte[][] value = entry.getValue();
            WritableUtils.writeVInt(output, value.length);
            for (int i=0; i<value.length; i++) {
                Bytes.writeByteArray(output, value[i]);
            }
        }
    }
}
