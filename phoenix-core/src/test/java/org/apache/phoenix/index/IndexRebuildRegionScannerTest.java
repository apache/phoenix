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
package org.apache.phoenix.index;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.getPerTaskIndexMutationMaps;

public class IndexRebuildRegionScannerTest{
    private static final Random RAND = new Random(7);
    @Test
    public void testGetPerTaskIndexKeyToMutationMaps() {
        TreeMap<byte[], List<Mutation>> indexKeyToMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        final int MAP_SIZE = 64*1024;
        final int REPEAT = 32;
        final int MAX_SPLIT_SIZE = 32;
        for (int i = 0; i < MAP_SIZE; i++) {
            byte[] indexKey = Bytes.toBytes(RAND.nextLong() % (MAP_SIZE * 11));
            indexKeyToMutationMap.put(indexKey, null);
        }
        for (int i = 1; i <= REPEAT; i++) {
            // i is the number of regions and endKeys are the region end row keys
            byte[][] endKeys = new byte[i][Long.SIZE / Byte.SIZE];
            for (int j = 0; j < i - 1; j++) {
                endKeys[j] = Bytes.toBytes(RAND.nextLong() % (MAP_SIZE * 5)); // 5 vs 11 is to create some imbalance
            }
            // The last end key is always null
            endKeys[i - 1] = null;
            List<Map<byte[], List<Mutation>>> mapList = getPerTaskIndexMutationMaps(indexKeyToMutationMap, endKeys, MAX_SPLIT_SIZE);
            int regionIndex = 0;
            int regionCount = i;
            for (Map<byte[], List<Mutation>> map : mapList) {
                // Check map sizes
                Assert.assertTrue(map.size() <= MAX_SPLIT_SIZE);
                // Check map boundaries
                NavigableMap<byte[], List<Mutation>> treeMap = (NavigableMap<byte[], List<Mutation>>) map;
                byte[] firstKey = treeMap.firstKey();
                // Find the region including the first key of the of the map
                while (regionIndex < regionCount -1  && Bytes.BYTES_COMPARATOR.compare(firstKey, endKeys[regionIndex]) > 0) {
                    regionIndex++;
                }
                // The last key of the map also must fall into the same region
                if (regionIndex != regionCount - 1) {
                    Assert.assertTrue(Bytes.BYTES_COMPARATOR.compare(treeMap.lastKey(), endKeys[regionIndex]) <= 0);
                }
            }
            // The number of splits must be more than or equal to (map size / MAX_SPLIT_SIZE) and must be less than or
            // equal to (map size / MAX_SPLIT_SIZE) + i
            Assert.assertTrue(mapList.size() >= indexKeyToMutationMap.size() / MAX_SPLIT_SIZE);
            Assert.assertTrue(mapList.size() <= (indexKeyToMutationMap.size() / MAX_SPLIT_SIZE) + i);
        }
    }


}
