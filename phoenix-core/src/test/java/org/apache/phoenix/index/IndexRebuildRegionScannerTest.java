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

import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static org.apache.phoenix.coprocessor.IndexRebuildRegionScanner.getPerTaskIndexKeyToMutationMaps;

public class IndexRebuildRegionScannerTest{
    private static final Random RAND = new Random(7);
    @Test
    public void testGetPerTaskIndexKeyToMutationMaps() {
        TreeMap<byte[], List<Mutation>> indexKeyToMutationMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        final int MAP_SIZE = 64*1024;
        final int REPEAT = 16;
        final int MAX_SPLIT_SIZE = 32;
        for (int i = 0; i < MAP_SIZE; i++) {
            byte[] indexKey = Bytes.toBytes(RAND.nextLong());
            indexKeyToMutationMap.put(indexKey, null);
        }
        for (int i = 0; i < REPEAT; i++) {
            byte[][] endKeys = new byte[i + 1][Long.SIZE / Byte.SIZE];
            for (int j = 0; j < i; j++) {
                endKeys[j] = Bytes.toBytes(RAND.nextLong());
            }
            // The last end key is always null
            endKeys[i] = null;
            List<Map<byte[], List<Mutation>>> mapList = getPerTaskIndexKeyToMutationMaps(indexKeyToMutationMap, endKeys, MAX_SPLIT_SIZE);
            int regionIndex = 0;
            int regionCount = i + 1;
            for (Map<byte[], List<Mutation>> map : mapList) {
                // Check map sizes
                Assert.assertTrue(map.size() <= MAX_SPLIT_SIZE);
                // Check map boundaries
                TreeMap<byte[], List<Mutation>> treeMap = (TreeMap<byte[], List<Mutation>>) map;
                byte[] firstKey = treeMap.firstKey();
                // Find the region including the first key of the of the map
                while (regionIndex < regionCount -1  && Bytes.compareTo(firstKey, endKeys[regionIndex]) != 1) {
                    regionIndex++;
                }
                // The last key of the map also must fall into the same region
                if (regionIndex != regionCount - 1) {
                    Assert.assertTrue(Bytes.compareTo(treeMap.lastKey(), endKeys[regionIndex]) != 1);
                }
            }
        }
    }


}
