/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DecodedGuidePostChunkCacheTest {
    private static boolean verifyDecodeGuidePostChunkCache(
            DecodedGuidePostChunkCache cache, List<Integer> expectedGuidePostChunkIndexes) {
        List<DecodedGuidePostChunk> chunks = cache.getCachedDecodedGuidePostChunks();

        if (expectedGuidePostChunkIndexes.size() != chunks.size()) {
            return false;
        }

        for (int i = 0; i < chunks.size(); i++) {
            if (chunks.get(i).getGuidePostChunkIndex() != expectedGuidePostChunkIndexes.get(i)) {
                return false;
            }
        }

        return true;
    }

    @Test
    public void testGetOperationBoundaryTest() {
        GuidePostsInfo info = StatsTestUtil.buildGuidePostInfo(1, 1, 100, 10);
        List<GuidePostChunk> chunks = info.getGuidePostChunks();
        DecodedGuidePostChunkCache cache = new DecodedGuidePostChunkCache(chunks);

        DecodedGuidePostChunk chunk = cache.get(-5, false);
        assertEquals(null, chunk);
        chunk = cache.get(-1, false);
        assertEquals(null, chunk);
        chunk = cache.get(chunks.size(), false);
        assertEquals(null, chunk);
        chunk = cache.get(chunks.size() + 5, false);
        assertEquals(null, chunk);
        chunk = cache.get(0, false);
        assertEquals(0, chunk.getGuidePostChunkIndex());
        chunk = cache.get(1, false);
        assertEquals(1, chunk.getGuidePostChunkIndex());
        chunk = cache.get(5, false);
        assertEquals(5, chunk.getGuidePostChunkIndex());
        chunk = cache.get(chunks.size() - 1, false);
        assertEquals(chunks.size() - 1, chunk.getGuidePostChunkIndex());
    }

    @Test
    public void testGetOperationWithRemovePreviousEntryModeDisabled() {
        GuidePostsInfo info = StatsTestUtil.buildGuidePostInfo(1, 1, 100, 10);
        List<GuidePostChunk> chunks = info.getGuidePostChunks();
        DecodedGuidePostChunkCache cache = new DecodedGuidePostChunkCache(chunks);

        DecodedGuidePostChunk chunk0 = cache.get(0, false);
        assertEquals(0, chunk0.getGuidePostChunkIndex());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0))));
        DecodedGuidePostChunk chunk1 = cache.get(1, false);
        assertEquals(1, chunk1.getGuidePostChunkIndex());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0, 1))));
        DecodedGuidePostChunk chunk10 = cache.get(10, false);
        assertEquals(10, chunk10.getGuidePostChunkIndex());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0, 1, 10))));
        DecodedGuidePostChunk chunk8 = cache.get(8, false);
        assertEquals(8, chunk8.getGuidePostChunkIndex());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0, 1, 8, 10))));
        DecodedGuidePostChunk chunk9 = cache.get(9, false);
        assertEquals(9, chunk9.getGuidePostChunkIndex());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0, 1, 8, 9, 10))));
        DecodedGuidePostChunk chunk5 = cache.get(5, false);
        assertEquals(5, chunk5.getGuidePostChunkIndex());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0, 1, 5, 8, 9, 10))));

        // All the entries to get have already been in the cache and the get operation shouldn't remove any entry.
        DecodedGuidePostChunk chunk = cache.get(0, false);
        assertEquals(chunk0.hashCode(), chunk.hashCode());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0, 1, 5, 8, 9, 10))));
        chunk = cache.get(1, false);
        assertEquals(chunk1.hashCode(), chunk.hashCode());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0, 1, 5, 8, 9, 10))));
        chunk = cache.get(5, false);
        assertEquals(chunk5.hashCode(), chunk.hashCode());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0, 1, 5, 8, 9, 10))));
        chunk = cache.get(8, false);
        assertEquals(chunk8.hashCode(), chunk.hashCode());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0, 1, 5, 8, 9, 10))));
        chunk = cache.get(9, false);
        assertEquals(chunk9.hashCode(), chunk.hashCode());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0, 1, 5, 8, 9, 10))));
        chunk = cache.get(10, false);
        assertEquals(chunk10.hashCode(), chunk.hashCode());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0, 1, 5, 8, 9, 10))));
    }

    @Test
    public void testGetOperationWithRemovePreviousEntryModeEnabled() {
        GuidePostsInfo info = StatsTestUtil.buildGuidePostInfo(1, 1, 100, 10);
        List<GuidePostChunk> chunks = info.getGuidePostChunks();
        DecodedGuidePostChunkCache cache = new DecodedGuidePostChunkCache(chunks);

        DecodedGuidePostChunk chunk0 = cache.get(0, true);
        assertEquals(0, chunk0.getGuidePostChunkIndex());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0))));
        DecodedGuidePostChunk chunk1 = cache.get(1, true);
        assertEquals(1, chunk1.getGuidePostChunkIndex());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(1))));
        DecodedGuidePostChunk chunk10 = cache.get(10, true);
        assertEquals(10, chunk10.getGuidePostChunkIndex());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(10))));
        DecodedGuidePostChunk chunk8 = cache.get(8, true);
        assertEquals(8, chunk8.getGuidePostChunkIndex());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(8, 10))));
        DecodedGuidePostChunk chunk9 = cache.get(9, true);
        assertEquals(9, chunk9.getGuidePostChunkIndex());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(9, 10))));
        DecodedGuidePostChunk chunk5 = cache.get(5, true);
        assertEquals(5, chunk5.getGuidePostChunkIndex());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(5, 9, 10))));

        // {5, 9 , 10} now are in the cache and every get operation should remove the entries with the index
        // <= the index to get.
        DecodedGuidePostChunk chunk = cache.get(0, true);
        assertEquals(chunk0.hashCode(), chunk.hashCode());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(0, 5, 9, 10))));
        chunk = cache.get(1, true);
        assertEquals(chunk1.hashCode(), chunk.hashCode());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(1, 5, 9, 10))));
        chunk = cache.get(5, true);
        assertEquals(chunk5.hashCode(), chunk.hashCode());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(5, 9, 10))));
        chunk = cache.get(8, true);
        assertEquals(chunk8.hashCode(), chunk.hashCode());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(8, 9, 10))));
        chunk = cache.get(9, true);
        assertEquals(chunk9.hashCode(), chunk.hashCode());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(9, 10))));
        chunk = cache.get(10, true);
        assertEquals(chunk10.hashCode(), chunk.hashCode());
        assertTrue(verifyDecodeGuidePostChunkCache(cache, new ArrayList<Integer>(Arrays.asList(10))));
    }
}
