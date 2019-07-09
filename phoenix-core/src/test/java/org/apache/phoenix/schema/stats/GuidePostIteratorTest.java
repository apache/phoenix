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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GuidePostIteratorTest {
    private static void createAndVerifyGuidePostIterator(List<GuidePostChunk> chunks,
            int indexOfFirstChunk, int indexOfFirstGuidePost, int indexOfLastChunk, int indexOfLastGuidePost,
            int expectedStartKey, int step, int expectedEndKey, boolean hitEndingChunk) {
        DecodedGuidePostChunkCache cache = new DecodedGuidePostChunkCache(chunks);
        GuidePostIterator iter = new GuidePostIterator(chunks, cache,
                indexOfFirstChunk, indexOfFirstGuidePost, indexOfLastChunk, indexOfLastGuidePost);

        for (int key = expectedStartKey; key <= expectedEndKey; key += step) {
            assertTrue(iter.hasNext());
            GuidePost guidePost = iter.next();
            byte[] guidePostKey = guidePost.getGuidePostKey();
            byte[] keyInBytes = PInteger.INSTANCE.toBytes(key);
            assertTrue(Arrays.equals(keyInBytes, guidePostKey));
        }

        if (hitEndingChunk) {
            assertTrue(iter.hasNext());
            GuidePost guidePost = iter.next();
            assertTrue(Arrays.equals(ByteUtil.EMPTY_BYTE_ARRAY, guidePost.getGuidePostKey()));
            assertFalse(iter.hasNext());
            guidePost = iter.last();
            assertTrue(Arrays.equals(ByteUtil.EMPTY_BYTE_ARRAY, guidePost.getGuidePostKey()));
            assertFalse(iter.hasNext());
        }
        else {
            GuidePost guidePost = iter.last();
            byte[] expectedEndKeyInBytes = PInteger.INSTANCE.toBytes(expectedEndKey);
            assertTrue(Arrays.equals(expectedEndKeyInBytes, guidePost.getGuidePostKey()));
            assertFalse(iter.hasNext());
        }
    }

    @Test
    public void testGuidePostIteratorWithEndingChunkOnly() {
        GuidePostsInfoBuilder builder = new GuidePostsInfoBuilder();
        GuidePostsInfo info = builder.build();
        List<GuidePostChunk> chunks = info.getGuidePostChunks();

        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 0, 0, 1, -1, true);
    }

    @Test
    public void testGuidePostIteratorWithOneGuidePostOneChunk() {
        GuidePostsInfo info = StatsTestUtil.buildGuidePostInfo(1, 1, 1, 10);
        List<GuidePostChunk> chunks = info.getGuidePostChunks();
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 0, 1, 1, 1, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 1, 0, 1, 1, 1, true);
        createAndVerifyGuidePostIterator(chunks, 1, 0, 1, 0, 1, 1, -1, true);
    }

    @Test
    public void testBuildGuidePostsInfoWithMultipleGuidePostsOneChunk() {
        GuidePostsInfo info = StatsTestUtil.buildGuidePostInfo(1, 1, 2, 10);
        List<GuidePostChunk> chunks = info.getGuidePostChunks();
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 0, 1, 1, 1, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 1, 1, 1, 2, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 1, 0, 1, 1, 2, true);
        createAndVerifyGuidePostIterator(chunks, 0, 1, 0, 1, 2, 1, 2, false);
        createAndVerifyGuidePostIterator(chunks, 0, 1, 1, 0, 2, 1, 2, true);
        createAndVerifyGuidePostIterator(chunks, 1, 0, 1, 0, 2, 1, -1, true);

        info = StatsTestUtil.buildGuidePostInfo(1, 1, 5, 10);
        chunks = info.getGuidePostChunks();
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 0, 1, 1, 1, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 1, 1, 1, 2, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 3, 1, 1, 4, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 4, 1, 1, 5, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 1, 0, 1, 1, 5, true);
        createAndVerifyGuidePostIterator(chunks, 0, 1, 0, 1, 2, 1, 2, false);
        createAndVerifyGuidePostIterator(chunks, 0, 1, 0, 3, 2, 1, 4, false);
        createAndVerifyGuidePostIterator(chunks, 0, 1, 0, 4, 2, 1, 5, false);
        createAndVerifyGuidePostIterator(chunks, 0, 1, 1, 0, 2, 1, 5, true);
        createAndVerifyGuidePostIterator(chunks, 0, 2, 0, 3, 3, 1, 4, false);
        createAndVerifyGuidePostIterator(chunks, 1, 0, 1, 0, 5, 1, -1, true);

        info = StatsTestUtil.buildGuidePostInfo(1, 1, 10, 10);
        chunks = info.getGuidePostChunks();
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 0, 1, 1, 1, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 1, 1, 1, 2, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 5, 1, 1, 6, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 9, 1, 1, 10, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 1, 0, 1, 1, 10, true);
        createAndVerifyGuidePostIterator(chunks, 0, 1, 0, 1, 2, 1, 2, false);
        createAndVerifyGuidePostIterator(chunks, 0, 1, 0, 3, 2, 1, 4, false);
        createAndVerifyGuidePostIterator(chunks, 0, 1, 0, 4, 2, 1, 5, false);
        createAndVerifyGuidePostIterator(chunks, 0, 1, 0, 9, 2, 1, 10, false);
        createAndVerifyGuidePostIterator(chunks, 0, 1, 1, 0, 2, 1, 10, true);
        createAndVerifyGuidePostIterator(chunks, 0, 3, 0, 7, 4, 1, 8, false);
        createAndVerifyGuidePostIterator(chunks, 1, 0, 1, 0, 10, 1, -1, true);
    }

    @Test
    public void testBuildGuidePostsInfoWithMultipleGuidePostsMultipleChunks() {
        GuidePostsInfo info = StatsTestUtil.buildGuidePostInfo(1, 1, 100, 10);
        List<GuidePostChunk> chunks = info.getGuidePostChunks();
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 0, 1, 1, 1, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 1, 1, 1, 2, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 5, 1, 1, 6, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 0, 9, 1, 1, 10, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 1, 0, 1, 1, 11, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 1, 5, 1, 1, 16, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 1, 9, 1, 1, 20, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 9, 0, 1, 1, 91, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 9, 5, 1, 1, 96, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 9, 9, 1, 1, 100, false);
        createAndVerifyGuidePostIterator(chunks, 0, 0, 10, 0, 1, 1, 100, true);

        createAndVerifyGuidePostIterator(chunks, 0, 5, 0, 5, 6, 1, 6, false);
        createAndVerifyGuidePostIterator(chunks, 0, 5, 0, 9, 6, 1, 10, false);
        createAndVerifyGuidePostIterator(chunks, 0, 5, 1, 0, 6, 1, 11, false);
        createAndVerifyGuidePostIterator(chunks, 0, 5, 1, 5, 6, 1, 16, false);
        createAndVerifyGuidePostIterator(chunks, 0, 5, 1, 9, 6, 1, 20, false);
        createAndVerifyGuidePostIterator(chunks, 0, 5, 9, 0, 6, 1, 91, false);
        createAndVerifyGuidePostIterator(chunks, 0, 5, 9, 5, 6, 1, 96, false);
        createAndVerifyGuidePostIterator(chunks, 0, 5, 9, 9, 6, 1, 100, false);
        createAndVerifyGuidePostIterator(chunks, 0, 5, 10, 0, 6, 1, 100, true);

        createAndVerifyGuidePostIterator(chunks, 1, 9, 2, 0, 20, 1, 21, false);
        createAndVerifyGuidePostIterator(chunks, 1, 9, 2, 1, 20, 1, 22, false);
        createAndVerifyGuidePostIterator(chunks, 1, 9, 2, 5, 20, 1, 26, false);
        createAndVerifyGuidePostIterator(chunks, 1, 9, 2, 9, 20, 1, 30, false);
        createAndVerifyGuidePostIterator(chunks, 1, 9, 5, 0, 20, 1, 51, false);
        createAndVerifyGuidePostIterator(chunks, 1, 9, 5, 5, 20, 1, 56, false);
        createAndVerifyGuidePostIterator(chunks, 1, 9, 5, 9, 20, 1, 60, false);
        createAndVerifyGuidePostIterator(chunks, 1, 9, 9, 0, 20, 1, 91, false);
        createAndVerifyGuidePostIterator(chunks, 1, 9, 9, 5, 20, 1, 96, false);
        createAndVerifyGuidePostIterator(chunks, 1, 9, 9, 9, 20, 1, 100, false);
        createAndVerifyGuidePostIterator(chunks, 1, 9, 10, 0, 20, 1, 100, true);

        createAndVerifyGuidePostIterator(chunks, 5, 5, 5, 5, 56, 1, 56, false);
        createAndVerifyGuidePostIterator(chunks, 5, 5, 5, 9, 56, 1, 60, false);
        createAndVerifyGuidePostIterator(chunks, 5, 5, 9, 0, 56, 1, 91, false);
        createAndVerifyGuidePostIterator(chunks, 5, 5, 9, 5, 56, 1, 96, false);
        createAndVerifyGuidePostIterator(chunks, 5, 5, 9, 9, 56, 1, 100, false);
        createAndVerifyGuidePostIterator(chunks, 5, 5, 10, 0, 56, 1, 100, true);

        createAndVerifyGuidePostIterator(chunks, 9, 0, 9, 0, 91, 1, 91, false);
        createAndVerifyGuidePostIterator(chunks, 9, 0, 9, 5, 91, 1, 96, false);
        createAndVerifyGuidePostIterator(chunks, 9, 0, 9, 9, 91, 1, 100, false);
        createAndVerifyGuidePostIterator(chunks, 9, 0, 10, 0, 91, 1, 100, true);

        createAndVerifyGuidePostIterator(chunks, 9, 9, 9, 9, 100, 1, 100, false);
        createAndVerifyGuidePostIterator(chunks, 9, 9, 10, 0, 100, 1, 100, true);

        createAndVerifyGuidePostIterator(chunks, 10, 0, 10, 0, 100, 1, -1, true);
    }
}