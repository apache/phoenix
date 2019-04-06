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

import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PInteger;
import org.junit.Test;

import java.util.List;

public class DecodedGuidePostChunkTest {
    @Test
    public void testDecodedEndingChunkWithUnboundedLowerRange() {
        GuidePostEstimation estimation = new GuidePostEstimation();
        GuidePostChunk chunk = GuidePostChunkBuilder.buildEndingChunk(
                0, KeyRange.UNBOUND, estimation);

        DecodedGuidePostChunk decodedChunk = chunk.decode();
        assertEquals(0, decodedChunk.getGuidePostChunkIndex());
        List<byte[]> guidePosts = decodedChunk.getGuidePosts();
        assertEquals(1, guidePosts.size());

        assertEquals(0, decodedChunk.locateGuidePost(KeyRange.UNBOUND, true));
        assertEquals(0, decodedChunk.locateGuidePost(KeyRange.UNBOUND, false));
        assertEquals(0, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(1), true));
        assertEquals(0, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(1), false));
    }

    @Test
    public void testDecodedEndingChunkWithBoundedLowerRange() {
        GuidePostEstimation estimation = new GuidePostEstimation();
        int lowerBound = 10;
        byte[] lowerBoundOfTheChunk = PInteger.INSTANCE.toBytes(lowerBound);
        GuidePostChunk chunk = GuidePostChunkBuilder.buildEndingChunk(
                0, lowerBoundOfTheChunk, estimation);

        DecodedGuidePostChunk decodedChunk = chunk.decode();
        assertEquals(0, decodedChunk.getGuidePostChunkIndex());
        List<byte[]> guidePosts = decodedChunk.getGuidePosts();
        assertEquals(1, guidePosts.size());

        assertEquals(0, decodedChunk.locateGuidePost(KeyRange.UNBOUND, true));
        assertEquals(0, decodedChunk.locateGuidePost(KeyRange.UNBOUND, false));
        assertEquals(0, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(1), true));
        assertEquals(0, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(1), false));
    }

    @Test
    public void testDecodedChunkWithSingleGuidePost() {
        int lowerBound = 0;
        int startKey = 10;
        int step = 1;
        int guidePostsCount = 1;
        GuidePostChunk chunk = StatsTestUtil.buildGuidePostChunk(0, lowerBound, startKey, step, guidePostsCount);
        DecodedGuidePostChunk decodedChunk = chunk.decode();
        List<byte[]> guidePosts = decodedChunk.getGuidePosts();
        assertEquals(guidePostsCount, guidePosts.size());

        assertEquals(0, decodedChunk.locateGuidePost(KeyRange.UNBOUND, true));
        assertEquals(guidePostsCount, decodedChunk.locateGuidePost(KeyRange.UNBOUND, false));
        assertEquals(0, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(startKey - 1), true));
        assertEquals(0, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(startKey - 1), false));
        assertEquals(guidePostsCount, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(startKey), true));
        assertEquals(0, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(startKey), false));
        assertEquals(guidePostsCount, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(startKey + 1), true));
        assertEquals(guidePostsCount, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(startKey + 1), false));
    }

    @Test
    public void testDecodedChunkWithMultipleGuidePosts() {
        int lowerBound = 0;
        int startKey = 10;
        int step = 5;
        int guidePostsCount = 10;
        GuidePostChunk chunk = StatsTestUtil.buildGuidePostChunk(0, lowerBound, startKey, step, guidePostsCount);
        DecodedGuidePostChunk decodedChunk = chunk.decode();
        List<byte[]> guidePosts = decodedChunk.getGuidePosts();
        assertEquals(guidePostsCount, guidePosts.size());

        assertEquals(0, decodedChunk.locateGuidePost(KeyRange.UNBOUND, true));
        assertEquals(guidePostsCount, decodedChunk.locateGuidePost(KeyRange.UNBOUND, false));

        // Test the following cases:
        //
        // case #    isLowerBound    Found exact match (index >= 0)?    index returned
        //   1           Y                      Y                         (ret + 1)
        //     comment for case 1 above:
        //         If lower bound is inclusive, return the ret + 1, as the current guide post only
        //              contains one row, so ignore it.
        //         If lower bound is exclusive, return the ret + 1, as the exclusive key will be
        //              contained in the next guide post and we're matching on the end key of the guide post.
        //   2           Y                      N                         -(ret + 1)
        //   3           N                      Y                            ret
        //   4           N                      N                         -(ret + 1)
        for (int j = 0; j < 3; j += 5) {
            int i = j * 5;
            int key = startKey + step * j * 5;
            assertEquals(i, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(key - 1), true));
            assertEquals(i, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(key - 1), false));
            assertEquals(i + 1, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(key), true));
            assertEquals(i, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(key), false));
            assertEquals(i + 1, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(key + 1), true));
            assertEquals(i + 1, decodedChunk.locateGuidePost(PInteger.INSTANCE.toBytes(key + 1), false));
        }
    }
}