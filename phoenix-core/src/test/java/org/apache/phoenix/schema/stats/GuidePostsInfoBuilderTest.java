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
import static org.junit.Assert.assertTrue;

import com.google.common.base.Preconditions;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PInteger;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class GuidePostsInfoBuilderTest {
    private static int getTotalEstimatedSize(List<GuidePostChunk> chunks) {
        int sum = 0;

        for (GuidePostChunk chunk : chunks) {
            sum += chunk.getEstimatedSize();
        }

        return sum;
    }

    private static GuidePostsInfo buildAndVerifyGuidePostIfo(
            int expectedGuidePostsCount, int startKey, int step, int endKey, int guidePostsCountPerChunk) {
        List<GuidePostEstimation> estimations = StatsTestUtil.buildGuidePostEstimationList(
                expectedGuidePostsCount, 3, 5, 7, 1);
        GuidePostsInfo info = StatsTestUtil.buildGuidePostInfo(startKey, step, endKey, guidePostsCountPerChunk, estimations);
        assertEquals(expectedGuidePostsCount, info.getGuidePostsCount());
        assertEquals(getTotalEstimatedSize(info.getGuidePostChunks()), info.getEstimatedSize());
        assertEquals(new GuidePostEstimation(3 * expectedGuidePostsCount, 5 * expectedGuidePostsCount, 7), info.getTotalEstimation());

        return info;
    }

    private static void verifyGuidePostChunksBuilt(
            List<GuidePostChunk> chunks, int startKey, int step, int endKey, int guidePostsCountPerChunk) {
        Preconditions.checkArgument(startKey != Integer.MIN_VALUE);
        Preconditions.checkArgument(endKey != Integer.MAX_VALUE);
        Preconditions.checkArgument((startKey <= endKey) && ((endKey - startKey) % step == 0));
        Preconditions.checkArgument(step >= 1);
        Preconditions.checkArgument(guidePostsCountPerChunk > 0);

        int guidePostsCount = (endKey - startKey) / step + 1;
        int guidePostsChunkCount = guidePostsCount / guidePostsCountPerChunk;
        if ((guidePostsCount % guidePostsCountPerChunk) != 0) {
            guidePostsChunkCount++;
        }
        guidePostsChunkCount++; // Plus ending chunk
        assertEquals(guidePostsChunkCount, chunks.size());

        int lowerBound = Integer.MIN_VALUE;
        int startKeyOfThisChunk = startKey;
        for (int i = 0; i < guidePostsChunkCount - 1; i++) {
            assertEquals(i, chunks.get(i).getGuidePostChunkIndex());

            int guidePostsCountOfThisChunk = guidePostsCountPerChunk;
            if (i == (guidePostsChunkCount - 2)) {
                guidePostsCountOfThisChunk = guidePostsCount - (guidePostsCountPerChunk * i);
            }
            assertEquals(guidePostsCountOfThisChunk, chunks.get(i).getGuidePostsCount());

            int endKeyOfThisChunk = startKeyOfThisChunk + (guidePostsCountOfThisChunk - 1) * step;
            assertEquals(StatsTestUtil.getKeyRange(lowerBound, false, endKeyOfThisChunk, true), chunks.get(i).getKeyRange());

            DecodedGuidePostChunk decodedChunk = chunks.get(i).decode();
            List<byte[]> decodedGuidePosts = decodedChunk.getGuidePosts();
            for (int j = 0; j < guidePostsCountOfThisChunk; j++) {
                int key = startKeyOfThisChunk + step * j;
                assertTrue(Arrays.equals(PInteger.INSTANCE.toBytes(key), decodedGuidePosts.get(j)));
            }

            lowerBound = endKeyOfThisChunk;
            startKeyOfThisChunk = endKeyOfThisChunk + step;
        }

        assertEquals(guidePostsChunkCount - 1, chunks.get(guidePostsChunkCount - 1).getGuidePostChunkIndex());
        assertEquals(StatsTestUtil.getKeyRange(lowerBound, false, Integer.MAX_VALUE, false),
                chunks.get(guidePostsChunkCount - 1).getKeyRange());
    }

    @Test
    public void testGetSetGuidePostsKey() {
        GuidePostsInfoBuilder builder = new GuidePostsInfoBuilder();
        GuidePostsInfo info = builder.build();
        assertEquals(null, info.getGuidePostsKey());

        String tableName = "Table0";
        String columnFamilyName = "CF0";

        info.setGuidePostsKey(new GuidePostsKey(tableName.getBytes(), columnFamilyName.getBytes()));
        assertEquals(new GuidePostsKey(tableName.getBytes(), columnFamilyName.getBytes()), info.getGuidePostsKey());
    }

    @Test
    public void testBuildGuidePostsInfoWithEmptyGuidePostsWithoutExtraEstimation() {
        GuidePostsInfoBuilder builder = new GuidePostsInfoBuilder();

        GuidePostsInfo info = builder.build();
        assertEquals(0, info.getGuidePostsCount());
        assertEquals(getTotalEstimatedSize(info.getGuidePostChunks()), info.getEstimatedSize());
        assertEquals(new GuidePostEstimation(), info.getTotalEstimation());

        List<GuidePostChunk> chunks = info.getGuidePostChunks();
        assertEquals(1, chunks.size());
    }

    @Test
    public void testBuildGuidePostsInfoWithEmptyGuidePostsWithExtraEstimation() {
        GuidePostsInfoBuilder builder = new GuidePostsInfoBuilder();

        GuidePostEstimation extraEstimation = new GuidePostEstimation(1, 2, 3);
        GuidePostsInfo info = builder.build(extraEstimation);

        assertEquals(0, info.getGuidePostsCount());
        assertEquals(getTotalEstimatedSize(info.getGuidePostChunks()), info.getEstimatedSize());
        assertEquals(new GuidePostEstimation(1, 2, 3), info.getTotalEstimation());

        List<GuidePostChunk> chunks = info.getGuidePostChunks();
        assertEquals(1, chunks.size());
    }

    @Test
    public void testBuildGuidePostsInfoWithOneGuidePostOneChunk() {
        GuidePostsInfo info = buildAndVerifyGuidePostIfo(1, 1, 1, 1, 10);
        verifyGuidePostChunksBuilt(info.getGuidePostChunks(), 1, 1, 1, 10);
    }

    @Test
    public void testBuildGuidePostsInfoWithMultipleGuidePostsOneChunk() {
        GuidePostsInfo info = buildAndVerifyGuidePostIfo(2, 1, 2, 3, 10);
        verifyGuidePostChunksBuilt(info.getGuidePostChunks(), 1, 2, 3, 10);

        info = buildAndVerifyGuidePostIfo(9, 1, 2, 17, 10);
        verifyGuidePostChunksBuilt(info.getGuidePostChunks(), 1, 2, 17, 10);

        info = buildAndVerifyGuidePostIfo(10, 1, 2, 19, 10);
        verifyGuidePostChunksBuilt(info.getGuidePostChunks(), 1, 2, 19, 10);
    }

    @Test
    public void testBuildGuidePostsInfoWithMultipleGuidePostsMultipleChunks() {
        // 5 chunks and each chunk contains 1 guide post
        GuidePostsInfo info = buildAndVerifyGuidePostIfo(5, 1, 2, 9, 1);
        verifyGuidePostChunksBuilt(info.getGuidePostChunks(), 1, 2, 9, 1);

        // 5 chunks and each chunk contains 10 guide posts
        info = buildAndVerifyGuidePostIfo(50, 1, 2, 99, 10);
        verifyGuidePostChunksBuilt(info.getGuidePostChunks(), 1, 2, 99, 10);
    }
}
