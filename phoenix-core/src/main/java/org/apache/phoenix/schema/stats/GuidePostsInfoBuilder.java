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

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.query.KeyRange;

/**
 * Builder to help in adding guidePosts and building guidePostInfo.
 * This is used when we are collecting stats or reading stats for a table.
 */
public class GuidePostsInfoBuilder {
    /**
     * The targeted size of the guide post chunk measured in the number of guide posts.
     */
    private final int targetedChunkSize;

    /**
     * Guide posts organized in chunks
     */
    private List<GuidePostChunk> guidePostChunks;

    /**
     * Builder for the guide post chunk
     */
    private GuidePostChunkBuilder chunkBuilder;

    /**
     * The index of the guide post chunk in the chunk array
     */
    private int guidePostChunkIndex;

    /**
     * Use to track the lower bound of generated guide post chunks.
     * It's always exclusive.
     */
    private ImmutableBytesWritable lastRow;

    /**
     * The total count of guide posts collected
     */
    private int guidePostsCount;

    public GuidePostsInfoBuilder() {
        this(1);
    }

    public GuidePostsInfoBuilder(int targetedChunkSize){
        this.targetedChunkSize = targetedChunkSize;
        this.guidePostChunks = Lists.newArrayListWithExpectedSize(1);
        this.guidePostChunkIndex = 0;
        this.lastRow = new ImmutableBytesWritable(KeyRange.UNBOUND);
        this.guidePostsCount = 0;
    }

    /**
     * Track a new guide post
     * @param row number of rows in the guidepost
     * @param estimation estimation of the guidepost
     */
    public boolean trackGuidePost(ImmutableBytesWritable row, GuidePostEstimation estimation) {
        if (row.getLength() != 0 && lastRow.compareTo(row) < 0) {
            if (chunkBuilder == null) {
                chunkBuilder = new GuidePostChunkBuilder(lastRow.get(), targetedChunkSize);
            }

            boolean added = chunkBuilder.trackGuidePost(row, estimation);
            if (added) {
                guidePostsCount++;
                if (chunkBuilder.getGuidePostsCount() == targetedChunkSize) {
                    GuidePostChunk chunk = chunkBuilder.build(guidePostChunkIndex);
                    guidePostChunks.add(chunk);
                    guidePostChunkIndex++;
                    chunkBuilder = null;
                }
            }

            lastRow = row;
            return added;
        }

        return false;
    }

    public GuidePostsInfo build() {
        return build(new GuidePostEstimation());
    }

    public GuidePostsInfo build(GuidePostEstimation extraEstimation) {
        if (chunkBuilder != null) {
            GuidePostChunk chunk = chunkBuilder.build(guidePostChunkIndex);
            guidePostChunks.add(chunk);
            guidePostChunkIndex++;
            chunkBuilder = null;
        }

        byte[] lowerBound = KeyRange.UNBOUND;
        if (guidePostChunks.size() > 0) {
            lowerBound = guidePostChunks.get(guidePostChunks.size() - 1).getKeyRange().getUpperRange();
        }

        GuidePostChunk endingChunk = GuidePostChunkBuilder.buildEndingChunk(guidePostChunkIndex, lowerBound, extraEstimation);
        guidePostChunks.add(endingChunk);
        guidePostChunkIndex++;

        return new GuidePostsInfo(null, guidePostsCount, guidePostChunks);
    }

    public int getGuidePostsCount() {
        return guidePostsCount;
    }
}