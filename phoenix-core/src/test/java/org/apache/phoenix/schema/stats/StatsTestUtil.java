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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.PrefixByteEncoder;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

import java.io.DataOutputStream;
import java.util.Collections;
import java.util.List;

public class StatsTestUtil {
    public static final int DEFAULT_BYTE_COUNT_PER_GUIDEPOST = 3;
    public static final int DEFAULT_ROW_COUNT_PER_GUIDEPOST = 5;
    public static final int DEFAULT_START_LEAST_UPDATE_TIMESTAMP = 7;
    public static final int DEFAULT_INCREMENT_OF_LEAST_UPDATE_TIMESTAMP = 1;

    /**
     * Convert the boundary in Integer into bytes. if it is Integer.MIN_VALUE or Integer.MAX_VALUE
     * return KeyRange.UNBOUND.
     *
     * @param boundary
     * @return
     */
    public static byte[] getBoundary(int boundary) {
        return (boundary == Integer.MIN_VALUE || boundary == Integer.MAX_VALUE) ?
                KeyRange.UNBOUND : PInteger.INSTANCE.toBytes(boundary);
    }

    /**
     * Return a KeyRange object. If lower is Integer.MIN_VALUE, the lower range is exclusive,
     * otherwise it is inclusive. The upper range is always exclusive.
     *
     * @param lower
     * @param upper
     * @return
     */
    public static KeyRange getKeyRange(int lower, int upper) {
        boolean isLowerInclusive = (lower != Integer.MIN_VALUE);
        return KeyRange.getKeyRange(getBoundary(lower), isLowerInclusive, getBoundary(upper), false);
    }

    /**
     * Return a KeyRange object.
     *
     * @param lower
     * @param isLowerInclusive
     * @param upper
     * @param isUpperInclusive
     * @return
     */
    public static KeyRange getKeyRange(int lower, boolean isLowerInclusive, int upper, boolean isUpperInclusive) {
        return KeyRange.getKeyRange(getBoundary(lower), isLowerInclusive, getBoundary(upper), isUpperInclusive);
    }

    /**
     * Build guide post estimation list
     *
     * @param count - the count of estimation objects in the list
     * @param byteCountPerGuidePost - the byte count per guide post
     * @param rowCountPerGuidePost - the row count per guide post
     * @param startTimeStamp - the least update timestamp of the first estimation object
     * @param timeStampIncrement - the increment to the least update timestamp of the next estimation object
     * @return the list estimation objects built
     */
    public static List<GuidePostEstimation> buildGuidePostEstimationList(
            int count, int byteCountPerGuidePost, int rowCountPerGuidePost, int startTimeStamp, int timeStampIncrement) {
        Preconditions.checkArgument(count > 0);

        List<GuidePostEstimation> estimations = Lists.newArrayListWithCapacity(count);

        for (int i = 0; i < count; i++) {
            GuidePostEstimation estimation = new GuidePostEstimation(
                    byteCountPerGuidePost, rowCountPerGuidePost, startTimeStamp + i * timeStampIncrement);
            estimations.add(estimation);
        }

        return estimations;
    }

    /**
     * Build a guide post chunk with the given range (lower bound, startKey + step * (guidePostsCount - 1)]
     *
     * @param chunkIndex - zero-based chunk index
     * @param lowerBound - the lower bound of the range covered by this chunk
     * @param startKey - the key of the first guide post in this chunk
     * @param step - the increment to the next key
     * @param guidePostsCount - the count of guide posts in this chunk
     * @return - the guide chunk built
     */
    public static GuidePostChunk buildGuidePostChunk(
            int chunkIndex, int lowerBound, int startKey, int step, int guidePostsCount) {
        Preconditions.checkArgument(guidePostsCount > 0);

        // Build default estimation list
        List<GuidePostEstimation> estimations = buildGuidePostEstimationList(
                guidePostsCount, DEFAULT_BYTE_COUNT_PER_GUIDEPOST, DEFAULT_ROW_COUNT_PER_GUIDEPOST,
                DEFAULT_START_LEAST_UPDATE_TIMESTAMP, DEFAULT_INCREMENT_OF_LEAST_UPDATE_TIMESTAMP);
        return buildGuidePostChunk(chunkIndex, lowerBound, startKey, step, guidePostsCount, estimations);
    }

    /**
     * Build a guide post chunk with the given range (lower bound, startKey + step * (guidePostsCount - 1)]
     *
     * @param chunkIndex - zero-based chunk index
     * @param lowerBound - the lower bound of the range covered by this chunk
     * @param startKey - the key of the first guide post in this chunk
     * @param step - the increment to the key of the next guide post
     * @param guidePostsCount - the count of guide posts in this chunk
     * @param estimations - the estimation list for the guide posts in this chunk
     * @return - the guide chunk built
     */
    public static GuidePostChunk buildGuidePostChunk(
            int chunkIndex, int lowerBound, int startKey, int step, int guidePostsCount, List<GuidePostEstimation> estimations) {
        PrefixByteEncoder encoder = new PrefixByteEncoder();
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(1);
        DataOutputStream output = new DataOutputStream(stream);

        byte[] lowerBoundInBytes = lowerBound == Integer.MIN_VALUE ? KeyRange.UNBOUND : PInteger.INSTANCE.toBytes(lowerBound);
        GuidePostChunkBuilder builder = new GuidePostChunkBuilder(lowerBoundInBytes, 1);

        try {
            for (int i = 0; i < guidePostsCount; i++) {
                byte[] key = PInteger.INSTANCE.toBytes(startKey + i * step);
                encoder.encode(output, key, 0, key.length);
                builder.trackGuidePost(new ImmutableBytesWritable(key), estimations.get(i));
            }
        } catch (Exception e) {
            return null;
        }

        return builder.build(chunkIndex);
    }

    /**
     * Build guide post info which contains a list of guide post chunks built
     *
     * @param startKey - the key of the first guide post in guide post info
     * @param step - the increment to the key of the next guide post
     * @param endKey - the key of the last guide post in guide post info
     * @param guidePostsCountPerChunk
     * @return - the guide post info built
     */
    public static GuidePostsInfo buildGuidePostInfo(
            int startKey, int step, int endKey, int guidePostsCountPerChunk) {
        Preconditions.checkArgument(startKey != Integer.MIN_VALUE);
        Preconditions.checkArgument(endKey != Integer.MAX_VALUE);
        Preconditions.checkArgument((startKey <= endKey) && ((endKey - startKey) % step == 0));
        Preconditions.checkArgument(step >= 1);

        // Build default estimation list
        int guidePostsCount = (endKey - startKey) / step + 1;
        List<GuidePostEstimation> estimations = buildGuidePostEstimationList(
                guidePostsCount, DEFAULT_BYTE_COUNT_PER_GUIDEPOST, DEFAULT_ROW_COUNT_PER_GUIDEPOST,
                DEFAULT_INCREMENT_OF_LEAST_UPDATE_TIMESTAMP, DEFAULT_INCREMENT_OF_LEAST_UPDATE_TIMESTAMP);

        return buildGuidePostInfo(startKey, step, endKey, guidePostsCountPerChunk, estimations);
    }

    /**
     * Build guide post info which contains a list of guide post chunks built
     *
     * @param startKey - the key of the first guide post in guide post info
     * @param step - the increment to the key of the next guide post
     * @param endKey - the key of the last guide post in guide post info
     * @param guidePostsCountPerChunk
     * @param estimations - contains an estimation for each guide post in corresponding position in the chunk list built
     * @return - the guide post info built
     */
    public static GuidePostsInfo buildGuidePostInfo(
            int startKey, int step, int endKey, int guidePostsCountPerChunk, List<GuidePostEstimation> estimations) {
        Preconditions.checkArgument(startKey != Integer.MIN_VALUE);
        Preconditions.checkArgument(endKey != Integer.MAX_VALUE);
        Preconditions.checkArgument((startKey <= endKey) && ((endKey - startKey) % step == 0));
        Preconditions.checkArgument(step >= 1);
        Preconditions.checkArgument(guidePostsCountPerChunk > 0);

        GuidePostsInfoBuilder builder = new GuidePostsInfoBuilder(guidePostsCountPerChunk);

        int guidePostsCount = (endKey - startKey) / step + 1;
        for (int i = 0; i < guidePostsCount; i++) {
            byte[] key = PInteger.INSTANCE.toBytes(startKey + i * step);
            builder.trackGuidePost(new ImmutableBytesWritable(key), estimations.get(i));
        }

        return builder.build();
    }
}