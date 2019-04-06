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

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PrefixByteEncoder;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

/**
 * The guide post boundaries:
 *     gp_0, gp_1, ..., gp_i0, ..., gp_i1, ..., gp_i2, ..., gp_in, ..., gp_n
 * The guide post chunk boundaries:
 *     gp_i0, gp_i1, ..., gp_in, ..., gp_n
 * The key space split by the guide post chunks:
 *     (UNBOUND, gp_i0](gp_i0, gp_i1](gp_i1, gp_i2]...(gp_in, gp_n](gp_n, UNBOUND)
 * The last guide post chunk is an ending chunk which contains one ending guide post
 * KeyRang.UNBOUND with 0 estimated rows/bytes and Long.MAX as last update timestamp.
 */
public final class GuidePostChunkBuilder {
    private PrefixByteEncoder encoder;
    private TrustedByteArrayOutputStream stream;
    private DataOutputStream output;
    private ImmutableBytesWritable lastRow;
    private int guidePostsCount;
    private ImmutableBytesWritable guidePosts;

    /**
     * The total count of bytes in this chunk
     */
    private long totalByteCount;

    /**
     * The total count of rows in this chunk
     */
    private long totalRowCount;

    /**
     * The least update time across all guide posts
     */
    private long leastUpdateTime;

    /**
     * Maximum length of a guide post key collected
     */
    private int maxGuidePostKeyLength;

    /**
     * The estimation info of each guide post traversed.
     * estimations[i].rowCount is the sum of the estimated rows of  guide post [0, ..., i]
     * estimations[i].byteCount is the sum of the estimated bytes of  guide post [0, ..., i]
     * estimations[i].timestamp is the last update time stamp of guide post i
     * Eventually the whole guide post chunk will maintain one least update timestamp, and there is
     * no need to maintain a timestamp for every guide post, because writing guide posts to stats
     * table is an atomic operation on region level.
     */
    private List<GuidePostEstimation> estimations;

    /**
     * lowerBound is fixed. It's always exclusive.
     */
    private final byte[] lowerBound;

    public GuidePostChunkBuilder() {
        this(KeyRange.UNBOUND, 1);
    }

    public GuidePostChunkBuilder(byte[] lowerBound, int targetedCountOfGuidePosts) {
        this.lowerBound = lowerBound;
        this.encoder = new PrefixByteEncoder();
        this.stream = new TrustedByteArrayOutputStream(1);
        this.output = new DataOutputStream(stream);
        this.lastRow = new ImmutableBytesWritable(this.lowerBound);
        this.guidePostsCount = 0;
        this.guidePosts = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
        this.totalRowCount = 0;
        this.totalByteCount = 0;
        this.leastUpdateTime = Long.MAX_VALUE;
        this.maxGuidePostKeyLength = 0;
        this.estimations = Lists.newArrayListWithExpectedSize(targetedCountOfGuidePosts);
    }

    /**
     * Track a new guide post
     * @param row number of rows in the guidepost
     * @param estimation estimation of the guidepost
     * @throws IOException
     */
    public boolean trackGuidePost(ImmutableBytesWritable row, GuidePostEstimation estimation) {
        if (row.getLength() != 0 && lastRow.compareTo(row) < 0) {
            try {
                encoder.encode(output, row.get(), row.getOffset(), row.getLength());
                maxGuidePostKeyLength = encoder.getMaxLength();

                totalByteCount += estimation.getByteCount();
                totalRowCount += estimation.getRowCount();
                estimations.add(new GuidePostEstimation(totalByteCount, totalRowCount, estimation.getTimestamp()));

                leastUpdateTime = Math.min(leastUpdateTime, estimation.getTimestamp());
                guidePostsCount++;
                lastRow = row;
                return true;
            } catch (IOException e) {
                return false;
            }
        }

        return false;
    }

    public GuidePostChunk build(int guidePostChunkIndex) {
        if (guidePostsCount > 0) {
            byte[] upperBound = lastRow.copyBytes();
            KeyRange keyRange = KeyRange.getKeyRange(lowerBound, false, upperBound, true);
            GuidePostEstimation totalEstimation = new GuidePostEstimation(totalByteCount, totalRowCount, leastUpdateTime);
            guidePosts.set(stream.getBuffer(), 0, stream.size());
            GuidePostChunk guidePostChunk = new GuidePostChunk(guidePostChunkIndex,
                    keyRange, totalEstimation, guidePostsCount, guidePosts, estimations, maxGuidePostKeyLength);
            return guidePostChunk;
        }

        return null;
    }

    public static GuidePostChunk buildEndingChunk(int guidePostChunkIndex, byte[] lowerBound, GuidePostEstimation estimation) {
        KeyRange keyRange = KeyRange.getKeyRange(lowerBound, false, KeyRange.UNBOUND, false);
        ImmutableBytesWritable guidePosts = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
        List<GuidePostEstimation> estimations = Lists.newArrayList(estimation);

        GuidePostChunk guidePostChunk = new GuidePostChunk(
                guidePostChunkIndex, keyRange, estimation, 1, guidePosts, estimations, 0);

        return guidePostChunk;
    }

    public int getGuidePostsCount() {
        return guidePostsCount;
    }
}