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
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.PrefixByteCodec;
import org.apache.phoenix.util.PrefixByteDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.util.List;

/**
 * A guide post chunk is comprised of a group of guide posts, and it has one of key ranges below:
 *     (UNBOUND, gp_i0], (gp_i0, gp_i1], (gp_i1, gp_i2], ..., (gp_in, gp_n], (gp_n, UNBOUND)
 * where gp_x is one of guide post collected on the server side. The last guide post chunk is an ending chunk
 * which contains one ending guide post.
 *
 * Eventually the guide post chunk should be built and compressed on the server side when collecting stats.
 * It is now the minimal data unit for compression/decompression and will be the minimal data unit stored
 * into the SYSTEM_STATS table in the future.
 */
public final class GuidePostChunk {
    public static final int INVALID_GUIDEPOST_CHUNK_INDEX = -1;
    public static final Logger LOGGER = LoggerFactory.getLogger(GuidePostChunk.class);

    /**
     * The index of the guide post chunk in the chunk array.
     */
    private final int guidePostChunkIndex;

    /**
     * The total count of guide posts covered by this chunk
     */
    private int guidePostsCount;

    /**
     * The key range of the guide posts in this chunk
     */
    private KeyRange keyRange;

    /**
     * Maximum length of a guide post collected. This will be used as the size of buffer
     * when decoding guide posts.
     */
    private int decodingBufferSize;

    /**
     * The guide posts encoded.
     */
    private ImmutableBytesWritable encodedGuidePosts;

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
     * The accumulated estimation info of the guide posts in this chunk
     */
    private GuidePostEstimation totalEstimation;

    /**
     * Estimation of byte size that this instance contributes to cache
     */
    private final int estimatedSize;

    public GuidePostChunk(int guidePostChunkIndex, KeyRange keyRange, GuidePostEstimation totalEstimation,
            int guidePostsCount, ImmutableBytesWritable encodedGuidePosts,
            List<GuidePostEstimation> estimations, int decodingBufferSize) {
        this.guidePostChunkIndex = guidePostChunkIndex;
        this.keyRange = keyRange;
        this.totalEstimation = totalEstimation;
        this.guidePostsCount = guidePostsCount;
        this.encodedGuidePosts = encodedGuidePosts;
        this.estimations = estimations;
        this.decodingBufferSize = decodingBufferSize;

        // We calculate how much this instance contributes to cache approximately,
        // so we only count the size of encoded guide posts and the size of estimation array.
        // The contribution to cache from other fields are ignorable.
        int estimatedSizeInCache = this.encodedGuidePosts.getLength();
        for (GuidePostEstimation estimation : this.estimations) {
            estimatedSizeInCache += estimation.getEstimatedSize();
        }
        this.estimatedSize = estimatedSizeInCache;
    }

    public int getGuidePostChunkIndex() {
        return guidePostChunkIndex;
    }

    public int getGuidePostsCount() {
        return guidePostsCount;
    }

    public KeyRange getKeyRange() {
        return keyRange;
    }

    public ImmutableBytesWritable getEncodedGuidePosts() {
        return encodedGuidePosts;
    }

    public int getDecodingBufferSize() {
        return decodingBufferSize;
    }

    public GuidePostEstimation getTotalEstimation() {
        return totalEstimation;
    }

    public int getEstimatedSize() {
        return estimatedSize;
    }

    public DecodedGuidePostChunk decode() {
        if (encodedGuidePosts.getLength() == 0) {
            // Handling the ending guide post chunk when it has everything range
            List<byte[]> decodedGuidePosts = Lists.newArrayListWithCapacity(1);
            decodedGuidePosts.add(KeyRange.UNBOUND);
            return new DecodedGuidePostChunk(guidePostChunkIndex, keyRange, decodedGuidePosts);
        }

        // A guide post chunk should have at least one guide post
        assert(guidePostsCount > 0);
        List<byte[]> decodedGuidePosts = Lists.newArrayListWithCapacity(guidePostsCount);
        ByteArrayInputStream stream = null;

        try {
            stream = new ByteArrayInputStream(encodedGuidePosts.get(),
                    encodedGuidePosts.getOffset(), encodedGuidePosts.getLength());
            DataInput input = new DataInputStream(stream);
            PrefixByteDecoder decoder = new PrefixByteDecoder(decodingBufferSize);
            ImmutableBytesWritable currentGuidePost;

            for (int i = 0; i < guidePostsCount; i++) {
                try {
                    currentGuidePost = PrefixByteCodec.decode(decoder, input);
                } catch (EOFException e) {
                    // No additional guide posts -- should never hit
                    LOGGER.warn("Failed to decode all the guide posts unexpected EOF occurred.",e);
                    break;
                }
                decodedGuidePosts.add(currentGuidePost.copyBytes());
            }
        } finally {
            if (stream != null) {
                Closeables.closeQuietly(stream);
            }
        }

        return new DecodedGuidePostChunk(guidePostChunkIndex, keyRange, decodedGuidePosts);
    }

    public GuidePostEstimation getTotalEstimationFromStart(int endIndex) {
        return getTotalEstimation(0, endIndex);
    }

    public GuidePostEstimation getTotalEstimationToEnd(int startIndex) {
        return getTotalEstimation(startIndex, guidePostsCount);
    }

    /**
     * Get the estimation of the guide post at the position of "index".
     *
     * @param index
     * @return
     */
    public GuidePostEstimation getEstimation(int index) {
        return getTotalEstimation(index, index + 1);
    }

    /**
     * Get the accumulated estimation of the guide posts between startIndex (inclusive) and endIndex (exclusive).
     *
     * @param startIndex
     * @param endIndex
     * @return the accumulated estimation in [startIndex, endIndex)
     */
    public GuidePostEstimation getTotalEstimation(int startIndex, int endIndex) {
        if (startIndex >= endIndex || startIndex < 0 || endIndex > estimations.size()) {
            return null;
        }

        long byteCount = estimations.get(endIndex - 1).getByteCount();
        long rowCount = estimations.get(endIndex - 1).getRowCount();
        if (startIndex > 0) {
            byteCount -= estimations.get(startIndex - 1).getByteCount();
            rowCount -= estimations.get(startIndex - 1).getRowCount();
        }

        // Eventually the least update timestamp will be recorded on chunk level, so we
        // don't walk through every guide post within the given range.
        long leastTimestamp = GuidePostEstimation.MAX_TIMESTAMP;
        for (int i = startIndex; i < endIndex; i++) {
            leastTimestamp = Math.min(leastTimestamp, estimations.get(i).getTimestamp());
        }

        return new GuidePostEstimation(byteCount, rowCount, leastTimestamp);
    }
}