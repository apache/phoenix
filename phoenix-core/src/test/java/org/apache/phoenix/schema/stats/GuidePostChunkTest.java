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
import static org.junit.Assert.assertFalse;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.PrefixByteEncoder;
import org.apache.phoenix.util.TrustedByteArrayOutputStream;
import org.junit.Test;

import java.io.DataOutputStream;
import java.util.List;

public class GuidePostChunkTest {
    @Test
    public void testEndingChunkWithUnboundedLowerRange() {
        int guidePostsCount = 1;
        GuidePostEstimation estimation = new GuidePostEstimation();
        GuidePostChunk chunk = GuidePostChunkBuilder.buildEndingChunk(
                0, KeyRange.UNBOUND, estimation);
        assertEquals(0, chunk.getGuidePostChunkIndex());
        assertEquals(1, chunk.getGuidePostsCount());
        assertEquals(KeyRange.EVERYTHING_RANGE, chunk.getKeyRange());
        assertEquals(new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY), chunk.getEncodedGuidePosts());
        assertEquals(estimation.getEstimatedSize() * guidePostsCount, chunk.getEstimatedSize());
        assertEquals(0, chunk.getDecodingBufferSize(), 0);

        assertEquals(new GuidePostEstimation(), chunk.getTotalEstimation());
        assertEquals(new GuidePostEstimation(), chunk.getTotalEstimation(0, guidePostsCount));
        assertEquals(new GuidePostEstimation(), chunk.getTotalEstimationFromStart(guidePostsCount));
        assertEquals(new GuidePostEstimation(), chunk.getTotalEstimationToEnd(0));
        assertEquals(new GuidePostEstimation(), chunk.getEstimation(0));
        assertEquals(null, chunk.getEstimation(-1));
        assertEquals(null, chunk.getEstimation(guidePostsCount));
        assertEquals(null, chunk.getTotalEstimation(-1, guidePostsCount));
        assertEquals(null, chunk.getTotalEstimation(0, guidePostsCount + 1));
        assertEquals(null, chunk.getTotalEstimationFromStart(0));
        assertEquals(null, chunk.getTotalEstimationFromStart(guidePostsCount + 1));
        assertEquals(null, chunk.getTotalEstimationToEnd(-1));
        assertEquals(null, chunk.getTotalEstimationToEnd(guidePostsCount));

        List<byte[]> decodedGuidePosts = Lists.newArrayListWithCapacity(1);
        decodedGuidePosts.add(KeyRange.UNBOUND);
        assertEquals(new DecodedGuidePostChunk(0, chunk.getKeyRange(), decodedGuidePosts), chunk.decode());
    }

    @Test
    public void testEndingChunkWithBoundedLowerRange() {
        int guidePostsCount = 1;
        GuidePostEstimation estimation = new GuidePostEstimation(1, 2, 3);
        GuidePostChunk chunk = GuidePostChunkBuilder.buildEndingChunk(
                10, PInteger.INSTANCE.toBytes(1), estimation);
        assertEquals(10, chunk.getGuidePostChunkIndex());
        assertEquals(1, chunk.getGuidePostsCount());
        assertEquals(KeyRange.getKeyRange(PInteger.INSTANCE.toBytes(1), false, KeyRange.UNBOUND, false), chunk.getKeyRange());
        assertEquals(new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY), chunk.getEncodedGuidePosts());
        assertEquals(estimation.getEstimatedSize() * guidePostsCount, chunk.getEstimatedSize());
        assertEquals(0, chunk.getDecodingBufferSize());

        assertEquals(new GuidePostEstimation(1, 2, 3), chunk.getTotalEstimation());
        assertEquals(new GuidePostEstimation(1, 2, 3), chunk.getEstimation(0));
        assertEquals(null, chunk.getEstimation(-1));
        assertEquals(null, chunk.getEstimation(guidePostsCount));

        List<byte[]> decodedGuidePosts = Lists.newArrayListWithCapacity(1);
        decodedGuidePosts.add(KeyRange.UNBOUND);
        assertEquals(new DecodedGuidePostChunk(10, chunk.getKeyRange(), decodedGuidePosts), chunk.decode());
    }

    @Test
    public void testChunkWithSingleGuidePost() {
        int guidePostsCount = 1;
        PrefixByteEncoder encoder = new PrefixByteEncoder();
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(1);
        DataOutputStream output = new DataOutputStream(stream);

        GuidePostChunkBuilder builder = new GuidePostChunkBuilder(); // lower range is UNBOUND
        byte[] key1 = PInteger.INSTANCE.toBytes(2);
        GuidePostEstimation estimation = new GuidePostEstimation(1, 5, 13);
        try {
            encoder.encode(output, key1, 0, key1.length);
        } catch (Exception e) {
            assertTrue(false); // Should never happen
        }
        boolean succeeded = builder.trackGuidePost(new ImmutableBytesWritable(key1), estimation);
        assertTrue(succeeded);
        byte[] key2 = PInteger.INSTANCE.toBytes(1);
        succeeded = builder.trackGuidePost(new ImmutableBytesWritable(key2), estimation);
        // The second guide post to add is out of order, so the operation failed and nothing is done.
        assertFalse(succeeded);

        GuidePostChunk chunk = builder.build(0);
        assertEquals(0, chunk.getGuidePostChunkIndex());
        assertEquals(1, chunk.getGuidePostsCount());
        assertEquals(KeyRange.getKeyRange(KeyRange.UNBOUND, false, key1, true), chunk.getKeyRange());

        ImmutableBytesWritable encodedGuidePosts = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
        encodedGuidePosts.set(stream.getBuffer(), 0, stream.size());
        assertEquals(encodedGuidePosts, chunk.getEncodedGuidePosts());
        assertEquals(encodedGuidePosts.getLength() + estimation.getEstimatedSize() * guidePostsCount, chunk.getEstimatedSize());
        assertEquals(encoder.getMaxLength(), chunk.getDecodingBufferSize());

        assertEquals(estimation, chunk.getTotalEstimation());
        assertEquals(estimation, chunk.getEstimation(0));
        assertEquals(null, chunk.getEstimation(-1));
        assertEquals(null, chunk.getEstimation(guidePostsCount));

        List<byte[]> decodedGuidePosts = Lists.newArrayListWithCapacity(1);
        decodedGuidePosts.add(key1);
        assertEquals(new DecodedGuidePostChunk(0, chunk.getKeyRange(), decodedGuidePosts), chunk.decode());
    }

    @Test
    public void testChunkWithMultipleGuidePosts() {
        int guidePostsCount = 10;
        PrefixByteEncoder encoder = new PrefixByteEncoder();
        TrustedByteArrayOutputStream stream = new TrustedByteArrayOutputStream(1);
        DataOutputStream output = new DataOutputStream(stream);

        int lowerBoundOfTheChunkInInteger = 1;
        byte[] lowerBoundOfTheChunk = PInteger.INSTANCE.toBytes(lowerBoundOfTheChunkInInteger);
        byte[] upperBoundOfTheChunk = PInteger.INSTANCE.toBytes(lowerBoundOfTheChunkInInteger + guidePostsCount);
        GuidePostChunkBuilder builder = new GuidePostChunkBuilder(lowerBoundOfTheChunk, 1);
        GuidePostEstimation estimation = new GuidePostEstimation(3, 5, 13);
        List<byte[]> decodedGuidePosts = Lists.newArrayListWithCapacity(1);

        try {
            byte[] key1 = lowerBoundOfTheChunk;
            boolean succeeded = builder.trackGuidePost(new ImmutableBytesWritable(key1), estimation);
            // The first guide post's key is equal to the lower bound of the chunk which is out of order,
            // so the operation failed and nothing is done.
            assertFalse(succeeded);

            for (int i = 0; i < guidePostsCount; i++) {
                key1 = PInteger.INSTANCE.toBytes(lowerBoundOfTheChunkInInteger + i + 1);
                encoder.encode(output, key1, 0, key1.length);
                estimation.setTimestamp(13 + i);
                succeeded = builder.trackGuidePost(new ImmutableBytesWritable(key1), estimation);
                assertTrue(succeeded);
                decodedGuidePosts.add(key1);
            }

            byte[] key2 = PInteger.INSTANCE.toBytes(lowerBoundOfTheChunkInInteger + guidePostsCount - 1);
            succeeded = builder.trackGuidePost(new ImmutableBytesWritable(key2), estimation);
            // The last guide post to add is out of order, so the operation failed and nothing is done.
            assertFalse(succeeded);
        } catch (Exception e) {
            assertTrue(false); // Should never happen
        }

        GuidePostChunk chunk = builder.build(0);
        assertEquals(0, chunk.getGuidePostChunkIndex());
        assertEquals(guidePostsCount, chunk.getGuidePostsCount());
        assertEquals(KeyRange.getKeyRange(lowerBoundOfTheChunk, false, upperBoundOfTheChunk, true), chunk.getKeyRange());

        ImmutableBytesWritable encodedGuidePosts = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);
        encodedGuidePosts.set(stream.getBuffer(), 0, stream.size());
        assertEquals(encodedGuidePosts, chunk.getEncodedGuidePosts());
        assertEquals(encodedGuidePosts.getLength() + estimation.getEstimatedSize() * guidePostsCount, chunk.getEstimatedSize());
        assertEquals(encoder.getMaxLength(), chunk.getDecodingBufferSize());

        GuidePostEstimation totalEstimation = new GuidePostEstimation(3 * guidePostsCount, 5 * guidePostsCount, 13);
        assertEquals(totalEstimation, chunk.getTotalEstimation());
        assertEquals(totalEstimation, chunk.getTotalEstimation(0, guidePostsCount));
        assertEquals(new GuidePostEstimation(3 * 3, 5 * 3, 13), chunk.getTotalEstimation(0, 3));
        assertEquals(new GuidePostEstimation(3 * (6 - 3), 5 * (6 - 3), 13 + 3), chunk.getTotalEstimation(3, 6));
        assertEquals(new GuidePostEstimation(3 * (guidePostsCount - 3), 5 * (guidePostsCount - 3), 13 + 3), chunk.getTotalEstimation(3, guidePostsCount));
        assertEquals(totalEstimation, chunk.getTotalEstimationFromStart(guidePostsCount));
        assertEquals(new GuidePostEstimation(3 * 7, 5 * 7, 13), chunk.getTotalEstimationFromStart(7));
        assertEquals(totalEstimation, chunk.getTotalEstimationToEnd(0));
        assertEquals(new GuidePostEstimation(3 * (guidePostsCount - 7), 5 * (guidePostsCount - 7), 13 + 7), chunk.getTotalEstimationToEnd(7));

        for (int i = 0; i < guidePostsCount; i++) {
            assertEquals(new GuidePostEstimation(3, 5, 13 + i), chunk.getEstimation(i));
        }

        assertEquals(null, chunk.getEstimation(-1));
        assertEquals(null, chunk.getEstimation(guidePostsCount));
        assertEquals(null, chunk.getTotalEstimation(-1, guidePostsCount));
        assertEquals(null, chunk.getTotalEstimation(0, guidePostsCount + 1));
        assertEquals(null, chunk.getTotalEstimationFromStart(0));
        assertEquals(null, chunk.getTotalEstimationFromStart(guidePostsCount + 1));
        assertEquals(null, chunk.getTotalEstimationToEnd(-1));
        assertEquals(null, chunk.getTotalEstimationToEnd(guidePostsCount));

        assertEquals(new DecodedGuidePostChunk(0, chunk.getKeyRange(), decodedGuidePosts), chunk.decode());
    }
}
