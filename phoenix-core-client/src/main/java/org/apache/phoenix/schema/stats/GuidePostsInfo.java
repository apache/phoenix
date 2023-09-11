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

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.SizedUtil;

import org.apache.phoenix.thirdparty.com.google.common.primitives.Longs;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;

/**
 *  A class that holds the guidePosts of a region and also allows combining the 
 *  guidePosts of different regions when the GuidePostsInfo is formed for a table.
 */
public class GuidePostsInfo {
    public final static GuidePostsInfo NO_GUIDEPOST =
            new GuidePostsInfo(Collections.<Long> emptyList(),
                    new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY),
                    Collections.<Long> emptyList(), 0, 0, Collections.<Long> emptyList()) {
                @Override
                public int getEstimatedSize() {
                    return 0;
                }
            };
    
    public final static byte[] EMPTY_GUIDEPOST_KEY = ByteUtil.EMPTY_BYTE_ARRAY;
    
    /**
     * the total number of guidePosts for the table combining all the guidePosts per region per cf.
     */
    private final ImmutableBytesWritable guidePosts;
    /**
     * Maximum length of a guidePost collected
     */
    private final int maxLength;
    /**
     * Number of guidePosts
     */
    private final int guidePostsCount;
    /**
     * The rowCounts of each guidePost traversed
     */
    private final long[] rowCounts;
    /**
     * The bytecounts of each guidePost traversed
     */
    private final long[] byteCounts;
    /**
     * Estimate of byte size of this instance
     */
    private final int estimatedSize;
    /**
     * The timestamps at which guideposts were created/updated
     */
    private final long[] gpTimestamps;

    /**
     * Constructor that creates GuidePostsInfo per region
     * 
     * @param byteCounts
     *            The bytecounts of each guidePost traversed
     * @param guidePosts
     *            Prefix byte encoded guidePosts
     * @param rowCounts
     *            The rowCounts of each guidePost traversed
     * @param maxLength
     *            Maximum length of a guidePost collected
     * @param guidePostsCount
     *            Number of guidePosts
     * @param updateTimes
     *            Times at which guidePosts were updated/created
     */
    public GuidePostsInfo(List<Long> byteCounts, ImmutableBytesWritable guidePosts, List<Long> rowCounts, int maxLength,
            int guidePostsCount, List<Long> updateTimes) {
        this.guidePosts = new ImmutableBytesWritable(guidePosts);
        this.maxLength = maxLength;
        this.guidePostsCount = guidePostsCount;
        this.rowCounts = Longs.toArray(rowCounts);
        this.byteCounts = Longs.toArray(byteCounts);
        this.gpTimestamps = Longs.toArray(updateTimes);
        // Those Java equivalents of sizeof() in C/C++, mentioned on the Web, might be overkilled here.
        int estimatedSize = SizedUtil.OBJECT_SIZE
                + SizedUtil.IMMUTABLE_BYTES_WRITABLE_SIZE + guidePosts.getLength() // guidePosts
                + SizedUtil.INT_SIZE // maxLength
                + SizedUtil.INT_SIZE // guidePostsCount
                + SizedUtil.ARRAY_SIZE + this.rowCounts.length * SizedUtil.LONG_SIZE // rowCounts
                + SizedUtil.ARRAY_SIZE + this.byteCounts.length * SizedUtil.LONG_SIZE // byteCounts
                + SizedUtil.ARRAY_SIZE + this.gpTimestamps.length * SizedUtil.LONG_SIZE // gpTimestamps
                + SizedUtil.INT_SIZE; // estimatedSize
        this.estimatedSize = estimatedSize;
    }
    
    public ImmutableBytesWritable getGuidePosts() {
        return guidePosts;
    }

    public int getGuidePostsCount() {
        return guidePostsCount;
    }
    
    public int getMaxLength() {
        return maxLength;
    }

    public long[] getRowCounts() {
        return rowCounts;
    }

    public long[] getByteCounts() {
        return byteCounts;
    }

    public long[] getGuidePostTimestamps() {
        return gpTimestamps;
    }

    public int getEstimatedSize() {
        return estimatedSize;
    }

    @SuppressWarnings(value="EC_ARRAY_AND_NONARRAY",
            justification="ImmutableBytesWritable DOES implement equals(byte])")
    public boolean isEmptyGuidePost() {
        return guidePosts.equals(EMPTY_GUIDEPOST_KEY) && guidePostsCount == 0
                && byteCounts.length == 1 && gpTimestamps.length == 1;
    }

    public static GuidePostsInfo createEmptyGuidePost(long byteCount, long guidePostUpdateTime) {
        return new GuidePostsInfo(Collections.singletonList(byteCount),
                new ImmutableBytesWritable(EMPTY_GUIDEPOST_KEY), Collections.<Long> emptyList(), 0,
                0, Collections.<Long> singletonList(guidePostUpdateTime));
    }
    
    public static boolean isEmptyGpsKey(byte[] key) {
        return Bytes.equals(key, GuidePostsInfo.EMPTY_GUIDEPOST_KEY);
    }

}