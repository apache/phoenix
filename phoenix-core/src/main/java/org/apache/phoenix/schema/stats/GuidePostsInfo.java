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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.util.ByteUtil;
/**
 *  A class that holds the guidePosts of a region and also allows combining the 
 *  guidePosts of different regions when the GuidePostsInfo is formed for a table.
 */
public class GuidePostsInfo {

    /**
     * the total number of guidePosts for the table combining all the guidePosts per region per cf.
     */
    private ImmutableBytesWritable guidePosts;

    /**
     * Maximum length of a guidePost collected
     */
    private int maxLength;
    
    public final static GuidePostsInfo NO_GUIDEPOST = new GuidePostsInfo(new ArrayList<Long>(),
            new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY), new ArrayList<Long>(), 0, 0);

    public int getMaxLength() {
        return maxLength;
    }

    /**
     * Number of guidePosts
     */
    private int guidePostsCount;

    /**
     * The rowCounts of each guidePost traversed
     */
    private List<Long> rowCounts;

    /**
     * The bytecounts of each guidePost traversed
     */
    private List<Long> byteCounts;

    public List<Long> getRowCounts() {
        return rowCounts;
    }

    public List<Long> getByteCounts() {
        return byteCounts;
    }

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
     */
    public GuidePostsInfo(List<Long> byteCounts, ImmutableBytesWritable guidePosts, List<Long> rowCounts, int maxLength,
            int guidePostsCount) {
        this.guidePosts = new ImmutableBytesWritable(guidePosts);
        this.maxLength = maxLength;
        this.guidePostsCount = guidePostsCount;
        this.rowCounts = rowCounts;
        this.byteCounts = byteCounts;
    }
    
    public ImmutableBytesWritable getGuidePosts() {
        return guidePosts;
    }

    public int getGuidePostsCount() {
        return guidePostsCount;
    }

}