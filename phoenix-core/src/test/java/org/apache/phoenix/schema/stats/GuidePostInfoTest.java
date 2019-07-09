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
import static org.junit.Assert.assertNull;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.KeyRange;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

public class GuidePostInfoTest {
    /**
     *  When there is a query range which only hit the ending chunk, the last update time will be
     *  the whole guide post info's the last update time.
     * @param info
     * @param queryKeyRanges
     * @param iterators
     * @param expectedScanRanges
     */
    private void verifyQueryResult(GuidePostsInfo info, List<KeyRange> queryKeyRanges,
            List<GuidePostIterator> iterators, List<KeyRange> expectedScanRanges) {
        verifyQueryResult(info, queryKeyRanges, iterators, expectedScanRanges, false);
    }

    private void verifyQueryResult(GuidePostsInfo info, List<KeyRange> queryKeyRanges,
            List<GuidePostIterator> iterators, List<KeyRange> expectedScanRanges, boolean useTimestampOfInfo) {
        // Calculate expected estimation from iterators
        GuidePostEstimation expectedEstimation = new GuidePostEstimation();
        if (iterators != null) {
            for (GuidePostIterator iterator : iterators) {
                while (iterator.hasNext()) {
                    GuidePost guidePost = iterator.next();
                    expectedEstimation.merge(guidePost.getEstimation());
                }
            }
        }

        if (useTimestampOfInfo) {
            expectedEstimation.setTimestamp(info.getTotalEstimation().getTimestamp());
        }

        verifyQueryResult(info, queryKeyRanges, expectedEstimation, expectedScanRanges);
    }

    private void verifyQueryResult(GuidePostsInfo info, List<KeyRange> queryKeyRanges,
            GuidePostEstimation expectedEstimation, List<KeyRange> expectedScanRanges) {
        GuidePostEstimation estimation = info.getEstimationOnly(queryKeyRanges);
        assertEquals(expectedEstimation, estimation);

        Pair<List<KeyRange>, GuidePostEstimation> scanPlan = info.generateParallelScanRanges(queryKeyRanges);
        List<KeyRange> actualScanRanges = scanPlan.getFirst();
        assertEquals(expectedScanRanges.size(), actualScanRanges.size());
        for (int i = 0; i < actualScanRanges.size(); i++) {
            assertEquals(expectedScanRanges.get(i), actualScanRanges.get(i));
        }
        assertEquals(expectedEstimation, scanPlan.getSecond());
    }

    private void addSequentialKeyRanges(List<KeyRange> ranges, int startKey, int step, int endKey, int lowerRange, int upperRange) {
        if (lowerRange < startKey) {
            ranges.add(StatsTestUtil.getKeyRange(lowerRange, startKey));
        }

        int key = startKey;
        for ( ; key < endKey; key += step) {
            ranges.add(StatsTestUtil.getKeyRange(key, Math.min(endKey, key + step)));
        }

        if (endKey < upperRange) {
            ranges.add(StatsTestUtil.getKeyRange(Math.max(lowerRange, endKey), upperRange));
        }
    }

    private void addJumpingKeyRanges(List<KeyRange> ranges, int count, int jump, int startLowerRange, int startUpperRange) {
        for (int i = 0; i < count; i++) {
            int lowerRange = startLowerRange + i * jump;
            int upperRange = startUpperRange + i * jump;
            ranges.add(StatsTestUtil.getKeyRange(lowerRange, upperRange));
        }
    }

    private void clearData(List<KeyRange> queryRanges, List<KeyRange> scanRanges,  List<GuidePostIterator> iterators) {
        if (queryRanges != null) {
            queryRanges.clear();
        }

        if (scanRanges != null) {
            scanRanges.clear();
        }

        if (iterators != null) {
            iterators.clear();
        }
    }

    @Test
    public void testNoGuidePost() {
        assertNull(GuidePostsInfo.NO_GUIDEPOST.getGuidePostsKey());
        assertEquals(0, GuidePostsInfo.NO_GUIDEPOST.getGuidePostsCount());
        assertEquals(0, GuidePostsInfo.NO_GUIDEPOST.getGuidePostChunks().size());
        assertEquals(0, GuidePostsInfo.NO_GUIDEPOST.getEstimatedSize());
    }

    private void testOnlyEndingChunk(GuidePostEstimation extraEstimation) {
        // GuidePostInfo object built contains the ending chunk only.
        // The representation of chunks with each chunk in the format of {Key Range of chunk i | [guide post 0, guide 1, ...]}:
        // {(UNBOUND, UNBOUND) | [UNBOUND])}
        GuidePostsInfoBuilder builder = new GuidePostsInfoBuilder();
        GuidePostsInfo info = builder.build(extraEstimation);

        List<KeyRange> queryRanges = Lists.newArrayList();
        queryRanges.add(KeyRange.EVERYTHING_RANGE);
        List<KeyRange> scanRanges = Lists.newArrayList();
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, Integer.MAX_VALUE, false));
        verifyQueryResult(info, queryRanges, extraEstimation, scanRanges);

        clearData(queryRanges, scanRanges, null);
        queryRanges.add(KeyRange.EMPTY_RANGE);
        // Should always have no estimation and no scan queryRanges returned for empty query range.
        verifyQueryResult(info, queryRanges, new GuidePostEstimation(), scanRanges);

        clearData(queryRanges, scanRanges, null);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 0));
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 0, false));
        verifyQueryResult(info, queryRanges, extraEstimation, scanRanges);

        clearData(queryRanges, scanRanges, null);
        queryRanges.add(StatsTestUtil.getKeyRange(0, 1));
        scanRanges.add(StatsTestUtil.getKeyRange(0, true, 1, false));
        verifyQueryResult(info, queryRanges, extraEstimation, scanRanges);

        clearData(queryRanges, scanRanges, null);
        queryRanges.add(StatsTestUtil.getKeyRange(1, Integer.MAX_VALUE));
        scanRanges.add(StatsTestUtil.getKeyRange(1, true, Integer.MAX_VALUE, false));
        verifyQueryResult(info, queryRanges, extraEstimation, scanRanges);

        clearData(queryRanges, scanRanges, null);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 0));
        queryRanges.add(StatsTestUtil.getKeyRange(1, 10));
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 10, false));
        verifyQueryResult(info, queryRanges, extraEstimation, scanRanges);
        queryRanges.add(StatsTestUtil.getKeyRange(10, 20));
        scanRanges.clear();
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 20, false));
        verifyQueryResult(info, queryRanges, extraEstimation, scanRanges);
        queryRanges.add(StatsTestUtil.getKeyRange(30, Integer.MAX_VALUE));
        scanRanges.clear();
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, Integer.MAX_VALUE, false));
        verifyQueryResult(info, queryRanges, extraEstimation, scanRanges);
    }

    @Test
    public void testOnlyEndingChunkWithoutExtraEstimation() {
        testOnlyEndingChunk(new GuidePostEstimation());
    }

    @Test
    public void testOnlyEndingChunkWithExtraEstimation() {
        testOnlyEndingChunk(new GuidePostEstimation(3, 5, 7));
    }

    @Test
    public void testSingleDataChunkSingleGuidePost() {
        // GuidePostInfo object built contains one data chunk plus the ending chunk.
        // The representation of chunks with each chunk in the format of {Key Range of chunk i | [guide post 0, guide 1, ...]}:
        // {(UNBOUND, 10] | [10]}, {(10, UNBOUND) | [UNBOUND])}
        GuidePostsInfo info = StatsTestUtil.buildGuidePostInfo(10, 1, 10, 10);
        DecodedGuidePostChunkCache cache = new DecodedGuidePostChunkCache(info.getGuidePostChunks());


        List<KeyRange> queryRanges = Lists.newArrayList();
        queryRanges.add(KeyRange.EVERYTHING_RANGE);
        List<KeyRange> scanRanges = Lists.newArrayList();
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 10, false));
        scanRanges.add(StatsTestUtil.getKeyRange(10, true, Integer.MAX_VALUE, false));
        List<GuidePostIterator> iterators = Lists.newArrayList();
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(KeyRange.EMPTY_RANGE);
        // Should always have no estimation and no scan queryRanges returned for empty query range.
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 0));
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 0, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 0, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 20));
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 10, false));
        scanRanges.add(StatsTestUtil.getKeyRange(10, true, 20, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(0, 1));
        scanRanges.add(StatsTestUtil.getKeyRange(0, true, 1, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 0, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(0, 20));
        scanRanges.add(StatsTestUtil.getKeyRange(0, true, 10, false));
        scanRanges.add(StatsTestUtil.getKeyRange(10, true, 20, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(10, 20));
        scanRanges.add(StatsTestUtil.getKeyRange(10, true, 20, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 1, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(20, 30));
        scanRanges.add(StatsTestUtil.getKeyRange(20, true, 30, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 1, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(1, Integer.MAX_VALUE));
        scanRanges.add(StatsTestUtil.getKeyRange(1, true, 10, false));
        scanRanges.add(StatsTestUtil.getKeyRange(10, true, Integer.MAX_VALUE, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(10, Integer.MAX_VALUE));
        scanRanges.add(StatsTestUtil.getKeyRange(10, true, Integer.MAX_VALUE, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 1, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(20, Integer.MAX_VALUE));
        scanRanges.add(StatsTestUtil.getKeyRange(20, true, Integer.MAX_VALUE, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 1, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        clearData(queryRanges, scanRanges, iterators);
        // Add query key ranges {(UNBOUND, 0), [1, 2), [3, 5)} which are within the first guide post with key 10
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 0));
        queryRanges.add(StatsTestUtil.getKeyRange(1, 2));
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 2, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 0, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);
        queryRanges.add(StatsTestUtil.getKeyRange(3, 5));
        scanRanges.clear();
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 5, false));
        iterators.clear();
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 0, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);
        // Add one more query key ranges [7, 13) which is across the first guide post with key 10
        queryRanges.add(StatsTestUtil.getKeyRange(7, 15));
        scanRanges.clear();
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 10, false));
        scanRanges.add(StatsTestUtil.getKeyRange(10, true, 15, false));
        iterators.clear();
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);
        // Add one more query key ranges [16, 20) which is after the first guide post with key 10
        queryRanges.add(StatsTestUtil.getKeyRange(16, 20));
        scanRanges.clear();
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 10, false));
        scanRanges.add(StatsTestUtil.getKeyRange(10, true, 20, false));
        iterators.clear();
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);
        // Add one more query key ranges [30, UNBOUND) which is after the first guide post with key 10
        queryRanges.add(StatsTestUtil.getKeyRange(30, Integer.MAX_VALUE));
        scanRanges.clear();
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 10, false));
        scanRanges.add(StatsTestUtil.getKeyRange(10, true, Integer.MAX_VALUE, false));
        iterators.clear();
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        // Add query key ranges {[1, 2), [3, 5), [7, 10)} with the query range [7, 10) on the boundary of the first guide post with key 10
        queryRanges.add(StatsTestUtil.getKeyRange(1, 2));
        queryRanges.add(StatsTestUtil.getKeyRange(3, 5));
        queryRanges.add(StatsTestUtil.getKeyRange(7, 10));
        scanRanges.add(StatsTestUtil.getKeyRange(1, true, 10, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 01, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);
        // Add query key ranges {[10, 20), [20, 30)} with the query ranges [10, 20) on the boundary of the first guide post with key 10
        queryRanges.add(StatsTestUtil.getKeyRange(10, 20));
        queryRanges.add(StatsTestUtil.getKeyRange(20, 30));
        scanRanges.clear();
        scanRanges.add(StatsTestUtil.getKeyRange(1, true, 10, false));
        scanRanges.add(StatsTestUtil.getKeyRange(10, true, 30, false));
        iterators.clear();
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        // Add query key ranges {[1, 2), [3, 5), [7, 9)} within the first guide post with key 10
        queryRanges.add(StatsTestUtil.getKeyRange(1, 2));
        queryRanges.add(StatsTestUtil.getKeyRange(3, 5));
        queryRanges.add(StatsTestUtil.getKeyRange(7, 9));
        scanRanges.add(StatsTestUtil.getKeyRange(1, true, 9, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 01, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);
        // Add query key ranges {[15, 20), [20, 30)} after the first guide post with key 10
        queryRanges.add(StatsTestUtil.getKeyRange(15, 20));
        queryRanges.add(StatsTestUtil.getKeyRange(20, 30));
        scanRanges.clear();
        scanRanges.add(StatsTestUtil.getKeyRange(1, true, 9, false));
        scanRanges.add(StatsTestUtil.getKeyRange(15, true, 30, false));
        iterators.clear();
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);
    }

    @Test
    public void testSingleDataChunkMultipleGuidePosts() {
        // GuidePostInfo object built contains one data chunk plus the ending chunk.
        // The representation of chunks with each chunk in the format of {Key Range of chunk i | [guide post 0, guide 1, ...]}:
        // {(UNBOUND, 100] | [10, 20, ..., 100]}, {(100, UNBOUND) | [UNBOUND])}
        GuidePostsInfo info = StatsTestUtil.buildGuidePostInfo(10, 10, 100, 10);
        DecodedGuidePostChunkCache cache = new DecodedGuidePostChunkCache(info.getGuidePostChunks());

        List<KeyRange> queryRanges = Lists.newArrayList();
        queryRanges.add(KeyRange.EVERYTHING_RANGE);
        List<KeyRange> scanRanges = Lists.newArrayList();
        addSequentialKeyRanges(scanRanges, 10, 10, 100, Integer.MIN_VALUE, Integer.MAX_VALUE);
        List<GuidePostIterator> iterators = Lists.newArrayList();
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(KeyRange.EMPTY_RANGE);
        // Should always have no estimation and no scan queryRanges returned for empty query range.
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 0));
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 0, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 0, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 31));
        addSequentialKeyRanges(scanRanges, 10, 10, 31, Integer.MIN_VALUE, 31);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 0, 3));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 120));
        addSequentialKeyRanges(scanRanges, 10, 10, 100, Integer.MIN_VALUE, 120);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(0, 1));
        scanRanges.add(StatsTestUtil.getKeyRange(0, true, 1, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 0, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(5, 95));
        addSequentialKeyRanges(scanRanges, 10, 10, 95, 5, 95);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 0, 9));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(5, 101));
        addSequentialKeyRanges(scanRanges, 10, 10, 100, 5, 101);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(100, 101));
        addSequentialKeyRanges(scanRanges, 100, 10, 100, 100, 101);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 1, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(120, 130));
        addSequentialKeyRanges(scanRanges, 100, 10, 100, 120, 130);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 1, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(1, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 10, 10, 100, 1, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(10, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 20, 10, 100, 10, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 1, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(35, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 40, 10, 100, 35, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 3, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(120, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 100, 10, 100, 120, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 1, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        // Evey guide post is hit by a query range.
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 11, 10, 4, 6);
        addJumpingKeyRanges(scanRanges, 11, 10, 4, 6);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Evey guide post is hit by three query ranges without covering guide post boundaries.
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 11, 10, 1, 2);
        addJumpingKeyRanges(queryRanges, 11, 10, 4, 6);
        addJumpingKeyRanges(queryRanges, 11, 10, 8, 9);
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        addJumpingKeyRanges(scanRanges, 11, 10, 1, 9);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Evey guide post is hit by three query ranges with covering guide post boundaries.
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 11, 10, 0, 2);
        addJumpingKeyRanges(queryRanges, 11, 10, 4, 6);
        addJumpingKeyRanges(queryRanges, 11, 10, 8, 10);
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        addJumpingKeyRanges(scanRanges, 11, 10, 0, 10);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Evey guide post is hit by three query ranges without covering guide post boundaries.
        // Plus (UNBOUND, 0) and (120, UNBOUND) at the head and tail.
        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 0));
        addJumpingKeyRanges(queryRanges, 11, 10, 1, 2);
        addJumpingKeyRanges(queryRanges, 11, 10, 4, 6);
        addJumpingKeyRanges(queryRanges, 11, 10, 8, 9);
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        queryRanges.add(StatsTestUtil.getKeyRange(120, Integer.MAX_VALUE));
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 9));
        addJumpingKeyRanges(scanRanges, 9, 10, 11, 19);
        scanRanges.add(StatsTestUtil.getKeyRange(101, Integer.MAX_VALUE));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Evey guide post is hit by three query ranges with covering guide post boundaries.
        // Plus (UNBOUND, 0) and (120, UNBOUND) at the head and tail.
        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 0));
        addJumpingKeyRanges(queryRanges, 11, 10, 0, 2);
        addJumpingKeyRanges(queryRanges, 11, 10, 4, 6);
        addJumpingKeyRanges(queryRanges, 11, 10, 8, 10);
        queryRanges.add(StatsTestUtil.getKeyRange(120, Integer.MAX_VALUE));
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 10));
        addJumpingKeyRanges(scanRanges, 9, 10, 10, 20);
        scanRanges.add(StatsTestUtil.getKeyRange(100, Integer.MAX_VALUE));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Every guide post at the even position is hit by a query range
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 6, 20, 4, 6);
        addJumpingKeyRanges(scanRanges, 6, 20, 4, 6);
        for (int i = 0; i < 10; i += 2) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, i, 0, i));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 1, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Every guide post at the odd position is hit by a query range
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 6, 20, 14, 16);
        addJumpingKeyRanges(scanRanges, 6, 20, 14, 16);
        for (int i = 1; i < 10; i += 2) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, i, 0, i));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 1, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        // Multiple query ranges are within a guide post plus the last query range covering the current range
        // and the next several ranges.
        // [1, 3), [5, 6), [8, 28), [41, 43), [45, 46), [48, 68), [81, 83), [85, 86), [88, 108),
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 3, 40, 1, 3);
        addJumpingKeyRanges(queryRanges, 3, 40, 5, 6);
        addJumpingKeyRanges(queryRanges, 3, 40, 8, 28);
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        scanRanges.add(StatsTestUtil.getKeyRange(1, 10));
        scanRanges.add(StatsTestUtil.getKeyRange(10, 20));
        scanRanges.add(StatsTestUtil.getKeyRange(20, 28));
        scanRanges.add(StatsTestUtil.getKeyRange(41, 50));
        scanRanges.add(StatsTestUtil.getKeyRange(50, 60));
        scanRanges.add(StatsTestUtil.getKeyRange(60, 68));
        scanRanges.add(StatsTestUtil.getKeyRange(81, 90));
        scanRanges.add(StatsTestUtil.getKeyRange(90, 100));
        scanRanges.add(StatsTestUtil.getKeyRange(100, 108));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 0, 2));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 4, 0, 6));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 8, 0, 9));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 1, 0, 1, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);
    }

    /**
     * The tree data structure built for guide post info is a complete binary tree.
     */
    @Test
    public void testMultipleDataChunksMultipleGuidePostsWithCompleteBinaryTree() {
        // GuidePostInfo object built contains 15 data chunk plus the ending chunk.
        // The representation of chunks with each chunk in the format of {Key Range of chunk i | [guide post 0, guide 1, ...]}:
        // {(UNBOUND, 100] | [10, 20, ..., 100]}, {(100, 200] | [110, 120, ..., 200]}, ...,
        // {(1410, 1500] | [1410, 1420, ..., 1500]}, {(1500, UNBOUND) | [UNBOUND])}
        GuidePostsInfo info = StatsTestUtil.buildGuidePostInfo(10, 10, 1500, 10);
        DecodedGuidePostChunkCache cache = new DecodedGuidePostChunkCache(info.getGuidePostChunks());

        List<KeyRange> queryRanges = Lists.newArrayList();
        queryRanges.add(KeyRange.EVERYTHING_RANGE);
        List<KeyRange> scanRanges = Lists.newArrayList();
        addSequentialKeyRanges(scanRanges, 10, 10, 1500, Integer.MIN_VALUE, Integer.MAX_VALUE);
        List<GuidePostIterator> iterators = Lists.newArrayList();
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(KeyRange.EMPTY_RANGE);
        // Should always have no estimation and no scan queryRanges returned for empty query range.
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 0));
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 0, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 0, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 1035));
        addSequentialKeyRanges(scanRanges, 10, 10, 1035, Integer.MIN_VALUE, 1035);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 10, 3));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 1700));
        addSequentialKeyRanges(scanRanges, 10, 10, 1500, Integer.MIN_VALUE, 1700);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(0, 1));
        scanRanges.add(StatsTestUtil.getKeyRange(0, true, 1, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 0, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(5, 1495));
        addSequentialKeyRanges(scanRanges, 10, 10, 1490, 5, 1495);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 14, 9));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(5, 1501));
        addSequentialKeyRanges(scanRanges, 10, 10, 1500, 5, 1501);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(1500, 1501));
        addSequentialKeyRanges(scanRanges, 1500, 10, 1500, 1500, 1501);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 15, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(1700, 1800));
        addSequentialKeyRanges(scanRanges, 1500, 10, 1500, 1700, 1800);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 15, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(1, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 10, 10, 1500, 1, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(10, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 20, 10, 1500, 10, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 1, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(95, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 100, 10, 1500, 95, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 9, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(800, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 810, 10, 1500, 800, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 8, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(1700, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 1500, 10, 1500, 1700, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 15, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        // Evey guide post is hit by a query range.
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 151, 10, 4, 6);
        addJumpingKeyRanges(scanRanges, 151, 10, 4, 6);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Evey guide post is hit by three query ranges without covering guide post boundaries.
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 151, 10, 1, 2);
        addJumpingKeyRanges(queryRanges, 151, 10, 4, 6);
        addJumpingKeyRanges(queryRanges, 151, 10, 8, 9);
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        addJumpingKeyRanges(scanRanges, 151, 10, 1, 9);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Evey guide post is hit by three query ranges with covering guide post boundaries.
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 151, 10, 0, 2);
        addJumpingKeyRanges(queryRanges, 151, 10, 4, 6);
        addJumpingKeyRanges(queryRanges, 151, 10, 8, 10);
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        addJumpingKeyRanges(scanRanges, 151, 10, 0, 10);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Evey guide post is hit by three query ranges without covering guide post boundaries.
        // Plus (UNBOUND, 0) and (1700, UNBOUND) at the head and tail.
        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 0));
        addJumpingKeyRanges(queryRanges, 151, 10, 1, 2);
        addJumpingKeyRanges(queryRanges, 151, 10, 4, 6);
        addJumpingKeyRanges(queryRanges, 151, 10, 8, 9);
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        queryRanges.add(StatsTestUtil.getKeyRange(1700, Integer.MAX_VALUE));
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 9));
        addJumpingKeyRanges(scanRanges, 149, 10, 11, 19);
        scanRanges.add(StatsTestUtil.getKeyRange(1501, Integer.MAX_VALUE));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Evey guide post is hit by three query ranges with covering guide post boundaries.
        // Plus (UNBOUND, 0) and (1700, UNBOUND) at the head and tail.
        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 0));
        addJumpingKeyRanges(queryRanges, 151, 10, 0, 2);
        addJumpingKeyRanges(queryRanges, 151, 10, 4, 6);
        addJumpingKeyRanges(queryRanges, 151, 10, 8, 10);
        queryRanges.add(StatsTestUtil.getKeyRange(1700, Integer.MAX_VALUE));
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 10));
        addJumpingKeyRanges(scanRanges, 149, 10, 10, 20);
        scanRanges.add(StatsTestUtil.getKeyRange(1500, Integer.MAX_VALUE));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Every guide post at the even position is hit by a query range
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 76, 20, 4, 6);
        addJumpingKeyRanges(scanRanges, 76, 20, 4, 6);
        for (int i = 0; i < 150; i += 2) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i / 10, i % 10, i / 10, i % 10));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 15, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Every guide post at the odd position is hit by a query range
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 76, 20, 14, 16);
        addJumpingKeyRanges(scanRanges, 76, 20, 14, 16);
        for (int i = 1; i < 150; i += 2) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i / 10, i % 10, i / 10, i % 10));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 15, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        // Multiple query ranges are within a guide post chunk plus the last query range covering the current chunk
        // and the next several chunks.
        // [10, 30), [50, 60), [80, 280), [410, 430), [450, 460), [480, 680), [810, 830), [850, 860), [880, 1080),
        // [1210, 1230), [1250, 1260), [1280, 1480), [1610, 1630), [1650, 1660), [1680, 1880)
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 5, 400, 10, 30);
        addJumpingKeyRanges(queryRanges, 5, 400, 50, 60);
        addJumpingKeyRanges(queryRanges, 5, 400, 80, 280);
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        for (int i = 0; i < 1500; i += 400) {
            scanRanges.add(StatsTestUtil.getKeyRange(10 + i, 20 + i));
            scanRanges.add(StatsTestUtil.getKeyRange(20 + i, 30 + i));
            scanRanges.add(StatsTestUtil.getKeyRange(50 + i, 60 + i));
            addSequentialKeyRanges(scanRanges, 80 + i, 10, 280 + i, 80 + i, 280 + i);
        }
        scanRanges.add(StatsTestUtil.getKeyRange(1610, 1880));
        for (int i = 0; i < 15; i += 4) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 1, i, 2));
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 5, i, 5));
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 8, i + 2, 7));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 15, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);
    }

    /**
     * Test the case when the query key range's lower boundary is equal to the node range's upper boundary and both are inclusive.
     * The tree data structure built for guide post info is a complete binary tree.
     */
    @Test
    public void testQueryLowerRangeEqualToChunkUpperRangeWithCompleteBinaryTree() {
        // GuidePostInfo object built contains 15 data chunk plus the ending chunk.
        // The representation of chunks with each chunk in the format of {Key Range of chunk i | [guide post 0, guide 1, ...]}:
        // {(UNBOUND, 100] | [10, 20, ..., 100]}, {(100, 200] | [110, 120, ..., 200]}, ...,
        // {(1410, 1500] | [1410, 1420, ..., 1500]}, {(1500, UNBOUND) | [UNBOUND])}
        GuidePostsInfo info = StatsTestUtil.buildGuidePostInfo(10, 10, 1500, 10);
        DecodedGuidePostChunkCache cache = new DecodedGuidePostChunkCache(info.getGuidePostChunks());

        List<KeyRange> queryRanges = Lists.newArrayList();
        List<KeyRange> scanRanges = Lists.newArrayList();
        List<GuidePostIterator> iterators = Lists.newArrayList();

        // [0, 5), [100, 105), [200, 205), ..., [1500, 1505)
        // Every query key range is within the first guide post of a chunk
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 16, 100, 0, 5);
        for (int i = 0; i < 1500; i += 100) {
            addSequentialKeyRanges(scanRanges, i, 10, i, i, i + 5);
        }
        addSequentialKeyRanges(scanRanges, 1500, 10, 1500, 1500, 1505);
        for (int i = 0; i < 15; i++) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 0, i, 0));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 15, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // [0, 65), [100, 165), [200, 265), ..., [1400, 1465), [1500, 1565)
        // Every query key range is across the first several guide posts of a chunk
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 16, 100, 0, 65);
        for (int i = 0; i < 1500; i += 100) {
            addSequentialKeyRanges(scanRanges, i, 10, i + 60, i, i + 65);
        }
        addSequentialKeyRanges(scanRanges, 1500, 10, 1500, 1500, 1565);
        for (int i = 0; i < 15; i++) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 0, i, 6));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 15, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // [0, 165), [200, 365), [400, 565), ..., [1400, 1565)
        // Every query key range is across the first several guide posts of a chunk
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 8, 200, 0, 165);
        for (int i = 0; i < 1300; i += 200) {
            addSequentialKeyRanges(scanRanges, i, 10, i + 160, i, i + 165);
        }
        addSequentialKeyRanges(scanRanges, 1400, 10, 1500, 1400, 1565);
        for (int i = 0; i < 13; i += 2) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 0, i + 1, 6));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 14, 0, 14, 9));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 15, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // [0, 265), [300, 565), [600, 865), [900, 1165), [1200, 1465), [1500, 1765]
        // Every query key range is across the first several guide posts of a chunk
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 6, 300, 0, 265);
        for (int i = 0; i < 1300; i += 300) {
            addSequentialKeyRanges(scanRanges, i, 10, i + 260, i, i + 265);
        }
        addSequentialKeyRanges(scanRanges, 1500, 10, 1500, 1500, 1765);
        for (int i = 0; i < 13; i += 3) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 0, i + 2, 6));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 15, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // [0, 365), [400, 765), [800, 1165), [1200, 1565]
        // Every query key range is across the first several guide posts of a chunk
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 4, 400, 0, 365);
        for (int i = 0; i < 900; i += 400) {
            addSequentialKeyRanges(scanRanges, i, 10, i + 360, i, i + 365);
        }
        addSequentialKeyRanges(scanRanges, 1200, 10, 1500, 1200, 1565);
        for (int i = 0; i < 9; i += 4) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 0, i + 3, 6));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 12, 0, 14, 9));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 15, 0, 15, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // [0, 565), [800, 1365)
        // Every query key range is across the first several guide posts of a chunk
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 2, 800, 0, 565);
        for (int i = 0; i < 900; i += 800) {
            addSequentialKeyRanges(scanRanges, i, 10, i + 560, i, i + 565);
        }
        for (int i = 0; i < 9; i += 8) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 0, i + 5, 6));
        }
        verifyQueryResult(info, queryRanges, iterators, scanRanges);
    }

    /**
     *  The tree data structure built for guide post info is an incomplete binary tree.
     */
    @Test
    public void testMultipleDataChunksMultipleGuidePostsWithIncompleteBinaryTree() {
        // GuidePostInfo object built contains 20 data chunk plus the ending chunk.
        // The representation of chunks with each chunk in the format of {Key Range of chunk i | [guide post 0, guide 1, ...]}:
        // {(UNBOUND, 100] | [10, 20, ..., 100]}, {(100, 200] | [110, 120, ..., 200]}, ...,
        // {(1910, 2000] | [1910, 1920, ..., 2000]}, {(2000, UNBOUND) | [UNBOUND])}
        GuidePostsInfo info = StatsTestUtil.buildGuidePostInfo(10, 10, 2000, 10);
        DecodedGuidePostChunkCache cache = new DecodedGuidePostChunkCache(info.getGuidePostChunks());

        List<KeyRange> queryRanges = Lists.newArrayList();
        queryRanges.add(KeyRange.EVERYTHING_RANGE);
        List<KeyRange> scanRanges = Lists.newArrayList();
        addSequentialKeyRanges(scanRanges, 10, 10, 2000, Integer.MIN_VALUE, Integer.MAX_VALUE);
        List<GuidePostIterator> iterators = Lists.newArrayList();
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(KeyRange.EMPTY_RANGE);
        // Should always have no estimation and no scan queryRanges returned for empty query range.
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 0));
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, false, 0, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 0, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 1035));
        addSequentialKeyRanges(scanRanges, 10, 10, 1035, Integer.MIN_VALUE, 1035);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 10, 3));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 2100));
        addSequentialKeyRanges(scanRanges, 10, 10, 2000, Integer.MIN_VALUE, 2100);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(0, 1));
        scanRanges.add(StatsTestUtil.getKeyRange(0, true, 1, false));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 0, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(5, 1995));
        addSequentialKeyRanges(scanRanges, 10, 10, 1990, 5, 1995);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 19, 9));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(5, 2000));
        addSequentialKeyRanges(scanRanges, 10, 10, 2000, 5, 2000);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 19, 9));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(5, 2001));
        addSequentialKeyRanges(scanRanges, 10, 10, 2000, 5, 2001);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(2000, 2001));
        addSequentialKeyRanges(scanRanges, 2000, 10, 2000, 2000, 2001);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 20, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(2100, 2200));
        addSequentialKeyRanges(scanRanges, 2000, 10, 2000, 2100, 2200);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 20, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(1, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 10, 10, 2000, 1, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(10, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 20, 10, 2000, 10, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 1, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(95, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 100, 10, 2000, 95, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 9, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(800, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 810, 10, 2000, 800, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 8, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(2200, Integer.MAX_VALUE));
        addSequentialKeyRanges(scanRanges, 2000, 10, 2000, 2200, Integer.MAX_VALUE);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 20, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        // Evey guide post is hit by a query range.
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 201, 10, 4, 6);
        addJumpingKeyRanges(scanRanges, 201, 10, 4, 6);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Evey guide post is hit by three query ranges without covering guide post boundaries.
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 201, 10, 1, 2);
        addJumpingKeyRanges(queryRanges, 201, 10, 4, 6);
        addJumpingKeyRanges(queryRanges, 201, 10, 8, 9);
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        addJumpingKeyRanges(scanRanges, 201, 10, 1, 9);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Evey guide post is hit by three query ranges with covering guide post boundaries.
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 201, 10, 0, 2);
        addJumpingKeyRanges(queryRanges, 201, 10, 4, 6);
        addJumpingKeyRanges(queryRanges, 201, 10, 8, 10);
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        addJumpingKeyRanges(scanRanges, 201, 10, 0, 10);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Evey guide post is hit by three query ranges without covering guide post boundaries.
        // Plus (UNBOUND, 0) and (2100, UNBOUND) at the head and tail.
        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 0));
        addJumpingKeyRanges(queryRanges, 201, 10, 1, 2);
        addJumpingKeyRanges(queryRanges, 201, 10, 4, 6);
        addJumpingKeyRanges(queryRanges, 201, 10, 8, 9);
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        queryRanges.add(StatsTestUtil.getKeyRange(2100, Integer.MAX_VALUE));
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 9));
        addJumpingKeyRanges(scanRanges, 199, 10, 11, 19);
        scanRanges.add(StatsTestUtil.getKeyRange(2001, Integer.MAX_VALUE));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Evey guide post is hit by three query ranges with covering guide post boundaries.
        // Plus (UNBOUND, 0) and (2100, UNBOUND) at the head and tail.
        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 0));
        addJumpingKeyRanges(queryRanges, 201, 10, 0, 2);
        addJumpingKeyRanges(queryRanges, 201, 10, 4, 6);
        addJumpingKeyRanges(queryRanges, 201, 10, 8, 10);
        queryRanges.add(StatsTestUtil.getKeyRange(2100, Integer.MAX_VALUE));
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        scanRanges.add(StatsTestUtil.getKeyRange(Integer.MIN_VALUE, 10));
        addJumpingKeyRanges(scanRanges, 199, 10, 10, 20);
        scanRanges.add(StatsTestUtil.getKeyRange(2000, Integer.MAX_VALUE));
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 0, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Every guide post at the even position is hit by a query range
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 101, 20, 4, 6);
        addJumpingKeyRanges(scanRanges, 101, 20, 4, 6);
        for (int i = 0; i < 200; i += 2) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i / 10, i % 10, i / 10, i % 10));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 20, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // Every guide post at the odd position is hit by a query range
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 101, 20, 14, 16);
        addJumpingKeyRanges(scanRanges, 101, 20, 14, 16);
        for (int i = 1; i < 200; i += 2) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i / 10, i % 10, i / 10, i % 10));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 20, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);

        // Multiple query ranges are within a guide post chunk plus the last query range covering the current chunk
        // and the next several chunks.
        // [10, 30), [50, 60), [80, 280), [410, 430), [450, 460), [480, 680),
        // [810, 830), [850, 860), [880, 1080), [1210, 1230), [1250, 1260), [1280, 1480),
        // [1610, 1630), [1650, 1660), [1680, 1880), [2010, 2030), [2050, 2060), [2080, 2280),
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 6, 400, 10, 30);
        addJumpingKeyRanges(queryRanges, 6, 400, 50, 60);
        addJumpingKeyRanges(queryRanges, 6, 400, 80, 280);
        Collections.sort(queryRanges, KeyRange.COMPARATOR);
        for (int i = 0; i < 2000; i += 400) {
            scanRanges.add(StatsTestUtil.getKeyRange(10 + i, 20 + i));
            scanRanges.add(StatsTestUtil.getKeyRange(20 + i, 30 + i));
            scanRanges.add(StatsTestUtil.getKeyRange(50 + i, 60 + i));
            addSequentialKeyRanges(scanRanges, 80 + i, 10, 280 + i, 80 + i, 280 + i);
        }
        scanRanges.add(StatsTestUtil.getKeyRange(2010, 2280));
        for (int i = 0; i < 20; i += 4) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 1, i, 2));
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 5, i, 5));
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 8, i + 2, 7));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 20, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges, true);
    }

    /**
     * Test the case when the query key range's lower boundary is equal to the node range's upper boundary and both are inclusive.
     * The tree data structure built for guide post info is an incomplete binary tree.
     */
    @Test
    public void testQueryLowerRangeEqualToChunkUpperRangeWithIncompleteBinaryTree() {
        // GuidePostInfo object built contains 20 data chunk plus the ending chunk.
        // The representation of chunks with each chunk in the format of {Key Range of chunk i | [guide post 0, guide 1, ...]}:
        // {(UNBOUND, 100] | [10, 20, ..., 100]}, {(100, 200] | [110, 120, ..., 200]}, ...,
        // {(1910, 2000] | [1910, 1920, ..., 2000]}, {(2000, UNBOUND) | [UNBOUND])}
        GuidePostsInfo info = StatsTestUtil.buildGuidePostInfo(10, 10, 2000, 10);
        DecodedGuidePostChunkCache cache = new DecodedGuidePostChunkCache(info.getGuidePostChunks());

        List<KeyRange> queryRanges = Lists.newArrayList();
        List<KeyRange> scanRanges = Lists.newArrayList();
        List<GuidePostIterator> iterators = Lists.newArrayList();

        // [0, 5), [100, 105), [200, 205), ..., [2000, 2005)
        // Every query key range, with step 100, is within the first guide post of a chunk
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 21, 100, 0, 5);
        for (int i = 0; i < 2000; i += 100) {
            addSequentialKeyRanges(scanRanges, i, 10, i, i, i + 5);
        }
        addSequentialKeyRanges(scanRanges, 2000, 10, 2000, 2000, 2005);
        for (int i = 0; i < 20; i++) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 0, i, 0));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 20, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // [0, 65), [100, 165), [200, 265), ..., [2000, 2065)
        // Every query key range, with step 100, is across the first several guide posts of a chunk
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 21, 100, 0, 65);
        for (int i = 0; i < 2000; i += 100) {
            addSequentialKeyRanges(scanRanges, i, 10, i + 60, i, i + 65);
        }
        addSequentialKeyRanges(scanRanges, 2000, 10, 2000, 2000, 2065);
        for (int i = 0; i < 20; i++) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 0, i, 6));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 20, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // [0, 165), [200, 365), [400, 565), ..., [2000, 2165)
        // Every query key range, with step 200, is across the first several guide posts of a chunk
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 11, 200, 0, 165);
        for (int i = 0; i < 2000; i += 200) {
            addSequentialKeyRanges(scanRanges, i, 10, i + 160, i, i + 165);
        }
        addSequentialKeyRanges(scanRanges, 2000, 10, 2000, 2000, 2165);
        for (int i = 0; i < 20; i += 2) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 0, i + 1, 6));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 20, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // [0, 265), [300, 565), [600, 865), [900, 1165), ..., [1800, 2065]
        // Every query key range, with step 300, is across the first several guide posts of a chunk
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 7, 300, 0, 265);
        for (int i = 0; i < 1800; i += 300) {
            addSequentialKeyRanges(scanRanges, i, 10, i + 260, i, i + 265);
        }
        addSequentialKeyRanges(scanRanges, 1800, 10, 2000, 1800, 2000);
        addSequentialKeyRanges(scanRanges, 2000, 10, 2000, 2000, 2065);
        for (int i = 0; i < 18; i += 3) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 0, i + 2, 6));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 18, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // [0, 365), [400, 765), [800, 1165), ..., [2000, 2365]
        // Every query key range, with step 400, is across the first several guide posts of a chunk
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 6, 400, 0, 365);
        for (int i = 0; i < 2000; i += 400) {
            addSequentialKeyRanges(scanRanges, i, 10, i + 360, i, i + 365);
        }
        addSequentialKeyRanges(scanRanges, 2000, 10, 2000, 2000, 2365);
        for (int i = 0; i < 20; i += 4) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 0, i + 3, 6));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 20, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // [0, 565), [800, 1365), [1600, 2165]
        // Every query key range, with step 800, is across the first several guide posts of a chunk
        clearData(queryRanges, scanRanges, iterators);
        addJumpingKeyRanges(queryRanges, 3, 800, 0, 565);
        for (int i = 0; i < 1600; i += 800) {
            addSequentialKeyRanges(scanRanges, i, 10, i + 560, i, i + 565);
        }
        addSequentialKeyRanges(scanRanges, 1600, 10, 2000, 1600, 2000);
        addSequentialKeyRanges(scanRanges, 2000, 10, 2000, 2000, 2165);

        for (int i = 0; i < 16; i += 8) {
            iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, i, 0, i + 5, 6));
        }
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 16, 0, 20, 0));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);

        // [100, 1000), [1000, 2000)
        // Every query key range, with step 900, is across the first several guide posts of a chunk
        clearData(queryRanges, scanRanges, iterators);
        queryRanges.add(StatsTestUtil.getKeyRange(100, 1000));
        queryRanges.add(StatsTestUtil.getKeyRange(1000, 2000));
        addSequentialKeyRanges(scanRanges, 100, 10, 1000, 100, 1000);
        addSequentialKeyRanges(scanRanges, 1000, 10, 2000, 1000, 2000);
        iterators.add(new GuidePostIterator(info.getGuidePostChunks(), cache, 1, 0, 19, 9));
        verifyQueryResult(info, queryRanges, iterators, scanRanges);
    }
}