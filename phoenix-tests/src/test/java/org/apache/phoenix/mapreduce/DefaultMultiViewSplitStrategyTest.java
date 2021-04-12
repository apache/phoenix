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
package org.apache.phoenix.mapreduce;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.phoenix.mapreduce.util.DefaultMultiViewSplitStrategy;
import org.apache.phoenix.mapreduce.util.ViewInfoTracker;
import org.apache.phoenix.mapreduce.util.ViewInfoWritable;
import org.apache.phoenix.query.BaseTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_MAPPER_SPLIT_SIZE;
import static org.junit.Assert.assertEquals;

public class DefaultMultiViewSplitStrategyTest extends BaseTest {
    DefaultMultiViewSplitStrategy defaultMultiViewSplitStrategy =
            new DefaultMultiViewSplitStrategy();
    @Test
    public void testGetUpperBound() {
        // given split policy to be 10 with view size 12
        // we expect 2 mappers with range [0,10) and [10,12)
        assertEquals(10,
                defaultMultiViewSplitStrategy.getUpperBound(10, 0, 12));
        assertEquals(12,
                defaultMultiViewSplitStrategy.getUpperBound(10, 1, 12));

        // given split policy to be 8 with view size 12
        // we expect 2 mappers with range [0,8) and [8,12)
        assertEquals(8,
                defaultMultiViewSplitStrategy.getUpperBound(8, 0, 12));
        assertEquals(12,
                defaultMultiViewSplitStrategy.getUpperBound(8, 1, 12));

        // given split policy to be 5 with view size 12
        // we expect 1 mappers with range [0,1)
        assertEquals(1,
                defaultMultiViewSplitStrategy.getUpperBound(5, 0, 1));
    }

    @Test
    public void testGetNumberOfMappers() {
        int viewSize = 0;
        int numViewsInSplit = 10;

        // test empty cluster, which is view size is 0
        assertEquals(0,
                defaultMultiViewSplitStrategy.getNumberOfMappers(viewSize,numViewsInSplit));

        viewSize = 9;
        // test viewSize is less than numViewsInSplit
        assertEquals(1,
                defaultMultiViewSplitStrategy.getNumberOfMappers(viewSize,numViewsInSplit));

        // test viewSize is equal to numViewsInSplit
        viewSize = 10;
        assertEquals(1,
                defaultMultiViewSplitStrategy.getNumberOfMappers(viewSize,numViewsInSplit));

        // test viewSize is greater than numViewsInSplit
        viewSize = 11;
        assertEquals(2,
                defaultMultiViewSplitStrategy.getNumberOfMappers(viewSize,numViewsInSplit));
    }

    @Test
    public void testGenerateSplits() {
        // test number of views greater than split policy
        testGenerateSplits(11, 10, 2);

        // test number of views equal to split policy
        testGenerateSplits(10, 10, 1);

        // test number of views equal to split policy
        testGenerateSplits(8, 10, 1);

        // test number of views is 0
        testGenerateSplits(0, 10, 0);

        // test split policy is 0
        testGenerateSplits(8, 0, 1);
    }

    private void testGenerateSplits(int numberOfViews, int splitPolicy, int expectedResultSize) {
        List<ViewInfoWritable> views = new ArrayList<>();
        for (int i = 0; i < numberOfViews; i++) {
            views.add(new ViewInfoTracker());
        }
        config.set(MAPREDUCE_MULTI_INPUT_MAPPER_SPLIT_SIZE, String.valueOf(splitPolicy));
        List<InputSplit> result = defaultMultiViewSplitStrategy.generateSplits(views, config);
        assertEquals(expectedResultSize, result.size());
    }
}