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
package org.apache.phoenix.mapreduce.util;

import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.phoenix.mapreduce.PhoenixMultiViewInputSplit;

import java.util.List;

import static org.apache.phoenix.mapreduce.PhoenixTTLTool.DEFAULT_MAPPER_SPLIT_SIZE;

public class DefaultMultiViewSplitStrategy implements MultiViewSplitStrategy {

    public List<InputSplit> generateSplits(List<ViewInfoWritable> views,
                                           Configuration configuration) {
        int numViewsInSplit = PhoenixConfigurationUtil.getMultiViewSplitSize(configuration);

        if (numViewsInSplit < 1) {
            numViewsInSplit = DEFAULT_MAPPER_SPLIT_SIZE;
        }

        int numberOfMappers = getNumberOfMappers(views.size(),numViewsInSplit);

        final List<InputSplit> pSplits = Lists.newArrayListWithExpectedSize(numberOfMappers);
        // Split the views into splits

        for (int i = 0; i < numberOfMappers; i++) {
            pSplits.add(new PhoenixMultiViewInputSplit(views.subList(
                    i * numViewsInSplit, getUpperBound(numViewsInSplit, i, views.size()))));
        }

        return pSplits;
    }

    /*
        Calculate number of mappers are needed based on split policy and
        number of views on the cluster
     */
    public int getNumberOfMappers(int viewSize, int numViewsInSplit) {
        int numberOfMappers = viewSize / numViewsInSplit;
        if (viewSize % numViewsInSplit > 0) {
            numberOfMappers++;
        }
        return numberOfMappers;
    }

    /*
        Calculate the upper bound for each mapper. For example, given
        split policy is 10 cleanup jobs per mapper, and the total view size at the cluster
        is 12.
        The first mapper will take from [0 - 10), this method will return 10 as upper bound
        The second mapper will take from [10 - 12), this method will return 12 as upper bound.
     */
    public int getUpperBound(int numViewsInSplit, int i, int viewSize) {
        int upper = (i + 1) * numViewsInSplit;
        if (viewSize < upper) {
            upper = viewSize;
        }

        return upper;
    }
}