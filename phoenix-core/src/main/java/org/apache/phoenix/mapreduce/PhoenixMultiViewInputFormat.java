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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.phoenix.mapreduce.util.DefaultPhoenixMultiViewListProvider;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMultiViewListProvider;
import org.apache.phoenix.mapreduce.util.DefaultMultiViewSplitStrategy;
import org.apache.phoenix.mapreduce.util.ViewInfoWritable;
import org.apache.phoenix.mapreduce.util.MultiViewSplitStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PhoenixMultiViewInputFormat<T extends Writable> extends InputFormat<NullWritable,T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixMultiViewInputFormat.class);

    public PhoenixMultiViewInputFormat() {
    }

    @Override public List<InputSplit> getSplits(JobContext context) {
        List<InputSplit> listOfInputSplit = new ArrayList<>();
        try {
            final Configuration configuration = context.getConfiguration();
            Class<?> defaultViewTtlDeletionInputStrategyClazz = DefaultPhoenixMultiViewListProvider.class;
            if (configuration.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_INPUT_STRATEGY_CLAZZ) != null) {
                defaultViewTtlDeletionInputStrategyClazz = Class.forName(
                        configuration.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_INPUT_STRATEGY_CLAZZ));
            }
            PhoenixMultiViewListProvider phoenixMultiViewListProvider =
                    (PhoenixMultiViewListProvider) defaultViewTtlDeletionInputStrategyClazz.newInstance();
            List<ViewInfoWritable> viewsWithTTL = phoenixMultiViewListProvider.getPhoenixMultiViewList(configuration);

            Class<?> defaultViewTtlDeletionSplitStrategyClazz = DefaultMultiViewSplitStrategy.class;
            if (configuration.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_SPLIT_STRATEGY_CLAZZ) != null) {
                defaultViewTtlDeletionSplitStrategyClazz = Class.forName(
                        configuration.get(PhoenixConfigurationUtil.MAPREDUCE_VIEW_TTL_SPLIT_STRATEGY_CLAZZ));
            }
            MultiViewSplitStrategy multiViewSplitStrategy =
                    (MultiViewSplitStrategy) defaultViewTtlDeletionSplitStrategyClazz.newInstance();
            listOfInputSplit = multiViewSplitStrategy.generateSplits(viewsWithTTL, configuration);
        } catch (Exception e) {
            LOGGER.debug(e.getStackTrace().toString());
        }

        return listOfInputSplit;
    }

    @Override
    public RecordReader<NullWritable,T> createRecordReader(InputSplit split, TaskAttemptContext context) {
        final Configuration configuration = context.getConfiguration();

        final Class<T> inputClass = (Class<T>) PhoenixConfigurationUtil.getInputClass(configuration);
        return getPhoenixRecordReader(inputClass, configuration);
    }

    private RecordReader<NullWritable,T> getPhoenixRecordReader(Class<T> inputClass, Configuration configuration) {
        return new PhoenixMultiViewReader<>(inputClass , configuration);
    }
}