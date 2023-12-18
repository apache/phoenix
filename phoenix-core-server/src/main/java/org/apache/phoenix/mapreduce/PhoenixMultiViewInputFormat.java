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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
    This is a generic MultiViewInputFormat class that using by the MR job. You can
    provide your own split strategy and provider class to customize your own business needed by
    overwrite and load class blow:
        MAPREDUCE_MULTI_INPUT_STRATEGY_CLAZZ
        MAPREDUCE_MULTI_INPUT_SPLIT_STRATEGY_CLAZZ
 */
public class PhoenixMultiViewInputFormat<T extends Writable> extends InputFormat<NullWritable,T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixMultiViewInputFormat.class);

    @Override public List<InputSplit> getSplits(JobContext context) throws IOException {
        List<InputSplit> listOfInputSplit;
        try {
            final Configuration configuration = context.getConfiguration();
            Class<?> defaultMultiInputStrategyClazz = DefaultPhoenixMultiViewListProvider.class;
            if (configuration.get(
                    PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_STRATEGY_CLAZZ) != null) {
                defaultMultiInputStrategyClazz = Class.forName(configuration.get(
                        PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_STRATEGY_CLAZZ));
            }
            PhoenixMultiViewListProvider phoenixMultiViewListProvider =
                    (PhoenixMultiViewListProvider) defaultMultiInputStrategyClazz.newInstance();
            List<ViewInfoWritable> views =
                    phoenixMultiViewListProvider.getPhoenixMultiViewList(configuration);

            Class<?> defaultDeletionMultiInputSplitStrategyClazz =
                    DefaultMultiViewSplitStrategy.class;
            if (configuration.get(
                    PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_SPLIT_STRATEGY_CLAZZ) != null) {
                defaultDeletionMultiInputSplitStrategyClazz = Class.forName(configuration.get(
                        PhoenixConfigurationUtil.MAPREDUCE_MULTI_INPUT_SPLIT_STRATEGY_CLAZZ));
            }
            MultiViewSplitStrategy multiViewSplitStrategy = (MultiViewSplitStrategy)
                    defaultDeletionMultiInputSplitStrategyClazz.newInstance();
            listOfInputSplit = multiViewSplitStrategy.generateSplits(views, configuration);
        } catch (ClassNotFoundException e) {
            LOGGER.debug("PhoenixMultiViewInputFormat is getting ClassNotFoundException : " +
                    e.getMessage());
            throw new IOException(
                    "PhoenixMultiViewInputFormat is getting ClassNotFoundException : " +
                            e.getMessage(), e.getCause());
        } catch (InstantiationException e) {
            LOGGER.debug("PhoenixMultiViewInputFormat is getting InstantiationException : " +
                    e.getMessage());
            throw new IOException(
                    "PhoenixMultiViewInputFormat is getting InstantiationException : " +
                            e.getMessage(), e.getCause());
        } catch (IllegalAccessException e) {
            LOGGER.debug("PhoenixMultiViewInputFormat is getting IllegalAccessException : " +
                    e.getMessage());
            throw new IOException(
                    "PhoenixMultiViewInputFormat is getting IllegalAccessException : " +
                            e.getMessage(), e.getCause());
        }

        return listOfInputSplit == null ? new ArrayList<InputSplit>() : listOfInputSplit;
    }

    @Override
    public RecordReader<NullWritable,T> createRecordReader(InputSplit split,
                                                           TaskAttemptContext context) {
        final Configuration configuration = context.getConfiguration();

        final Class<T> inputClass =
                (Class<T>) PhoenixConfigurationUtil.getInputClass(configuration);
        return getPhoenixRecordReader(inputClass, configuration);
    }

    private RecordReader<NullWritable,T> getPhoenixRecordReader(Class<T> inputClass,
                                                                Configuration configuration) {
        return new PhoenixMultiViewReader<>(inputClass , configuration);
    }
}