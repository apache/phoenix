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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.phoenix.mapreduce.util.ViewInfoWritable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class PhoenixMultiViewReader<T extends Writable> extends RecordReader<NullWritable,T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixMultiViewReader.class);

    private Configuration  configuration;
    private Class<T> inputClass;
    Iterator<ViewInfoWritable> it;

    public PhoenixMultiViewReader() {

    }

    public PhoenixMultiViewReader(final Class<T> inputClass, final Configuration configuration) {
        this.configuration = configuration;
        this.inputClass = inputClass;
    }

    @Override public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException {
        if (split instanceof PhoenixMultiViewInputSplit) {
            final PhoenixMultiViewInputSplit pSplit = (PhoenixMultiViewInputSplit)split;
            final List<ViewInfoWritable> viewInfoTracker = pSplit.getViewInfoTrackerList();
            it = viewInfoTracker.iterator();
        } else {
            LOGGER.error("InputSplit class cannot cast to PhoenixMultiViewInputSplit.");
            throw new IOException("InputSplit class cannot cast to PhoenixMultiViewInputSplit");
        }
    }

    @Override public boolean nextKeyValue() throws IOException, InterruptedException {
        return it.hasNext();
    }

    @Override public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override public T getCurrentValue() throws IOException, InterruptedException {
        ViewInfoWritable currentValue = null;
        if (it.hasNext()) {
            currentValue = it.next();
        }
        return (T)currentValue;
    }

    @Override public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override public void close() throws IOException {

    }
}