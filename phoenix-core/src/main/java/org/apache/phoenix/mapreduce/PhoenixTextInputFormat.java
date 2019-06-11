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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around TextInputFormat which can ignore the first line in the first InputSplit
 * for a file.
 */
public class PhoenixTextInputFormat extends TextInputFormat {
  public static final String SKIP_HEADER_KEY = "phoenix.input.format.skip.header";

  public static void setSkipHeader(Configuration conf) {
    conf.setBoolean(SKIP_HEADER_KEY, true);
  }

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    RecordReader<LongWritable,Text> rr = super.createRecordReader(split, context);
    
    return new PhoenixLineRecordReader((LineRecordReader) rr);
  }

  public static class PhoenixLineRecordReader extends RecordReader<LongWritable,Text> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixLineRecordReader.class);
    private final LineRecordReader rr;
    private PhoenixLineRecordReader(LineRecordReader rr) {
      this.rr = rr;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
      rr.initialize(genericSplit, context);
      final Configuration conf = context.getConfiguration();
      final FileSplit split = (FileSplit) genericSplit;
      if (conf.getBoolean(SKIP_HEADER_KEY, false) && split.getStart() == 0) {
        LOGGER.trace("Consuming first key-value from {}", genericSplit);
        nextKeyValue();
      } else {
        LOGGER.trace("Not configured to skip header or not the first input split: {}", split);
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return rr.nextKeyValue();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException {
      return rr.getCurrentKey();
    }

    @Override
    public Text getCurrentValue() throws IOException {
      return rr.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException {
      return rr.getProgress();
    }

    @Override
    public void close() throws IOException {
      rr.close();
    }
  }
}
