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
package org.apache.phoenix.mapreduce.index;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.phoenix.schema.PIndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reducer class that does only one task and that is to update the index state of the table.
 */
public class PhoenixIndexImportDirectReducer extends
        Reducer<ImmutableBytesWritable, IntWritable, NullWritable, NullWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(PhoenixIndexImportDirectReducer.class);
    private Configuration configuration;

    /**
     * Called once at the start of the task.
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        configuration = context.getConfiguration();
    }

    @Override
    protected void reduce(ImmutableBytesWritable arg0, Iterable<IntWritable> arg1,
            Reducer<ImmutableBytesWritable, IntWritable, NullWritable, NullWritable>.Context arg2)
            throws IOException, InterruptedException {
        try {
            IndexToolUtil.updateIndexState(configuration, PIndexState.ACTIVE);
        } catch (SQLException e) {
            LOG.error(" Failed to update the status to Active");
            throw new RuntimeException(e.getMessage());
        }
    }
}
