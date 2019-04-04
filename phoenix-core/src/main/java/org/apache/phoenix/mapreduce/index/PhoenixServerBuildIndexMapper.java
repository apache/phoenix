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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.PhoenixJobCounters;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapper that does not do much as regions servers actually build the index from the data table regions directly
 */
public class PhoenixServerBuildIndexMapper extends
        Mapper<NullWritable, PhoenixIndexDBWritable, ImmutableBytesWritable, IntWritable> {

    private static final Logger logger = LoggerFactory.getLogger(PhoenixServerBuildIndexMapper.class);

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(NullWritable key, PhoenixIndexDBWritable record, Context context)
            throws IOException, InterruptedException {
        context.getCounter(PhoenixJobCounters.INPUT_RECORDS).increment(1);
        // Make sure progress is reported to Application Master.
        context.progress();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new ImmutableBytesWritable(UUID.randomUUID().toString().getBytes()), new IntWritable(0));
        super.cleanup(context);
    }
}
