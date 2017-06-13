/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.mapreduce;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;

/**
 * Reads input from multiple Phoenix tables.
 */
public class MultiTableInputFormat extends InputFormat<ImmutableBytesWritable, MultiTableResults> {
    private static final Log LOG = LogFactory.getLog(MultiTableInputFormat.class);

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        final QueryPlan sourceQueryPlan;
        final QueryPlan targetQueryPlan;
        try {
            sourceQueryPlan = PhoenixMapReduceUtil.getQueryPlan(configuration);
            targetQueryPlan = PhoenixMapReduceUtil.getQueryPlan(configuration, true);
        } catch (SQLException e) {
            LOG.error("Could not create query plans", e);
            throw new IOException(e);
        }
        return PhoenixMapReduceUtil
                .generateMultiTableSplits(sourceQueryPlan, targetQueryPlan, configuration);
    }

    @Override
    public RecordReader<ImmutableBytesWritable, MultiTableResults> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        final Configuration configuration = context.getConfiguration();
        final QueryPlan queryPlan;
        try {
            queryPlan = PhoenixMapReduceUtil.getQueryPlan(configuration);
        } catch (SQLException e) {
            LOG.error("Could not get query plan using provided configuration", e);
            throw new IOException(e);
        }
        return new MultiTableRecordReader(configuration, queryPlan);
    }
}
