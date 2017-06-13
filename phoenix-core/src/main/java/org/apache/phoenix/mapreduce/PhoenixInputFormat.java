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
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;
import org.apache.phoenix.query.KeyRange;

/**
 * {@link InputFormat} implementation from Phoenix.
 * 
 */
public class PhoenixInputFormat<T extends DBWritable> extends InputFormat<NullWritable,T> {

    private static final Log LOG = LogFactory.getLog(PhoenixInputFormat.class);
       
    /**
     * instantiated by framework
     */
    public PhoenixInputFormat() {
    }

    @Override
    public RecordReader<NullWritable,T> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        
        final Configuration configuration = context.getConfiguration();
        final QueryPlan queryPlan;
        try {
            queryPlan = PhoenixMapReduceUtil.getQueryPlan(configuration);
        } catch (SQLException e) {
            LOG.error(String.format("Failed to get the query plan with error [%s]",
                    e.getMessage()));
            throw new IOException(e);
        }
        @SuppressWarnings("unchecked")
        final Class<T> inputClass = (Class<T>) PhoenixConfigurationUtil.getInputClass(configuration);
        return new PhoenixRecordReader<T>(inputClass , configuration, queryPlan);
    }
    
   

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {  
        final Configuration configuration = context.getConfiguration();
        final QueryPlan queryPlan;
        try {
            queryPlan = PhoenixMapReduceUtil.getQueryPlan(configuration);
        } catch (SQLException e) {
            LOG.error(String.format("Failed to get the query plan with error [%s]",
                    e.getMessage()));
            throw new IOException(e);
        }
        final List<KeyRange> allSplits = queryPlan.getSplits();
        final List<InputSplit> splits =
                PhoenixMapReduceUtil.generateSplits(queryPlan, allSplits, configuration);
        return splits;
    }

}
