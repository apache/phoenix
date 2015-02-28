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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.util.PhoenixRuntime;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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
        final QueryPlan queryPlan = getQueryPlan(context,configuration);
        @SuppressWarnings("unchecked")
        final Class<T> inputClass = (Class<T>) PhoenixConfigurationUtil.getInputClass(configuration);
        return new PhoenixRecordReader<T>(inputClass , configuration, queryPlan);
    }
    
   

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {  
        final Configuration configuration = context.getConfiguration();
        final QueryPlan queryPlan = getQueryPlan(context,configuration);
        final List<KeyRange> allSplits = queryPlan.getSplits();
        final List<InputSplit> splits = generateSplits(queryPlan,allSplits);
        return splits;
    }

    private List<InputSplit> generateSplits(final QueryPlan qplan, final List<KeyRange> splits) throws IOException {
        Preconditions.checkNotNull(qplan);
        Preconditions.checkNotNull(splits);
        final List<InputSplit> psplits = Lists.newArrayListWithExpectedSize(splits.size());
        for (List<Scan> scans : qplan.getScans()) {
            psplits.add(new PhoenixInputSplit(scans));
        }
        return psplits;
    }
    
    /**
     * Returns the query plan associated with the select query.
     * @param context
     * @return
     * @throws IOException
     * @throws SQLException
     */
    private QueryPlan getQueryPlan(final JobContext context, final Configuration configuration)
            throws IOException {
        Preconditions.checkNotNull(context);
        try {
            final String currentScnValue = configuration.get(PhoenixConfigurationUtil.CURRENT_SCN_VALUE);
            final Properties overridingProps = new Properties();
            if(currentScnValue != null) {
                overridingProps.put(PhoenixRuntime.CURRENT_SCN_ATTRIB, currentScnValue);
            }
            final Connection connection = ConnectionUtil.getInputConnection(configuration, overridingProps);
            final String selectStatement = PhoenixConfigurationUtil.getSelectStatement(configuration);
            Preconditions.checkNotNull(selectStatement);
            final Statement statement = connection.createStatement();
            final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
            // Optimize the query plan so that we potentially use secondary indexes
            final QueryPlan queryPlan = pstmt.optimizeQuery(selectStatement);
            // Initialize the query plan so it sets up the parallel scans
            queryPlan.iterator();
            return queryPlan;
        } catch (Exception exception) {
            LOG.error(String.format("Failed to get the query plan with error [%s]",
                exception.getMessage()));
            throw new RuntimeException(exception);
        }
    }

}
