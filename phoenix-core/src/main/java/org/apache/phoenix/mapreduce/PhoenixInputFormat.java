/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.mapreduce.util.ConfigurationUtil;
import org.apache.phoenix.mapreduce.util.ConfigurationUtil.SchemaType;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.SaltingUtil;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.ScanUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Describe your class here.
 *
 */
public class PhoenixInputFormat<T extends DBWritable> extends InputFormat<NullWritable,T> {

    private static final Log LOG = LogFactory.getLog(PhoenixInputFormat.class);
    private Configuration configuration;
    private Connection connection;
    private QueryPlan  queryPlan;
   
    /**
     * instantiated by framework
     */
    public PhoenixInputFormat() {
    }
    
    @Override
    public RecordReader<NullWritable,T> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        setConf(context.getConfiguration());
        final QueryPlan queryPlan = getQueryPlan(context);
        @SuppressWarnings("unchecked")
		final Class<T> inputClass = (Class<T>) ConfigurationUtil.getInputClass(configuration);
        return new PhoenixRecordReader<T>(inputClass , configuration, queryPlan);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    	setConf(context.getConfiguration());
        final QueryPlan queryPlan = getQueryPlan(context);
        final List<KeyRange> allSplits = queryPlan.getSplits();
        final List<InputSplit> splits = generateSplits(queryPlan,allSplits);
        return splits;
    }

    private List<InputSplit> generateSplits(final QueryPlan qplan, final List<KeyRange> splits) throws IOException {
        Preconditions.checkNotNull(qplan);
        Preconditions.checkNotNull(splits);
        final List<InputSplit> psplits = Lists.newArrayListWithExpectedSize(splits.size());
        final StatementContext context = qplan.getContext();
        final TableRef tableRef = qplan.getTableRef();
        for (KeyRange split : splits) {
            final Scan splitScan = new Scan(context.getScan());
            if (tableRef.getTable().getBucketNum() != null) {
                KeyRange minMaxRange = context.getMinMaxRange();
                if (minMaxRange != null) {
                    minMaxRange = SaltingUtil.addSaltByte(split.getLowerRange(), minMaxRange);
                    split = split.intersect(minMaxRange);
                }
            }
            // as the intersect code sets the actual start and stop row within the passed splitScan, we are fetching it back below.
            if (ScanUtil.intersectScanRange(splitScan, split.getLowerRange(), split.getUpperRange(), context.getScanRanges().useSkipScanFilter())) {
                final PhoenixInputSplit inputSplit = new PhoenixInputSplit(KeyRange.getKeyRange(splitScan.getStartRow(), splitScan.getStopRow()));
                psplits.add(inputSplit);     
            }
        }
        return psplits;
    }
    
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConf() {
        return this.configuration;
    }
    
    private Connection getConnection() {
        try {
            if (this.connection == null) {
                this.connection = ConnectionUtil.getConnection(this.configuration);
           }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return connection;
    }
    
    /**
     * Returns the query plan associated with the select query.
     * @param context
     * @return
     * @throws IOException
     * @throws SQLException
     */
    private QueryPlan getQueryPlan(final JobContext context) throws IOException {
        Preconditions.checkNotNull(context);
        if(queryPlan == null) {
            try{
                final Connection connection = getConnection();
                final String selectStatement = ConfigurationUtil.getSelectStatement(this.configuration);
                Preconditions.checkNotNull(selectStatement);
                final Statement statement = connection.createStatement();
                final PhoenixStatement pstmt = statement.unwrap(PhoenixStatement.class);
                this.queryPlan = pstmt.compileQuery(selectStatement);
                this.queryPlan.iterator();
            } catch(Exception exception) {
                LOG.error(String.format("Failed to get the query plan with error [%s]",exception.getMessage()));
                throw new RuntimeException(exception);
            }
        }
        return queryPlan;
    }
    
    /**
     * 
     * @param job
     * @param inputClass
     * @param tableName
     * @param fieldNames
     */
    public static void setInput(final Job job, final Class<? extends DBWritable> inputClass, final String tableName , final String conditions, final String... fieldNames) {
          job.setInputFormatClass(PhoenixInputFormat.class);
          final Configuration configuration = job.getConfiguration();
          ConfigurationUtil.setInputTableName(configuration, tableName);
          ConfigurationUtil.setSelectColumnNames(configuration,fieldNames);
          ConfigurationUtil.setInputClass(configuration,inputClass);
          ConfigurationUtil.setSchemaType(configuration, SchemaType.TABLE);
    }
    
    public static void setInput(final Job job, final Class<? extends DBWritable> inputClass, final String tableName, final String inputQuery) {
          job.setInputFormatClass(PhoenixInputFormat.class);
          final Configuration configuration = job.getConfiguration();
          ConfigurationUtil.setInputTableName(configuration, tableName);
          ConfigurationUtil.setInputQuery(configuration, inputQuery);
          ConfigurationUtil.setInputClass(configuration,inputClass);
          ConfigurationUtil.setSchemaType(configuration, SchemaType.QUERY);
     }
    
    /**
     * 
     */
    public static class NullDBWritable implements DBWritable, Writable {
      @Override
      public void readFields(DataInput in) throws IOException { }
      @Override
      public void readFields(ResultSet arg0) throws SQLException { }
      @Override
      public void write(DataOutput out) throws IOException { }
      @Override
      public void write(PreparedStatement arg0) throws SQLException { }
    }
}
