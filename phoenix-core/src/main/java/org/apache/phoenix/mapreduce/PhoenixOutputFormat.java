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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.mapreduce.util.ConfigurationUtil;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;

public class PhoenixOutputFormat<T extends DBWritable> extends OutputFormat<NullWritable,T> {
    private static final Log LOG = LogFactory.getLog(PhoenixOutputFormat.class);
    
    private Connection connection;
    private Configuration config;

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {      
    }

    /**
     * TODO Implement {@link OutputCommitter} to rollback in case of task failure
     */
    
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new PhoenixOutputCommitter(this);
    }

    @Override
    public RecordWriter<NullWritable, T> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            return new PhoenixRecordWriter<T>(getConnection(context.getConfiguration()), config);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
    
    /**
     * This method creates a database connection. A single instance is created
     * and passed around for re-use.
     * 
     * @param configuration
     * @return
     * @throws IOException
     */
    synchronized Connection getConnection(Configuration configuration) throws IOException {
        if (connection != null) { 
            return connection; 
        }
        
        config = configuration;       
        try {
            LOG.info("Initializing new Phoenix connection...");
            connection = ConnectionUtil.getConnection(configuration);
            LOG.info("Initialized Phoenix connection, autoCommit="+ connection.getAutoCommit());
            return connection;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
   
   
    
    /**
     * 
     * @param job
     * @param outputClass
     * @param tableName
     * @param columns
     */
    public static void setOutput(final Job job, final String tableName,final String columns) {
        job.setOutputFormatClass(PhoenixOutputFormat.class);
        final Configuration configuration = job.getConfiguration();
        ConfigurationUtil.setOutputTableName(configuration, tableName);
        ConfigurationUtil.setUpsertColumnNames(configuration,columns);
    }
    
    
    /**
     * 
     * @param job
     * @param outputClass
     * @param tableName
     * @param fieldNames
     */
    public static void setOutput(final Job job, final String tableName , final String... fieldNames) {
          job.setOutputFormatClass(PhoenixOutputFormat.class);
          final Configuration configuration = job.getConfiguration();
          ConfigurationUtil.setOutputTableName(configuration, tableName);
          ConfigurationUtil.setUpsertColumnNames(configuration,fieldNames);
    }

}
