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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.phoenix.compile.QueryPlan;

import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;


import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * {@link RecordReader} implementation that iterates over the the records.
 */
public class PhoenixRecordReader<T extends DBWritable> extends RecordReader<NullWritable,T> {
    
    private static final Log LOG = LogFactory.getLog(PhoenixRecordReader.class);
    private final Configuration  configuration;
    private final QueryPlan queryPlan;
    private NullWritable key =  NullWritable.get();
    private T value = null;
    private Class<T> inputClass;
    private ResultIterator resultIterator = null;
    private PhoenixResultSet resultSet;
    
    public PhoenixRecordReader(Class<T> inputClass,final Configuration configuration,final QueryPlan queryPlan) {
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(queryPlan);
        this.inputClass = inputClass;
        this.configuration = configuration;
        this.queryPlan = queryPlan;
    }

    @Override
    public void close() throws IOException {
       if(resultIterator != null) {
           try {
               resultIterator.close();
        } catch (SQLException e) {
           LOG.error(" Error closing resultset.");
           throw new RuntimeException(e);
        }
       }
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public T getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void  initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        final PhoenixInputSplit pSplit = (PhoenixInputSplit)split;
        final List<Scan> scans = pSplit.getScans();
        try {
            this.resultIterator = PhoenixMapReduceUtil.initializeIterator(this.configuration, scans, queryPlan);
            this.resultSet = PhoenixMapReduceUtil.initializeResultSet(this.resultIterator, this.queryPlan);
        } catch (SQLException e) {
            LOG.error(String.format(" Error [%s] initializing PhoenixRecordReader. ",e.getMessage()));
            Throwables.propagate(e);
        }
   }

   @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = NullWritable.get();
        }
        if (value == null) {
            value =  ReflectionUtils.newInstance(inputClass, this.configuration);
        }
        Preconditions.checkNotNull(this.resultSet);
        try {
            if(!resultSet.next()) {
                return false;
            }
            value.readFields(resultSet);
            return true;
        } catch (SQLException e) {
            LOG.error(String.format(" Error [%s] occurred while iterating over the resultset. ",e.getMessage()));
            throw new RuntimeException(e);
        }
    }
}
