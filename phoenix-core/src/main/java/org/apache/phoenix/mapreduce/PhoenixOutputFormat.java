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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * {@link OutputFormat} implementation for Phoenix.
 *
 */
public class PhoenixOutputFormat <T extends DBWritable> extends OutputFormat<NullWritable,T> {
    private static final Log LOG = LogFactory.getLog(PhoenixOutputFormat.class);
    
    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {      
    }
    
    /**
     * 
     */
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new PhoenixOutputCommitter();
    }

    @Override
    public RecordWriter<NullWritable, T> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            return new PhoenixRecordWriter<T>(context.getConfiguration());
        } catch (SQLException e) {
            LOG.error("Error calling PhoenixRecordWriter "  + e.getMessage());
            throw new RuntimeException(e);
        }
    }

}
