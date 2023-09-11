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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * A no-op {@link OutputCommitter}
 */
public class PhoenixOutputCommitter extends OutputCommitter {
    
    public PhoenixOutputCommitter() {}

    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
        return true;
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {        
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
    }

}
