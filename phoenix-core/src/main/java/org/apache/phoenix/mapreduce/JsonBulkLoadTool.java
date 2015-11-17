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

import java.sql.SQLException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.phoenix.util.ColumnInfo;

/**
 * A tool for running MapReduce-based ingests of JSON data. Nested JSON data structures are not
 * handled, though lists are converted into typed ARRAYS.
 */
public class JsonBulkLoadTool extends AbstractBulkLoadTool {

    @Override
    protected void configureOptions(CommandLine cmdLine, List<ColumnInfo> importColumns,
                                         Configuration conf) throws SQLException {
        // noop
    }

    @Override
    protected void setupJob(Job job) {
        // Allow overriding the job jar setting by using a -D system property at startup
        if (job.getJar() == null) {
            job.setJarByClass(JsonToKeyValueMapper.class);
        }
        job.setMapperClass(JsonToKeyValueMapper.class);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new JsonBulkLoadTool(), args);
    }
}
