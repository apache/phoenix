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
package org.apache.phoenix.hive;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.Progressable;
import org.apache.phoenix.hive.util.HiveConnectionUtil;

/**
* HivePhoenixOutputFormat
* Need to extend the standard PhoenixOutputFormat but also implement the mapred OutputFormat for Hive compliance
*
* @version 1.0
* @since   2015-02-08 
*/


public class HivePhoenixOutputFormat<T extends DBWritable> extends org.apache.phoenix.mapreduce.PhoenixOutputFormat<T> implements
org.apache.hadoop.mapred.OutputFormat<NullWritable, T> {
    private static final Log LOG = LogFactory.getLog(HivePhoenixOutputFormat.class);
    private Connection connection;
    private Configuration config;

    public RecordWriter<NullWritable, T> getRecordWriter(FileSystem ignored, JobConf job,
            String name, Progressable progress) throws IOException {
        try {
            return new HivePhoenixRecordWriter(getConnection(job), job);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        LOG.debug("checkOutputSpecs");
        
    }
    
    synchronized Connection getConnection(Configuration configuration) throws IOException {
        if (this.connection != null) {
            return this.connection;
        }

        this.config = configuration;
        try {
            LOG.info("Initializing new Phoenix connection...");
            this.connection = HiveConnectionUtil.getConnection(configuration);
            LOG.info("Initialized Phoenix connection, autoCommit="
                    + this.connection.getAutoCommit());
            return this.connection;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

}
