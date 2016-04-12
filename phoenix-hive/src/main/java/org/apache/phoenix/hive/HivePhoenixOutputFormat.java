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
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.Progressable;

/**
* HivePhoenixOutputFormat
* Custom Phoenix OutputFormat to feed into Hive
*/


public class HivePhoenixOutputFormat<T extends DBWritable>
        implements org.apache.hadoop.mapred.OutputFormat<NullWritable, T> {
    private static final Log LOG = LogFactory.getLog(HivePhoenixOutputFormat.class);

    public RecordWriter<NullWritable, T> getRecordWriter(FileSystem ignored, JobConf job,
            String name, Progressable progress) throws IOException {
        try {
            return new HivePhoenixRecordWriter(job);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
        LOG.debug("checkOutputSpecs");
        
    }
}
