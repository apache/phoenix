/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.hive.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.RecordUpdater;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.util.Progressable;
import org.apache.phoenix.hive.util.PhoenixStorageHandlerUtil;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Custom OutputFormat to feed into Hive. Describes the output-specification for a Map-Reduce job.
 */
public class PhoenixOutputFormat<T extends DBWritable> implements OutputFormat<NullWritable, T>,
        AcidOutputFormat<NullWritable, T> {

    private static final Log LOG = LogFactory.getLog(PhoenixOutputFormat.class);

    public PhoenixOutputFormat() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("PhoenixOutputFormat created");
        }
    }

    @Override
    public RecordWriter<NullWritable, T> getRecordWriter(FileSystem ignored, JobConf jobConf,
                                                         String name, Progressable progress)
            throws IOException {
        return createRecordWriter(jobConf, new Properties());
    }

    @Override
    public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {

    }

    @Override
    public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter
            (JobConf jobConf, Path finalOutPath, Class<? extends Writable> valueClass, boolean
                    isCompressed, Properties tableProperties, Progressable progress) throws
            IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get RecordWriter for finalOutPath : " + finalOutPath + ", valueClass" +
                    " : " +
                    valueClass
                            .getName() + ", isCompressed : " + isCompressed + ", tableProperties " +
                    ": " + tableProperties + ", progress : " + progress);
        }

        return createRecordWriter(jobConf, new Properties());
    }

    @Override
    public RecordUpdater getRecordUpdater(Path path, org.apache.hadoop.hive.ql.io
            .AcidOutputFormat.Options options) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get RecordWriter for  path : " + path + ", options : " +
                    PhoenixStorageHandlerUtil
                            .getOptionsValue(options));
        }
        return new PhoenixRecordWriter<T>(path, options);
    }

    @Override
    public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getRawRecordWriter(Path path,
            org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options options) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get RawRecordWriter for path : " + path + ", options : " +
                    PhoenixStorageHandlerUtil.getOptionsValue(options));
        }

        return new PhoenixRecordWriter<T>(path, options);
    }

    private PhoenixRecordWriter<T> createRecordWriter(Configuration config, Properties properties) {
        try {
            return new PhoenixRecordWriter<T>(config, properties);
        } catch (SQLException e) {
            LOG.error("Error during PhoenixRecordWriter instantiation :" + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
