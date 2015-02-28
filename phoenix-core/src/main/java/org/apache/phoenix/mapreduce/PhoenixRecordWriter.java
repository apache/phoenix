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
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.mapreduce.util.ConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;

/**
 * Default {@link RecordWriter} implementation from Phoenix
 *
 */
public class PhoenixRecordWriter<T extends DBWritable>  extends RecordWriter<NullWritable, T> {
    
    private static final Log LOG = LogFactory.getLog(PhoenixRecordWriter.class);
    
    private final Connection conn;
    private final PreparedStatement statement;
    private final long batchSize;
    private long numRecords = 0;
    
    public PhoenixRecordWriter(final Configuration configuration) throws SQLException {
        this.conn = ConnectionUtil.getOutputConnection(configuration);
        this.batchSize = PhoenixConfigurationUtil.getBatchSize(configuration);
        final String upsertQuery = PhoenixConfigurationUtil.getUpsertStatement(configuration);
        this.statement = this.conn.prepareStatement(upsertQuery);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            statement.executeBatch();
            conn.commit();
         } catch (SQLException e) {
             LOG.error("SQLException while performing the commit for the task.");
             throw new RuntimeException(e);
          } finally {
            try {
              statement.close();
              conn.close();
            }
            catch (SQLException ex) {
              LOG.error("SQLException while closing the connection for the task.");
              throw new RuntimeException(ex);
            }
          }
    }

    @Override
    public void write(NullWritable n, T record) throws IOException, InterruptedException {      
        try {
            record.write(statement);
            numRecords++;
            statement.addBatch();
            if (numRecords % batchSize == 0) {
                LOG.debug("commit called on a batch of size : " + batchSize);
                statement.executeBatch();
                conn.commit();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Exception while committing to database.", e);
        }
    }

}
