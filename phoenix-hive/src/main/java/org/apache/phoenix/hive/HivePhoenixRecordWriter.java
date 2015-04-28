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
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.phoenix.hive.util.HiveConnectionUtil;
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil;

public class HivePhoenixRecordWriter<T extends DBWritable> implements RecordWriter<NullWritable, T> {
    private static final Log LOG = LogFactory.getLog(HivePhoenixRecordWriter.class);

    private long numRecords = 0L;
    private  Connection conn;
    private final PreparedStatement statement;
    private Configuration config;
    private final long batchSize;

    public HivePhoenixRecordWriter(Configuration config) throws SQLException, IOException {
    	this.conn = this.getConnection(config);
        this.batchSize = PhoenixConfigurationUtil.getBatchSize(config);
        String upsertQuery = PhoenixConfigurationUtil.getUpsertStatement(config);
        this.statement = this.conn.prepareStatement(upsertQuery);
    }

    public void write(NullWritable n, T record) throws IOException {
        try {
            record.write(this.statement);
            this.numRecords += 1L;
            this.statement.addBatch();

            if (this.numRecords % this.batchSize == 0L) {
                LOG.info("log commit called on a batch of size : " + this.batchSize);
                this.statement.executeBatch();
                this.conn.commit();
            }
        } catch (SQLException e) {
            throw new IOException("Exception while committing to database.", e);
        }
    }

    public void close(Reporter arg0) throws IOException {
        try {
            this.statement.executeBatch();
            this.conn.commit();
        } catch (SQLException e) {
            try {
                this.conn.rollback();
            } catch (SQLException ex) {
                throw new IOException("Exception while closing the connection", e);
            }
            throw new IOException(e.getMessage());
        } finally {
            try {
                this.statement.close();
                this.conn.close();
            } catch (SQLException ex) {
                throw new IOException(ex.getMessage());
            }
        }
    }
    
   private Connection getConnection(Configuration configuration) throws IOException {
        if (this.conn != null) {
            return this.conn;
        }

        this.config = configuration;
        try {
            LOG.info("Initializing new Phoenix connection...");
            this.conn = HiveConnectionUtil.getConnection(configuration);
            LOG.info("Initialized Phoenix connection, autoCommit="
                    + this.conn.getAutoCommit());
            return this.conn;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }
}