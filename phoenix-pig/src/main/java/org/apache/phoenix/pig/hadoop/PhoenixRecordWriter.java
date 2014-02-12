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

package org.apache.phoenix.pig.hadoop;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.phoenix.pig.PhoenixPigConfiguration;

/**
 * 
 * {@link RecordWriter} implementation for Phoenix
 * 
 * 
 *
 */
public class PhoenixRecordWriter extends RecordWriter<NullWritable, PhoenixRecord> {
	
	private static final Log LOG = LogFactory.getLog(PhoenixRecordWriter.class);
	
	private long numRecords = 0;
	
	private final Connection conn;
	private final PreparedStatement statement;
	private final PhoenixPigConfiguration config;
	private final long batchSize;
	
	public PhoenixRecordWriter(Connection conn, PhoenixPigConfiguration config) throws SQLException {
		this.conn = conn;
		this.config = config;
		this.batchSize = config.getBatchSize();
		this.statement = this.conn.prepareStatement(config.getUpsertStatement());
	}


	/**
	 * Committing and closing the connection is handled by {@link PhoenixOutputCommitter}.
	 * 
	 */
	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
	}

	@Override
	public void write(NullWritable n, PhoenixRecord record) throws IOException, InterruptedException {		
		try {
			record.write(statement, config.getColumnMetadataList());
			numRecords++;

			if (numRecords % batchSize == 0) {
				LOG.debug("commit called on a batch of size : " + batchSize);
				conn.commit();
			}
		} catch (SQLException e) {
			throw new IOException("Exception while committing to database.", e);
		}
	}

}
