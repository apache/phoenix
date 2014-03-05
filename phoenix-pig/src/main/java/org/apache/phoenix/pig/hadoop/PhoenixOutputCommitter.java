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
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.phoenix.jdbc.PhoenixStatement;

/**
 * 
 * {@link OutputCommitter} implementation for Pig tasks using Phoenix
 * connections to upsert to HBase
 * 
 * 
 *
 */
public class PhoenixOutputCommitter extends OutputCommitter {
	private final Log LOG = LogFactory.getLog(PhoenixOutputCommitter.class);
	
	private final PhoenixOutputFormat outputFormat;
	
	public PhoenixOutputCommitter(PhoenixOutputFormat outputFormat) {
		if(outputFormat == null) {
			throw new IllegalArgumentException("PhoenixOutputFormat must not be null.");
		}
		this.outputFormat = outputFormat;
	}

	/**
	 *  TODO implement rollback functionality. 
	 *  
	 *  {@link PhoenixStatement#execute(String)} is buffered on the client, this makes 
	 *  it difficult to implement rollback as once a commit is issued it's hard to go 
	 *  back all the way to undo. 
	 */
	@Override
	public void abortTask(TaskAttemptContext context) throws IOException {
	}

	@Override
	public void commitTask(TaskAttemptContext context) throws IOException {
		commit(outputFormat.getConnection(context.getConfiguration()));
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

	/**
	 * Commit a transaction on task completion
	 * 
	 * @param connection
	 * @throws IOException
	 */
	private void commit(Connection connection) throws IOException {
		try {
			if (connection == null || connection.isClosed()) {
				throw new IOException("Trying to commit a connection that is null or closed: "+ connection);
			}
		} catch (SQLException e) {
			throw new IOException("Exception calling isClosed on connection", e);
		}

		try {
			LOG.debug("Commit called on task completion");
			connection.commit();
		} catch (SQLException e) {
			throw new IOException("Exception while trying to commit a connection. ", e);
		} finally {
			try {
				LOG.debug("Closing connection to database on task completion");
				connection.close();
			} catch (SQLException e) {
				LOG.warn("Exception while trying to close database connection", e);
			}
		}
	}
}
