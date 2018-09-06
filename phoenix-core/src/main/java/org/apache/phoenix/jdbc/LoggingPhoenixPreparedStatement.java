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
package org.apache.phoenix.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Connection;


public class LoggingPhoenixPreparedStatement extends DelegatePreparedStatement {
    
    private PhoenixMetricsLog phoenixMetricsLog;
    private String sql;
    private Connection conn;
    
    public LoggingPhoenixPreparedStatement(PreparedStatement stmt,
                                           PhoenixMetricsLog phoenixMetricsLog, String sql, Connection conn) {
        super(stmt);
        this.phoenixMetricsLog = phoenixMetricsLog;
        this.sql = sql;
        this.conn = conn;
    }
    
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        ResultSet rs = new LoggingPhoenixResultSet(super.executeQuery(), phoenixMetricsLog, sql);
        this.loggingAutoCommitHelper();
        return rs;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        // Re-use the cached ResultSet value since call to getResultSet() is not idempotent
        ResultSet resultSet = super.getResultSet();
        return (resultSet == null) ? null : new LoggingPhoenixResultSet(resultSet,
                phoenixMetricsLog, sql);
    }

    @Override
    public int executeUpdate() throws SQLException {
        int res = super.executeUpdate();
        this.loggingAutoCommitHelper();
        return res;
    }
    
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return new LoggingPhoenixResultSet(super.getGeneratedKeys(), phoenixMetricsLog, sql);
    }

    private void loggingAutoCommitHelper() throws SQLException {
        if(conn.getAutoCommit() && (conn instanceof LoggingPhoenixConnection)) {
            ((LoggingPhoenixConnection)conn).loggingMetricsHelper();
        }
    }
}
