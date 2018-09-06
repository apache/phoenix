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


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LoggingPhoenixStatement extends DelegateStatement {

    private PhoenixMetricsLog phoenixMetricsLog;
    private String sql;
    private Connection conn;

    public LoggingPhoenixStatement(Statement stmt, PhoenixMetricsLog phoenixMetricsLog, Connection conn) {
        super(stmt);
        this.phoenixMetricsLog = phoenixMetricsLog;
        this.conn = conn;
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        boolean result;
        this.sql = sql;
        result = super.execute(sql);
        this.loggingAutoCommitHelper();
        return result;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        this.sql = sql;
        ResultSet rs = new LoggingPhoenixResultSet(super.executeQuery(sql), phoenixMetricsLog, this.sql);
        this.loggingAutoCommitHelper();
        return rs;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        int result;
        this.sql = sql;
        result = super.executeUpdate(sql);
        this.loggingAutoCommitHelper();
        return result;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        // Re-use the cached ResultSet value since call to getResultSet() is not idempotent
        ResultSet resultSet = super.getResultSet();
        return (resultSet == null) ? null : new LoggingPhoenixResultSet(resultSet,
                phoenixMetricsLog, sql);
    }
    
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return new LoggingPhoenixResultSet(super.getGeneratedKeys(), phoenixMetricsLog, this.sql);
    }

    private void loggingAutoCommitHelper() throws SQLException {
        if(conn.getAutoCommit() && (conn instanceof LoggingPhoenixConnection)) {
            ((LoggingPhoenixConnection)conn).loggingMetricsHelper();
        }
    }

}
