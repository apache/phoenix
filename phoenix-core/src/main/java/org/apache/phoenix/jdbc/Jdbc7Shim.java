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

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.Executor;
import java.util.logging.Logger;

/**
 * Interfaces to be implemented by classes that need to be "JDK7" compliant,
 * but also run in JDK6
 */
public final class Jdbc7Shim {

    public interface Statement {  // Note: do not extend "regular" statement or else eclipse 3.7 complains
        void closeOnCompletion() throws SQLException;
        boolean isCloseOnCompletion() throws SQLException;
    }

    public interface CallableStatement extends Statement {
        public <T> T getObject(int columnIndex, Class<T> type) throws SQLException;
        public <T> T getObject(String columnLabel, Class<T> type) throws SQLException;
    }

    public interface Connection {
         void setSchema(String schema) throws SQLException;
         String getSchema() throws SQLException;
         void abort(Executor executor) throws SQLException;
         void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException;
         int getNetworkTimeout() throws SQLException;
    }

    public interface ResultSet {
         public <T> T getObject(int columnIndex, Class<T> type) throws SQLException;
         public <T> T getObject(String columnLabel, Class<T> type) throws SQLException;
    }

    public interface DatabaseMetaData {
        java.sql.ResultSet getPseudoColumns(String catalog, String schemaPattern,
                             String tableNamePattern, String columnNamePattern)
            throws SQLException;
        boolean  generatedKeyAlwaysReturned() throws SQLException;
    }

    public interface Driver {
        public Logger getParentLogger() throws SQLFeatureNotSupportedException;
    }
}