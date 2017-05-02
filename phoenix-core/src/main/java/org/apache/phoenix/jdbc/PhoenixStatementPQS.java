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

import com.google.inject.Inject;
import org.apache.phoenix.monitoring.PhoenixQueryServerSink;
import org.apache.phoenix.util.PhoenixRuntime;

import java.sql.SQLException;


/**
 *
 *  JDBC statement but with extra functionality of generating
 *  phoenix metrics after statement is closed.
 *
 *
 * @since 0.1
 */
public class PhoenixStatementPQS extends PhoenixStatement {

    @Inject
    private PhoenixQueryServerSink phoenixQueryServerSink;

    public PhoenixStatementPQS(PhoenixConnection connection) {
        super(connection);
    }

    @Override
    public void close() throws SQLException {
        super.close();
        phoenixQueryServerSink.putMetricsOverall(PhoenixRuntime.getOverAllReadRequestMetrics(this.getResultSet()));
        phoenixQueryServerSink.putMetrics(PhoenixRuntime.getRequestReadMetrics(this.getResultSet()));
        this.getResultSet().unwrap(PhoenixResultSet.class).resetMetrics();

    }

}
