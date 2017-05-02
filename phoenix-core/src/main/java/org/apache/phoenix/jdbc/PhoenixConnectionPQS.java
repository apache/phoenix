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
import com.google.inject.servlet.RequestScoped;
import org.apache.commons.codec.language.bm.Rule;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.monitoring.PhoenixQueryServerSink;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PMetaData;
import org.apache.phoenix.util.PhoenixRuntime;


import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 *
 * Implementation of commit and close methods for PQS.
 * Currently the following are supported:
 *
 *
 * @since
 */
public class PhoenixConnectionPQS extends PhoenixConnection {

    @Inject
    private PhoenixQueryServerSink phoenixQueryServerSink;

    public PhoenixConnectionPQS(PhoenixConnection connection, boolean isDescRowKeyOrderUpgrade, boolean isRunningUpgrade) throws SQLException {
        super(connection, isDescRowKeyOrderUpgrade, isRunningUpgrade);
    }

    public PhoenixConnectionPQS(PhoenixConnection connection) throws SQLException {
        super(connection);
    }

    public PhoenixConnectionPQS(PhoenixConnection connection, MutationState mutationState) throws SQLException {
        super(connection, mutationState);
    }

    public PhoenixConnectionPQS(PhoenixConnection connection, long scn) throws SQLException {
        super(connection, scn);
    }

    public PhoenixConnectionPQS(ConnectionQueryServices services, PhoenixConnection connection, long scn) throws SQLException {
        super(services, connection, scn);
    }

    public PhoenixConnectionPQS(ConnectionQueryServices services, String url, Properties info, PMetaData metaData) throws SQLException {
        super(services, url, info, metaData);
    }

    public PhoenixConnectionPQS(PhoenixConnection connection, ConnectionQueryServices services, Properties info) throws SQLException {
        super(connection, services, info);
    }

    public PhoenixConnectionPQS(ConnectionQueryServices services, String url, Properties info, PMetaData metaData, MutationState mutationState, boolean isDescVarLengthRowKeyUpgrade, boolean isRunningUpgrade) throws SQLException {
        super(services, url, info, metaData, mutationState, isDescVarLengthRowKeyUpgrade, isRunningUpgrade);
    }

    @Override
    public void commit() throws SQLException {
        super.commit();
        phoenixQueryServerSink.putMetrics(PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(this));
        phoenixQueryServerSink.putMetrics(PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(this));
        PhoenixRuntime.resetMetrics(this);
    }


    @Override
    public Statement createStatement() throws SQLException {
        Statement statement=super.createStatement(new PhoenixStatementFactory() {
            @Override
            public PhoenixStatement newStatement(PhoenixConnection connection) {
                return new PhoenixStatementPQS(connection);
            }
        });
        return statement;
    }
}
