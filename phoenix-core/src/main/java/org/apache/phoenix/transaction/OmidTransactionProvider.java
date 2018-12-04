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
package org.apache.phoenix.transaction;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.phoenix.coprocessor.OmidGCProcessor;
import org.apache.phoenix.coprocessor.OmidTransactionalProcessor;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.transaction.TransactionFactory.Provider;

public class OmidTransactionProvider implements PhoenixTransactionProvider {
    private static final OmidTransactionProvider INSTANCE = new OmidTransactionProvider();
    public static final String OMID_TSO_PORT = "phoenix.omid.tso.port";
    public static final String OMID_TSO_CONFLICT_MAP_SIZE = "phoenix.omid.tso.conflict.map.size";
    public static final String OMID_TSO_TIMESTAMP_TYPE = "phoenix.omid.tso.timestamp.type";
    public static final int DEFAULT_OMID_TSO_CONFLICT_MAP_SIZE = 1000;
    public static final String DEFAULT_OMID_TSO_TIMESTAMP_TYPE = "WORLD_TIME";

    public static final OmidTransactionProvider getInstance() {
        return INSTANCE;
    }

    private OmidTransactionProvider() {
    }

    @Override
    public String toString() {
        return getProvider().toString();
    }
    
    @Override
    public PhoenixTransactionContext getTransactionContext(byte[] txnBytes) throws IOException {
        // Remove last byte (which is used to identify transaction provider)
        return new OmidTransactionContext(Arrays.copyOf(txnBytes,txnBytes.length-1));
    }

    @Override
    public PhoenixTransactionContext getTransactionContext(PhoenixConnection connection) throws SQLException {
        return new OmidTransactionContext(connection);
    }

    @Override
    public PhoenixTransactionClient getTransactionClient(Configuration config, ConnectionInfo connectionInfo) throws SQLException{
        return new OmidTransactionClient();
    }

    static class OmidTransactionClient implements PhoenixTransactionClient {
        @Override
        public void close() throws IOException {}
    }

    @Override
    public PhoenixTransactionService getTransactionService(Configuration config, ConnectionInfo connectionInfo, int port) throws  SQLException{
        return new OmidTransactionService();
    }

    static class OmidTransactionService implements PhoenixTransactionService {

        public void start() {
        }

        @Override
        public void close() throws IOException {
        }
    }

    @Override
    public Class<? extends RegionObserver> getCoprocessor() {
        return OmidTransactionalProcessor.class;
    }

    @Override
    public Class<? extends RegionObserver> getGCCoprocessor() {
        return OmidGCProcessor.class;
    }

    @Override
    public Provider getProvider() {
        return TransactionFactory.Provider.OMID;
    }

    @Override
    public boolean isUnsupported(Feature feature) {
        return true;
    }

    @Override
    public Put markPutAsCommitted(Put put, long timestamp, long commitTimestamp) {
        return put;
    }
}
