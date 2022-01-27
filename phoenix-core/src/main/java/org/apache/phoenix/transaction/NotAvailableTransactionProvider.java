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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.transaction.TransactionFactory.Provider;

public class NotAvailableTransactionProvider implements PhoenixTransactionProvider {
    private static final NotAvailableTransactionProvider INSTANCE = new NotAvailableTransactionProvider();

    private static final String message="This Phoenix has been built without Tephra support";

    public static final NotAvailableTransactionProvider getInstance() {
        return INSTANCE;
    }

    private NotAvailableTransactionProvider() {
    }

    @Override
    public String toString() {
        throw new UnsupportedOperationException(message);
    }

    @Override
    public PhoenixTransactionContext getTransactionContext(byte[] txnBytes) throws IOException {
        throw new UnsupportedOperationException(message);
    }

    @Override
    public PhoenixTransactionContext getTransactionContext(PhoenixConnection connection) throws SQLException {
        throw new UnsupportedOperationException(message);
    }

    @Override
    public PhoenixTransactionClient getTransactionClient(Configuration config, ConnectionInfo connectionInfo) {
        throw new UnsupportedOperationException(message);
    }

    @Override
    public Provider getProvider() {
        return TransactionFactory.Provider.TEPHRA;
    }

    @Override
    public String getCoprocessorClassName() {
        throw new UnsupportedOperationException(message);
    }

    @Override
    public String getGCCoprocessorClassName() {
        throw new UnsupportedOperationException(message);
    }

    @Override
    public boolean isUnsupported(Feature feature) {
        throw new UnsupportedOperationException(message);
    }

    @Override
    public Put markPutAsCommitted(Put put, long timestamp, long commitTimestamp) {
        throw new UnsupportedOperationException(message);
    }
}
