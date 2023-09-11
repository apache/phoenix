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
import org.apache.omid.committable.CommitTable;
import org.apache.omid.transaction.HBaseOmidClientConfiguration;
import org.apache.omid.transaction.HBaseTransactionManager;
import org.apache.omid.transaction.TTable;
import org.apache.omid.tso.client.OmidClientConfiguration;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.ConnectionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.transaction.TransactionFactory.Provider;

public class OmidTransactionProvider implements PhoenixTransactionProvider {
    private static final OmidTransactionProvider INSTANCE = new OmidTransactionProvider();

    private HBaseTransactionManager transactionManager = null;
    private volatile CommitTable.Client commitTableClient = null;

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
        if (transactionManager == null) {
            try {
                HBaseOmidClientConfiguration clientConf = new HBaseOmidClientConfiguration();
                clientConf.setConflictAnalysisLevel(OmidClientConfiguration.ConflictDetectionLevel.ROW);
                transactionManager = (HBaseTransactionManager) HBaseTransactionManager.newInstance(clientConf);
            } catch (IOException | InterruptedException e) {
                throw new SQLExceptionInfo.Builder(
                        SQLExceptionCode.TRANSACTION_FAILED)
                        .setMessage(e.getMessage()).setRootCause(e).build()
                        .buildException();
            }
        }

        return new OmidTransactionClient(transactionManager);
    }

    static class OmidTransactionClient implements PhoenixTransactionClient {
        private final HBaseTransactionManager transactionManager;

        public OmidTransactionClient(HBaseTransactionManager transactionManager) {
            this.transactionManager = transactionManager;
        }

        public HBaseTransactionManager getTransactionClient() {
            return transactionManager;
        }

        @Override
        public void close() throws IOException {}
    }

    // For testing only
    public CommitTable.Client getCommitTableClient() {
        return commitTableClient;
    }

    // For testing only
    public void injectTestService(HBaseTransactionManager transactionManager, CommitTable.Client commitTableClient) {
        this.transactionManager = transactionManager;
        this.commitTableClient = commitTableClient;
    }

    @Override
    public String getCoprocessorClassName() {
        return "org.apache.phoenix.coprocessor.OmidTransactionalProcessor";
    }

    @Override
    public String getGCCoprocessorClassName() {
        return "org.apache.phoenix.coprocessor.OmidGCProcessor";
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
        return TTable.markPutAsCommitted(put, timestamp, timestamp);
    }
}
