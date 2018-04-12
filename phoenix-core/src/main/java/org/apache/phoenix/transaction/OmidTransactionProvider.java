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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.transaction.TransactionFactory.Provider;

public class OmidTransactionProvider implements PhoenixTransactionProvider {
    private static final OmidTransactionProvider INSTANCE = new OmidTransactionProvider();

    public static final OmidTransactionProvider getInstance() {
        return INSTANCE;
    }

    private OmidTransactionProvider() {
    }

    @Override
    public PhoenixTransactionContext getTransactionContext(byte[] txnBytes) throws IOException {
        //return new OmidTransactionContext(txnBytes);
        return null;
    }

    @Override
    public PhoenixTransactionContext getTransactionContext(PhoenixConnection connection) {
        //return new OmidTransactionContext(connection);
        return null;
    }

    @Override
    public PhoenixTransactionClient getTransactionClient(Configuration config, ConnectionInfo connectionInfo) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PhoenixTransactionService getTransactionService(Configuration config, ConnectionInfo connectionInfo) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Class<? extends RegionObserver> getCoprocessor() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Provider getProvider() {
        return TransactionFactory.Provider.OMID;
    }

    @Override
    public boolean isUnsupported(Feature feature) {
        // FIXME: if we initialize a Set with the unsupported features
        // and check for containment, we run into a test failure
        // in SetPropertyOnEncodedTableIT.testSpecifyingColumnFamilyForTTLFails()
        // due to TableProperty.colFamSpecifiedException being null
        // (though it's set in the constructor). I suspect some
        // mysterious class loader issue. The below works fine
        // as a workaround.
        return (feature == Feature.ALTER_NONTX_TO_TX);
    }
}
