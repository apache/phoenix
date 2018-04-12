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
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;

public interface PhoenixTransactionProvider {
    public enum Feature {
        ALTER_NONTX_TO_TX(SQLExceptionCode.CANNOT_ALTER_TABLE_FROM_NON_TXN_TO_TXNL);
        
        private final SQLExceptionCode code;
        
        Feature(SQLExceptionCode code) {
            this.code = code;
        }
        
        public SQLExceptionCode getCode() {
            return code;
        }
    }
    public PhoenixTransactionContext getTransactionContext(byte[] txnBytes) throws IOException;
    public PhoenixTransactionContext getTransactionContext(PhoenixConnection connection);
    
    public PhoenixTransactionClient getTransactionClient(Configuration config, ConnectionInfo connectionInfo);
    public PhoenixTransactionService getTransactionService(Configuration config, ConnectionInfo connectionInfo);
    public Class<? extends RegionObserver> getCoprocessor();
    
    public TransactionFactory.Provider getProvider();
    public boolean isUnsupported(Feature feature);
}
