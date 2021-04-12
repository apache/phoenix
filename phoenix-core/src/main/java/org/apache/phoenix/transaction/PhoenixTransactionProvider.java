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
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;

public interface PhoenixTransactionProvider {
    public enum Feature {
        ALTER_NONTX_TO_TX(SQLExceptionCode.CANNOT_ALTER_TABLE_FROM_NON_TXN_TO_TXNL),
        COLUMN_ENCODING(SQLExceptionCode.UNSUPPORTED_COLUMN_ENCODING_FOR_TXN_PROVIDER),
        MAINTAIN_LOCAL_INDEX_ON_SERVER(null),
        SET_TTL(SQLExceptionCode.TTL_UNSUPPORTED_FOR_TXN_TABLE),
        ALLOW_LOCAL_INDEX(SQLExceptionCode.CANNOT_CREATE_LOCAL_INDEX_FOR_TXN_TABLE)
        ;
        
        private final SQLExceptionCode code;
        
        Feature(SQLExceptionCode code) {
            this.code = code;
        }
        
        public SQLExceptionCode getCode() {
            return code;
        }
    }
    public PhoenixTransactionContext getTransactionContext(byte[] txnBytes) throws IOException;
    public PhoenixTransactionContext getTransactionContext(PhoenixConnection connection) throws SQLException;

    public PhoenixTransactionClient getTransactionClient(Configuration config, ConnectionInfo connectionInfo) throws SQLException;
    public String getCoprocessorClassName();
    public String getGCCoprocessorClassName();

    public TransactionFactory.Provider getProvider();
    public boolean isUnsupported(Feature feature);

    /**
     * Converts put operation to autocommit operation
     *  @param  put put operation
     *  @param  timestamp - start timestamp
     *  @param  commitTimestamp - commit timestamp
     * @return put operation with metadata
     */
    public Put markPutAsCommitted(Put put, long timestamp, long commitTimestamp);
}
