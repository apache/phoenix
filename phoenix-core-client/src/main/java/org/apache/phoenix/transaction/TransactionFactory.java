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
import org.apache.phoenix.coprocessorclient.MetaDataProtocol;

public class TransactionFactory {

    public enum Provider {
        // The provider formerly known as TEPHRA, deliberately renamed to warn downstreams
        // that Tephra support no longer exists, while preserving the ordinal for backwards
        // compatible use in system schema.
        NOTAVAILABLE((byte)1, NotAvailableTransactionProvider.getInstance()),
        // The OMID provider.
        OMID((byte)2, OmidTransactionProvider.getInstance());

        private final byte code;
        private final PhoenixTransactionProvider provider;

        Provider(byte code, PhoenixTransactionProvider provider) {
            this.code = code;
            this.provider = provider;
        }

        public static Provider[] available() {
            return new Provider[] { OMID };
        }

        public byte getCode() {
            return this.code;
        }

        public static Provider fromCode(int code) {
            if (code < 1 || code > Provider.values().length) {
                throw new IllegalArgumentException("Invalid TransactionFactory.Provider " + code);
            }
            return Provider.values()[code-1];
        }

        public static Provider getDefault() {
            return OMID;
        }

        public PhoenixTransactionProvider getTransactionProvider()  {
            return provider;
        }
    }

    public static PhoenixTransactionProvider getTransactionProvider(Provider provider) {
        return provider.getTransactionProvider();
    }

    public static PhoenixTransactionProvider getTransactionProvider(byte[] txState, int clientVersion) {
        if (txState == null || txState.length == 0) {
            return null;
        }
        Provider provider = (clientVersion < MetaDataProtocol.MIN_SYSTEM_TABLE_TIMESTAMP_4_14_0) 
                ? Provider.NOTAVAILABLE
                : Provider.fromCode(txState[txState.length-1]);
        return provider.getTransactionProvider();
    }

    public static PhoenixTransactionContext getTransactionContext(byte[] txState, int clientVersion) throws IOException {
        PhoenixTransactionProvider provider = getTransactionProvider(txState, clientVersion);
        if (provider == null) {
            return null;
        }
        return provider.getTransactionContext(txState);
    }
}
