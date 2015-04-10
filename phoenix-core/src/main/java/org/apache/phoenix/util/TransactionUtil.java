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
package org.apache.phoenix.util;

import java.io.IOException;
import java.sql.SQLException;

import co.cask.tephra.Transaction;
import co.cask.tephra.TransactionCodec;
import co.cask.tephra.TransactionConflictException;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TxConstants;
import co.cask.tephra.hbase98.TransactionAwareHTable;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;

public class TransactionUtil {
    private TransactionUtil() {
    }
    
    private static final TransactionCodec codec = new TransactionCodec();
    
    public static long translateMillis(long serverTimeStamp) {
        return serverTimeStamp * 1000000;
    }
    
    public static byte[] encodeTxnState(Transaction txn) throws SQLException {
        try {
            return codec.encode(txn);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }
    
    public static Transaction decodeTxnState(byte[] txnBytes) throws IOException {
        return txnBytes == null ? null : codec.decode(txnBytes);
    }

    public static SQLException getSQLException(TransactionFailureException e) {
        if (e instanceof TransactionConflictException) { 
            return new SQLExceptionInfo.Builder(SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION)
                .setMessage(e.getMessage())
                .setRootCause(e)
                .build().buildException();

        }
        return new SQLExceptionInfo.Builder(SQLExceptionCode.TRANSACTION_EXCEPTION)
            .setMessage(e.getMessage())
            .setRootCause(e)
            .build().buildException();
    }
    
    public static TransactionAwareHTable getTransactionAwareHTable(HTableInterface htable) {
    	return new TransactionAwareHTable(htable, TxConstants.ConflictDetection.ROW);
    }
}
