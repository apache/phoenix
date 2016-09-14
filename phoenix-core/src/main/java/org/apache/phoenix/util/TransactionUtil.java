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

import java.sql.SQLException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.tephra.TransactionConflictException;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TxConstants;
import org.apache.tephra.hbase.TransactionAwareHTable;

public class TransactionUtil {
    private TransactionUtil() {
    }
    
    public static boolean isDelete(Cell cell) {
        return (CellUtil.matchingValue(cell, HConstants.EMPTY_BYTE_ARRAY));
    }
    
    public static long convertToNanoseconds(long serverTimeStamp) {
        return serverTimeStamp * TxConstants.MAX_TX_PER_MS;
    }
    
    public static long convertToMilliseconds(long serverTimeStamp) {
        return serverTimeStamp / TxConstants.MAX_TX_PER_MS;
    }
    
    public static SQLException getTransactionFailureException(TransactionFailureException e) {
        if (e instanceof TransactionConflictException) { 
            return new SQLExceptionInfo.Builder(SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION)
                .setMessage(e.getMessage())
                .setRootCause(e)
                .build().buildException();

        }
        return new SQLExceptionInfo.Builder(SQLExceptionCode.TRANSACTION_FAILED)
            .setMessage(e.getMessage())
            .setRootCause(e)
            .build().buildException();
    }
    
    public static TransactionAwareHTable getTransactionAwareHTable(HTableInterface htable, boolean isImmutableRows) {
    	// Conflict detection is not needed for tables with write-once/append-only data
    	return new TransactionAwareHTable(htable, isImmutableRows ? TxConstants.ConflictDetection.NONE : TxConstants.ConflictDetection.ROW);
    }
    
    // we resolve transactional tables at the txn read pointer
	public static long getResolvedTimestamp(PhoenixConnection connection, boolean isTransactional, long defaultResolvedTimestamp) {
		MutationState mutationState = connection.getMutationState();
		Long scn = connection.getSCN();
	    return scn != null ?  scn : (isTransactional && mutationState.isTransactionStarted()) ? convertToMilliseconds(mutationState.getInitialWritePointer()) : defaultResolvedTimestamp;
	}

	public static long getResolvedTime(PhoenixConnection connection, MetaDataMutationResult result) {
		PTable table = result.getTable();
		boolean isTransactional = table!=null && table.isTransactional();
		return getResolvedTimestamp(connection, isTransactional, result.getMutationTime());
	}

	public static long getResolvedTimestamp(PhoenixConnection connection, MetaDataMutationResult result) {
		PTable table = result.getTable();
		MutationState mutationState = connection.getMutationState();
		boolean txInProgress = table != null && table.isTransactional() && mutationState.isTransactionStarted();
		return  txInProgress ? convertToMilliseconds(mutationState.getInitialWritePointer()) : result.getMutationTime();
	}

	public static Long getTableTimestamp(PhoenixConnection connection, boolean transactional) throws SQLException {
		Long timestamp = null;
		if (!transactional) {
			return timestamp;
		}
		MutationState mutationState = connection.getMutationState();
		if (!mutationState.isTransactionStarted()) {
			mutationState.startTransaction();
		}
		timestamp = convertToMilliseconds(mutationState.getInitialWritePointer());
		return timestamp;
	}
}
