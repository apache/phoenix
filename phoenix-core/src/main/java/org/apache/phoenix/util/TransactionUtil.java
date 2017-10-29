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
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.transaction.PhoenixTransactionContext;
import org.apache.phoenix.transaction.PhoenixTransactionalTable;
import org.apache.phoenix.transaction.TephraTransactionTable;
import org.apache.phoenix.transaction.TransactionFactory;
import org.apache.tephra.util.TxUtils;

public class TransactionUtil {
    private TransactionUtil() {
    }
    
    public static boolean isTransactionalTimestamp(long ts) {
        return !TxUtils.isPreExistingVersion(ts);
    }
    
    public static boolean isDelete(Cell cell) {
        return (CellUtil.matchingValue(cell, HConstants.EMPTY_BYTE_ARRAY));
    }
    
    public static long convertToNanoseconds(long serverTimeStamp) {
        return serverTimeStamp * TransactionFactory.getTransactionFactory().getTransactionContext().getMaxTransactionsPerSecond();
    }
    
    public static long convertToMilliseconds(long serverTimeStamp) {
        return serverTimeStamp / TransactionFactory.getTransactionFactory().getTransactionContext().getMaxTransactionsPerSecond();
    }
    
    public static PhoenixTransactionalTable getPhoenixTransactionTable(PhoenixTransactionContext phoenixTransactionContext, Table htable, PTable pTable) {
        return new TephraTransactionTable(phoenixTransactionContext, htable, pTable);
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
