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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.execute.MutationState;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.transaction.TransactionFactory;

public class TransactionUtil {
    // All transaction providers must use an empty byte array as the family delete marker
    // (see TxConstants.FAMILY_DELETE_QUALIFIER)
    public static final byte[] FAMILY_DELETE_MARKER = HConstants.EMPTY_BYTE_ARRAY;
    // All transaction providers must multiply timestamps by this constant.
    // (see TxConstants.MAX_TX_PER_MS)
    public static final int MAX_TRANSACTIONS_PER_MILLISECOND = 1000000;
    // Constant used to empirically determine if a timestamp is a transactional or
    // non transactional timestamp (see TxUtils.MAX_NON_TX_TIMESTAMP)
    private static final long MAX_NON_TX_TIMESTAMP =
        (long) (EnvironmentEdgeManager.currentTimeMillis() * 1.1);
    
    private TransactionUtil() {
        
    }
    
    public static boolean isTransactionalTimestamp(long ts) {
        return ts >= MAX_NON_TX_TIMESTAMP;
    }
    
    public static boolean isDelete(Cell cell) {
        return CellUtil.matchingValue(cell, HConstants.EMPTY_BYTE_ARRAY);
    }
    
    public static boolean isDeleteFamily(Cell cell) {
        return CellUtil.matchingQualifier(cell, FAMILY_DELETE_MARKER) && CellUtil.matchingValue(cell, HConstants.EMPTY_BYTE_ARRAY);
    }
    
    private static Cell newDeleteFamilyMarker(byte[] row, byte[] family, long timestamp) {
        return CellUtil.createCell(row, family, FAMILY_DELETE_MARKER, timestamp, KeyValue.Type.Put.getCode(), HConstants.EMPTY_BYTE_ARRAY);
    }
    
    private static Cell newDeleteColumnMarker(byte[] row, byte[] family, byte[] qualifier, long timestamp) {
        return CellUtil.createCell(row, family, qualifier, timestamp, KeyValue.Type.Put.getCode(), HConstants.EMPTY_BYTE_ARRAY);
    }

    public static long convertToNanoseconds(long serverTimeStamp) {
        return serverTimeStamp * MAX_TRANSACTIONS_PER_MILLISECOND;
    }
    
    public static long convertToMilliseconds(long serverTimeStamp) {
        return serverTimeStamp / MAX_TRANSACTIONS_PER_MILLISECOND;
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

	public static Long getTableTimestamp(PhoenixConnection connection, boolean transactional, TransactionFactory.Provider provider) throws SQLException {
		Long timestamp = null;
		if (!transactional) {
			return timestamp;
		}
		MutationState mutationState = connection.getMutationState();
		if (!mutationState.isTransactionStarted()) {
			mutationState.startTransaction(provider);
		}
		timestamp = convertToMilliseconds(mutationState.getInitialWritePointer());
		return timestamp;
	}
	
    // Convert HBase Delete into Put so that it can be undone if transaction is rolled back
	public static Mutation convertIfDelete(Mutation mutation) throws IOException {
        if (mutation instanceof Delete) {
            Put deleteMarker = null;
            for (Map.Entry<byte[],List<Cell>> entry : mutation.getFamilyCellMap().entrySet()) {
                byte[] family = entry.getKey();
                List<Cell> familyCells = entry.getValue();
                if (familyCells.size() == 1) {
                    if (CellUtil.isDeleteFamily(familyCells.get(0))) {
                        if (deleteMarker == null) {
                            deleteMarker = new Put(mutation.getRow());
                        }
                        deleteMarker.add(newDeleteFamilyMarker(
                                deleteMarker.getRow(), 
                                family, 
                                familyCells.get(0).getTimestamp()));
                    }
                } else {
                    for (Cell cell : familyCells) {
                        if (CellUtil.isDeleteColumns(cell)) {
                            if (deleteMarker == null) {
                                deleteMarker = new Put(mutation.getRow());
                            }
                            deleteMarker.add(newDeleteColumnMarker(
                                    deleteMarker.getRow(),
                                    family,
                                    CellUtil.cloneQualifier(cell), 
                                    cell.getTimestamp()));
                        }
                    }
                }
            }
            if (deleteMarker != null) {
                for (Map.Entry<String, byte[]> entry : mutation.getAttributesMap().entrySet()) {
                    deleteMarker.setAttribute(entry.getKey(), entry.getValue());
                }
                mutation = deleteMarker;
            }
        }
        return mutation;
	}
}
