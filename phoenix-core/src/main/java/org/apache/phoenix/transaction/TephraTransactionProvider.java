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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.tephra.TxConstants;

public class TephraTransactionProvider implements TransactionProvider {
    private static final TephraTransactionProvider INSTANCE = new TephraTransactionProvider();
    
    public static final TephraTransactionProvider getInstance() {
        return INSTANCE;
    }
    
    private TephraTransactionProvider() {
    }
    
    
    @Override
    public PhoenixTransactionContext getTransactionContext()  {
        return new TephraTransactionContext();
    }

    @Override
    public PhoenixTransactionContext getTransactionContext(byte[] txnBytes) throws IOException {
       return new TephraTransactionContext(txnBytes);
    }
    
    @Override
    public PhoenixTransactionContext getTransactionContext(PhoenixConnection connection) {
        return new TephraTransactionContext(connection);
    }

    @Override
    public PhoenixTransactionContext getTransactionContext(PhoenixTransactionContext contex, PhoenixConnection connection, boolean subTask) {
        return new TephraTransactionContext(contex, connection, subTask);
    }

    @Override
    public PhoenixTransactionalTable getTransactionalTable(PhoenixTransactionContext ctx, Table htable) {
        return new TephraTransactionTable(ctx, htable);
    }
    
    @Override
    public Cell newDeleteFamilyMarker(byte[] row, byte[] family, long timestamp) {
        return CellUtil.createCell(row, family, TxConstants.FAMILY_DELETE_QUALIFIER, timestamp, KeyValue.Type.Put.getCode(), HConstants.EMPTY_BYTE_ARRAY);
    }
    
    @Override
    public Cell newDeleteColumnMarker(byte[] row, byte[] family, byte[] qualifier, long timestamp) {
        return CellUtil.createCell(row, family, qualifier, timestamp, KeyValue.Type.Put.getCode(), HConstants.EMPTY_BYTE_ARRAY);
    }

}
