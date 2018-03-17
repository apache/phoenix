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

public class OmidTransactionProvider implements TransactionProvider {
    private static final OmidTransactionProvider INSTANCE = new OmidTransactionProvider();
    
    public static final OmidTransactionProvider getInstance() {
        return INSTANCE;
    }
    
    private OmidTransactionProvider() {
    }
    
    @Override
    public PhoenixTransactionContext getTransactionContext()  {
        return new OmidTransactionContext();
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
    public PhoenixTransactionContext getTransactionContext(PhoenixTransactionContext contex, PhoenixConnection connection, boolean subTask) {
        //return new OmidTransactionContext(contex, connection, subTask);
        return null;
    }

    @Override
    public PhoenixTransactionalTable getTransactionalTable(PhoenixTransactionContext ctx, Table htable) {
        //return new OmidTransactionTable(ctx, htable);
        return null;
    }
    
    @Override
    public Cell newDeleteFamilyMarker(byte[] row, byte[] family, long timestamp) {
        return CellUtil.createCell(row, family, HConstants.EMPTY_BYTE_ARRAY, timestamp, KeyValue.Type.Put.getCode(), HConstants.EMPTY_BYTE_ARRAY);
    }
    
    @Override
    public Cell newDeleteColumnMarker(byte[] row, byte[] family, byte[] qualifier, long timestamp) {
        return CellUtil.createCell(row, family, qualifier, timestamp, KeyValue.Type.Put.getCode(), HConstants.EMPTY_BYTE_ARRAY);
    }

}
