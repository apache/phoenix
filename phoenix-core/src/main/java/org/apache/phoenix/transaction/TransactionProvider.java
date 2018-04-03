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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;

public interface TransactionProvider {
    public PhoenixTransactionContext getTransactionContext();
    public PhoenixTransactionContext getTransactionContext(byte[] txnBytes) throws IOException;
    public PhoenixTransactionContext getTransactionContext(PhoenixConnection connection);
    public PhoenixTransactionContext getTransactionContext(PhoenixTransactionContext contex, PhoenixConnection connection, boolean subTask);

    public PhoenixTransactionalTable getTransactionalTable() throws SQLException;
    public PhoenixTransactionalTable getTransactionalTable(PhoenixTransactionContext ctx, HTableInterface htable) throws SQLException;
    public PhoenixTransactionalTable getTransactionalTable(PhoenixTransactionContext ctx, HTableInterface htable, PTable pTable) throws SQLException;
    
    public Cell newDeleteFamilyMarker(byte[] row, byte[] family, long timestamp);
    public Cell newDeleteColumnMarker(byte[] row, byte[] family, byte[] qualifier, long timestamp);
}
