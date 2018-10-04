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

import java.sql.SQLException;

import org.apache.hadoop.hbase.client.Table;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.transaction.TransactionFactory.Provider;

import com.google.protobuf.InvalidProtocolBufferException;
//import org.apache.omid.tso.TSOMockModule;

public class OmidTransactionContext implements PhoenixTransactionContext {

    public OmidTransactionContext() {
    }

    public OmidTransactionContext(PhoenixConnection connection) throws SQLException {
    }

    public OmidTransactionContext(byte[] txnBytes) throws InvalidProtocolBufferException {
    }

    public OmidTransactionContext(PhoenixTransactionContext ctx, boolean subTask) {
    }

    @Override
    public void begin() throws SQLException {
    }

    @Override
    public void commit() throws SQLException {
    }

    @Override
    public void abort() throws SQLException {
    }

    @Override
    public void checkpoint(boolean hasUncommittedData) throws SQLException {
    }

    @Override
    public void commitDDLFence(PTable dataTable) throws SQLException {
    }

    @Override
    public void join(PhoenixTransactionContext ctx) {
    }

    @Override
    public boolean isTransactionRunning() {
        return false;
    }

    @Override
    public void reset() {
    }

    @Override
    public long getTransactionId() {
        return 0;
    }

    @Override
    public long getReadPointer() {
        return 0;
    }

    @Override
    public long getWritePointer() {
        return 0;
    }

    @Override
    public PhoenixVisibilityLevel getVisibilityLevel() {
        return null;
    }

    @Override
    public void setVisibilityLevel(PhoenixVisibilityLevel visibilityLevel) {
    }

    @Override
    public byte[] encodeTransaction() throws SQLException {
        return null;
    }

    @Override
    public Provider getProvider() {
        return TransactionFactory.Provider.OMID;
    }

    @Override
    public PhoenixTransactionContext newTransactionContext(PhoenixTransactionContext context, boolean subTask) {
        return null;
    }

    @Override
    public void markDMLFence(PTable dataTable) {
    }

    @Override
    public Table getTransactionalTable(Table htable, boolean isConflictFree) throws SQLException {
        return new OmidTransactionTable(this, htable, isConflictFree);
    }

    @Override
    public Table getTransactionalTableWriter(PhoenixConnection connection, PTable table, Table htable, boolean isIndex) throws SQLException {
        return new OmidTransactionTable(this, htable, table.isImmutableRows() || isIndex);
    }
}
