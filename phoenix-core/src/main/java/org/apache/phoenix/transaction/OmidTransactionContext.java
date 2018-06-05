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

public class OmidTransactionContext implements PhoenixTransactionContext {

    @Override
    public void begin() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void commit() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void abort() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void checkpoint(boolean hasUncommittedData) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void commitDDLFence(PTable dataTable) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void join(PhoenixTransactionContext ctx) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isTransactionRunning() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void reset() {
        // TODO Auto-generated method stub

    }

    @Override
    public long getTransactionId() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getReadPointer() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getWritePointer() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public PhoenixVisibilityLevel getVisibilityLevel() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setVisibilityLevel(PhoenixVisibilityLevel visibilityLevel) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public byte[] encodeTransaction() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Provider getProvider() {
        return Provider.OMID;
    }

    @Override
    public PhoenixTransactionContext newTransactionContext(PhoenixTransactionContext contex, boolean subTask) {
        return null;
    }

    @Override
    public void markDMLFence(PTable dataTable) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Table getTransactionalTable(Table htable, boolean isImmutable) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Table getTransactionalTableWriter(PhoenixConnection connection, PTable table, Table htable, boolean isIndex) {
        // TODO Auto-generated method stub
        return null;
    }
}
