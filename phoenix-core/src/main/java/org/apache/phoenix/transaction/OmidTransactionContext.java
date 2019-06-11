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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.Table;
import org.apache.omid.proto.TSOProto;
import org.apache.omid.transaction.AbstractTransaction.VisibilityLevel;
import org.apache.omid.transaction.HBaseCellId;
import org.apache.omid.transaction.HBaseTransaction;
import org.apache.omid.transaction.HBaseTransactionManager;
import org.apache.omid.transaction.RollbackException;
import org.apache.omid.transaction.Transaction;
import org.apache.omid.transaction.Transaction.Status;
import org.apache.omid.transaction.TransactionException;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.transaction.TransactionFactory.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import com.google.protobuf.InvalidProtocolBufferException;

public class OmidTransactionContext implements PhoenixTransactionContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(OmidTransactionContext.class);

    private HBaseTransactionManager tm;
    private HBaseTransaction tx;

    public OmidTransactionContext() {
        this.tx = null;
        this.tm = null;
    }

    public OmidTransactionContext(PhoenixConnection connection) throws SQLException {
        PhoenixTransactionClient client = connection.getQueryServices().initTransactionClient(getProvider());
        assert (client instanceof OmidTransactionProvider.OmidTransactionClient);
        this.tm = ((OmidTransactionProvider.OmidTransactionClient)client).getTransactionClient();
        this.tx = null;
    }

    public OmidTransactionContext(byte[] txnBytes) throws InvalidProtocolBufferException {
        this();
        if (txnBytes != null && txnBytes.length > 0) {
            TSOProto.Transaction transaction = TSOProto.Transaction.parseFrom(txnBytes);
            tx = new HBaseTransaction(transaction.getTimestamp(), transaction.getEpoch(), new HashSet<HBaseCellId>(),
                    new HashSet<HBaseCellId>(), null, tm.isLowLatency());
        } else {
            tx = null;
        }
    }

    public OmidTransactionContext(PhoenixTransactionContext ctx, boolean subTask) {
        assert (ctx instanceof OmidTransactionContext);
        OmidTransactionContext omidTransactionContext = (OmidTransactionContext) ctx;

        this.tm = omidTransactionContext.tm;

        if (subTask) {
            if (omidTransactionContext.isTransactionRunning()) {
                Transaction transaction = omidTransactionContext.getTransaction();
                this.tx = new HBaseTransaction(transaction.getTransactionId(), transaction.getEpoch(),
                        new HashSet<HBaseCellId>(), new HashSet<HBaseCellId>(), this.tm,
                        transaction.getReadTimestamp(), transaction.getWriteTimestamp(), tm.isLowLatency());
            } else {
                this.tx = null;
            }

            this.tm = null;
        } else {
            this.tx = omidTransactionContext.getTransaction();
        }
    }

    @Override
    public void begin() throws SQLException {
        if (tm == null) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.NULL_TRANSACTION_CONTEXT).build()
                    .buildException();
        }


        try {
            tx = (HBaseTransaction) tm.begin();
        } catch (TransactionException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }
    }

    @Override
    public void commit() throws SQLException {
        if (tx == null || tm == null)
            return;

        try {
            tm.commit(tx);
        } catch (TransactionException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        } catch (RollbackException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }
    }

    @Override
    public void abort() throws SQLException {
        if (tx == null || tm == null || tx.getStatus() != Status.RUNNING) {
            return;
        }

        try {
            tm.rollback(tx);
        } catch (TransactionException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TRANSACTION_FAILED)
                    .setMessage(e.getMessage()).setRootCause(e).build()
                    .buildException();
        }
    }

    @Override
    public void checkpoint(boolean hasUncommittedData) throws SQLException {
        try {
            tx.checkpoint();
        } catch (TransactionException e) {
            throw new SQLException(e);
        }
        tx.setVisibilityLevel(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);
    }

    @Override
    public void commitDDLFence(PTable dataTable) throws SQLException {

        try {
            tx = (HBaseTransaction) tm.fence(dataTable.getName().getBytes());
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Added write fence at ~"
                        + tx.getReadTimestamp());
            }
        } catch (TransactionException e) {
            throw new SQLExceptionInfo.Builder(
                    SQLExceptionCode.TX_UNABLE_TO_GET_WRITE_FENCE)
            .setSchemaName(dataTable.getSchemaName().getString())
            .setTableName(dataTable.getTableName().getString()).build()
            .buildException();
        }
    }

    @Override
    public void join(PhoenixTransactionContext ctx) {

        if (ctx == PhoenixTransactionContext.NULL_CONTEXT) {
            return;
        }

        assert (ctx instanceof OmidTransactionContext);
        OmidTransactionContext omidContext = (OmidTransactionContext) ctx;

        HBaseTransaction transaction = omidContext.getTransaction();
        if (transaction == null || tx == null) return;

        Set<HBaseCellId> writeSet = transaction.getWriteSet();

        for (HBaseCellId cell : writeSet) {
            tx.addWriteSetElement(cell);
        }
    }

    @Override
    public boolean isTransactionRunning() {
        return (tx != null);
    }

    @Override
    public void reset() {
        tx = null;
    }

    @Override
    public long getTransactionId() {
        return tx.getTransactionId();
    }

    @Override
    public long getReadPointer() {
        return tx.getReadTimestamp();
    }

    @Override
    public long getWritePointer() {
        return tx.getWriteTimestamp();
    }

    @Override
    public PhoenixVisibilityLevel getVisibilityLevel() {
        VisibilityLevel visibilityLevel = null;

        assert(tx != null);
        visibilityLevel = tx.getVisibilityLevel();

        PhoenixVisibilityLevel phoenixVisibilityLevel;
        switch (visibilityLevel) {
        case SNAPSHOT:
            phoenixVisibilityLevel = PhoenixVisibilityLevel.SNAPSHOT;
            break;
        case SNAPSHOT_EXCLUDE_CURRENT:
            phoenixVisibilityLevel = PhoenixVisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT;
            break;
        case SNAPSHOT_ALL:
            phoenixVisibilityLevel = PhoenixVisibilityLevel.SNAPSHOT_ALL;
        default:
            phoenixVisibilityLevel = null;
        }

        return phoenixVisibilityLevel;
    }

    @Override
    public void setVisibilityLevel(PhoenixVisibilityLevel visibilityLevel) {

        VisibilityLevel omidVisibilityLevel = null;

        switch (visibilityLevel) {
        case SNAPSHOT:
            omidVisibilityLevel = VisibilityLevel.SNAPSHOT;
            break;
        case SNAPSHOT_EXCLUDE_CURRENT:
            omidVisibilityLevel = VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT;
            break;
        case SNAPSHOT_ALL:
            omidVisibilityLevel = VisibilityLevel.SNAPSHOT_ALL;
            break;
        default:
            assert (false);
        }

        assert(tx != null);
        tx.setVisibilityLevel(omidVisibilityLevel);

    }

    @Override
    public byte[] encodeTransaction() throws SQLException {
        assert(tx != null);

        TSOProto.Transaction.Builder transactionBuilder = TSOProto.Transaction.newBuilder();

        transactionBuilder.setTimestamp(tx.getTransactionId());
        transactionBuilder.setEpoch(tx.getEpoch());

        byte[] encodedTxBytes = transactionBuilder.build().toByteArray();
        // Add code of TransactionProvider at end of byte array
        encodedTxBytes = Arrays.copyOf(encodedTxBytes, encodedTxBytes.length + 1);
        encodedTxBytes[encodedTxBytes.length - 1] = getProvider().getCode();
        return encodedTxBytes;
    }

    @Override
    public Provider getProvider() {
        return TransactionFactory.Provider.OMID;
    }

    @Override
    public PhoenixTransactionContext newTransactionContext(PhoenixTransactionContext context, boolean subTask) {
        return new OmidTransactionContext(context, subTask);
    }

    @Override
    public void markDMLFence(PTable dataTable) {
    }

    /**
    *  OmidTransactionContext specific functions
    */

    public HBaseTransaction getTransaction() {
        return tx;
    }


    @Override
    public Table getTransactionalTable(Table htable, boolean isConflictFree) throws SQLException {
        return new OmidTransactionTable(this, htable, isConflictFree);
    }

    @Override
    public Table getTransactionalTableWriter(PhoenixConnection connection, PTable table, Table htable, boolean isIndex) throws SQLException {
        // When we're getting a table for writing, if the table being written to is an index,
        // write the shadow cells immediately since the only time we write to an index is
        // when we initially populate it synchronously.
        return new OmidTransactionTable(this, htable, table.isImmutableRows() || isIndex, isIndex);
    }
}
