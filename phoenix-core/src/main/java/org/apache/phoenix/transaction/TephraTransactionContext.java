package org.apache.phoenix.transaction;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.transaction.PhoenixTransactionContext.PhoenixVisibilityLevel;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionCodec;
import org.apache.tephra.TransactionConflictException;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.Transaction.VisibilityLevel;
import org.apache.tephra.TxConstants;
import org.apache.tephra.inmemory.InMemoryTxSystemClient;
import org.apache.tephra.visibility.FenceWait;
import org.apache.tephra.visibility.VisibilityFence;

import com.google.common.collect.Lists;

import org.slf4j.Logger;

public class TephraTransactionContext implements PhoenixTransactionContext {

    private static final TransactionCodec CODEC = new TransactionCodec();

    private final List<TransactionAware> txAwares;
    private final TransactionContext txContext;
    private Transaction tx;
    private TransactionSystemClient txServiceClient;
    private TransactionFailureException e;

    public TephraTransactionContext() {
        this.txServiceClient = null;
        this.txAwares = Lists.newArrayList();
        this.txContext = null;
    }

    public TephraTransactionContext(byte[] txnBytes) throws IOException {
        this();
        this.tx = (txnBytes != null && txnBytes.length > 0) ? CODEC.decode(txnBytes) : null;
    }

    public TephraTransactionContext(PhoenixConnection connection) {
        this.txServiceClient = connection.getQueryServices().getTransactionSystemClient();
        this.txAwares = Collections.emptyList();
        this.txContext = new TransactionContext(txServiceClient);
    }

    public TephraTransactionContext(PhoenixTransactionContext ctx, PhoenixConnection connection, boolean subTask) {
        this.txServiceClient = connection.getQueryServices().getTransactionSystemClient();

        assert(ctx instanceof TephraTransactionContext);
        TephraTransactionContext tephraTransactionContext = (TephraTransactionContext) ctx;

        if (subTask) {
            this.tx = tephraTransactionContext.getTransaction();
            this.txAwares = Lists.newArrayList();
            this.txContext = null;
        } else {
            this.txAwares = Collections.emptyList();
            this.txContext = tephraTransactionContext.getContext();
        }

        this.e = null;
    }

    @Override
    public void begin() throws SQLException {
        if (txContext == null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.NULL_TRANSACTION_CONTEXT).build().buildException();
        }

        System.out.println("BEGIN");
        try {
            txContext.start();
        } catch (TransactionFailureException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.TRANSACTION_FAILED)
            .setMessage(e.getMessage())
            .setRootCause(e)
            .build().buildException();
        }
    }

    @Override
    public void commit() throws SQLException {
        
        if (txContext == null || !isTransactionRunning()) {
            return;
        }
        
        try {
            txContext.finish();
        } catch (TransactionFailureException e) {
            this.e = e;
            if (e instanceof TransactionConflictException) {
                throw new SQLExceptionInfo.Builder(SQLExceptionCode.TRANSACTION_CONFLICT_EXCEPTION)
                    .setMessage(e.getMessage())
                    .setRootCause(e)
                    .build().buildException();
            }
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.TRANSACTION_FAILED)
                .setMessage(e.getMessage())
                .setRootCause(e)
                .build().buildException();
        }
    }

    @Override
    public void abort() throws SQLException {
        
        if (txContext == null || !isTransactionRunning()) {
            return;
        }
            
        try {
            if (e != null) {
                txContext.abort(e);
                e = null;
            } else {
                txContext.abort();
            }
        } catch (TransactionFailureException e) {
            this.e = null;
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.TRANSACTION_FAILED)
                .setMessage(e.getMessage())
                .setRootCause(e)
                .build().buildException();
        }
    }

    @Override
    public void checkpoint(boolean hasUncommittedData) throws SQLException {
        if (hasUncommittedData) {
            try {
                if (txContext == null) {
                    tx = txServiceClient.checkpoint(tx);
                }  else {
                    assert(txContext != null);
                    txContext.checkpoint();
                    tx = txContext.getCurrentTransaction();
                }
            } catch (TransactionFailureException e) {
                throw new SQLException(e);
            }
        }

        // Since we're querying our own table while mutating it, we must exclude
        // see our current mutations, otherwise we can get erroneous results (for DELETE)
        // or get into an infinite loop (for UPSERT SELECT).
        if (txContext == null) {
            tx.setVisibility(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);
        }
        else {
            assert(txContext != null);
            txContext.getCurrentTransaction().setVisibility(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);
        }
    }

    private Transaction getCurrentTransaction() {
        if (this.txContext != null) {
            return this.txContext.getCurrentTransaction();
        }

        return this.tx;
    }

    @Override
    public void commitDDLFence(PTable dataTable, Logger logger) throws SQLException {
        byte[] key = dataTable.getName().getBytes();

        try {
            FenceWait fenceWait = VisibilityFence.prepareWait(key, txServiceClient);
            fenceWait.await(10000, TimeUnit.MILLISECONDS);
            
            if (logger.isInfoEnabled()) {
                logger.info("Added write fence at ~" + getCurrentTransaction().getReadPointer());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.INTERRUPTED_EXCEPTION).setRootCause(e).build().buildException();
        } catch (TimeoutException | TransactionFailureException e) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.TX_UNABLE_TO_GET_WRITE_FENCE)
            .setSchemaName(dataTable.getSchemaName().getString())
            .setTableName(dataTable.getTableName().getString())
            .build().buildException();
        }
    }

    @Override
    public void markDMLFence(PTable table) {
        byte[] logicalKey = table.getName().getBytes();
        TransactionAware logicalTxAware = VisibilityFence.create(logicalKey);

        if (this.txContext == null) {
            this.txAwares.add(logicalTxAware);
        } else {
            this.txContext.addTransactionAware(logicalTxAware);
        }

        byte[] physicalKey = table.getPhysicalName().getBytes();
        if (Bytes.compareTo(physicalKey, logicalKey) != 0) {
            TransactionAware physicalTxAware = VisibilityFence.create(physicalKey);
            if (this.txContext == null) {
                this.txAwares.add(physicalTxAware);
            } else {
                this.txContext.addTransactionAware(physicalTxAware);
            }
        }
    }

    @Override
    public void join(PhoenixTransactionContext ctx) {
        assert(ctx instanceof TephraTransactionContext);
        TephraTransactionContext tephraContext = (TephraTransactionContext) ctx;

        if (txContext != null) {
            for (TransactionAware txAware : tephraContext.getAwares()) {
                txContext.addTransactionAware(txAware);
            }
        } else {
            txAwares.addAll(tephraContext.getAwares());
        }
    }

    @Override
    public boolean isTransactionRunning() {
        if (this.txContext != null) {
            return (this.txContext.getCurrentTransaction() != null);
        }

        if (this.tx != null) {
            return true;
        }

        return false;
    }

    @Override
    public void reset() {
        tx = null;
        txAwares.clear();
    }

    @Override
    public long getTransactionId() {
        if (this.txContext != null) {
            return txContext.getCurrentTransaction().getTransactionId();
        }

        if (tx != null) {
            return tx.getTransactionId();
        }

        return HConstants.LATEST_TIMESTAMP;
    }

    @Override
    public long getReadPointer() {
        if (this.txContext != null) {
            return txContext.getCurrentTransaction().getReadPointer();
        }

        if (tx != null) {
            return tx.getReadPointer();
        }

        return (-1);
    }

    // For testing
    @Override
    public long getWritePointer() {
        if (this.txContext != null) {
            return txContext.getCurrentTransaction().getWritePointer();
        }

        if (tx != null) {
            return tx.getWritePointer();
        }

        return HConstants.LATEST_TIMESTAMP;
    }

    @Override
    public void setVisibilityLevel(PhoenixVisibilityLevel visibilityLevel) {
        VisibilityLevel tephraVisibilityLevel = null;

        switch(visibilityLevel) {
        case SNAPSHOT:
            tephraVisibilityLevel = VisibilityLevel.SNAPSHOT;
            break;
        case SNAPSHOT_EXCLUDE_CURRENT:
            tephraVisibilityLevel = VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT;
            break;
        case SNAPSHOT_ALL:
            tephraVisibilityLevel = VisibilityLevel.SNAPSHOT_ALL;
            break;
        default:
            assert(false);               
        }

        if (this.txContext != null) {
            txContext.getCurrentTransaction().setVisibility(tephraVisibilityLevel);
        } else if (tx != null) {
            tx.setVisibility(tephraVisibilityLevel);
        } else {
            assert(false);
        }
    }
    
    // For testing
    @Override
    public PhoenixVisibilityLevel getVisibilityLevel() {
        VisibilityLevel visibilityLevel = null;

        if (this.txContext != null) {
            visibilityLevel = txContext.getCurrentTransaction().getVisibilityLevel();
        } else if (tx != null) {
            visibilityLevel = tx.getVisibilityLevel();
        }

        PhoenixVisibilityLevel phoenixVisibilityLevel;
        switch(visibilityLevel) {
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
    public byte[] encodeTransaction() throws SQLException {

        Transaction transaction = null;

        if (this.txContext != null) {
            transaction = txContext.getCurrentTransaction();
        } else if (tx != null) {
            transaction =  tx;
        }

        assert (transaction != null);

        try {
            return CODEC.encode(transaction);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }
    
    @Override
    public long getMaxTransactionsPerSecond() {
        return TxConstants.MAX_TX_PER_MS;
    }


    /**
    * TephraTransactionContext specific functions
    */

    Transaction getTransaction() {
        return this.tx;
    }

    TransactionContext getContext() {
        return this.txContext;
    }

    List<TransactionAware> getAwares() {
        return txAwares;
    }

    void addTransactionAware(TransactionAware txAware) {
        if (this.txContext != null) {
            txContext.addTransactionAware(txAware);
        } else if (this.tx != null) {
            txAwares.add(txAware);
            assert(tx != null);
            txAware.startTx(tx);
        }
    }
}
