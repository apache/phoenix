package org.apache.phoenix.transaction;

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
import org.apache.phoenix.schema.PTable;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionConflictException;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionFailureException;
import org.apache.tephra.TransactionSystemClient;
import org.apache.tephra.Transaction.VisibilityLevel;
import org.apache.tephra.visibility.FenceWait;
import org.apache.tephra.visibility.VisibilityFence;

import com.google.common.collect.Lists;

public class TephraTransactionContext implements PhoenixTransactionContext {

    private final List<TransactionAware> txAwares;
    private final TransactionContext txContext;
    private Transaction tx;
    private TransactionSystemClient txServiceClient;
    private TransactionFailureException e;

    public TephraTransactionContext(PhoenixTransactionContext ctx, PhoenixConnection connection, boolean threadSafe) {

        this.txServiceClient = connection.getQueryServices().getTransactionSystemClient();

        assert(ctx instanceof TephraTransactionContext);
        TephraTransactionContext tephraTransactionContext = (TephraTransactionContext) ctx;

        if (threadSafe) {
            this.tx = tephraTransactionContext.getTransaction();
            this.txAwares = Lists.newArrayList();
            this.txContext = null;
        } else {
            this.txAwares = Collections.emptyList();
            if (ctx == null) {
                this.txContext = new TransactionContext(txServiceClient);
            } else {
                this.txContext = tephraTransactionContext.getContext();
            }
        }

        this.e = null;
    }

    @Override
    public void begin() throws SQLException {
        if (txContext == null) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.NULL_TRANSACTION_CONTEXT).build().buildException();
        }

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
        try {
            assert(txContext != null);
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
  
        if (txContext == null) {
            tx.setVisibility(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);
        }
        else {
            assert(txContext != null);
            txContext.getCurrentTransaction().setVisibility(VisibilityLevel.SNAPSHOT_EXCLUDE_CURRENT);
        }
    }

    @Override
    public void commitDDLFence(PTable dataTable) throws SQLException,
            InterruptedException, TimeoutException {
        byte[] key = dataTable.getName().getBytes();
        try {
            FenceWait fenceWait = VisibilityFence.prepareWait(key, txServiceClient);
            fenceWait.await(10000, TimeUnit.MILLISECONDS);
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

        tephraContext.getAwares();

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
        }
    }

    // For testing
    public long getWritePointer() {
        if (this.txContext != null) {
            return txContext.getCurrentTransaction().getWritePointer();
        } 

        if (tx != null) {
            return tx.getWritePointer();
        }

        return HConstants.LATEST_TIMESTAMP;
    }

    // For testing
    public VisibilityLevel getVisibilityLevel() {
        if (this.txContext != null) {
            return txContext.getCurrentTransaction().getVisibilityLevel();
        } 

        if (tx != null) {
            return tx.getVisibilityLevel();
        }

        return null;
    }
}
