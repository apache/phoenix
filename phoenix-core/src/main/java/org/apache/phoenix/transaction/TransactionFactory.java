package org.apache.phoenix.transaction;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.phoenix.jdbc.PhoenixConnection;

public class TransactionFactory {

    static private TransactionFactory transactionFactory = null;

    private TransactionProcessor tp = TransactionProcessor.Tephra;

    enum TransactionProcessor {
        Tephra,
        Omid
    }

    private TransactionFactory(TransactionProcessor tp) {
        this.tp = tp;
    }

    static public void createTransactionFactory(TransactionProcessor tp) {
        if (transactionFactory == null) {
            transactionFactory = new TransactionFactory(tp);
        }
    }

    static public TransactionFactory getTransactionFactory() {
        if (transactionFactory == null) {
            createTransactionFactory(TransactionProcessor.Tephra);
        }

        return transactionFactory;
    }

    public PhoenixTransactionContext getTransactionContext()  {

        PhoenixTransactionContext ctx = null;

        switch(tp) {
        case Tephra:
            ctx = new TephraTransactionContext();
            break;
        case Omid:
            ctx = new OmidTransactionContext();
            break;
        default:
            ctx = null;
        }
        
        return ctx;
    }

    public PhoenixTransactionContext getTransactionContext(byte[] txnBytes) throws IOException {

        PhoenixTransactionContext ctx = null;

        switch(tp) {
        case Tephra:
            ctx = new TephraTransactionContext(txnBytes);
            break;
        case Omid:
//            ctx = new OmidTransactionContext(txnBytes);
            break;
        default:
            ctx = null;
        }
        
        return ctx;
    }
    
    public PhoenixTransactionContext getTransactionContext(PhoenixConnection connection) {

        PhoenixTransactionContext ctx = null;

        switch(tp) {
        case Tephra:
            ctx = new TephraTransactionContext(connection);
            break;
        case Omid:
//            ctx = new OmidTransactionContext(connection);
            break;
        default:
            ctx = null;
        }
        
        return ctx;
    }

    public PhoenixTransactionContext getTransactionContext(PhoenixTransactionContext contex, PhoenixConnection connection, boolean subTask) {

        PhoenixTransactionContext ctx = null;

        switch(tp) {
        case Tephra:
            ctx = new TephraTransactionContext(contex, connection, subTask);
            break;
        case Omid:
//            ctx = new OmidTransactionContext(contex, connection, subTask);
            break;
        default:
            ctx = null;
        }
        
        return ctx;
    }

    public PhoenixTransactionalTable getTransactionalTable(PhoenixTransactionContext ctx, HTableInterface htable) {

        PhoenixTransactionalTable table = null;

        switch(tp) {
        case Tephra:
            table = new TephraTransactionTable(ctx, htable);
            break;
        case Omid:
//            table = new OmidTransactionContext(contex, connection, subTask);
            break;
        default:
            table = null;
        }
        
        return table;
    }
}
