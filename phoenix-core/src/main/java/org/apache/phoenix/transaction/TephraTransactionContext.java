package org.apache.phoenix.transaction;

import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import org.apache.phoenix.schema.PTable;

public class TephraTransactionContext implements PhoenixTransactionContext {

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
    public void abort(SQLException e) throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void checkpoint() throws SQLException {
        // TODO Auto-generated method stub

    }

    @Override
    public void commitDDL(PTable dataTable) throws SQLException,
            InterruptedException, TimeoutException {
        // TODO Auto-generated method stub

    }

    @Override
    public void markDML(PTable table) {
        // TODO Auto-generated method stub

    }

    @Override
    public void join(PhoenixTransactionContext ctx) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addTransactionTable(PhoenixTransactionalTable table) {
        // TODO Auto-generated method stub

    }

    @Override
    public void addTransactionToTable(PhoenixTransactionalTable table) {
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

}
