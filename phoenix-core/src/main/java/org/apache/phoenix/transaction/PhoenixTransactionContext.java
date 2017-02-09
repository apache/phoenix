package org.apache.phoenix.transaction;

import org.apache.phoenix.schema.PTable;

import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

public interface PhoenixTransactionContext {

    /**
     * Starts a transaction
     * 
     * @throws SQLException
     */
    public void begin() throws SQLException;
    
    /**
     * Commits a transaction
     * 
     * @throws SQLException
     */
    public void commit() throws SQLException;
    
    /**
     * Rollback a transaction
     * 
     * @throws SQLException
     */
    public void abort() throws SQLException;

    /**
     * Rollback a transaction
     * 
     * @param e  
     * @throws SQLException
     */
    public void abort(SQLException e) throws SQLException;
    
    /**
     * Create a checkpoint in a transaction as defined in [TEPHRA-96]
     * @throws SQLException
     */
    public void checkpoint() throws SQLException;
    
    /**
     * Commit DDL to guarantee that no transaction started before create index 
     * and committed afterwards, as explained in [PHOENIX-2478], [TEPHRA-157] and [OMID-56].
     * 
     * @param dataTable  the table that the DDL command works on
     * @throws SQLException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public void commitDDL(PTable dataTable)
            throws SQLException, InterruptedException, TimeoutException;
    
    /**
     * mark DML with table information for conflict detection of concurrent 
     * DDL operation, as explained in [PHOENIX-2478], [TEPHRA-157] and [OMID-56].
     * 
     * @param table  the table that the DML command works on
     */
    public void markDML(PTable table);
    
    /**
     * Augment the current context with ctx modified keys
     * 
     * @param ctx
     */
    public void join(PhoenixTransactionContext ctx);
    
    /** 
     * Add transaction table to the context.
     * Will be mostly used by Tephra, since Omid keeps the changeset inside the transaction while 
     * Tephra keeps it distributed at the different awares.
     * 
     * @param table
     */
    public void addTransactionTable(PhoenixTransactionalTable table);
    
    /**
     * Add transaction to the table. 
     * @param table
     */
    public void addTransactionToTable(PhoenixTransactionalTable table);
    
    /**
     * Is there a transaction in flight?
     */
    public boolean isTransactionRunning();

    /**
     * Reset transaction state
     */
    public void reset();
    
    /** 
     * Returns transaction unique identifier
     */
    long getTransactionId();
    
    /**
     * Returns transaction snapshot id
     */
    long getReadPointer();
}
