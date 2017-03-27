package org.apache.phoenix.transaction;

import org.apache.phoenix.schema.PTable;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

public interface PhoenixTransactionContext {

    /**
     * 
     * Visibility levels needed for checkpointing and  
     *
     */
    public enum PhoenixVisibilityLevel {
        SNAPSHOT,
        SNAPSHOT_EXCLUDE_CURRENT,
        SNAPSHOT_ALL
      }

    public static final String TX_ROLLBACK_ATTRIBUTE_KEY = "phoenix.tx.rollback"; 

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
     * Create a checkpoint in a transaction as defined in [TEPHRA-96]
     * @throws SQLException
     */
    public void checkpoint(boolean hasUncommittedData) throws SQLException;

    /**
     * Commit DDL to guarantee that no transaction started before create index
     * and committed afterwards, as explained in [PHOENIX-2478], [TEPHRA-157] and [OMID-56].
     *
     * @param dataTable  the table that the DDL command works on
     * @throws SQLException
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public void commitDDLFence(PTable dataTable, Logger logger)
            throws SQLException;

    /**
     * mark DML with table information for conflict detection of concurrent
     * DDL operation, as explained in [PHOENIX-2478], [TEPHRA-157] and [OMID-56].
     *
     * @param table  the table that the DML command works on
     */
    public void markDMLFence(PTable table);

    /**
     * Augment the current context with ctx modified keys
     *
     * @param ctx
     */
    public void join(PhoenixTransactionContext ctx);

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
    public long getTransactionId();

    /**
     * Returns transaction snapshot id
     */
    public long getReadPointer();

    /**
     * Returns transaction write pointer. After checkpoint the write pointer is different than the initial one  
     */
    public long getWritePointer();

    /**
     * Set visibility level
     */
    public void setVisibilityLevel(PhoenixVisibilityLevel visibilityLevel);

    /**
     * Returns visibility level
     */
    public PhoenixVisibilityLevel getVisibilityLevel();

    /**
     * Encode transaction
     */
    public byte[] encodeTransaction() throws SQLException;

    /**
     * 
     * @return max transactions per second
     */
    public long getMaxTransactionsPerSecond();
}
