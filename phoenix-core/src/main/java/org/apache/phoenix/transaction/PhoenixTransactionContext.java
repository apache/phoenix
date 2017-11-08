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
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.phoenix.jdbc.PhoenixEmbeddedDriver.ConnectionInfo;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;

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

    public static final String TX_ROLLBACK_ATTRIBUTE_KEY = "tephra.tx.rollback"; //"phoenix.tx.rollback"; 

    public static final String PROPERTY_TTL = "dataset.table.ttl";

    public static final String READ_NON_TX_DATA = "data.tx.read.pre.existing";

    /**
     * Set the in memory client connection to the transaction manager (for testing purpose)
     *
     * @param config
     */
    public void setInMemoryTransactionClient(Configuration config);

    /**
     * Set the client connection to the transaction manager
     *
     * @param config
     * @param props
     * @param connectionInfo
     */
    public ZKClientService setTransactionClient(Configuration config, ReadOnlyProps props, ConnectionInfo connectionInfo);

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

    /**
     *
     * @param version
     */
    public boolean isPreExistingVersion(long version);

    /**
     *
     * @return the coprocessor
     */
    public RegionObserver getCoProcessor();

    /**
     * 
     * @return the family delete marker
     */
    public byte[] getFamilyDeleteMarker();

    /**
     * Setup transaction manager's configuration for testing
     */
     public void setTxnConfigs(Configuration config, String tmpFolder, int defaultTxnTimeoutSeconds) throws IOException;

    /**
     * Setup transaction manager for testing
     */
    public void setupTxManager(Configuration config, String url) throws SQLException;

    /**
     * Tear down transaction manager for testing
     */
    public void tearDownTxManager();
}
