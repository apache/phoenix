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
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.transaction.TransactionFactory.Provider;

public interface PhoenixTransactionContext {
    public static PhoenixTransactionContext NULL_CONTEXT = new PhoenixTransactionContext() {

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
        public void setVisibilityLevel(PhoenixVisibilityLevel visibilityLevel) {
        }

        @Override
        public PhoenixVisibilityLevel getVisibilityLevel() {
            return null;
        }

        @Override
        public byte[] encodeTransaction() throws SQLException {
            return null;
        }

        @Override
        public Provider getProvider() {
            return null;
        }

        @Override
        public PhoenixTransactionContext newTransactionContext(PhoenixTransactionContext contex, boolean subTask) {
            return NULL_CONTEXT;
        }

        @Override
        public void markDMLFence(PTable dataTable) {
            
        }

        @Override
        public Table getTransactionalTable(Table htable, boolean isConflictFree) {
            return null;
        }

        @Override
        public Table getTransactionalTableWriter(PhoenixConnection connection, PTable table, Table htable, boolean isIndex) {
            return null;
        }
    };
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
    public static final byte[] PROPERTY_TTL_BYTES = Bytes.toBytes(PROPERTY_TTL);

    public static final String READ_NON_TX_DATA = "data.tx.read.pre.existing";

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
     */
    public void commitDDLFence(PTable dataTable)
            throws SQLException;


    /**
     * Mark the start of DML go ensure that updates to indexed rows are not
     * missed.
     * @param dataTable the table on which DML command is working
     */
    public void markDMLFence(PTable dataTable);

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
     * Returns transaction unique identifier which is also
     * assumed to be the earliest write pointer.
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

    public Provider getProvider();
    public PhoenixTransactionContext newTransactionContext(PhoenixTransactionContext contex, boolean subTask);

    public Table getTransactionalTable(Table htable, boolean isConflictFree) throws SQLException;
    public Table getTransactionalTableWriter(PhoenixConnection connection, PTable table, Table htable, boolean isIndex) throws SQLException;
}
