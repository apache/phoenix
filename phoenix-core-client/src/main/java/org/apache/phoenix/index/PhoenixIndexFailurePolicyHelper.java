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
package org.apache.phoenix.index;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.hbase.index.exception.IndexWriteException;
import org.apache.phoenix.hbase.index.exception.MultiIndexWriteFailureException;
import org.apache.phoenix.hbase.index.exception.SingleIndexWriteFailureException;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoenixIndexFailurePolicyHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixIndexFailurePolicyHelper.class);

    public static interface MutateCommand {
        void doMutation() throws IOException;

        List<Mutation> getMutationList();
    }

    /**
     * Retries a mutationBatch where the index write failed.
     * One attempt should have already been made before calling this.
     * Max retries and exponential backoff logic mimics that of HBase's client
     * If max retries are hit, the index is disabled.
     * If the write is successful on a subsequent retry, the index is set back to ACTIVE
     * @param mutateCommand mutation command to execute
     * @param iwe original IndexWriteException
     * @param connection connection to use
     * @param config config used to get retry settings
     * @throws IOException
     */
    public static void doBatchWithRetries(MutateCommand mutateCommand,
                                          IndexWriteException iwe, PhoenixConnection connection, ReadOnlyProps config)
            throws IOException {
        if (!PhoenixIndexMetaData.isIndexRebuild(
                mutateCommand.getMutationList().get(0).getAttributesMap())) {
            incrementPendingDisableCounter(iwe, connection);
        }
        int maxTries = config.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
        long pause = config.getLong(HConstants.HBASE_CLIENT_PAUSE,
                HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
        int numRetry = 1; // already tried once
        // calculate max time to retry for
        int timeout = 0;
        for (int i = 0; i < maxTries; ++i) {
            timeout = (int) (timeout + ConnectionUtils.getPauseTime(pause, i));
        }
        long canRetryUntil = EnvironmentEdgeManager.currentTime() + timeout;
        while (canRetryMore(numRetry++, maxTries, canRetryUntil)) {
            try {
                Thread.sleep(ConnectionUtils.getPauseTime(pause, numRetry)); // HBase's exponential backoff
                mutateCommand.doMutation();
                // success - change the index state from PENDING_DISABLE back to ACTIVE
                // If it's not Index Rebuild
                if (!PhoenixIndexMetaData.isIndexRebuild(
                        mutateCommand.getMutationList().get(0).getAttributesMap())){
                    handleIndexWriteSuccessFromClient(iwe, connection);
                }
                return;
            } catch (IOException e) {
                SQLException inferredE = ClientUtil.parseLocalOrRemoteServerException(e);
                if (inferredE != null && inferredE.getErrorCode() != SQLExceptionCode.INDEX_WRITE_FAILURE.getErrorCode()) {
                    // If this call is from phoenix client, we also need to check if SQLException
                    // error is INDEX_METADATA_NOT_FOUND or not
                    // if it's not an INDEX_METADATA_NOT_FOUND, throw exception,
                    // to be handled normally in caller's try-catch
                    if (inferredE.getErrorCode() != SQLExceptionCode.INDEX_METADATA_NOT_FOUND
                            .getErrorCode()) {
                        throw e;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }
        if (!PhoenixIndexMetaData.isIndexRebuild(
                mutateCommand.getMutationList().get(0).getAttributesMap())) {
            // max retries hit - disable the index
            handleIndexWriteFailureFromClient(iwe, connection);
        }
        throw new DoNotRetryIOException(iwe); // send failure back to client
    }

    /**
     * If we're leaving the index active after index write failures on the server side, then we get
     * the exception on the client side here after hitting the max # of hbase client retries. We
     * disable the index as it may now be inconsistent. The indexDisableTimestamp was already set
     * on the server side, so the rebuilder will be run.
     */
    private static void handleIndexWriteFailureFromClient(IndexWriteException indexWriteException,
                                                          PhoenixConnection conn) {
        handleExceptionFromClient(indexWriteException, conn, PIndexState.DISABLE);
    }

    private static void handleIndexWriteSuccessFromClient(IndexWriteException indexWriteException,
                                                          PhoenixConnection conn) {
        handleExceptionFromClient(indexWriteException, conn, PIndexState.ACTIVE);
    }

    private static void handleExceptionFromClient(IndexWriteException indexWriteException,
                                                  PhoenixConnection conn, PIndexState indexState) {
        try {
            Set<String> indexesToUpdate = new HashSet<>();
            if (indexWriteException instanceof MultiIndexWriteFailureException) {
                MultiIndexWriteFailureException indexException =
                        (MultiIndexWriteFailureException) indexWriteException;
                List<HTableInterfaceReference> failedIndexes = indexException.getFailedTables();
                if (indexException.isDisableIndexOnFailure() && failedIndexes != null) {
                    for (HTableInterfaceReference failedIndex : failedIndexes) {
                        String failedIndexTable = failedIndex.getTableName();
                        if (!indexesToUpdate.contains(failedIndexTable)) {
                            updateIndex(failedIndexTable, conn, indexState);
                            indexesToUpdate.add(failedIndexTable);
                        }
                    }
                }
            } else if (indexWriteException instanceof SingleIndexWriteFailureException) {
                SingleIndexWriteFailureException indexException =
                        (SingleIndexWriteFailureException) indexWriteException;
                String failedIndex = indexException.getTableName();
                if (indexException.isDisableIndexOnFailure() && failedIndex != null) {
                    updateIndex(failedIndex, conn, indexState);
                }
            }
        } catch (Exception handleE) {
            LOGGER.warn("Error while trying to handle index write exception", indexWriteException);
        }
    }

    private static void incrementPendingDisableCounter(IndexWriteException indexWriteException,PhoenixConnection conn) {
        try {
            Set<String> indexesToUpdate = new HashSet<>();
            if (indexWriteException instanceof MultiIndexWriteFailureException) {
                MultiIndexWriteFailureException indexException =
                        (MultiIndexWriteFailureException) indexWriteException;
                List<HTableInterfaceReference> failedIndexes = indexException.getFailedTables();
                if (indexException.isDisableIndexOnFailure() && failedIndexes != null) {
                    for (HTableInterfaceReference failedIndex : failedIndexes) {
                        String failedIndexTable = failedIndex.getTableName();
                        if (!indexesToUpdate.contains(failedIndexTable)) {
                            incrementCounterForIndex(conn,failedIndexTable);
                            indexesToUpdate.add(failedIndexTable);
                        }
                    }
                }
            } else if (indexWriteException instanceof SingleIndexWriteFailureException) {
                SingleIndexWriteFailureException indexException =
                        (SingleIndexWriteFailureException) indexWriteException;
                String failedIndex = indexException.getTableName();
                if (indexException.isDisableIndexOnFailure() && failedIndex != null) {
                    incrementCounterForIndex(conn,failedIndex);
                }
            }
        } catch (Exception handleE) {
            LOGGER.warn("Error while trying to handle index write exception", indexWriteException);
        }
    }

    private static void incrementCounterForIndex(PhoenixConnection conn, String failedIndexTable) throws IOException {
        IndexUtil.incrementCounterForIndex(conn, failedIndexTable, 1);
    }

    private static void decrementCounterForIndex(PhoenixConnection conn, String failedIndexTable) throws IOException {
        IndexUtil.incrementCounterForIndex(conn, failedIndexTable, -1);
    }

    private static boolean canRetryMore(int numRetry, int maxRetries, long canRetryUntil) {
        // If there is a single try we must not take into account the time.
        return numRetry < maxRetries
                || (maxRetries > 1 && EnvironmentEdgeManager.currentTime() < canRetryUntil);
    }

    /**
     * Converts from SQLException to IndexWriteException
     * @param sqlE the SQLException
     * @return the IndexWriteException
     */
    public static IndexWriteException getIndexWriteException(SQLException sqlE) {
        String sqlMsg = sqlE.getMessage();
        if (sqlMsg.contains(MultiIndexWriteFailureException.FAILURE_MSG)) {
            return new MultiIndexWriteFailureException(sqlMsg);
        } else if (sqlMsg.contains(SingleIndexWriteFailureException.FAILED_MSG)) {
            return new SingleIndexWriteFailureException(sqlMsg);
        }
        return null;
    }

    private static void updateIndex(String indexFullName, PhoenixConnection conn,
                                    PIndexState indexState) throws SQLException, IOException {
        //Decrement the counter because we will be here when client give retry after getting failed or succeed
        decrementCounterForIndex(conn,indexFullName);
        Long indexDisableTimestamp = null;
        if (PIndexState.DISABLE.equals(indexState)) {
            LOGGER.info("Disabling index after hitting max number of index write retries: "
                    + indexFullName);
            IndexUtil.updateIndexState(conn, indexFullName, indexState, indexDisableTimestamp);
        } else if (PIndexState.ACTIVE.equals(indexState)) {
            LOGGER.debug("Resetting index to active after subsequent success " + indexFullName);
            //At server disabled timestamp will be reset only if there is no other client is in PENDING_DISABLE state
            indexDisableTimestamp = 0L;
            try {
                IndexUtil.updateIndexState(conn, indexFullName, indexState, indexDisableTimestamp);
            } catch (SQLException e) {
                // It's possible that some other client had made the Index DISABLED already , so we can ignore unallowed
                // transition(DISABLED->ACTIVE)
                if (e.getErrorCode() != SQLExceptionCode.INVALID_INDEX_STATE_TRANSITION.getErrorCode()) { throw e; }
            }
        }
    }
}
