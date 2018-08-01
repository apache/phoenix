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

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.TABLE_FAMILY_BYTES;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessor.MetaDataProtocol.MutationCode;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.hbase.index.exception.IndexWriteException;
import org.apache.phoenix.hbase.index.exception.MultiIndexWriteFailureException;
import org.apache.phoenix.hbase.index.exception.SingleIndexWriteFailureException;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.write.DelegateIndexFailurePolicy;
import org.apache.phoenix.hbase.index.write.KillServerOnFailurePolicy;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

/**
 * 
 * Handler called in the event that index updates cannot be written to their
 * region server. First attempts to disable the index and failing that falls
 * back to the default behavior of killing the region server.
 *
 */
public class PhoenixIndexFailurePolicy extends DelegateIndexFailurePolicy {
    private static final Log LOG = LogFactory.getLog(PhoenixIndexFailurePolicy.class);
    public static final String THROW_INDEX_WRITE_FAILURE = "THROW_INDEX_WRITE_FAILURE";
    public static final String DISABLE_INDEX_ON_WRITE_FAILURE = "DISABLE_INDEX_ON_WRITE_FAILURE";
    public static final String REBUILD_INDEX_ON_WRITE_FAILURE = "REBUILD_INDEX_ON_WRITE_FAILURE";
    public static final String BLOCK_DATA_TABLE_WRITES_ON_WRITE_FAILURE = "BLOCK_DATA_TABLE_WRITES_ON_WRITE_FAILURE";
    private RegionCoprocessorEnvironment env;
    private boolean blockDataTableWritesOnFailure;
    private boolean disableIndexOnFailure;
    private boolean rebuildIndexOnFailure;
    private boolean throwIndexWriteFailure;

    public PhoenixIndexFailurePolicy() {
        super(new KillServerOnFailurePolicy());
    }

    @Override
    public void setup(Stoppable parent, RegionCoprocessorEnvironment env) {
        super.setup(parent, env);
        this.env = env;
        rebuildIndexOnFailure = env.getConfiguration().getBoolean(QueryServices.INDEX_FAILURE_HANDLING_REBUILD_ATTRIB,
                QueryServicesOptions.DEFAULT_INDEX_FAILURE_HANDLING_REBUILD);
        TableDescriptor htd = env.getRegion().getTableDescriptor();
        // If rebuild index is turned off globally, no need to check the table because the background thread
        // won't be running in this case
        if (rebuildIndexOnFailure) {
            String value = htd.getValue(REBUILD_INDEX_ON_WRITE_FAILURE);
            if (value != null) {
                rebuildIndexOnFailure = Boolean.parseBoolean(value);
            }
        }
        disableIndexOnFailure = getDisableIndexOnFailure(env);
        String value = htd.getValue(BLOCK_DATA_TABLE_WRITES_ON_WRITE_FAILURE);
        if (value == null) {
            blockDataTableWritesOnFailure = env.getConfiguration().getBoolean(QueryServices.INDEX_FAILURE_BLOCK_WRITE, 
                QueryServicesOptions.DEFAULT_INDEX_FAILURE_BLOCK_WRITE);
        } else {
            blockDataTableWritesOnFailure = Boolean.parseBoolean(value);
        }
        
        value = htd.getValue(THROW_INDEX_WRITE_FAILURE);
        if (value == null) {
	        throwIndexWriteFailure = env.getConfiguration().getBoolean(QueryServices.INDEX_FAILURE_THROW_EXCEPTION_ATTRIB,
	                QueryServicesOptions.DEFAULT_INDEX_FAILURE_THROW_EXCEPTION);
        } else {
        	throwIndexWriteFailure = Boolean.parseBoolean(value);
        }
    }

    /**
     * Attempt to disable the index table when we can't write to it, preventing future updates until the index is
     * brought up to date, but allowing historical reads to continue until then.
     * <p>
     * In the case that we cannot reach the metadata information, we will fall back to the default policy and kill
     * this server, so we can attempt to replay the edits on restart.
     * </p>
     * @param attempted the mutations that were attempted to be written and the tables to which they were written
     * @param cause root cause of the failure
     */
    @Override
    public void handleFailure(Multimap<HTableInterfaceReference, Mutation> attempted, Exception cause) throws IOException {
        boolean throwing = true;
        long timestamp = HConstants.LATEST_TIMESTAMP;
        try {
            timestamp = handleFailureWithExceptions(attempted, cause);
            throwing = false;
        } catch (Throwable t) {
            LOG.warn("handleFailure failed", t);
            super.handleFailure(attempted, cause);
            throwing = false;
        } finally {
            if (!throwing) {
                SQLException sqlException =
                        new SQLExceptionInfo.Builder(SQLExceptionCode.INDEX_WRITE_FAILURE)
                                .setRootCause(cause).setMessage(cause.getLocalizedMessage()).build()
                                .buildException();
                IOException ioException = ServerUtil.wrapInDoNotRetryIOException(null, sqlException, timestamp);
            	Mutation m = attempted.entries().iterator().next().getValue();
            	boolean isIndexRebuild = PhoenixIndexMetaData.isIndexRebuild(m.getAttributesMap());
            	// Always throw if rebuilding index since the rebuilder needs to know if it was successful
            	if (throwIndexWriteFailure || isIndexRebuild) {
            		throw ioException;
            	} else {
                    LOG.warn("Swallowing index write failure", ioException);
            	}
            }
        }
    }

    private long handleFailureWithExceptions(Multimap<HTableInterfaceReference, Mutation> attempted,
            final Exception cause) throws Throwable {
        Set<HTableInterfaceReference> refs = attempted.asMap().keySet();
        final Map<String, Long> indexTableNames = new HashMap<String, Long>(refs.size());
        // start by looking at all the tables to which we attempted to write
        long timestamp = 0;
        final boolean leaveIndexActive = blockDataTableWritesOnFailure || !disableIndexOnFailure;
        // if using TrackingParallelWriter, we know which indexes failed and only disable those
        Set<HTableInterfaceReference> failedTables = cause instanceof MultiIndexWriteFailureException 
                ? new HashSet<HTableInterfaceReference>(((MultiIndexWriteFailureException)cause).getFailedTables())
                : Collections.<HTableInterfaceReference>emptySet();
        
        for (HTableInterfaceReference ref : refs) {
            if (failedTables.size() > 0 && !failedTables.contains(ref)) {
                continue; // leave index active if its writes succeeded
            }
            long minTimeStamp = 0;

            // get the minimum timestamp across all the mutations we attempted on that table
            // FIXME: all cell timestamps should be the same
            Collection<Mutation> mutations = attempted.get(ref);
            if (mutations != null) {
                for (Mutation m : mutations) {
                    for (List<Cell> kvs : m.getFamilyCellMap().values()) {
                        for (Cell kv : kvs) {
                            if (minTimeStamp == 0 || (kv.getTimestamp() >= 0 && minTimeStamp > kv.getTimestamp())) {
                                minTimeStamp = kv.getTimestamp();
                            }
                        }
                    }
                }
            }
            timestamp = minTimeStamp;

            // If the data table has local index column families then get local indexes to disable.
            if (ref.getTableName().equals(env.getRegion().getTableDescriptor().getTableName().getNameAsString())
                    && MetaDataUtil.hasLocalIndexColumnFamily(env.getRegion().getTableDescriptor())) {
                for (String tableName : getLocalIndexNames(ref, mutations)) {
                    indexTableNames.put(tableName, minTimeStamp);
                }
                // client disables the index, so we pass the index names in the thrown exception
                if (cause instanceof MultiIndexWriteFailureException) {
                    List<HTableInterfaceReference> failedLocalIndexes =
                            Lists.newArrayList(Iterables.transform(indexTableNames.entrySet(),
                                new Function<Map.Entry<String, Long>, HTableInterfaceReference>() {
                                    @Override
                                    public HTableInterfaceReference apply(Entry<String, Long> input) {
                                        return new HTableInterfaceReference(new ImmutableBytesPtr(
                                                Bytes.toBytes(input.getKey())));
                                    }
                                }));
                    ((MultiIndexWriteFailureException) cause).setFailedTables(failedLocalIndexes);
                }
            } else {
                indexTableNames.put(ref.getTableName(), minTimeStamp);
            }
        }

        // Nothing to do if we're not disabling the index and not rebuilding on failure
        if (!disableIndexOnFailure && !rebuildIndexOnFailure) {
            return timestamp;
        }

        final PIndexState newState = disableIndexOnFailure ? PIndexState.PENDING_DISABLE : PIndexState.PENDING_ACTIVE;
        final long fTimestamp = timestamp;
        // for all the index tables that we've found, try to disable them and if that fails, try to
        return User.runAsLoginUser(new PrivilegedExceptionAction<Long>() {
            @Override
            public Long run() throws Exception {
                for (Map.Entry<String, Long> tableTimeElement : indexTableNames.entrySet()) {
                    String indexTableName = tableTimeElement.getKey();
                    long minTimeStamp = tableTimeElement.getValue();
                    // We need a way of differentiating the block writes to data table case from
                    // the leave index active case. In either case, we need to know the time stamp
                    // at which writes started failing so we can rebuild from that point. If we
                    // keep the index active *and* have a positive INDEX_DISABLE_TIMESTAMP_BYTES,
                    // then writes to the data table will be blocked (this is client side logic
                    // and we can't change this in a minor release). So we use the sign of the
                    // time stamp to differentiate.
                    if (!disableIndexOnFailure && !blockDataTableWritesOnFailure) {
                        minTimeStamp *= -1;
                    }
                    // Disable the index by using the updateIndexState method of MetaDataProtocol end point coprocessor.
                    try (Table systemTable = env.getConnection().getTable(SchemaUtil.getPhysicalTableName(
                            PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME_BYTES, env.getConfiguration()))) {
                        MetaDataMutationResult result = IndexUtil.updateIndexState(indexTableName, minTimeStamp,
                                systemTable, newState);
                        if (result.getMutationCode() == MutationCode.TABLE_NOT_FOUND) {
                            LOG.info("Index " + indexTableName + " has been dropped. Ignore uncommitted mutations");
                            continue;
                        }
                        if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                            if (leaveIndexActive) {
                                LOG.warn("Attempt to update INDEX_DISABLE_TIMESTAMP " + " failed with code = "
                                        + result.getMutationCode());
                                // If we're not disabling the index, then we don't want to throw as throwing
                                // will lead to the RS being shutdown.
                                if (blockDataTableWritesOnFailure) { throw new DoNotRetryIOException(
                                        "Attempt to update INDEX_DISABLE_TIMESTAMP failed."); }
                            } else {
                                LOG.warn("Attempt to disable index " + indexTableName + " failed with code = "
                                        + result.getMutationCode() + ". Will use default failure policy instead.");
                                throw new DoNotRetryIOException("Attempt to disable " + indexTableName + " failed.");
                            }
                        }
                        LOG.info("Successfully update INDEX_DISABLE_TIMESTAMP for " + indexTableName
                            + " due to an exception while writing updates. indexState=" + newState,
                        cause);
                    } catch (Throwable t) {
                        if (t instanceof Exception) {
                            throw (Exception)t;
                        } else {
                            throw new Exception(t);
                        }
                    }
                }
                // Return the cell time stamp (note they should all be the same)
                return fTimestamp;
            }
        });
    }

    private Collection<? extends String> getLocalIndexNames(HTableInterfaceReference ref,
            Collection<Mutation> mutations) throws IOException {
        Set<String> indexTableNames = new HashSet<String>(1);
        PhoenixConnection conn = null;
        try {
            conn = QueryUtil.getConnectionOnServer(this.env.getConfiguration()).unwrap(
                    PhoenixConnection.class);
            PTable dataTable = PhoenixRuntime.getTableNoCache(conn, ref.getTableName());
            List<PTable> indexes = dataTable.getIndexes();
            // local index used to get view id from index mutation row key.
            PTable localIndex = null;
            Map<ImmutableBytesWritable, String> localIndexNames =
                    new HashMap<ImmutableBytesWritable, String>();
            for (PTable index : indexes) {
                if (localIndex == null) localIndex = index;
                localIndexNames.put(new ImmutableBytesWritable(MetaDataUtil.getViewIndexIdDataType().toBytes(
                        index.getViewIndexId())), index.getName().getString());
            }
            if (localIndex == null) {
                return Collections.emptySet();
            }

            IndexMaintainer indexMaintainer = localIndex.getIndexMaintainer(dataTable, conn);
            RegionInfo regionInfo = this.env.getRegion().getRegionInfo();
            int offset =
                    regionInfo.getStartKey().length == 0 ? regionInfo.getEndKey().length
                            : regionInfo.getStartKey().length;
            byte[] viewId = null;
            for (Mutation mutation : mutations) {
                viewId =
                        indexMaintainer.getViewIndexIdFromIndexRowKey(
                                new ImmutableBytesWritable(mutation.getRow(), offset,
                                        mutation.getRow().length - offset));
                String indexTableName = localIndexNames.get(new ImmutableBytesWritable(viewId));
                if (indexTableName == null) {
                    LOG.error("Unable to find local index on " + ref.getTableName() + " with viewID of " + Bytes.toStringBinary(viewId));
                } else {
                    indexTableNames.add(indexTableName);
                }
            }
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        } catch (SQLException e) {
            throw new IOException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
        }
        return indexTableNames;
    }

    /**
     * Check config for whether to disable index on index write failures
     * @param htd
     * @param config
     * @param connection
     * @return The table config for {@link PhoenixIndexFailurePolicy.DISABLE_INDEX_ON_WRITE_FAILURE}
     * @throws SQLException
     */
    public static boolean getDisableIndexOnFailure(RegionCoprocessorEnvironment env) {
        TableDescriptor htd = env.getRegion().getTableDescriptor();
        Configuration config = env.getConfiguration();
        String value = htd.getValue(PhoenixIndexFailurePolicy.DISABLE_INDEX_ON_WRITE_FAILURE);
        boolean disableIndexOnFailure;
        if (value == null) {
            disableIndexOnFailure =
                    config.getBoolean(QueryServices.INDEX_FAILURE_DISABLE_INDEX,
                        QueryServicesOptions.DEFAULT_INDEX_FAILURE_DISABLE_INDEX);
        } else {
            disableIndexOnFailure = Boolean.parseBoolean(value);
        }
        return disableIndexOnFailure;
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
            LOG.warn("Error while trying to handle index write exception", indexWriteException);
        }
    }

    public static interface MutateCommand {
        void doMutation() throws IOException;
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
     * @throws Exception
     */
    public static void doBatchWithRetries(MutateCommand mutateCommand,
            IndexWriteException iwe, PhoenixConnection connection, ReadOnlyProps config)
            throws IOException {
        incrementPendingDisableCounter(iwe, connection);
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
                handleIndexWriteSuccessFromClient(iwe, connection);
                return;
            } catch (IOException e) {
                SQLException inferredE = ServerUtil.parseLocalOrRemoteServerException(e);
                if (inferredE == null || inferredE.getErrorCode() != SQLExceptionCode.INDEX_WRITE_FAILURE.getErrorCode()) {
                    // if it's not an index write exception, throw exception, to be handled normally in caller's try-catch
                    throw e;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }
        // max retries hit - disable the index
        handleIndexWriteFailureFromClient(iwe, connection);
        throw new DoNotRetryIOException(iwe); // send failure back to client
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
            LOG.warn("Error while trying to handle index write exception", indexWriteException);
        }
    }

    private static void incrementCounterForIndex(PhoenixConnection conn, String failedIndexTable) throws IOException {
        incrementCounterForIndex(conn, failedIndexTable, 1);
    }

    private static void decrementCounterForIndex(PhoenixConnection conn, String failedIndexTable) throws IOException {
        incrementCounterForIndex(conn, failedIndexTable, -1);
    }
    
    private static void incrementCounterForIndex(PhoenixConnection conn, String failedIndexTable,long amount) throws IOException {
        byte[] indexTableKey = SchemaUtil.getTableKeyFromFullName(failedIndexTable);
        Increment incr = new Increment(indexTableKey);
        incr.addColumn(TABLE_FAMILY_BYTES, PhoenixDatabaseMetaData.PENDING_DISABLE_COUNT_BYTES, amount);
        try {
            conn.getQueryServices()
                    .getTable(SchemaUtil.getPhysicalTableName(PhoenixDatabaseMetaData.SYSTEM_CATALOG_NAME,
                            conn.getQueryServices().getProps()).getName())
                    .increment(incr);
        } catch (SQLException e) {
            throw new IOException(e);
        }
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
            LOG.info("Disabling index after hitting max number of index write retries: "
                    + indexFullName);
            IndexUtil.updateIndexState(conn, indexFullName, indexState, indexDisableTimestamp);
        } else if (PIndexState.ACTIVE.equals(indexState)) {
            LOG.debug("Resetting index to active after subsequent success " + indexFullName);
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