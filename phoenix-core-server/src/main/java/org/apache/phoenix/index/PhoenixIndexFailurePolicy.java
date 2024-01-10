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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MetaDataMutationResult;
import org.apache.phoenix.coprocessorclient.MetaDataProtocol.MutationCode;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.hbase.index.exception.MultiIndexWriteFailureException;
import org.apache.phoenix.hbase.index.table.HTableInterfaceReference;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.hbase.index.write.DelegateIndexFailurePolicy;
import org.apache.phoenix.hbase.index.write.KillServerOnFailurePolicy;
import org.apache.phoenix.hbase.index.write.LeaveIndexActiveFailurePolicy;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDatabaseMetaData;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.MetaDataUtil;
import org.apache.phoenix.util.QueryUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Function;
import org.apache.phoenix.thirdparty.com.google.common.collect.Iterables;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.thirdparty.com.google.common.collect.Multimap;

/**
 * 
 * Handler called in the event that index updates cannot be written to their
 * region server. First attempts to disable the index and failing that falls
 * back to the default behavior of killing the region server.
 *
 */
public class PhoenixIndexFailurePolicy extends DelegateIndexFailurePolicy {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixIndexFailurePolicy.class);
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

        boolean killServer = env.getConfiguration().getBoolean(QueryServices.INDEX_FAILURE_KILL_SERVER, true);
        if (!killServer) {
            setDelegate(new LeaveIndexActiveFailurePolicy());
        } // else, default in constructor is KillServerOnFailurePolicy
    }

    /**
     * Check config for whether to disable index on index write failures
     * @param env
     * @return
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
        // we should check if failed list of mutation are part of Index Rebuilder or not.
        // If its part of Index Rebuilder, we throw exception and do retries.
        // If succeeds, we don't update Index State.
        // Once those retries are exhausted, we transition Index to DISABLE
        // It's being handled as part of PhoenixIndexFailurePolicy.doBatchWithRetries
        Mutation checkMutationForRebuilder = attempted.entries().iterator().next().getValue();
        boolean isIndexRebuild =
                PhoenixIndexMetaData.isIndexRebuild(checkMutationForRebuilder.getAttributesMap());
        if (isIndexRebuild) {
            SQLException sqlException =
                    new SQLExceptionInfo.Builder(SQLExceptionCode.INDEX_WRITE_FAILURE)
                            .setRootCause(cause).setMessage(cause.getLocalizedMessage()).build()
                            .buildException();
            IOException ioException = ServerUtil.wrapInDoNotRetryIOException(
                        "Retrying Index rebuild mutation, we will update Index state to DISABLE "
                        + "if all retries are exhusated", sqlException, timestamp);
            throw ioException;
        }
        try {
            timestamp = handleFailureWithExceptions(attempted, cause);
            throwing = false;
        } catch (Throwable t) {
            LOGGER.warn("handleFailure failed", t);
            super.handleFailure(attempted, cause);
            throwing = false;
        } finally {
            if (!throwing) {
                SQLException sqlException =
                        new SQLExceptionInfo.Builder(SQLExceptionCode.INDEX_WRITE_FAILURE)
                                .setRootCause(cause).setMessage(cause.getLocalizedMessage()).build()
                                .buildException();
                IOException ioException = ServerUtil.wrapInDoNotRetryIOException(null, sqlException, timestamp);
                // Here we throw index write failure to client so it can retry index mutation.
                if (throwIndexWriteFailure) {
            		throw ioException;
            	} else {
                    LOGGER.warn("Swallowing index write failure", ioException);
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
                            LOGGER.info("Index " + indexTableName +
                                    " has been dropped. Ignore uncommitted mutations");
                            continue;
                        }
                        if (result.getMutationCode() != MutationCode.TABLE_ALREADY_EXISTS) {
                            if (leaveIndexActive) {
                                LOGGER.warn("Attempt to update INDEX_DISABLE_TIMESTAMP "
                                        + " failed with code = "
                                        + result.getMutationCode());
                                // If we're not disabling the index, then we don't want to throw as throwing
                                // will lead to the RS being shutdown.
                                if (blockDataTableWritesOnFailure) { throw new DoNotRetryIOException(
                                        "Attempt to update INDEX_DISABLE_TIMESTAMP failed."); }
                            } else {
                                LOGGER.warn("Attempt to disable index " + indexTableName +
                                        " failed with code = " + result.getMutationCode() +
                                        ". Will use default failure policy instead.");
                                throw new DoNotRetryIOException("Attempt to disable " + indexTableName + " failed.");
                            }
                        }
                        LOGGER.info("Successfully update INDEX_DISABLE_TIMESTAMP for " +
                                        indexTableName + " due to an exception while" +
                                        " writing updates. indexState=" + newState,
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
            PTable dataTable = conn.getTableNoCache(ref.getTableName());
            List<PTable> indexes = dataTable.getIndexes();
            // local index used to get view id from index mutation row key.
            PTable localIndex = null;
            Map<ImmutableBytesWritable, String> localIndexNames =
                    new HashMap<ImmutableBytesWritable, String>();
            for (PTable index : indexes) {
                if (localIndex == null) localIndex = index;
                localIndexNames.put(new ImmutableBytesWritable(index.getviewIndexIdType().toBytes(
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
                    LOGGER.error("Unable to find local index on " + ref.getTableName() +
                            " with viewID of " + Bytes.toStringBinary(viewId));
                } else {
                    indexTableNames.add(indexTableName);
                }
            }
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
}