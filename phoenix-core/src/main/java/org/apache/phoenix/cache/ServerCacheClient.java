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
package org.apache.phoenix.cache;

import static org.apache.phoenix.monitoring.TaskExecutionMetricsHolder.NO_OP_INSTANCE;
import static org.apache.phoenix.util.LogUtil.addCustomAnnotations;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import org.apache.phoenix.coprocessor.generated.ServerCacheFactoryProtos;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.AddServerCacheRequest;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.AddServerCacheResponse;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.RemoveServerCacheRequest;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.RemoveServerCacheResponse;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.ServerCachingService;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.job.JobManager.JobCallable;
import org.apache.phoenix.join.HashCacheFactory;
import org.apache.phoenix.memory.InsufficientMemoryException;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.monitoring.TaskExecutionMetricsHolder;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.ScanUtil;

import com.google.protobuf.ByteString;

/**
 * 
 * Client for sending cache to each region server
 * 
 * 
 * @since 0.1
 */
public class ServerCacheClient {
    public static final int UUID_LENGTH = Bytes.SIZEOF_LONG;
    public static final byte[] KEY_IN_FIRST_REGION = new byte[]{0};
    private static final Log LOG = LogFactory.getLog(ServerCacheClient.class);
    private static final Random RANDOM = new Random();
	public static final String HASH_JOIN_SERVER_CACHE_RESEND_PER_SERVER = "hash.join.server.cache.resend.per.server";
    private final PhoenixConnection connection;
    private final Map<Integer, PTable> cacheUsingTableMap = new ConcurrentHashMap<Integer, PTable>();

    /**
     * Construct client used to create a serialized cached snapshot of a table and send it to each region server
     * for caching during hash join processing.
     * @param connection the client connection
     * 
     * TODO: instead of minMaxKeyRange, have an interface for iterating through ranges as we may be sending to
     * servers when we don't have to if the min is in first region and max is in last region, especially for point queries.
     */
    public ServerCacheClient(PhoenixConnection connection) {
        this.connection = connection;
    }

    public PhoenixConnection getConnection() {
        return connection;
    }
    
    /**
     * Client-side representation of a server cache.  Call {@link #close()} when usage
     * is complete to free cache up on region server
     *
     * 
     * @since 0.1
     */
    public class ServerCache implements SQLCloseable {
        private final int size;
        private final byte[] id;
        private final Map<HRegionLocation, Long> servers;
        private ImmutableBytesWritable cachePtr;
        private MemoryChunk chunk;
        private File outputFile;
        private long maxServerCacheTTL;
        
        
        public ServerCache(byte[] id, Set<HRegionLocation> servers, ImmutableBytesWritable cachePtr,
                ConnectionQueryServices services, boolean storeCacheOnClient) throws IOException {
            maxServerCacheTTL = services.getProps().getInt(
                    QueryServices.MAX_SERVER_CACHE_TIME_TO_LIVE_MS_ATTRIB,
                    QueryServicesOptions.DEFAULT_MAX_SERVER_CACHE_TIME_TO_LIVE_MS);
            this.id = id;
            this.servers = new HashMap();
            long currentTime = System.currentTimeMillis();
            for(HRegionLocation loc : servers) {
                this.servers.put(loc, currentTime);
            }
            this.size =  cachePtr.getLength();
            if (storeCacheOnClient) {
                try {
                    this.chunk = services.getMemoryManager().allocate(cachePtr.getLength());
                    this.cachePtr = cachePtr;
                } catch (InsufficientMemoryException e) {
                    this.outputFile = File.createTempFile("HashJoinCacheSpooler", ".bin", new File(services.getProps()
                            .get(QueryServices.SPOOL_DIRECTORY, QueryServicesOptions.DEFAULT_SPOOL_DIRECTORY)));
                    try (FileOutputStream fio = new FileOutputStream(outputFile)) {
                        fio.write(cachePtr.get(), cachePtr.getOffset(), cachePtr.getLength());
                    }
                }
            }
            
        }

        public ImmutableBytesWritable getCachePtr() throws IOException {
            if(this.outputFile!=null){
                try (FileInputStream fio = new FileInputStream(outputFile)) {
                    byte[] b = new byte[this.size];
                    fio.read(b);
                    cachePtr = new ImmutableBytesWritable(b);
                }
            }
            return cachePtr;
        }

        /**
         * Gets the size in bytes of hash cache
         */
        public int getSize() {
            return size;
        }

        /**
         * Gets the unique identifier for this hash cache
         */
        public byte[] getId() {
            return id;
        }

        public boolean addServer(HRegionLocation loc) {
            if(this.servers.containsKey(loc)) {
                return false;
            } else {
                this.servers.put(loc, System.currentTimeMillis());
                return true;
            }
        }

        public boolean isExpired(HRegionLocation loc) {
            if(this.servers.containsKey(loc)) {
                Long time = this.servers.get(loc);
                if(System.currentTimeMillis() - time > maxServerCacheTTL)
                    return true; // cache was send more than maxTTL ms ago, expecting that it's expired
            } else {
                return false; // should be on server yet.
            }
            return false; // Unknown region location. Need to send the cache.
        }


        
        /**
         * Call to free up cache on region servers when no longer needed
         */
        @Override
        public void close() throws SQLException {
            try{
                removeServerCache(this, servers.keySet());
            }finally{
                cachePtr = null;
                if (chunk != null) {
                    chunk.close();
                }
                if (outputFile != null) {
                    outputFile.delete();
                }
            }
        }
    }

    public ServerCache createServerCache(byte[] cacheId, QueryPlan delegate)
            throws SQLException, IOException {
        PTable cacheUsingTable = delegate.getTableRef().getTable();
        ConnectionQueryServices services = delegate.getContext().getConnection().getQueryServices();
        List<HRegionLocation> locations = services.getAllTableRegions(
                cacheUsingTable.getPhysicalName().getBytes());
        int nRegions = locations.size();
        Set<HRegionLocation> servers = new HashSet<>(nRegions);
        cacheUsingTableMap.put(Bytes.mapKey(cacheId), cacheUsingTable);
        return new ServerCache(cacheId, servers, new ImmutableBytesWritable(
                new byte[]{}), services, false);
    }

    public ServerCache addServerCache(
            ScanRanges keyRanges, final ImmutableBytesWritable cachePtr, final byte[] txState,
            final ServerCacheFactory cacheFactory, final PTable cacheUsingTable)
            throws SQLException {
        return addServerCache(keyRanges, cachePtr, txState, cacheFactory, cacheUsingTable, false);
    }

    public ServerCache addServerCache(
            ScanRanges keyRanges, final ImmutableBytesWritable cachePtr, final byte[] txState,
            final ServerCacheFactory cacheFactory, final PTable cacheUsingTable,
            boolean storeCacheOnClient) throws SQLException {
        final byte[] cacheId = ServerCacheClient.generateId();
        return addServerCache(keyRanges, cacheId, cachePtr, txState, cacheFactory,
                cacheUsingTable, false, storeCacheOnClient);
    }

    public ServerCache addServerCache(
            ScanRanges keyRanges, final byte[] cacheId, final ImmutableBytesWritable cachePtr,
            final byte[] txState, final ServerCacheFactory cacheFactory,
            final PTable cacheUsingTable, final boolean usePersistentCache,
            boolean storeCacheOnClient) throws SQLException {
        ConnectionQueryServices services = connection.getQueryServices();
        List<Closeable> closeables = new ArrayList<Closeable>();
        ServerCache hashCacheSpec = null;
        SQLException firstException = null;
        /**
         * Execute EndPoint in parallel on each server to send compressed hash cache 
         */
        // TODO: generalize and package as a per region server EndPoint caller
        // (ideally this would be functionality provided by the coprocessor framework)
        boolean success = false;
        ExecutorService executor = services.getExecutor();
        List<Future<Boolean>> futures = Collections.emptyList();
        try {
            List<HRegionLocation> locations = services.getAllTableRegions(cacheUsingTable.getPhysicalName().getBytes());
            int nRegions = locations.size();
            // Size these based on worst case
            futures = new ArrayList<Future<Boolean>>(nRegions);
            Set<HRegionLocation> servers = new HashSet<HRegionLocation>(nRegions);
            for (HRegionLocation entry : locations) {
                // Keep track of servers we've sent to and only send once
                byte[] regionStartKey = entry.getRegion().getStartKey();
                byte[] regionEndKey = entry.getRegion().getEndKey();
                if ( ! servers.contains(entry) && 
                        keyRanges.intersectRegion(regionStartKey, regionEndKey,
                                cacheUsingTable.getIndexType() == IndexType.LOCAL)) {
                    // Call RPC once per server
                    servers.add(entry);
                    if (LOG.isDebugEnabled()) {LOG.debug(addCustomAnnotations("Adding cache entry to be sent for " + entry, connection));}
                    final byte[] key = getKeyInRegion(entry.getRegionInfo().getStartKey());
                    final Table htable = services.getTable(cacheUsingTable.getPhysicalName().getBytes());
                    closeables.add(htable);
                    futures.add(executor.submit(new JobCallable<Boolean>() {
                        
                        @Override
                        public Boolean call() throws Exception {
                            return addServerCache(htable, key, cacheUsingTable, cacheId, cachePtr, cacheFactory, txState, usePersistentCache);
                        }

                        /**
                         * Defines the grouping for round robin behavior.  All threads spawned to process
                         * this scan will be grouped together and time sliced with other simultaneously
                         * executing parallel scans.
                         */
                        @Override
                        public Object getJobId() {
                            return ServerCacheClient.this;
                        }
                        
                        @Override
                        public TaskExecutionMetricsHolder getTaskExecutionMetric() {
                            return NO_OP_INSTANCE;
                        }
                    }));
                } else {
                    if (LOG.isDebugEnabled()) {LOG.debug(addCustomAnnotations("NOT adding cache entry to be sent for " + entry + " since one already exists for that entry", connection));}
                }
            }
            
            hashCacheSpec = new ServerCache(cacheId,servers,cachePtr, services, storeCacheOnClient);
            // Execute in parallel
            int timeoutMs = services.getProps().getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, QueryServicesOptions.DEFAULT_THREAD_TIMEOUT_MS);
            for (Future<Boolean> future : futures) {
                future.get(timeoutMs, TimeUnit.MILLISECONDS);
            }

            cacheUsingTableMap.put(Bytes.mapKey(cacheId), cacheUsingTable);
            success = true;
        } catch (SQLException e) {
            firstException = e;
        } catch (Exception e) {
            firstException = new SQLException(e);
        } finally {
            try {
                if (!success) {
                    SQLCloseables.closeAllQuietly(Collections.singletonList(hashCacheSpec));
                    for (Future<Boolean> future : futures) {
                        future.cancel(true);
                    }
                }
            } finally {
                try {
                    Closeables.closeAll(closeables);
                } catch (IOException e) {
                    if (firstException == null) {
                        firstException = new SQLException(e);
                    }
                } finally {
                    if (firstException != null) {
                        throw firstException;
                    }
                }
            }
        }
        if (LOG.isDebugEnabled()) {LOG.debug(addCustomAnnotations("Cache " + cacheId + " successfully added to servers.", connection));}
        return hashCacheSpec;
    }
    
    /**
     * Remove the cached table from all region servers
     * @throws SQLException
     * @throws IllegalStateException if hashed table cannot be removed on any region server on which it was added
     */
    private void removeServerCache(final ServerCache cache, Set<HRegionLocation> remainingOnServers) throws SQLException {
        Table iterateOverTable = null;
        final byte[] cacheId = cache.getId();
        try {
            ConnectionQueryServices services = connection.getQueryServices();
            Throwable lastThrowable = null;
            final PTable cacheUsingTable = cacheUsingTableMap.get(Bytes.mapKey(cacheId));
            byte[] tableName = cacheUsingTable.getPhysicalName().getBytes();
            iterateOverTable = services.getTable(tableName);

            List<HRegionLocation> locations = services.getAllTableRegions(tableName);
            /**
             * Allow for the possibility that the region we based where to send our cache has split and been relocated
             * to another region server *after* we sent it, but before we removed it. To accommodate this, we iterate
             * through the current metadata boundaries and remove the cache once for each server that we originally sent
             * to.
             */
            if (LOG.isDebugEnabled()) {
                LOG.debug(addCustomAnnotations("Removing Cache " + cacheId + " from servers.", connection));
            }
            for (HRegionLocation entry : locations) {
             // Call once per server
                if (remainingOnServers.contains(entry)) { 
                    try {
                        byte[] key = getKeyInRegion(entry.getRegion().getStartKey());
                        iterateOverTable.coprocessorService(ServerCachingService.class, key, key,
                                new Batch.Call<ServerCachingService, RemoveServerCacheResponse>() {
                                    @Override
                                    public RemoveServerCacheResponse call(ServerCachingService instance)
                                            throws IOException {
                                        ServerRpcController controller = new ServerRpcController();
                                        BlockingRpcCallback<RemoveServerCacheResponse> rpcCallback = new BlockingRpcCallback<RemoveServerCacheResponse>();
                                        RemoveServerCacheRequest.Builder builder = RemoveServerCacheRequest
                                                .newBuilder();
                                        final byte[] tenantIdBytes;
                                        if (cacheUsingTable.isMultiTenant()) {
                                            try {
                                                tenantIdBytes = connection.getTenantId() == null ? null
                                                        : ScanUtil.getTenantIdBytes(cacheUsingTable.getRowKeySchema(),
                                                                cacheUsingTable.getBucketNum() != null,
                                                                connection.getTenantId(),
                                                                cacheUsingTable.getViewIndexId() != null);
                                            } catch (SQLException e) {
                                                throw new IOException(e);
                                            }
                                        } else {
                                            tenantIdBytes = connection.getTenantId() == null ? null
                                                    : connection.getTenantId().getBytes();
                                        }
                                        if (tenantIdBytes != null) {
                                            builder.setTenantId(ByteStringer.wrap(tenantIdBytes));
                                        }
                                        builder.setCacheId(ByteStringer.wrap(cacheId));
                                        instance.removeServerCache(controller, builder.build(), rpcCallback);
                                        if (controller.getFailedOn() != null) { throw controller.getFailedOn(); }
                                        return rpcCallback.get();
                                    }
                                });
                        remainingOnServers.remove(entry);
                    } catch (Throwable t) {
                        lastThrowable = t;
                        LOG.error(addCustomAnnotations("Error trying to remove hash cache for " + entry, connection),
                                t);
                    }
                }
            }
            if (!remainingOnServers.isEmpty()) {
                LOG.warn(addCustomAnnotations("Unable to remove hash cache for " + remainingOnServers, connection),
                        lastThrowable);
            }
        } finally {
            cacheUsingTableMap.remove(cacheId);
            Closeables.closeQuietly(iterateOverTable);
        }
    }

    /**
     * Create an ID to keep the cached information across other operations independent.
     * Using simple long random number, since the length of time we need this to be unique
     * is very limited. 
     */
    public static byte[] generateId() {
        long rand = RANDOM.nextLong();
        return Bytes.toBytes(rand);
    }
    
    public static String idToString(byte[] uuid) {
        assert(uuid.length == Bytes.SIZEOF_LONG);
        return Long.toString(Bytes.toLong(uuid));
    }

    private static byte[] getKeyInRegion(byte[] regionStartKey) {
        assert (regionStartKey != null);
        if (Bytes.equals(regionStartKey, HConstants.EMPTY_START_ROW)) {
            return KEY_IN_FIRST_REGION;
        }
        return regionStartKey;
    }

    public boolean addServerCache(byte[] startkeyOfRegion, ServerCache cache, HashCacheFactory cacheFactory,
             byte[] txState, PTable pTable) throws Exception {
        Table table = null;
        boolean success = true;
        byte[] cacheId = cache.getId();
        try {
            ConnectionQueryServices services = connection.getQueryServices();
            
            byte[] tableName = pTable.getPhysicalName().getBytes();
            table = services.getTable(tableName);
            HRegionLocation tableRegionLocation = services.getTableRegionLocation(tableName, startkeyOfRegion);
            if(cache.isExpired(tableRegionLocation)) {
                return false;
            }
			if (cache.addServer(tableRegionLocation) || services.getProps().getBoolean(HASH_JOIN_SERVER_CACHE_RESEND_PER_SERVER,false)) {
				success = addServerCache(table, startkeyOfRegion, pTable, cacheId, cache.getCachePtr(), cacheFactory,
						txState, false);
			}
			return success;
        } finally {
            Closeables.closeQuietly(table);
        }
    }
    
    public boolean addServerCache(Table htable, byte[] key, final PTable cacheUsingTable, final byte[] cacheId,
            final ImmutableBytesWritable cachePtr, final ServerCacheFactory cacheFactory, final byte[] txState, final boolean usePersistentCache)
            throws Exception {
        byte[] keyInRegion = getKeyInRegion(key);
        final Map<byte[], AddServerCacheResponse> results;
        try {
            results = htable.coprocessorService(ServerCachingService.class, keyInRegion, keyInRegion,
                    new Batch.Call<ServerCachingService, AddServerCacheResponse>() {
                        @Override
                        public AddServerCacheResponse call(ServerCachingService instance) throws IOException {
                            ServerRpcController controller = new ServerRpcController();
                            BlockingRpcCallback<AddServerCacheResponse> rpcCallback = new BlockingRpcCallback<AddServerCacheResponse>();
                            AddServerCacheRequest.Builder builder = AddServerCacheRequest.newBuilder();
                            final byte[] tenantIdBytes;
                            if (cacheUsingTable.isMultiTenant()) {
                                try {
                                    tenantIdBytes = connection.getTenantId() == null ? null
                                            : ScanUtil.getTenantIdBytes(cacheUsingTable.getRowKeySchema(),
                                                    cacheUsingTable.getBucketNum() != null, connection.getTenantId(),
                                                    cacheUsingTable.getViewIndexId() != null);
                                } catch (SQLException e) {
                                    throw new IOException(e);
                                }
                            } else {
                                tenantIdBytes = connection.getTenantId() == null ? null
                                        : connection.getTenantId().getBytes();
                            }
                            if (tenantIdBytes != null) {
                                builder.setTenantId(ByteStringer.wrap(tenantIdBytes));
                            }
                            builder.setCacheId(ByteStringer.wrap(cacheId));
                            builder.setUsePersistentCache(usePersistentCache);
                            builder.setCachePtr(org.apache.phoenix.protobuf.ProtobufUtil.toProto(cachePtr));
                            builder.setHasProtoBufIndexMaintainer(true);
                            ServerCacheFactoryProtos.ServerCacheFactory.Builder svrCacheFactoryBuider = ServerCacheFactoryProtos.ServerCacheFactory
                                    .newBuilder();
                            svrCacheFactoryBuider.setClassName(cacheFactory.getClass().getName());
                            builder.setCacheFactory(svrCacheFactoryBuider.build());
                            builder.setTxState(ByteStringer.wrap(txState));
                            builder.setClientVersion(MetaDataProtocol.PHOENIX_VERSION);
                            instance.addServerCache(controller, builder.build(), rpcCallback);
                            if (controller.getFailedOn() != null) { throw controller.getFailedOn(); }
                            return rpcCallback.get();
                        }
                    });
        } catch (Throwable t) {
            throw new Exception(t);
        }
        if (results != null && results.size() == 1) { return results.values().iterator().next().getReturn(); }
        return false;
    }
    
}
