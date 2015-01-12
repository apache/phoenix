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

import static org.apache.phoenix.util.LogUtil.addCustomAnnotations;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ScanRanges;
import org.apache.phoenix.coprocessor.ServerCachingProtocol.ServerCacheFactory;
import org.apache.phoenix.coprocessor.generated.ServerCacheFactoryProtos;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.AddServerCacheRequest;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.AddServerCacheResponse;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.RemoveServerCacheRequest;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.RemoveServerCacheResponse;
import org.apache.phoenix.coprocessor.generated.ServerCachingProtos.ServerCachingService;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.job.JobManager.JobCallable;
import org.apache.phoenix.memory.MemoryManager.MemoryChunk;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTable.IndexType;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.Closeables;
import org.apache.phoenix.util.SQLCloseable;
import org.apache.phoenix.util.SQLCloseables;
import org.apache.phoenix.util.ScanUtil;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.HBaseZeroCopyByteString;

/**
 * 
 * Client for sending cache to each region server
 * 
 * 
 * @since 0.1
 */
public class ServerCacheClient {
    public static final int UUID_LENGTH = Bytes.SIZEOF_LONG;
    private static final Log LOG = LogFactory.getLog(ServerCacheClient.class);
    private static final Random RANDOM = new Random();
    private final PhoenixConnection connection;
    private final Map<Integer, TableRef> cacheUsingTableRefMap = new ConcurrentHashMap<Integer, TableRef>();

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
        private final ImmutableSet<HRegionLocation> servers;
        
        public ServerCache(byte[] id, Set<HRegionLocation> servers, int size) {
            this.id = id;
            this.servers = ImmutableSet.copyOf(servers);
            this.size = size;
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

        /**
         * Call to free up cache on region servers when no longer needed
         */
        @Override
        public void close() throws SQLException {
            removeServerCache(id, servers);
        }

    }
    
    public ServerCache addServerCache(ScanRanges keyRanges, final ImmutableBytesWritable cachePtr, final ServerCacheFactory cacheFactory, final TableRef cacheUsingTableRef) throws SQLException {
        ConnectionQueryServices services = connection.getQueryServices();
        MemoryChunk chunk = services.getMemoryManager().allocate(cachePtr.getLength());
        List<Closeable> closeables = new ArrayList<Closeable>();
        closeables.add(chunk);
        ServerCache hashCacheSpec = null;
        SQLException firstException = null;
        final byte[] cacheId = generateId();
        /**
         * Execute EndPoint in parallel on each server to send compressed hash cache 
         */
        // TODO: generalize and package as a per region server EndPoint caller
        // (ideally this would be functionality provided by the coprocessor framework)
        boolean success = false;
        ExecutorService executor = services.getExecutor();
        List<Future<Boolean>> futures = Collections.emptyList();
        try {
            PTable cacheUsingTable = cacheUsingTableRef.getTable();
            List<HRegionLocation> locations = services.getAllTableRegions(cacheUsingTable.getPhysicalName().getBytes());
            int nRegions = locations.size();
            // Size these based on worst case
            futures = new ArrayList<Future<Boolean>>(nRegions);
            Set<HRegionLocation> servers = new HashSet<HRegionLocation>(nRegions);
            for (HRegionLocation entry : locations) {
                // Keep track of servers we've sent to and only send once
                byte[] regionStartKey = entry.getRegionInfo().getStartKey();
                byte[] regionEndKey = entry.getRegionInfo().getEndKey();
                if ( ! servers.contains(entry) && 
                        keyRanges.intersects(regionStartKey, regionEndKey,
                                cacheUsingTable.getIndexType() == IndexType.LOCAL ? 
                                    ScanUtil.getRowKeyOffset(regionStartKey, regionEndKey) : 0, true)) {  
                    // Call RPC once per server
                    servers.add(entry);
                    if (LOG.isDebugEnabled()) {LOG.debug(addCustomAnnotations("Adding cache entry to be sent for " + entry, connection));}
                    final byte[] key = entry.getRegionInfo().getStartKey();
                    final HTableInterface htable = services.getTable(cacheUsingTableRef.getTable().getPhysicalName().getBytes());
                    closeables.add(htable);
                    futures.add(executor.submit(new JobCallable<Boolean>() {
                        
                        @Override
                        public Boolean call() throws Exception {
                            final Map<byte[], AddServerCacheResponse> results;
                            try {
                                results = htable.coprocessorService(ServerCachingService.class, key, key, 
                                            new Batch.Call<ServerCachingService, AddServerCacheResponse>() {
                                                @Override
                                                public AddServerCacheResponse call(ServerCachingService instance) throws IOException {
                                                    ServerRpcController controller = new ServerRpcController();
                                                    BlockingRpcCallback<AddServerCacheResponse> rpcCallback =
                                                            new BlockingRpcCallback<AddServerCacheResponse>();
                                                    AddServerCacheRequest.Builder builder = AddServerCacheRequest.newBuilder();
                                                    if(connection.getTenantId() != null){
                                                        builder.setTenantId(HBaseZeroCopyByteString.wrap(connection.getTenantId().getBytes()));
                                                    }
                                                    builder.setCacheId(HBaseZeroCopyByteString.wrap(cacheId));
                                                    builder.setCachePtr(org.apache.phoenix.protobuf.ProtobufUtil.toProto(cachePtr));
                                                    ServerCacheFactoryProtos.ServerCacheFactory.Builder svrCacheFactoryBuider = ServerCacheFactoryProtos.ServerCacheFactory.newBuilder();
                                                    svrCacheFactoryBuider.setClassName(cacheFactory.getClass().getName());
                                                    builder.setCacheFactory(svrCacheFactoryBuider.build());
                                                    instance.addServerCache(controller, builder.build(), rpcCallback);
                                                    if(controller.getFailedOn() != null) {
                                                        throw controller.getFailedOn();
                                                    }
                                                    return rpcCallback.get(); 
                                                }
                                              });
                            } catch (Throwable t) {
                                throw new Exception(t);
                            }
                            if(results != null && results.size() == 1){
                                return results.values().iterator().next().getReturn();
                            }
                            return false;
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
                    }));
                } else {
                    if (LOG.isDebugEnabled()) {LOG.debug(addCustomAnnotations("NOT adding cache entry to be sent for " + entry + " since one already exists for that entry", connection));}
                }
            }
            
            hashCacheSpec = new ServerCache(cacheId,servers,cachePtr.getLength());
            // Execute in parallel
            int timeoutMs = services.getProps().getInt(QueryServices.THREAD_TIMEOUT_MS_ATTRIB, QueryServicesOptions.DEFAULT_THREAD_TIMEOUT_MS);
            for (Future<Boolean> future : futures) {
                future.get(timeoutMs, TimeUnit.MILLISECONDS);
            }
            
            cacheUsingTableRefMap.put(Bytes.mapKey(cacheId), cacheUsingTableRef);
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
     * @param cacheId unique identifier for the hash join (returned from {@link #addHashCache(HTable, Scan, Set)})
     * @param servers list of servers upon which table was cached (filled in by {@link #addHashCache(HTable, Scan, Set)})
     * @throws SQLException
     * @throws IllegalStateException if hashed table cannot be removed on any region server on which it was added
     */
    private void removeServerCache(final byte[] cacheId, Set<HRegionLocation> servers) throws SQLException {
    	ConnectionQueryServices services = connection.getQueryServices();
    	Throwable lastThrowable = null;
    	TableRef cacheUsingTableRef = cacheUsingTableRefMap.get(Bytes.mapKey(cacheId));
    	byte[] tableName = cacheUsingTableRef.getTable().getPhysicalName().getBytes();
    	HTableInterface iterateOverTable = services.getTable(tableName);
    	try {
    		List<HRegionLocation> locations = services.getAllTableRegions(tableName);
    		Set<HRegionLocation> remainingOnServers = new HashSet<HRegionLocation>(servers);
    		/**
    		 * Allow for the possibility that the region we based where to send our cache has split and been
    		 * relocated to another region server *after* we sent it, but before we removed it. To accommodate
    		 * this, we iterate through the current metadata boundaries and remove the cache once for each
    		 * server that we originally sent to.
    		 */
    		if (LOG.isDebugEnabled()) {LOG.debug(addCustomAnnotations("Removing Cache " + cacheId + " from servers.", connection));}
    		for (HRegionLocation entry : locations) {
    			if (remainingOnServers.contains(entry)) {  // Call once per server
    				try {
    					byte[] key = entry.getRegionInfo().getStartKey();
    					iterateOverTable.coprocessorService(ServerCachingService.class, key, key, 
    							new Batch.Call<ServerCachingService, RemoveServerCacheResponse>() {
    						@Override
    						public RemoveServerCacheResponse call(ServerCachingService instance) throws IOException {
    							ServerRpcController controller = new ServerRpcController();
    							BlockingRpcCallback<RemoveServerCacheResponse> rpcCallback =
    									new BlockingRpcCallback<RemoveServerCacheResponse>();
    							RemoveServerCacheRequest.Builder builder = RemoveServerCacheRequest.newBuilder();
    							if(connection.getTenantId() != null){
    								builder.setTenantId(HBaseZeroCopyByteString.wrap(connection.getTenantId().getBytes()));
    							}
    							builder.setCacheId(HBaseZeroCopyByteString.wrap(cacheId));
    							instance.removeServerCache(controller, builder.build(), rpcCallback);
    							if(controller.getFailedOn() != null) {
    								throw controller.getFailedOn();
    							}
    							return rpcCallback.get(); 
    						}
    					});
    					remainingOnServers.remove(entry);
    				} catch (Throwable t) {
    					lastThrowable = t;
    					LOG.error(addCustomAnnotations("Error trying to remove hash cache for " + entry, connection), t);
    				}
    			}
    		}
    		if (!remainingOnServers.isEmpty()) {
    			LOG.warn(addCustomAnnotations("Unable to remove hash cache for " + remainingOnServers, connection), lastThrowable);
    		}
    	} finally {
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
}
