/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.cache.ServerMetadataCache;
import org.apache.phoenix.cache.ServerMetadataCacheImpl;
import org.apache.phoenix.coprocessor.generated.RegionServerEndpointProtos;
import org.apache.phoenix.coprocessorclient.metrics.MetricsMetadataCachingSource;
import org.apache.phoenix.coprocessorclient.metrics.MetricsPhoenixCoprocessorSourceFactory;
import org.apache.phoenix.hbase.index.parallel.TaskRunner;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolBuilder;
import org.apache.phoenix.hbase.index.parallel.ThreadPoolManager;
import org.apache.phoenix.hbase.index.parallel.WaitForCompletionTaskRunner;
import org.apache.phoenix.jdbc.ClusterRoleRecord;
import org.apache.phoenix.jdbc.HAGroupStoreManager;
import org.apache.phoenix.protobuf.ProtobufUtil;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.replication.reader.ReplicationLogReplayService;
import org.apache.phoenix.util.ClientUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is first implementation of RegionServer coprocessor introduced by Phoenix.
 */
public class PhoenixRegionServerEndpoint extends
  RegionServerEndpointProtos.RegionServerEndpointService implements RegionServerCoprocessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixRegionServerEndpoint.class);
  private MetricsMetadataCachingSource metricsSource;
  protected Configuration conf;
  private ExecutorService prewarmExecutor;

  // regionserver level thread pool used by Uncovered Indexes to scan data table rows
  private static TaskRunner uncoveredIndexThreadPool;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    this.conf = env.getConfiguration();
    this.metricsSource =
      MetricsPhoenixCoprocessorSourceFactory.getInstance().getMetadataCachingSource();
    initUncoveredIndexThreadPool(this.conf);
    // Start async prewarming of HAGroupStoreClients if enabled
    if (
      conf.getBoolean(QueryServices.HA_GROUP_STORE_CLIENT_PREWARM_ENABLED,
        QueryServicesOptions.DEFAULT_HA_GROUP_STORE_CLIENT_PREWARM_ENABLED)
    ) {
      startHAGroupStoreClientPrewarming();
    } else {
      LOGGER.info("HAGroupStoreClient prewarming is disabled");
    }
    // Start replication log replay
    ReplicationLogReplayService.getInstance(conf).start();
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    // Stop replication log replay
    ReplicationLogReplayService.getInstance(conf).stop();
    RegionServerCoprocessor.super.stop(env);
    if (uncoveredIndexThreadPool != null) {
      uncoveredIndexThreadPool
        .stop("PhoenixRegionServerEndpoint is stopping. Shutting down uncovered index threadpool.");
    }
    ServerUtil.ConnectionFactory.shutdown();
    // Stop prewarming executor
    if (prewarmExecutor != null) {
      prewarmExecutor.shutdownNow();
    }
  }

  @Override
  public void validateLastDDLTimestamp(RpcController controller,
    RegionServerEndpointProtos.ValidateLastDDLTimestampRequest request,
    RpcCallback<RegionServerEndpointProtos.ValidateLastDDLTimestampResponse> done) {
    metricsSource.incrementValidateTimestampRequestCount();
    ServerMetadataCache cache = getServerMetadataCache();
    for (RegionServerEndpointProtos.LastDDLTimestampRequest lastDDLTimestampRequest : request
      .getLastDDLTimestampRequestsList()) {
      byte[] tenantID = lastDDLTimestampRequest.getTenantId().toByteArray();
      byte[] schemaName = lastDDLTimestampRequest.getSchemaName().toByteArray();
      byte[] tableName = lastDDLTimestampRequest.getTableName().toByteArray();
      long clientLastDDLTimestamp = lastDDLTimestampRequest.getLastDDLTimestamp();
      String tenantIDStr = Bytes.toString(tenantID);
      String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
      try {
        VerifyLastDDLTimestamp.verifyLastDDLTimestamp(cache, tenantID, schemaName, tableName,
          clientLastDDLTimestamp);
      } catch (Throwable t) {
        String errorMsg = String.format(
          "Verifying last ddl timestamp FAILED for " + "tenantID: %s,  fullTableName: %s",
          tenantIDStr, fullTableName);
        LOGGER.error(errorMsg, t);
        IOException ioe = ClientUtil.createIOException(errorMsg, t);
        ProtobufUtil.setControllerException(controller, ioe);
        // If an index was dropped and a client tries to query it, we will validate table
        // first and encounter stale metadata, if we don't break the coproc will run into
        // table not found error since it will not be able to validate the dropped index.
        // this should be fine for views too since we will update the entire hierarchy.
        break;
      }
    }
  }

  @Override
  public void invalidateServerMetadataCache(RpcController controller,
    RegionServerEndpointProtos.InvalidateServerMetadataCacheRequest request,
    RpcCallback<RegionServerEndpointProtos.InvalidateServerMetadataCacheResponse> done) {
    for (RegionServerEndpointProtos.InvalidateServerMetadataCache invalidateCacheRequest : request
      .getInvalidateServerMetadataCacheRequestsList()) {
      byte[] tenantID = invalidateCacheRequest.getTenantId().toByteArray();
      byte[] schemaName = invalidateCacheRequest.getSchemaName().toByteArray();
      byte[] tableName = invalidateCacheRequest.getTableName().toByteArray();
      String fullTableName = SchemaUtil.getTableName(schemaName, tableName);
      String tenantIDStr = Bytes.toString(tenantID);
      LOGGER.info(
        "PhoenixRegionServerEndpoint invalidating the cache for tenantID: {}," + " tableName: {}",
        tenantIDStr, fullTableName);
      ServerMetadataCache cache = getServerMetadataCache();
      cache.invalidate(tenantID, schemaName, tableName);
    }
  }

  @Override
  public void invalidateHAGroupStoreClient(RpcController controller,
    RegionServerEndpointProtos.InvalidateHAGroupStoreClientRequest request,
    RpcCallback<RegionServerEndpointProtos.InvalidateHAGroupStoreClientResponse> done) {
    LOGGER.info("PhoenixRegionServerEndpoint invalidating HAGroupStoreClient");
    try {
      HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(conf);
      if (haGroupStoreManager != null) {
        haGroupStoreManager.invalidateHAGroupStoreClient(request.getHaGroupName().toStringUtf8());
      } else {
        throw new IOException(
          "HAGroupStoreManager is null for " + "current cluster, check configuration");
      }
    } catch (Throwable t) {
      String errorMsg =
        "Invalidating HAGroupStoreClient FAILED, check exception for " + "specific details";
      LOGGER.error(errorMsg, t);
      IOException ioe = ClientUtil.createIOException(errorMsg, t);
      ProtobufUtil.setControllerException(controller, ioe);
    }
  }

  @Override
  public void getClusterRoleRecord(RpcController controller,
    RegionServerEndpointProtos.GetClusterRoleRecordRequest request,
    RpcCallback<RegionServerEndpointProtos.GetClusterRoleRecordResponse> done) {
    try {
      HAGroupStoreManager haGroupStoreManager = HAGroupStoreManager.getInstance(conf);
      if (haGroupStoreManager != null) {
        ClusterRoleRecord clusterRoleRecord =
          haGroupStoreManager.getClusterRoleRecord(request.getHaGroupName().toStringUtf8());
        RegionServerEndpointProtos.GetClusterRoleRecordResponse.Builder responseBuilder =
          RegionServerEndpointProtos.GetClusterRoleRecordResponse.newBuilder();
        responseBuilder.setHaGroupName(request.getHaGroupName());
        responseBuilder.setPolicy(ByteString.copyFromUtf8(clusterRoleRecord.getPolicy().name()));
        responseBuilder.setUrl1(ByteString.copyFromUtf8(clusterRoleRecord.getUrl1()));
        responseBuilder.setRole1(ByteString.copyFromUtf8(clusterRoleRecord.getRole1().name()));
        responseBuilder.setUrl2(ByteString.copyFromUtf8(clusterRoleRecord.getUrl2()));
        responseBuilder.setRole2(ByteString.copyFromUtf8(clusterRoleRecord.getRole2().name()));
        responseBuilder.setVersion(clusterRoleRecord.getVersion());
        done.run(responseBuilder.build());
      } else {
        throw new IOException(
          "HAGroupStoreManager is null for " + "current cluster, check configuration");
      }
    } catch (Throwable t) {
      String errorMsg =
        "Getting ClusterRoleRecord FAILED, check exception for " + "specific details";
      LOGGER.error(errorMsg, t);
      IOException ioe = ClientUtil.createIOException(errorMsg, t);
      ProtobufUtil.setControllerException(controller, ioe);
    }
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singletonList(this);
  }

  public ServerMetadataCache getServerMetadataCache() {
    return ServerMetadataCacheImpl.getInstance(conf);
  }

  public static TaskRunner getUncoveredIndexThreadPool() {
    return uncoveredIndexThreadPool;
  }

  private static void initUncoveredIndexThreadPool(Configuration conf) {
    uncoveredIndexThreadPool = new WaitForCompletionTaskRunner(
      ThreadPoolManager.getExecutor(new ThreadPoolBuilder("Uncovered Global Index", conf)
        .setMaxThread(QueryServices.PHOENIX_UNCOVERED_INDEX_MAX_POOL_SIZE,
          QueryServicesOptions.DEFAULT_PHOENIX_UNCOVERED_INDEX_MAX_POOL_SIZE)
        .setCoreTimeout(QueryServices.PHOENIX_UNCOVERED_INDEX_KEEP_ALIVE_TIME_SEC,
          QueryServicesOptions.DEFAULT_PHOENIX_UNCOVERED_INDEX_KEEP_ALIVE_TIME_SEC)));
    LOGGER.info("Initialized region level thread pool for Uncovered Global Indexes.");
  }

  /**
   * Prewarms HAGroupStoreClients in background thread with retry. Initializes all HA group clients
   * asynchronously at startup.
   * <p>
   * Phase 1 : Retry indefinitely until HAGroupStoreManager is initialized and HAGroupNames are
   * retrieved. If the SYSTEM.HA_GROUP table region is not ready, manager.getHAGroupNames() would
   * return an exception. So we need to retry until the SYSTEM.HA_GROUP table region is ready and
   * then retrieve the HAGroupNames for prewarming.
   * <p>
   * Phase 2 : Prewarm individual HAGroupStoreClients with retry. If the HAGroupStoreClient is not
   * ready/initialized, manager.getClusterRoleRecord(haGroup) would throw an exception. So we need
   * to retry until the HAGroupStoreClient is ready/initialized.
   */
  private void startHAGroupStoreClientPrewarming() {
    prewarmExecutor = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "HAGroupStoreClient-Prewarm");
      t.setDaemon(true);
      return t;
    });

    prewarmExecutor.submit(() -> {
      HAGroupStoreManager manager = null;
      List<String> pending = null;
      // Phase 1: Retry indefinitely until HAGroupStoreManager is initialized
      // and HAGroupNames are retrieved.
      while (pending == null) {
        try {
          manager = HAGroupStoreManager.getInstance(conf);
          if (manager != null) {
            pending = new ArrayList<>(manager.getHAGroupNames());
            LOGGER.info("Starting prewarming for {} HAGroupStoreClients", pending.size());
          } else {
            LOGGER.debug("HAGroupStoreManager is null, retrying in 2s...");
            Thread.sleep(2000);
          }
        } catch (InterruptedException e) {
          LOGGER.info("HAGroupStoreClient prewarming interrupted during " + "initialization");
          Thread.currentThread().interrupt();
          return;
        } catch (Exception e) {
          LOGGER.debug("Failed to initialize HAGroupStoreManager, retrying in " + "2s...", e);
          try {
            Thread.sleep(2000);
          } catch (InterruptedException ie) {
            LOGGER.info("HAGroupStoreClient prewarming interrupted");
            Thread.currentThread().interrupt();
            return;
          }
        }
      }

      // Phase 2: Prewarm individual HAGroupStoreClients with retry
      try {
        while (!pending.isEmpty()) {
          Iterator<String> iterator = pending.iterator();
          while (iterator.hasNext()) {
            String haGroup = iterator.next();
            try {
              manager.getClusterRoleRecord(haGroup);
              iterator.remove();
              LOGGER.info("Prewarmed HAGroupStoreClient: {} ({} remaining)", haGroup,
                pending.size());
            } catch (Exception e) {
              LOGGER.debug("Failed to prewarm {}, will retry", haGroup, e);
            }
          }

          if (!pending.isEmpty()) {
            Thread.sleep(2000);
          }
        }

        LOGGER.info("Completed prewarming all HAGroupStoreClients");
      } catch (InterruptedException e) {
        LOGGER.info("HAGroupStoreClient prewarming interrupted during warmup");
        Thread.currentThread().interrupt();
      }
    });
  }

}
