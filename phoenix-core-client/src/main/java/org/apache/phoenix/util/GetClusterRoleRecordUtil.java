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
package org.apache.phoenix.util;

import static org.apache.phoenix.jdbc.HighAvailabilityGroup.PHOENIX_HA_GROUP_ATTR;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

import com.google.protobuf.ByteString;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.coprocessor.generated.RegionServerEndpointProtos;
import org.apache.phoenix.jdbc.ClusterRoleRecord;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.jdbc.HighAvailabilityGroup;
import org.apache.phoenix.jdbc.HighAvailabilityPolicy;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.monitoring.GlobalClientMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for getting Cluster Role Record for the client from RegionServer Endpoints.
 */
public class GetClusterRoleRecordUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(GetClusterRoleRecordUtil.class);

  /**
   * Per-HA-group scheduler executors, keyed on haGroupName so multiple HA groups can run pollers
   * independently without sharing or overwriting each other's lifecycle state.
   */
  private static final Map<String, ScheduledExecutorService> schedulerMap =
    new ConcurrentHashMap<>();

  /**
   * Per-HA-group poller futures, keyed on haGroupName. A previous implementation kept a single
   * static {@code pollerFuture} field that was overwritten by each new {@code schedulePoller} call
   * regardless of haGroupName, so cancelling the poller for one HA group would cancel the future
   * most recently scheduled (which may belong to a different HA group). Keying by haGroupName
   * ensures each group's future is cancelled independently.
   */
  private static final Map<String, ScheduledFuture<?>> futureMap = new ConcurrentHashMap<>();

  private static final Object pollerLock = new Object();

  private GetClusterRoleRecordUtil() {
  }

  /**
   * Method to get ClusterRoleRecord from RegionServer Endpoints. it picks a random region server
   * and gets the CRR from it.
   * @param url         URL to create Connection to be used to get RegionServer Endpoint Service
   * @param haGroupName Name of the HA group
   * @param doRetry     Whether to retry if the operation fails
   * @param properties  Connection properties
   * @return ClusterRoleRecord from the first available cluster
   * @throws SQLException if there is an error getting the ClusterRoleRecord
   */
  private static ClusterRoleRecord getClusterRoleRecord(String url, String haGroupName,
    boolean doRetry, Properties properties) throws SQLException {
    Connection conn = getConnection(url, properties);
    PhoenixConnection connection = conn.unwrap(PhoenixConnection.class);
    try (Admin admin = connection.getQueryServices().getAdmin()) {
      // Get all live region servers. The list is only auto-populated at CQSI init when
      // LAST_DDL_TIMESTAMP_VALIDATION_ENABLED is true (default false), so on the first call
      // for the default config path the list is null. Refresh inline rather than letting the
      // catch block recover via NPE: keeps the retry path reserved for genuine failures
      // (auth, transport, RPC) and avoids logging an ERROR per first invocation.
      List<ServerName> regionServers = connection.getQueryServices().getLiveRegionServers();
      if (regionServers == null || regionServers.isEmpty()) {
        connection.getQueryServices().refreshLiveRegionServers();
        regionServers = connection.getQueryServices().getLiveRegionServers();
        if (regionServers == null || regionServers.isEmpty()) {
          throw new SQLException("No live region servers available for HA group " + haGroupName);
        }
      }
      // pick one at random
      ServerName regionServer =
        regionServers.get(ThreadLocalRandom.current().nextInt(regionServers.size()));

      // RPC to regionServer to get the CRR
      RegionServerEndpointProtos.RegionServerEndpointService.BlockingInterface service =
        RegionServerEndpointProtos.RegionServerEndpointService
          .newBlockingStub(admin.coprocessorService(regionServer));
      RegionServerEndpointProtos.GetClusterRoleRecordRequest request =
        getClusterRoleRecordRequest(haGroupName);
      RegionServerEndpointProtos.GetClusterRoleRecordResponse response =
        service.getClusterRoleRecord(null, request);

      // Check if the ClusterRoleRecord is valid, if not, throw an exception
      if (
        response.getHaGroupName() == null || response.getPolicy() == null
          || response.getUrl1() == null || response.getRole1() == null || response.getUrl2() == null
          || response.getRole2() == null
      ) {
        throw new SQLException("Invalid ClusterRoleRecord response from RegionServer");
      }

      // Generate the ClusterRoleRecord from the response
      return new ClusterRoleRecord(response.getHaGroupName().toStringUtf8(),
        HighAvailabilityPolicy.valueOf(response.getPolicy().toStringUtf8()),
        response.getUrl1().toStringUtf8(), ClusterRole.valueOf(response.getRole1().toStringUtf8()),
        response.getUrl2().toStringUtf8(), ClusterRole.valueOf(response.getRole2().toStringUtf8()),
        response.getVersion());

    } catch (Exception e) {
      SQLException parsedException = ClientUtil.parseServerException(e);
      // retry once for any exceptions other than StaleMetadataCacheException
      LOGGER.error("Error in getting ClusterRoleRecord for {} from url {}", haGroupName,
        connection.getURL(), parsedException);
      if (doRetry) {
        // update the list of live region servers
        connection.getQueryServices().refreshLiveRegionServers();
        return getClusterRoleRecord(url, haGroupName, false, properties);
      }
      throw parsedException;
    } finally {
      conn.close();
    }

  }

  /**
   * Method to schedule a poller to fetch ClusterRoleRecord every 5 seconds (or configured value)
   * until we get an Active ClusterRoleRecord (one role should be Active) if we receive an Active
   * roleRecord then client this method will return the roleRecord to be consumed and used, if not
   * then it will start a poller and return non-active roleRecord.
   * <p>
   * The poller alternates between {@code url1} and {@code url2} on successive ticks so a transient
   * outage on one cluster does not stall progress; both URLs are passed in even though the initial
   * fetch only targets one of them (selected by the caller via the {@code primaryUrl} hint).
   * @param url1           URL of the RegionServer Endpoint Service for cluster 1
   * @param url2           URL of the RegionServer Endpoint Service for cluster 2
   * @param primaryUrl     URL to use for the initial (non-poller) fetch; must be either url1 or
   *                       url2
   * @param haGroupName    Name of the HA group
   * @param haGroup        HighAvailabilityGroup object to refresh the ClusterRoleRecord when an
   *                       Active CRR is found
   * @param pollerInterval Interval in milliseconds to poll for ClusterRoleRecord
   * @param properties     Connection properties
   * @throws SQLException if there is an error getting the ClusterRoleRecord
   */
  public static ClusterRoleRecord fetchClusterRoleRecord(String url1, String url2,
    String primaryUrl, String haGroupName, HighAvailabilityGroup haGroup, long pollerInterval,
    Properties properties) throws SQLException {
    ClusterRoleRecord clusterRoleRecord =
      getClusterRoleRecord(primaryUrl, haGroupName, true, properties);
    if (
      clusterRoleRecord.getPolicy() == HighAvailabilityPolicy.FAILOVER
        && !clusterRoleRecord.getRole1().isActive() && !clusterRoleRecord.getRole2().isActive()
    ) {
      LOGGER.info(
        "Non-active ClusterRoleRecord found for HA group {}. Scheduling poller to check every {} ms,"
          + " alternating between url1 and url2 until we find an ACTIVE CRR",
        haGroupName, pollerInterval);
      // Schedule a poller to fetch ClusterRoleRecord every pollerInterval milliseconds
      // until we get an Active ClusterRoleRecord and return the Non-Active CRR
      schedulePoller(url1, url2, haGroupName, haGroup, pollerInterval, properties);
    }

    return clusterRoleRecord;
  }

  /**
   * Method to schedule a poller to fetch ClusterRoleRecord every pollerInterval milliseconds until
   * we get an Active ClusterRoleRecord. Poller will only start if client received a Non-Active
   * roleRecord (means neither role is Active and client can't create a connection).
   * <p>
   * The poller alternates between {@code url1} and {@code url2} on successive ticks. Alternating
   * (rather than pinning to a single URL) avoids stalling if the chosen cluster's RegionServer
   * Endpoint is transiently unreachable while the peer cluster is healthy and may already hold the
   * Active role.
   * <p>
   * Each haGroupName gets its own entry in {@link #futureMap} and {@link #schedulerMap}. Multiple
   * HA groups can therefore run independent pollers; cancellation of one HA group's poller does not
   * interfere with another's lifecycle.
   * @param url1           URL of the RegionServer Endpoint Service for cluster 1
   * @param url2           URL of the RegionServer Endpoint Service for cluster 2
   * @param haGroupName    Name of the HA group
   * @param haGroup        HighAvailabilityGroup object to refresh the ClusterRoleRecord when an
   *                       Active CRR is found
   * @param pollerInterval Interval in milliseconds to poll for ClusterRoleRecord
   * @param properties     Connection properties
   */
  private static void schedulePoller(String url1, String url2, String haGroupName,
    HighAvailabilityGroup haGroup, long pollerInterval, Properties properties) {

    synchronized (pollerLock) {
      if (schedulerMap.containsKey(haGroupName) && !schedulerMap.get(haGroupName).isShutdown()) {
        LOGGER.info("Poller already running for HA group {}.", haGroupName);
        return;
      }

      schedulerMap.put(haGroupName, Executors.newScheduledThreadPool(1));
      LOGGER.info(
        "Starting poller for HA group {} to check every {} milliseconds, alternating between {}"
          + " and {}.",
        haGroupName, pollerInterval, url1, url2);
      AtomicLong tickCount = new AtomicLong(0);
      Runnable pollingTask = () -> {
        // Increment unconditionally so a failed tick still alternates next iteration.
        long tick = tickCount.getAndIncrement();
        GlobalClientMetrics.GLOBAL_HA_POLLER_TICK_COUNT.increment();
        // Sample current CRR cache age into the gauge each tick. Without this, the
        // HA_CRR_CACHE_AGE_MS counter-backed gauge is only updated on connect() and would
        // not advance during idle periods between connects, making it look fresher than it
        // actually is. Sampling here puts the gauge on a steady wall-clock cadence matching
        // the poller's polling interval.
        try {
          GlobalClientMetrics.GLOBAL_HA_CRR_CACHE_AGE_MS.getMetric().set(haGroup.getCacheAgeMs());
        } catch (Throwable t) {
          // Metric sampling is best-effort; never let a metric write break the poller tick.
          LOGGER.warn(
            "Failed to sample HA_CRR_CACHE_AGE_MS on poller tick for HA group {}; " + "continuing",
            haGroupName, t);
        }
        String tickUrl = selectUrlForTick(url1, url2, tick);
        try {
          ClusterRoleRecord polledCrr =
            getClusterRoleRecord(tickUrl, haGroupName, true, properties);
          LOGGER.info("Polled CRR for HA group {} via {}: {}", haGroupName, tickUrl, polledCrr);
          if (polledCrr.getRole1().isActive() || polledCrr.getRole2().isActive()) {

            LOGGER.info("Active ClusterRoleRecord found for HA group {}. Cancelling poller.",
              haGroupName);
            synchronized (pollerLock) {
              ScheduledFuture<?> future = futureMap.remove(haGroupName);
              if (future != null) {
                future.cancel(false);
              }
              try {
                // Refresh ClusterRoleRecord for the HAGroup with appropriate transition
                haGroup.refreshClusterRoleRecord(true);
              } finally {
                ScheduledExecutorService scheduler = schedulerMap.remove(haGroupName);
                if (scheduler != null) {
                  scheduler.shutdown();
                }
              }
            }
          }
        } catch (SQLException e) {
          GlobalClientMetrics.GLOBAL_HA_POLLER_TICK_FAILURES.increment();
          LOGGER.error(
            "Exception found while polling for ClusterRoleRecord on {} for HA group" + " {}: {}",
            tickUrl, haGroupName, e.getMessage());
        }
      };

      // Schedule the task with a fixed delay; keyed by haGroupName so each HA group's future is
      // independently cancellable.
      ScheduledFuture<?> future = schedulerMap.get(haGroupName).scheduleWithFixedDelay(pollingTask,
        0, pollerInterval, TimeUnit.MILLISECONDS);
      futureMap.put(haGroupName, future);
    }
  }

  /**
   * Method to build the ClusterRoleRecordRequest for the given HA group name
   * @param haGroupName Name of the HA group
   * @return ClusterRoleRecordRequest for the given HA group name
   */
  private static RegionServerEndpointProtos.GetClusterRoleRecordRequest
    getClusterRoleRecordRequest(String haGroupName) {
    RegionServerEndpointProtos.GetClusterRoleRecordRequest.Builder requestBuilder =
      RegionServerEndpointProtos.GetClusterRoleRecordRequest.newBuilder();
    requestBuilder.setHaGroupName(ByteString.copyFromUtf8(haGroupName));

    return requestBuilder.build();
  }

  /**
   * Method to get a connection to the given URL
   * @param url        URL of the RegionServer Endpoint Service
   * @param properties Connection properties
   * @return Connection to the given URL
   * @throws SQLException if there is an error getting the connection
   */
  private static Connection getConnection(String url, Properties properties) throws SQLException {
    if (!url.startsWith(PhoenixRuntime.JDBC_PROTOCOL)) {
      url = PhoenixRuntime.JDBC_PROTOCOL_RPC + JDBC_PROTOCOL_SEPARATOR + url;
    }
    Properties propsCopy = PropertiesUtil.deepCopy(properties);
    propsCopy.remove(PHOENIX_HA_GROUP_ATTR);
    return DriverManager.getConnection(url, propsCopy);
  }

  /**
   * Pick which URL the poller should target on tick {@code tick}. Even ticks (0, 2, 4, ...) select
   * {@code url1}; odd ticks select {@code url2}. Package-private for unit-test access.
   */
  static String selectUrlForTick(String url1, String url2, long tick) {
    return (tick % 2 == 0) ? url1 : url2;
  }

  /**
   * Test-only accessor for the per-HA-group future map. Package-private so unit tests can verify
   * that distinct HA groups produce distinct entries (and that cancellation removes only the
   * corresponding entry).
   */
  static Map<String, ScheduledFuture<?>> getFutureMapForTesting() {
    return futureMap;
  }

  /**
   * Test-only accessor for the per-HA-group scheduler map. Package-private for unit-test use.
   */
  static Map<String, ScheduledExecutorService> getSchedulerMapForTesting() {
    return schedulerMap;
  }
}
