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
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.phoenix.coprocessor.generated.RegionServerEndpointProtos;
import org.apache.phoenix.jdbc.ClusterRoleRecord;
import org.apache.phoenix.jdbc.ClusterRoleRecord.ClusterRole;
import org.apache.phoenix.jdbc.HighAvailabilityGroup;
import org.apache.phoenix.jdbc.HighAvailabilityPolicy;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for getting Cluster Role Record for the client from RegionServer Endpoints.
 */
public class GetClusterRoleRecordUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(GetClusterRoleRecordUtil.class);

  /**
   * Scheduler to fetch ClusterRoleRecord until we get an Active ClusterRoleRecord
   */
  private static Map<String, ScheduledExecutorService> schedulerMap = new ConcurrentHashMap<>();

  private static final Object pollerLock = new Object();

  private static volatile ScheduledFuture<?> pollerFuture = null;

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
      // get all live region servers
      List<ServerName> regionServers = connection.getQueryServices().getLiveRegionServers();
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
   * @param url            URL of the RegionServer Endpoint Service
   * @param haGroupName    Name of the HA group
   * @param properties     Connection properties
   * @param pollerInterval Interval in seconds to poll for ClusterRoleRecord
   * @param haGroup        HighAvailabilityGroup object to refresh the ClusterRoleRecord when an
   *                       Active CRR is found
   * @throws SQLException if there is an error getting the ClusterRoleRecord
   */
  public static ClusterRoleRecord fetchClusterRoleRecord(String url, String haGroupName,
    HighAvailabilityGroup haGroup, long pollerInterval, Properties properties) throws SQLException {
    ClusterRoleRecord clusterRoleRecord = getClusterRoleRecord(url, haGroupName, true, properties);
    if (
      clusterRoleRecord.getPolicy() == HighAvailabilityPolicy.FAILOVER
        && !clusterRoleRecord.getRole1().isActive() && !clusterRoleRecord.getRole2().isActive()
    ) {
      LOGGER.info(
        "Non-active ClusterRoleRecord found for HA group {}. Scheduling poller to check every {} seconds,"
          + "until we find an ACTIVE CRR",
        haGroupName, pollerInterval);
      // Schedule a poller to fetch ClusterRoleRecord every 5 seconds (or configured value)
      // until we get an Active ClusterRoleRecord and return the Non-Active CRR
      schedulePoller(url, haGroupName, haGroup, pollerInterval, properties);
    }

    return clusterRoleRecord;
  }

  /**
   * Method to schedule a poller to fetch ClusterRoleRecord every pollerInterval seconds until we
   * get an Active ClusterRoleRecord, poller will only start if client will receive, a Non-Active
   * roleRecord (means either of the roles are not Active and client can't create a connection)
   * @param url            URL of the RegionServer Endpoint Service
   * @param haGroupName    Name of the HA group
   * @param haGroup        HighAvailabilityGroup object to refresh the ClusterRoleRecord when an
   *                       Active CRR is found
   * @param pollerInterval Interval in seconds to poll for ClusterRoleRecord
   * @param properties     Connection properties
   * @throws SQLException if there is an error getting or refreshing the ClusterRoleRecord when an
   *                      Active CRR is found
   */
  private static void schedulePoller(String url, String haGroupName, HighAvailabilityGroup haGroup,
    long pollerInterval, Properties properties) {

    synchronized (pollerLock) {
      if (schedulerMap.containsKey(haGroupName) && !schedulerMap.get(haGroupName).isShutdown()) {
        LOGGER.info("Poller already running for HA group {}.", haGroupName);
        return;
      }

      schedulerMap.put(haGroupName, Executors.newScheduledThreadPool(1));
      LOGGER.info("Starting poller for HA group {} to check every {} milliseconds.", haGroupName,
        pollerInterval);
      Runnable pollingTask = () -> {
        try {
          ClusterRoleRecord polledCrr = getClusterRoleRecord(url, haGroupName, true, properties);
          LOGGER.info("Polled CRR: {}", polledCrr);
          if (polledCrr.getRole1().isActive() || polledCrr.getRole2().isActive()) {

            LOGGER.info("Active ClusterRoleRecord found. Cancelling poller.");
            synchronized (pollerLock) {
              if (pollerFuture != null) {
                pollerFuture.cancel(false);
              }
              // Refresh ClusterRoleRecord for the HAGroup with appropriate transition
              haGroup.refreshClusterRoleRecord(true);
              schedulerMap.get(haGroupName).shutdown();
              schedulerMap.remove(haGroupName);
            }
          }
        } catch (SQLException e) {
          LOGGER.error("Exception found while polling for ClusterRoleRecord on {}: {}", url,
            e.getMessage());
        }
      };

      // Schedule the task with a fixed delay
      pollerFuture = schedulerMap.get(haGroupName).scheduleWithFixedDelay(pollingTask, 0,
        pollerInterval, TimeUnit.MILLISECONDS);
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
}
